package conformance

// Task-size probe. Real Cloud Tasks caps task size
// (documented as 100KB for App Engine targets, 1MB for HTTP targets) but the
// docs don't say which fields count toward the size or how it is measured.
// This probe discovers that empirically, adaptively - unlike the golden
// batteries it cannot be a fixed case list, because the interesting inputs
// ("exactly at the limit", "one byte over") depend on the answer.
//
// Method, per target type:
//
//  1. Binary-search the largest accepted body on a minimal baseline task.
//     That yields the boundary in body bytes and, at that boundary, the
//     serialized proto sizes of the task, the CreateTask request and the
//     HttpRequest/AppEngineHttpRequest submessage - the candidate quantities
//     the limit could be defined over.
//  2. For each perturbation (add a header, lengthen the URL, set an explicit
//     method, ...), compute how much it grows the serialized Task proto and
//     test the proto-size hypothesis directly: a task with the perturbation
//     and a body shrunk by exactly that delta should sit at the boundary
//     again (accepted at predicted max, rejected one byte over). Two calls
//     per perturbation when the hypothesis holds; a full binary search as
//     fallback when it doesn't, to measure the perturbation's actual weight.
//
// The consistency checks are offset-independent: even if the absolute limit
// is not literally proto.Size(task), uniform per-field consistency shows the
// measured quantity tracks the proto encoding, and the baseline numbers give
// the offset.
//
// Everything is control-plane only: the queue is paused and every task
// carries a far-future schedule time, so nothing ever dispatches.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// maxProbeBody bounds the search. It must stay above any plausible limit
// (documented max is 1MB) but below the 4MiB gRPC receive cap so a missing
// server-side limit (e.g. probing the emulator before enforcement lands)
// fails with a clear "no rejection" error instead of measuring the transport.
const maxProbeBody = 3 << 20

// SizeProbeOptions configure a size-probe run.
type SizeProbeOptions struct {
	Project  string
	Location string
	Prefix   string    // run-scoped resource-name prefix, keeps re-runs from colliding
	Targets  []string  // subset of {"http", "appengine"}; empty probes both
	Log      io.Writer // per-attempt progress log; nil discards
}

// TargetSizeReport is what the probe learned about one target type.
type TargetSizeReport struct {
	Target string `json:"target"`
	Err    string `json:"err,omitempty"` // set when the probe aborted for this target

	// MaxBody is the largest accepted body length on the baseline task.
	MaxBody int `json:"maxBody"`

	// Serialized proto sizes of the baseline task at the MaxBody boundary,
	// for matching the measured limit against candidate formulas.
	TaskProtoSizeAtMax    int `json:"taskProtoSizeAtMax"`
	RequestProtoSizeAtMax int `json:"requestProtoSizeAtMax"`
	MessageProtoSizeAtMax int `json:"messageProtoSizeAtMax"` // the HttpRequest / AppEngineHttpRequest submessage

	// The first over-limit rejection observed, verbatim - this is what the
	// emulator's error mapping should reproduce.
	RejectCode    string         `json:"rejectCode"`
	RejectMessage string         `json:"rejectMessage"`
	RejectDetails []DetailRecord `json:"rejectDetails,omitempty"`
	// OtherRejections lists any rejection whose digit-stripped template
	// differs from the first - non-empty means not every rejection during the
	// search was the same (size) error, so inspect before trusting MaxBody.
	OtherRejections []string `json:"otherRejections,omitempty"`

	Perturbations []PerturbationReport `json:"perturbations,omitempty"`

	Calls int `json:"calls"` // CreateTask calls spent on this target
}

// PerturbationReport records one field's measured contribution to task size.
type PerturbationReport struct {
	Name string `json:"name"`
	// ProtoDelta is how much the perturbation grows the serialized Task proto.
	ProtoDelta int `json:"protoDelta"`
	// PredictedMaxBody is baseline MaxBody - ProtoDelta: where the boundary
	// lands if the perturbed field counts exactly its proto encoding.
	PredictedMaxBody int    `json:"predictedMaxBody"`
	AtPredicted      string `json:"atPredicted"`   // outcome at PredictedMaxBody
	OverPredicted    string `json:"overPredicted"` // outcome at PredictedMaxBody+1
	// Consistent means accepted at the predicted max and size-rejected one
	// over: the field's weight matches its proto encoding exactly.
	Consistent bool `json:"consistentWithProtoSize"`
	// ActualMaxBody is measured by a full binary search when the prediction
	// failed; -1 when the prediction held (or the search was inconclusive).
	ActualMaxBody int    `json:"actualMaxBody"`
	Note          string `json:"note,omitempty"`
}

// RunSizeProbe probes each requested target type. Per-target failures land in
// the report's Err field rather than aborting the run, so one target's
// environment problem (e.g. no App Engine app in the project) doesn't cost the
// other's results; only context cancellation stops the whole run.
func RunSizeProbe(ctx context.Context, c *Client, opts SizeProbeOptions) []TargetSizeReport {
	targets := opts.Targets
	if len(targets) == 0 {
		targets = []string{"http", "appengine"}
	}
	var reports []TargetSizeReport
	for _, target := range targets {
		r := probeTarget(ctx, c, opts, target)
		reports = append(reports, r)
		if ctx.Err() != nil {
			break
		}
	}
	return reports
}

// perturbation is one field change whose size contribution the probe measures.
type perturbation struct {
	name     string
	idSuffix string // appended to the task ID (grows the name field)
	mod      func(*taskspb.Task)
}

// sizePerturbations returns the fields to weigh for a target type. The
// offending values are constants so proto deltas are exact; the oidc-token
// and routing-service entries can be rejected for non-size reasons (IAM,
// routing validation) - those come back as inconclusive, not as failures.
func sizePerturbations(target, project string) []perturbation {
	setHeader := func(k, v string) func(*taskspb.Task) {
		return func(t *taskspb.Task) {
			if hr := t.GetHttpRequest(); hr != nil {
				if hr.Headers == nil {
					hr.Headers = map[string]string{}
				}
				hr.Headers[k] = v
			}
			if ae := t.GetAppEngineHttpRequest(); ae != nil {
				if ae.Headers == nil {
					ae.Headers = map[string]string{}
				}
				ae.Headers[k] = v
			}
		}
	}
	ps := []perturbation{
		{name: "explicit-method", mod: func(t *taskspb.Task) {
			if hr := t.GetHttpRequest(); hr != nil {
				hr.HttpMethod = taskspb.HttpMethod_POST
			}
			if ae := t.GetAppEngineHttpRequest(); ae != nil {
				ae.HttpMethod = taskspb.HttpMethod_POST
			}
		}},
		{name: "header-value-100", mod: setHeader("X-Probe", strings.Repeat("v", 100))},
		{name: "header-name-100", mod: setHeader("X-Probe-"+strings.Repeat("n", 92), "v")},
		{name: "task-id-80-longer", idSuffix: strings.Repeat("x", 80)},
		{name: "dispatch-deadline-900s", mod: func(t *taskspb.Task) {
			t.DispatchDeadline = durationpb.New(900 * time.Second)
		}},
	}
	switch target {
	case "http":
		ps = append(ps,
			perturbation{name: "url-100-longer", mod: func(t *taskspb.Task) {
				t.GetHttpRequest().Url += strings.Repeat("p", 100)
			}},
			perturbation{name: "oidc-token", mod: func(t *taskspb.Task) {
				t.GetHttpRequest().AuthorizationHeader = &taskspb.HttpRequest_OidcToken{
					OidcToken: &taskspb.OidcToken{
						ServiceAccountEmail: fmt.Sprintf("size-probe@%s.iam.gserviceaccount.com", project),
					},
				}
			}},
		)
	case "appengine":
		ps = append(ps,
			perturbation{name: "relative-uri-100", mod: func(t *taskspb.Task) {
				t.GetAppEngineHttpRequest().RelativeUri = "/" + strings.Repeat("p", 99)
			}},
			perturbation{name: "routing-service", mod: func(t *taskspb.Task) {
				t.GetAppEngineHttpRequest().AppEngineRouting = &taskspb.AppEngineRouting{
					Service: "size-probe-service",
				}
			}},
		)
	}
	return ps
}

// sizeProber holds the per-target probe state.
type sizeProber struct {
	c      *Client
	opts   SizeProbeOptions
	target string
	queue  string // fully-qualified queue path
	sched  *timestamppb.Timestamp
	n      int // task counter, fixed-width in names so name length stays constant

	report *TargetSizeReport
	// Digit-stripped template and code of the first rejection, used to
	// classify later rejections as "the size error" vs something else.
	rejectTemplate string
	rejectCode     codes.Code
}

func probeTarget(ctx context.Context, c *Client, opts SizeProbeOptions, target string) TargetSizeReport {
	report := TargetSizeReport{Target: target}
	suffix := map[string]string{"http": "http", "appengine": "ae"}[target]
	if suffix == "" {
		report.Err = fmt.Sprintf("unknown target %q (want http|appengine)", target)
		return report
	}
	p := &sizeProber{
		c:      c,
		opts:   opts,
		target: target,
		queue: fmt.Sprintf("projects/%s/locations/%s/queues/%s-%s",
			opts.Project, opts.Location, opts.Prefix, suffix),
		// Whole seconds, far enough out that nothing dispatches even if the
		// pause were lost; constant across attempts so proto sizes compare.
		sched:  timestamppb.New(time.Unix(time.Now().Add(12*time.Hour).Unix(), 0)),
		report: &report,
	}

	if err := p.setup(ctx); err != nil {
		report.Err = err.Error()
		return report
	}
	defer p.teardown()

	if err := p.probe(ctx); err != nil {
		report.Err = err.Error()
	}
	return report
}

func (p *sizeProber) probe(ctx context.Context) error {
	base, err := p.searchMaxBody(ctx, perturbation{})
	if err != nil {
		return err
	}
	p.report.MaxBody = base

	boundary := p.buildTask(base, perturbation{})
	p.report.TaskProtoSizeAtMax = proto.Size(boundary)
	p.report.RequestProtoSizeAtMax = proto.Size(&taskspb.CreateTaskRequest{Parent: p.queue, Task: boundary})
	p.report.MessageProtoSizeAtMax = messageProtoSize(boundary)
	p.logf("%s: baseline max body=%d, task proto=%d, message proto=%d",
		p.target, base, p.report.TaskProtoSizeAtMax, p.report.MessageProtoSizeAtMax)

	for _, pt := range sizePerturbations(p.target, p.opts.Project) {
		pr, err := p.evalPerturbation(ctx, base, pt)
		if err != nil {
			return err
		}
		p.report.Perturbations = append(p.report.Perturbations, pr)
	}
	return nil
}

// evalPerturbation tests the proto-size hypothesis for one field: shrink the
// body by the perturbation's exact proto delta and the boundary should be
// unmoved. Falls back to a full search when the prediction misses for
// size-related reasons; a non-size rejection makes the perturbation
// inconclusive (recorded, not fatal).
func (p *sizeProber) evalPerturbation(ctx context.Context, base int, pt perturbation) (PerturbationReport, error) {
	delta := proto.Size(p.buildTask(0, pt)) - proto.Size(p.buildTask(0, perturbation{}))
	r := PerturbationReport{
		Name:             pt.name,
		ProtoDelta:       delta,
		PredictedMaxBody: base - delta,
		ActualMaxBody:    -1,
	}
	if r.PredictedMaxBody < 0 {
		r.Note = "proto delta exceeds baseline boundary; searching directly"
		return p.fullSearch(ctx, r, pt)
	}

	okAt, stAt, err := p.tryBody(ctx, r.PredictedMaxBody, pt)
	if err != nil {
		return r, err
	}
	okOver, stOver, err := p.tryBody(ctx, r.PredictedMaxBody+1, pt)
	if err != nil {
		return r, err
	}
	r.AtPredicted = outcome(okAt, stAt)
	r.OverPredicted = outcome(okOver, stOver)
	r.Consistent = okAt && !okOver && p.isSizeRejection(stOver)
	if r.Consistent {
		return r, nil
	}
	if (stAt != nil && !p.isSizeRejection(stAt)) || (stOver != nil && !p.isSizeRejection(stOver)) {
		r.Note = "non-size rejection observed; perturbation inconclusive (see outcomes)"
		return r, nil
	}
	return p.fullSearch(ctx, r, pt)
}

func (p *sizeProber) fullSearch(ctx context.Context, r PerturbationReport, pt perturbation) (PerturbationReport, error) {
	actual, err := p.searchMaxBody(ctx, pt)
	if err != nil {
		if ctx.Err() != nil {
			return r, err
		}
		r.Note = "fallback search failed: " + err.Error()
		return r, nil
	}
	r.ActualMaxBody = actual
	return r, nil
}

// searchMaxBody finds the largest accepted body length for the (possibly
// perturbed) task shape: exponential growth to bracket the boundary, then
// binary search. Every rejection along the way is treated as "over" but its
// template is recorded, so a rogue non-size error surfaces in OtherRejections.
func (p *sizeProber) searchMaxBody(ctx context.Context, pt perturbation) (int, error) {
	ok, st, err := p.tryBody(ctx, 0, pt)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("empty-body task rejected: %s: %s", st.Code(), st.Message())
	}

	lo, hi := 0, 64<<10
	for {
		if hi > maxProbeBody {
			hi = maxProbeBody
		}
		ok, _, err := p.tryBody(ctx, hi, pt)
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		lo = hi
		if hi == maxProbeBody {
			return 0, fmt.Errorf("no rejection up to body=%d bytes", lo)
		}
		hi *= 2
	}

	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		ok, _, err := p.tryBody(ctx, mid, pt)
		if err != nil {
			return 0, err
		}
		if ok {
			lo = mid
		} else {
			hi = mid
		}
	}
	return lo, nil
}

// buildTask assembles the baseline task for the target with a padded body and
// a run-unique name, then applies the perturbation. The counter is fixed-width
// so the name length is identical across attempts.
func (p *sizeProber) buildTask(bodyLen int, pt perturbation) *taskspb.Task {
	p.n++
	t := &taskspb.Task{
		Name:         fmt.Sprintf("%s/tasks/%s-t%06d%s", p.queue, p.opts.Prefix, p.n, pt.idSuffix),
		ScheduleTime: p.sched,
	}
	body := bytes.Repeat([]byte("a"), bodyLen)
	switch p.target {
	case "http":
		t.MessageType = &taskspb.Task_HttpRequest{HttpRequest: &taskspb.HttpRequest{
			Url:  "https://example.com/",
			Body: body,
		}}
	case "appengine":
		t.MessageType = &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
			Body: body,
		}}
	}
	if pt.mod != nil {
		pt.mod(t)
	}
	return t
}

func (p *sizeProber) tryBody(ctx context.Context, bodyLen int, pt perturbation) (bool, *status.Status, error) {
	ok, st, err := p.create(ctx, p.buildTask(bodyLen, pt))
	if err != nil {
		return false, nil, err
	}
	label := "accepted"
	if !ok {
		label = fmt.Sprintf("rejected %s", st.Code())
		p.noteRejection(st)
	}
	name := p.target
	if pt.name != "" {
		name += "/" + pt.name
	}
	p.logf("%s: body=%d %s", name, bodyLen, label)
	return ok, st, nil
}

// create issues one CreateTask under the per-step deadline (real Cloud Tasks
// rejects deadlines beyond 30s - see perStepTimeout). Transient failures are
// retried; AlreadyExists means our own earlier attempt landed despite a lost
// response, so it counts as accepted (task IDs are run-unique).
func (p *sizeProber) create(ctx context.Context, t *taskspb.Task) (bool, *status.Status, error) {
	for attempt := 0; ; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, perStepTimeout)
		_, err := p.c.CreateTask(callCtx, &taskspb.CreateTaskRequest{Parent: p.queue, Task: t})
		cancel()
		p.report.Calls++
		if err == nil {
			return true, nil, nil
		}
		if ctx.Err() != nil {
			return false, nil, ctx.Err()
		}
		st, ok := status.FromError(err)
		if !ok {
			return false, nil, err
		}
		switch st.Code() {
		case codes.AlreadyExists:
			return true, nil, nil
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
			if attempt < 4 {
				time.Sleep(time.Duration(attempt+1) * time.Second)
				continue
			}
		}
		return false, st, nil
	}
}

// noteRejection captures the first rejection verbatim as the reference size
// error and flags any later rejection with a different digit-stripped
// template (sizes interpolated into the message vary, the rest must not).
func (p *sizeProber) noteRejection(st *status.Status) {
	tmpl := stripDigits(st.Message())
	if p.report.RejectCode == "" {
		p.report.RejectCode = st.Code().String()
		p.report.RejectMessage = st.Message()
		p.report.RejectDetails = rejectionDetails(st)
		p.rejectTemplate = tmpl
		p.rejectCode = st.Code()
		return
	}
	if st.Code() == p.rejectCode && tmpl == p.rejectTemplate {
		return
	}
	key := st.Code().String() + ": " + tmpl
	if !slices.Contains(p.report.OtherRejections, key) {
		p.report.OtherRejections = append(p.report.OtherRejections, key)
	}
}

func (p *sizeProber) isSizeRejection(st *status.Status) bool {
	return st != nil && st.Code() == p.rejectCode && stripDigits(st.Message()) == p.rejectTemplate
}

func (p *sizeProber) setup(ctx context.Context) error {
	callCtx, cancel := context.WithTimeout(ctx, perStepTimeout)
	defer cancel()
	_, err := p.c.CreateQueue(callCtx, &taskspb.CreateQueueRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s", p.opts.Project, p.opts.Location),
		Queue:  &taskspb.Queue{Name: p.queue},
	})
	if err != nil {
		return fmt.Errorf("create queue %s: %w", p.queue, err)
	}
	if _, err := p.c.PauseQueue(callCtx, &taskspb.PauseQueueRequest{Name: p.queue}); err != nil {
		return fmt.Errorf("pause queue %s: %w", p.queue, err)
	}
	return nil
}

// teardown deletes the probe queue (and with it every task it accumulated).
// Best-effort on a fresh context so cleanup still runs after cancellation.
func (p *sizeProber) teardown() {
	ctx, cancel := context.WithTimeout(context.Background(), perStepTimeout)
	defer cancel()
	if err := p.c.DeleteQueue(ctx, &taskspb.DeleteQueueRequest{Name: p.queue}); err != nil {
		p.logf("%s: teardown: delete queue %s: %v", p.target, p.queue, err)
	}
}

func (p *sizeProber) logf(format string, args ...any) {
	if p.opts.Log != nil {
		_, _ = fmt.Fprintf(p.opts.Log, format+"\n", args...)
	}
}

func outcome(ok bool, st *status.Status) string {
	if ok {
		return "accepted"
	}
	return fmt.Sprintf("rejected %s: %s", st.Code(), st.Message())
}

func messageProtoSize(t *taskspb.Task) int {
	if hr := t.GetHttpRequest(); hr != nil {
		return proto.Size(hr)
	}
	if ae := t.GetAppEngineHttpRequest(); ae != nil {
		return proto.Size(ae)
	}
	return 0
}

// stripDigits removes decimal digits so messages that interpolate a byte
// count compare equal across differently-sized attempts.
func stripDigits(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return -1
		}
		return r
	}, s)
}

// rejectionDetails renders a status' error details the same way the errors
// battery does (prototext with whitespace canonicalised), minus template
// normalization - the probe report is read by a human, not diffed.
func rejectionDetails(st *status.Status) []DetailRecord {
	var out []DetailRecord
	for _, d := range st.Details() {
		msg, ok := d.(proto.Message)
		if !ok {
			continue
		}
		out = append(out, DetailRecord{
			Type:     fmt.Sprintf("%T", d),
			Template: canonicalSpace(prototext.MarshalOptions{}.Format(msg)),
		})
	}
	return out
}
