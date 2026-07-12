package conformance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file is the happy-path counterpart to the error battery: instead of
// capturing gRPC statuses from malformed RPCs, it creates a well-formed task and
// captures what Cloud Tasks echoes back when it is read. It pins down behaviours
// the emulator must match, none of which are stated unambiguously in the proto
// docs:
//
//   - Key casing at rest: does Cloud Tasks store header keys verbatim (so a
//     submitted "content-type" comes back lowercase) or canonicalize them to
//     "Content-Type"?
//   - Default Content-Type: for an AppEngine task with a body, is the default
//     "application/octet-stream" materialized in the stored task, or injected
//     only onto the dispatched wire request? This decides whether the emulator
//     should inject it at rest (setInitialTaskState) or at dispatch.
//   - View sensitivity: which fields does the BASIC response view withhold? The
//     body is documented as omitted under BASIC (callers must request FULL),
//     while headers are returned under both - each stage captures both so the
//     golden records the real division.
//
// Because the headers we submit are static (never derived from the run-scoped
// queue/task IDs), these observations need no template normalization and no
// multi-variant stability check - one capture per case is authoritative.

// observationBody is a non-empty body so both the AppEngine Content-Type default
// (which only applies when the task has a body) and the BASIC/FULL view division
// of the body are in play.
var observationBody = []byte(`{"hello":"world"}`)

// Captured is what one read observed - the task's headers and body - or the
// error that read returned. Headers and Body are nil when Err is set.
type Captured struct {
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"body,omitempty"`
	Err     string            `json:"err,omitempty"`
}

// HappyPathSnapshot is one observation's golden entry: the headers we submitted
// alongside what Cloud Tasks echoed back at each read stage.
type HappyPathSnapshot struct {
	Name        string            `json:"name"`
	RequestType string            `json:"requestType"` // http | appengine
	Sent        map[string]string `json:"sent"`
	CreateFull  Captured          `json:"createFull"` // CreateTask response, FULL view
	GetBasic    Captured          `json:"getBasic"`   // GetTask, BASIC view
	GetFull     Captured          `json:"getFull"`    // GetTask, FULL view
}

// happyPathCase names an observation, the headers it submits, and how to build
// its task.
type happyPathCase struct {
	name    string
	reqType string
	send    map[string]string
	build   func(p Params, headers map[string]string) *taskspb.Task
}

// scheduleFarOut pushes the task's dispatch time well into the future so it
// never fires against its (dummy) target during the capture window.
func scheduleFarOut() *timestamppb.Timestamp {
	return timestamppb.New(time.Now().Add(1 * time.Hour))
}

func buildHTTPTask(p Params, headers map[string]string) *taskspb.Task {
	return &taskspb.Task{
		Name:         p.TaskPath(),
		ScheduleTime: scheduleFarOut(),
		MessageType: &taskspb.Task_HttpRequest{
			HttpRequest: &taskspb.HttpRequest{
				Url:     "https://example.com/",
				Headers: headers,
				Body:    observationBody,
			},
		},
	}
}

func buildAppEngineTask(p Params, headers map[string]string) *taskspb.Task {
	return &taskspb.Task{
		Name:         p.TaskPath(),
		ScheduleTime: scheduleFarOut(),
		MessageType: &taskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
				HttpMethod:  taskspb.HttpMethod_POST,
				RelativeUri: "/task-handler",
				Headers:     headers,
				Body:        observationBody,
			},
		},
	}
}

// mixedCase submits a lowercase "content-type" and a
// mixed-case custom header, probing key-casing and default suppression.
var mixedCase = map[string]string{
	"content-type": "application/json",
	"X-Mixed-Case": "preserve-me",
}

// noContentType submits a body but no content-type of any casing, so the
// Content-Type default is free to apply. This is what reveals whether Cloud
// Tasks materializes the default at rest (it decides where the emulator should
// inject it).
var noContentType = map[string]string{
	"X-Mixed-Case": "preserve-me",
}

// happyPathObservations is the full happy-path battery. Names are stable golden
// keys - do not rename casually.
func happyPathObservations() []happyPathCase {
	return []happyPathCase{
		{name: "happypath/http", reqType: "http", send: mixedCase, build: buildHTTPTask},
		{name: "happypath/http-no-content-type", reqType: "http", send: noContentType, build: buildHTTPTask},
		{name: "happypath/appengine", reqType: "appengine", send: mixedCase, build: buildAppEngineTask},
		{name: "happypath/appengine-no-content-type", reqType: "appengine", send: noContentType, build: buildAppEngineTask},
	}
}

// RunHappyPath executes the happy-path battery against the client and returns
// one snapshot per observation. Like Run, it never aborts on an individual RPC
// failure - a failure is recorded in the relevant Captured.Err (e.g. a FULL-view
// read without cloudtasks.tasks.fullView) and is itself data.
func RunHappyPath(ctx context.Context, c *Client, opts RunOptions) []HappyPathSnapshot {
	obs := happyPathObservations()
	out := make([]HappyPathSnapshot, 0, len(obs))
	for i, o := range obs {
		p := opts.paramsFor(i, 0)
		out = append(out, observe(ctx, c, p, o))
	}
	return out
}

func observe(ctx context.Context, c *Client, p Params, obs happyPathCase) HappyPathSnapshot {
	snap := HappyPathSnapshot{Name: obs.name, RequestType: obs.reqType, Sent: obs.send}

	if err := withStep(ctx, c, p, createQueue); err != nil {
		// Without a queue there is nothing to observe; report the failure on
		// every stage so the divergence is unmistakable.
		snap.CreateFull.Err = err.Error()
		snap.GetBasic.Err = err.Error()
		snap.GetFull.Err = err.Error()
		return snap
	}
	defer func() { _ = withStep(ctx, c, p, deleteQueue) }() // best-effort cleanup

	var created *taskspb.Task
	err := within(ctx, func(ctx context.Context) error {
		t, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
			Parent:       p.QueuePath(),
			Task:         obs.build(p, obs.send),
			ResponseView: taskspb.Task_FULL,
		})
		created = t
		return err
	})
	if err != nil {
		snap.CreateFull.Err = err.Error()
		// A failed create means nothing to read back either.
		snap.GetBasic.Err = err.Error()
		snap.GetFull.Err = err.Error()
		return snap
	}
	snap.CreateFull = captureTask(created, obs.reqType)

	snap.GetBasic = read(ctx, c, p, obs.reqType, taskspb.Task_BASIC)
	snap.GetFull = read(ctx, c, p, obs.reqType, taskspb.Task_FULL)
	return snap
}

func read(ctx context.Context, c *Client, p Params, reqType string, view taskspb.Task_View) Captured {
	var got *taskspb.Task
	err := within(ctx, func(ctx context.Context) error {
		t, err := c.GetTask(ctx, &taskspb.GetTaskRequest{Name: p.TaskPath(), ResponseView: view})
		got = t
		return err
	})
	if err != nil {
		return Captured{Err: err.Error()}
	}
	return captureTask(got, reqType)
}

// captureTask pulls the headers and body out of whichever request type the task
// carries. Returns the zero Captured for a nil task or a mismatched type.
func captureTask(t *taskspb.Task, reqType string) Captured {
	if t == nil {
		return Captured{}
	}
	switch reqType {
	case "appengine":
		r := t.GetAppEngineHttpRequest()
		return Captured{Headers: r.GetHeaders(), Body: r.GetBody()}
	default:
		r := t.GetHttpRequest()
		return Captured{Headers: r.GetHeaders(), Body: r.GetBody()}
	}
}

// within bounds a single RPC to perStepTimeout so the deadline gRPC propagates
// to the server stays inside Cloud Tasks' 30s cap (see record.go).
func within(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, perStepTimeout)
	defer cancel()
	return fn(ctx)
}

// SaveHappyPath writes happy-path snapshots to path (see saveGolden).
func SaveHappyPath(path string, snaps []HappyPathSnapshot) error {
	return saveGolden(path, snaps, func(s HappyPathSnapshot) string { return s.Name })
}

// LoadHappyPath reads a happy-path golden keyed by observation name.
func LoadHappyPath(path string) (map[string]HappyPathSnapshot, error) {
	return loadGolden(path, func(s HappyPathSnapshot) string { return s.Name })
}

// CompareHappyPath checks recorded snapshots against a golden, returning a Diff
// per mismatched read stage (headers or body).
func CompareHappyPath(golden map[string]HappyPathSnapshot, got []HappyPathSnapshot) []Diff {
	return compareByName(golden, got,
		func(s HappyPathSnapshot) string { return s.Name },
		func(want, g HappyPathSnapshot) []Diff {
			var diffs []Diff
			stages := []struct {
				field string
				w, g  Captured
			}{
				{"createFull", want.CreateFull, g.CreateFull},
				{"getBasic", want.GetBasic, g.GetBasic},
				{"getFull", want.GetFull, g.GetFull},
			}
			for _, s := range stages {
				if w, gg := formatCaptured(s.w), formatCaptured(s.g); w != gg {
					diffs = append(diffs, Diff{Case: g.Name, Field: s.field, Want: w, Got: gg})
				}
			}
			return diffs
		})
}

// formatCaptured renders a Captured into a single canonical string for
// comparison: an "err: ..." line when the read failed, otherwise the sorted
// headers followed by the body (rendered verbatim, "(none)" when withheld).
func formatCaptured(c Captured) string {
	if c.Err != "" {
		return "err: " + c.Err
	}
	var b strings.Builder
	if len(c.Headers) == 0 {
		b.WriteString("headers: (none)")
	} else {
		lines := make([]string, 0, len(c.Headers))
		for k, v := range c.Headers {
			lines = append(lines, fmt.Sprintf("  %s: %s", k, v))
		}
		sort.Strings(lines)
		b.WriteString("headers:\n")
		b.WriteString(strings.Join(lines, "\n"))
	}
	b.WriteString("\nbody: ")
	if len(c.Body) == 0 {
		b.WriteString("(none)")
	} else {
		b.WriteString(string(c.Body))
	}
	return b.String()
}
