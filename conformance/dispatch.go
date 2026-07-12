package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"sort"
	"strings"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/aertje/cloud-tasks-emulator/conformance/receiver"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
)

// This file is the dispatch-headers battery: unlike the errors and happy-path
// batteries, which observe what Cloud Tasks stores and echoes back through the
// gRPC API, this one observes what Cloud Tasks puts on the wire when it
// actually dispatches a task. That is the only vantage point from which the two
// undocumented, optional retry headers - X-CloudTasks-TaskPreviousResponse /
// X-CloudTasks-TaskRetryReason and their X-AppEngine-* equivalents - can be
// seen at all: they never appear on a CreateTask/GetTask response, only on the
// dispatch request itself, and only once a task has already failed once.
//
// So this battery creates a task pointed at the receiver (see
// conformance/receiver), configures the queue to retry almost immediately, and
// forces two retries by having the receiver fail the first delivery with 503 and
// the second with 404 before succeeding - capturing the optional headers for
// both a 5XX and a 4XX prior response. It then reads back what the receiver saw
// for each attempt via the receiver's own /captures endpoint - the harness has
// no other way to observe the dispatch request, since Cloud Tasks (real or
// emulated) sends it directly to the target, not back through the gRPC API.
//
// Because dispatch requires a real network round trip to a running receiver
// (see conformance/receiver's doc comment on why that receiver exists and how
// it forces the retry), this battery is driven with an explicit receiverURL
// rather than being self-contained like the other two.

// DispatchAttempt is what the receiver observed for one delivery attempt of a
// dispatched task.
type DispatchAttempt struct {
	Attempt int               `json:"attempt"` // X-*-TaskRetryCount (0 on first delivery)
	Status  int               `json:"status"`  // status the receiver returned for this attempt
	Headers map[string]string `json:"headers"` // dispatch headers, filtered + normalized
}

// DispatchSnapshot is one case's golden entry: every delivery attempt the
// receiver observed for that case's task, in attempt order. A healthy case has
// three attempts (503 -> 404 -> 200); fewer means a retry did not happen (or the
// task never dispatched) - that gap is itself meaningful and is surfaced by the
// record command rather than silently swallowed.
type DispatchSnapshot struct {
	Name        string            `json:"name"`        // stable golden key: "dispatch/http" | "dispatch/appengine"
	RequestType string            `json:"requestType"` // "http" | "appengine"
	Attempts    []DispatchAttempt `json:"attempts"`    // sorted by Attempt ascending
}

// dispatchCase names an observation and which request family its task uses.
type dispatchCase struct {
	name    string
	reqType string
}

// dispatchCases is the full dispatch battery. Names are stable golden keys -
// do not rename casually.
func dispatchCases() []dispatchCase {
	return []dispatchCase{
		{name: "dispatch/http", reqType: "http"},
		{name: "dispatch/appengine", reqType: "appengine"},
	}
}

// dispatchPollInterval and dispatchPollDeadline bound how long RunDispatch
// waits for the receiver to observe a retry. Cloud Tasks doesn't dispatch
// instantly even with a fast retry config, and the emulator adds its own
// scheduling latency, so this is generous relative to the ~1s MinBackoff the
// queue is created with.
const (
	dispatchPollInterval = 2 * time.Second
	dispatchPollDeadline = 90 * time.Second
)

// RunDispatch executes the dispatch battery against the client and returns one
// snapshot per case. receiverURL is the base URL of a running receiver (see
// conformance/receiver) - either a deployed App Engine app (to record the
// golden from real Cloud Tasks) or a local server (for hermetic emulator
// validation). Like the other batteries, a failure at any stage is recorded as
// data (an empty or short Attempts slice) rather than aborting the run.
func RunDispatch(ctx context.Context, c *Client, opts RunOptions, receiverURL string) []DispatchSnapshot {
	cases := dispatchCases()
	out := make([]DispatchSnapshot, 0, len(cases))
	for i, cs := range cases {
		p := opts.paramsFor(i, 0)
		out = append(out, runDispatchCase(ctx, c, p, opts.Prefix, cs, receiverURL))
	}
	return out
}

func runDispatchCase(ctx context.Context, c *Client, p Params, prefix string, cs dispatchCase, receiverURL string) DispatchSnapshot {
	snap := DispatchSnapshot{Name: cs.name, RequestType: cs.reqType}

	if err := withStep(ctx, c, p, createFastRetryQueue); err != nil {
		// Without a queue there is nothing to dispatch; report an empty
		// Attempts slice, same as any other no-retry-observed shortfall.
		return snap
	}
	defer func() { _ = withStep(ctx, c, p, deleteQueue) }() // best-effort cleanup

	err := within(ctx, func(ctx context.Context) error {
		_, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
			Parent: p.QueuePath(),
			Task:   buildDispatchTask(p, cs.reqType, receiverURL),
		})
		return err
	})
	if err != nil {
		return snap
	}

	snap.Attempts = pollForAttempts(ctx, receiverURL, prefix, p)
	return snap
}

// createFastRetryQueue creates the queue with a retry config tuned so the
// forced retry happens within a few seconds rather than the (much longer)
// default backoff - the whole battery would otherwise take minutes per case.
func createFastRetryQueue(ctx context.Context, c *Client, p Params) error {
	_, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: p.Parent(),
		Queue: &taskspb.Queue{
			Name: p.QueuePath(),
			RetryConfig: &taskspb.RetryConfig{
				MaxAttempts:  5,
				MinBackoff:   durationpb.New(time.Second),
				MaxBackoff:   durationpb.New(5 * time.Second),
				MaxDoublings: 1,
			},
		},
	})
	return err
}

// buildDispatchTask builds a task with no ScheduleTime (so it dispatches
// immediately) pointed at the receiver's endpoint for the given request
// family. The App Engine case leaves AppEngineRouting nil, which routes to the
// default service - the deployed receiver when recording, or the emulator's
// APP_ENGINE_EMULATOR_HOST target when validating hermetically.
func buildDispatchTask(p Params, reqType string, receiverURL string) *taskspb.Task {
	if reqType == "appengine" {
		return &taskspb.Task{
			Name: p.TaskPath(),
			MessageType: &taskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
					HttpMethod:  taskspb.HttpMethod_POST,
					RelativeUri: receiver.PathAppEngine,
					Body:        []byte("{}"),
				},
			},
		}
	}
	return &taskspb.Task{
		Name: p.TaskPath(),
		MessageType: &taskspb.Task_HttpRequest{
			HttpRequest: &taskspb.HttpRequest{
				Url:        strings.TrimRight(receiverURL, "/") + receiver.PathHTTP,
				HttpMethod: taskspb.HttpMethod_POST,
				Body:       []byte("{}"),
			},
		},
	}
}

// pollForAttempts polls the receiver's readback endpoint until this case's task
// has run to completion (an attempt that got a 2xx, meaning the receiver's
// forced-retry sequence finished and every earlier attempt is already recorded)
// or the deadline elapses, whichever comes first. It returns whatever attempts
// were captured either way - a short result on timeout is data for the caller to
// report, not an error to abort on.
func pollForAttempts(ctx context.Context, receiverURL, prefix string, p Params) []DispatchAttempt {
	httpClient := &http.Client{Timeout: 10 * time.Second}
	readbackURL := strings.TrimRight(receiverURL, "/") + receiver.PathCaptures + "?run=" + url.QueryEscape(prefix)

	deadline := time.Now().Add(dispatchPollDeadline)
	var attempts []DispatchAttempt
	for {
		attempts = fetchAttempts(ctx, httpClient, readbackURL, p)
		if isComplete(attempts) || time.Now().After(deadline) {
			return attempts
		}
		select {
		case <-ctx.Done():
			return attempts
		case <-time.After(dispatchPollInterval):
		}
	}
}

// isComplete reports whether attempts contains a successful (2xx) delivery,
// which is the receiver's terminal response and so implies the full retry
// sequence has been observed.
func isComplete(attempts []DispatchAttempt) bool {
	for _, a := range attempts {
		if a.Status >= 200 && a.Status <= 299 {
			return true
		}
	}
	return false
}

// fetchAttempts does one readback of the receiver's capture log, keeping only
// captures for this case's task and deduplicating by attempt. Any transport or
// decode error is treated as "nothing captured yet" (returns nil) rather than
// propagated - the caller is polling, so a transient failure is retried on the
// next tick, and a persistent one just surfaces as a short Attempts slice.
func fetchAttempts(ctx context.Context, httpClient *http.Client, readbackURL string, p Params) []DispatchAttempt {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, readbackURL, nil)
	if err != nil {
		return nil
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	var captures []receiver.Capture
	if err := json.Unmarshal(body, &captures); err != nil {
		return nil
	}

	// Dedup by attempt: in principle the receiver logs exactly one capture per
	// attempt, but if a retry ever raced with a readback, keep the one with the
	// most headers as the higher-fidelity observation.
	byAttempt := make(map[int]receiver.Capture)
	for _, capture := range captures {
		if capture.TaskName != p.TaskID {
			continue
		}
		if existing, ok := byAttempt[capture.Attempt]; !ok || len(capture.Headers) > len(existing.Headers) {
			byAttempt[capture.Attempt] = capture
		}
	}

	out := make([]DispatchAttempt, 0, len(byAttempt))
	for attempt, capture := range byAttempt {
		out = append(out, DispatchAttempt{
			Attempt: attempt,
			Status:  capture.Status,
			Headers: normalizeDispatchHeaders(capture.Headers),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Attempt < out[j].Attempt })
	return out
}

// dispatchHeaderSuffixes are the per-task header names Cloud Tasks documents
// itself as sending on a dispatch, under both the X-CloudTasks- (HTTP target)
// and X-AppEngine- (App Engine target) families.
var dispatchHeaderSuffixes = []string{
	"QueueName", "TaskName", "TaskExecutionCount", "TaskRetryCount",
	"TaskETA", "TaskPreviousResponse", "TaskRetryReason",
}

// dispatchContractHeaders is the allowlist of headers this battery compares:
// User-Agent plus the per-task headers in both families. It is an explicit
// allowlist rather than an "X-CloudTasks-*/X-AppEngine-* prefix" filter for one
// reason discovered in the capture: because the recording receiver is itself an
// App Engine app, its HTTP-target endpoint receives a raft of App Engine
// *frontend* headers (X-Appengine-Api-Ticket, -User-Ip, -Request-Log-Id,
// -Appversionid, ...) that real Cloud Tasks never sends to an arbitrary HTTP
// target - they are an artifact of where the receiver is hosted, not part of
// the dispatch contract, and volatile per request, so they must not enter the
// golden.
//
// queueNameHeaders/taskNameHeaders/taskETAHeaders are the subset whose values
// are run-scoped (derived from opts.Prefix via paramsFor, or the dispatch time)
// and so must be replaced with placeholders before comparison, the same way
// normalize.go handles interpolated values in error messages.
//
// All sets are built with textproto.CanonicalMIMEHeaderKey so membership checks
// are robust to header casing; per Capture's doc comment, net/http canonicalizes
// keys on receipt (e.g. X-CloudTasks-TaskName arrives as X-Cloudtasks-Taskname).
var (
	dispatchContractHeaders = buildDispatchContractHeaders()

	queueNameHeaders = canonicalHeaderSet("X-CloudTasks-QueueName", "X-AppEngine-QueueName")
	taskNameHeaders  = canonicalHeaderSet("X-CloudTasks-TaskName", "X-AppEngine-TaskName")
	taskETAHeaders   = canonicalHeaderSet("X-CloudTasks-TaskETA", "X-AppEngine-TaskETA")
)

func buildDispatchContractHeaders() map[string]bool {
	m := canonicalHeaderSet("User-Agent")
	for _, family := range []string{"X-CloudTasks-", "X-AppEngine-"} {
		for _, suffix := range dispatchHeaderSuffixes {
			m[textproto.CanonicalMIMEHeaderKey(family+suffix)] = true
		}
	}
	return m
}

func canonicalHeaderSet(keys ...string) map[string]bool {
	m := make(map[string]bool, len(keys))
	for _, k := range keys {
		m[textproto.CanonicalMIMEHeaderKey(k)] = true
	}
	return m
}

// normalizeDispatchHeaders filters raw dispatch headers down to the dispatch
// contract (see dispatchContractHeaders) and replaces run-scoped values (queue
// name, task name, task ETA) with stable placeholders so a golden comparison
// isn't defeated by every run using a different resource-name prefix and
// dispatch time. Every other kept value (RetryCount, ExecutionCount,
// TaskPreviousResponse, TaskRetryReason, User-Agent) is left verbatim, since
// capturing exactly those values is the point of this battery.
func normalizeDispatchHeaders(raw map[string]string) map[string]string {
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		ck := textproto.CanonicalMIMEHeaderKey(k)
		if !dispatchContractHeaders[ck] {
			continue
		}
		switch {
		case queueNameHeaders[ck]:
			v = "{queue}"
		case taskNameHeaders[ck]:
			v = "{task}"
		case taskETAHeaders[ck]:
			v = "{eta}"
		}
		out[ck] = v
	}
	return out
}

// SaveDispatch writes dispatch snapshots to path (see saveGolden).
func SaveDispatch(path string, snaps []DispatchSnapshot) error {
	return saveGolden(path, snaps, func(s DispatchSnapshot) string { return s.Name })
}

// LoadDispatch reads a dispatch golden keyed by case name.
func LoadDispatch(path string) (map[string]DispatchSnapshot, error) {
	return loadGolden(path, func(s DispatchSnapshot) string { return s.Name })
}

// CompareDispatch checks recorded dispatch snapshots against a golden,
// returning a Diff per case whose formatted attempts differ.
func CompareDispatch(golden map[string]DispatchSnapshot, got []DispatchSnapshot) []Diff {
	return compareByName(golden, got,
		func(s DispatchSnapshot) string { return s.Name },
		func(want, g DispatchSnapshot) []Diff {
			if w, gg := formatDispatch(want), formatDispatch(g); w != gg {
				return []Diff{{Case: g.Name, Field: "attempts", Want: w, Got: gg}}
			}
			return nil
		})
}

// formatDispatch renders a snapshot's attempts into a single canonical string
// for comparison: one "attempt N (status S):" block per attempt, each
// followed by its sorted "  key: value" header lines, blocks joined in
// (already-ascending) attempt order. Analogous to formatCaptured/formatDetails
// in the other batteries.
func formatDispatch(snap DispatchSnapshot) string {
	if len(snap.Attempts) == 0 {
		return "(no attempts captured)"
	}
	blocks := make([]string, len(snap.Attempts))
	for i, a := range snap.Attempts {
		var b strings.Builder
		fmt.Fprintf(&b, "attempt %d (status %d):", a.Attempt, a.Status)
		if len(a.Headers) == 0 {
			b.WriteString("\n  (none)")
		} else {
			lines := make([]string, 0, len(a.Headers))
			for k, v := range a.Headers {
				lines = append(lines, fmt.Sprintf("  %s: %s", k, v))
			}
			sort.Strings(lines)
			b.WriteString("\n")
			b.WriteString(strings.Join(lines, "\n"))
		}
		blocks[i] = b.String()
	}
	return strings.Join(blocks, "\n---\n")
}
