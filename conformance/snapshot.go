package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file is the happy-path counterpart to the error battery: instead of
// capturing gRPC statuses from malformed RPCs, it captures the header map that
// Cloud Tasks echoes back after a task is created. It exists to pin down three
// behaviours the emulator must match, none of which are stated unambiguously in
// the proto docs:
//
//   - Key casing at rest: does Cloud Tasks store header keys verbatim (so a
//     submitted "content-type" comes back lowercase) or canonicalize them to
//     "Content-Type"?
//   - Default Content-Type: for an AppEngine task with a body, is the default
//     "application/octet-stream" materialized in the stored task, or injected
//     only onto the dispatched wire request? This decides whether the emulator
//     should inject it at rest (setInitialTaskState) or at dispatch.
//   - View sensitivity: are headers withheld under the BASIC response view
//     (forcing callers to request FULL), or returned under both?
//
// Because the headers we submit are static (never derived from the run-scoped
// queue/task IDs), these observations need no template normalization and no
// multi-variant stability check - one capture per case is authoritative.

// observationBody is a non-empty body so the AppEngine Content-Type default is
// in play (that default only applies when the task has a body).
var observationBody = []byte(`{"hello":"world"}`)

// Captured is the header map observed at one read, or the error that read
// returned. Headers is nil when Err is set.
type Captured struct {
	Headers map[string]string `json:"headers,omitempty"`
	Err     string            `json:"err,omitempty"`
}

// HeaderSnapshot is one observation's golden entry: the headers we submitted
// alongside the headers Cloud Tasks echoed back at each read stage.
type HeaderSnapshot struct {
	Name        string            `json:"name"`
	RequestType string            `json:"requestType"` // http | appengine
	Sent        map[string]string `json:"sent"`
	CreateFull  Captured          `json:"createFull"` // CreateTask response, FULL view
	GetBasic    Captured          `json:"getBasic"`   // GetTask, BASIC view
	GetFull     Captured          `json:"getFull"`    // GetTask, FULL view
}

// headerCase names an observation, the headers it submits, and how to build its
// task.
type headerCase struct {
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

// headerObservations is the full happy-path battery. Names are stable golden
// keys - do not rename casually.
func headerObservations() []headerCase {
	return []headerCase{
		{name: "headers/http", reqType: "http", send: mixedCase, build: buildHTTPTask},
		{name: "headers/http-no-content-type", reqType: "http", send: noContentType, build: buildHTTPTask},
		{name: "headers/appengine", reqType: "appengine", send: mixedCase, build: buildAppEngineTask},
		{name: "headers/appengine-no-content-type", reqType: "appengine", send: noContentType, build: buildAppEngineTask},
	}
}

// RunHeaderObservations executes the happy-path battery against the client and
// returns one snapshot per observation. Like Run, it never aborts on an
// individual RPC failure - a failure is recorded in the relevant Captured.Err
// (e.g. a FULL-view read without cloudtasks.tasks.fullView) and is itself data.
func RunHeaderObservations(ctx context.Context, c *Client, opts RunOptions) []HeaderSnapshot {
	obs := headerObservations()
	out := make([]HeaderSnapshot, 0, len(obs))
	for i, o := range obs {
		p := opts.paramsFor(i, 0)
		out = append(out, observeHeaders(ctx, c, p, o))
	}
	return out
}

func observeHeaders(ctx context.Context, c *Client, p Params, obs headerCase) HeaderSnapshot {
	snap := HeaderSnapshot{Name: obs.name, RequestType: obs.reqType, Sent: obs.send}

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
	snap.CreateFull.Headers = taskHeaders(created, obs.reqType)

	snap.GetBasic = readHeaders(ctx, c, p, obs.reqType, taskspb.Task_BASIC)
	snap.GetFull = readHeaders(ctx, c, p, obs.reqType, taskspb.Task_FULL)
	return snap
}

func readHeaders(ctx context.Context, c *Client, p Params, reqType string, view taskspb.Task_View) Captured {
	var got *taskspb.Task
	err := within(ctx, func(ctx context.Context) error {
		t, err := c.GetTask(ctx, &taskspb.GetTaskRequest{Name: p.TaskPath(), ResponseView: view})
		got = t
		return err
	})
	if err != nil {
		return Captured{Err: err.Error()}
	}
	return Captured{Headers: taskHeaders(got, reqType)}
}

// taskHeaders pulls the header map out of whichever request type the task
// carries. Returns nil for a nil task or a mismatched type.
func taskHeaders(t *taskspb.Task, reqType string) map[string]string {
	if t == nil {
		return nil
	}
	switch reqType {
	case "appengine":
		return t.GetAppEngineHttpRequest().GetHeaders()
	default:
		return t.GetHttpRequest().GetHeaders()
	}
}

// within bounds a single RPC to perStepTimeout so the deadline gRPC propagates
// to the server stays inside Cloud Tasks' 30s cap (see record.go).
func within(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, perStepTimeout)
	defer cancel()
	return fn(ctx)
}

// SaveHeaderSnapshots writes snapshots as indented JSON, sorted by name so
// diffs are stable across runs.
func SaveHeaderSnapshots(path string, snaps []HeaderSnapshot) error {
	sorted := make([]HeaderSnapshot, len(snaps))
	copy(sorted, snaps)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	b, err := json.MarshalIndent(sorted, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}

// LoadHeaderSnapshots reads a snapshot golden keyed by name.
func LoadHeaderSnapshots(path string) (map[string]HeaderSnapshot, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var snaps []HeaderSnapshot
	if err := json.Unmarshal(b, &snaps); err != nil {
		return nil, err
	}
	m := make(map[string]HeaderSnapshot, len(snaps))
	for _, s := range snaps {
		m[s.Name] = s
	}
	return m, nil
}

// CompareHeaderSnapshots checks recorded snapshots against a golden, returning
// a Diff per mismatched read stage. Cases present in one set but not the other
// are reported too.
func CompareHeaderSnapshots(golden map[string]HeaderSnapshot, got []HeaderSnapshot) []Diff {
	var diffs []Diff
	seen := make(map[string]bool, len(got))

	for _, g := range got {
		seen[g.Name] = true
		want, ok := golden[g.Name]
		if !ok {
			diffs = append(diffs, Diff{Case: g.Name, Field: "presence", Want: "absent in golden", Got: "recorded"})
			continue
		}
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
	}
	for name := range golden {
		if !seen[name] {
			diffs = append(diffs, Diff{Case: name, Field: "presence", Want: "recorded", Got: "missing"})
		}
	}
	return diffs
}

// formatCaptured renders a Captured into a single canonical string for
// comparison: sorted "key: value" lines, or "err: ..." when the read failed.
func formatCaptured(c Captured) string {
	if c.Err != "" {
		return "err: " + c.Err
	}
	if len(c.Headers) == 0 {
		return "(no headers)"
	}
	lines := make([]string, 0, len(c.Headers))
	for k, v := range c.Headers {
		lines = append(lines, fmt.Sprintf("%s: %s", k, v))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}
