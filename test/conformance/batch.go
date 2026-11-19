package conformance

import (
	"context"
	"crypto/rand"
	"fmt"
	"sort"
	"strings"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file is the BatchCreateTasks counterpart to the happy-path battery. The
// error battery captures one status per case, but a batch reports much of its
// outcome inside the long-running operation it returns rather than as an RPC
// error. Each observation here sends one batch and captures that outcome,
// pinning down behaviours the proto docs leave open:
//
//   - Whether the operation is already done when the RPC returns, the shape of
//     its name, and what it holds when the batch targets a missing queue.
//   - Which task failures reject the whole batch, and which are reported per
//     task in failed_requests - under which index, and with which status.
//   - The metadata state when every task is created, when some fail, and when
//     all of them fail.
//   - Whether the created tasks come back in request order. Callers map each
//     created task back to its request by position, so this is load-bearing.
//   - What resending a batch with the same request_id does.
//
// Task and operation names are run-scoped, so they are captured as templates
// (see templateTaskName, templateOperationName). One capture per observation is
// authoritative; there is no multi-variant stability check.

// BatchSnapshot is one observation's golden entry: what a single
// BatchCreateTasks call reported.
type BatchSnapshot struct {
	Name string `json:"name"`
	// Err is "Code: template" when the batch was rejected: its setup or the
	// BatchCreateTasks RPC itself failed.
	Err           string `json:"err,omitempty"`
	DoneOnReturn  bool   `json:"doneOnReturn"`
	OperationName string `json:"operationName,omitempty"`
	// WaitErr is "Code: template" when waiting for the operation failed - for
	// example an operation that finished without a result.
	WaitErr string `json:"waitErr,omitempty"`
	State   string `json:"state,omitempty"`
	// Tasks are the created tasks' names, templated, in response order.
	Tasks []string `json:"tasks,omitempty"`
	// Failed holds one "index: Code: template" entry per failed request, by
	// ascending index.
	Failed []string `json:"failed,omitempty"`
	// Listed is how many tasks the queue holds after the batch, or -1 when it
	// cannot be listed (e.g. the queue does not exist).
	Listed int `json:"listed"`
	// Replay is set when the observation resends the batch with the same
	// request_id.
	Replay *BatchReplay `json:"replay,omitempty"`
}

// BatchReplay is what resending a batch with the same request_id returned.
// Request IDs are run-scoped, so one echoed in Err is templated as
// {request_id}.
type BatchReplay struct {
	Err           string `json:"err,omitempty"`
	SameOperation bool   `json:"sameOperation"`
	SameTasks     bool   `json:"sameTasks"`
	Listed        int    `json:"listed"`
}

// batchObservation names an observation, how to prepare its queue, the batch
// it sends, and whether it resends that batch with the same request_id.
type batchObservation struct {
	name string
	// noQueue skips creating the queue, so the batch targets a queue that
	// never existed.
	noQueue bool
	setup   func(ctx context.Context, c *Client, p Params) error
	build   func(p Params) []*taskspb.CreateTaskRequest
	replay  bool
}

// suffixedTaskName names a batch task with the variant's task path plus a
// suffix, so one observation can create several distinct named tasks.
func suffixedTaskName(suffix string) func(p Params) string {
	return func(p Params) string { return p.TaskPath() + suffix }
}

func invalidTaskName(p Params) string { return p.QueuePath() + "/tasks/not a valid id" }

// otherQueueTaskName names a task in a queue other than the batch's.
func otherQueueTaskName(p Params) string {
	other := p
	other.QueueID = p.QueueID + "-other"
	return other.TaskPath()
}

// createFarOutTasks returns a setup that creates the variant's task path plus
// each suffix, scheduled far out so they are still in the queue - neither
// dispatched nor removed - when the batch runs.
func createFarOutTasks(suffixes ...string) func(ctx context.Context, c *Client, p Params) error {
	return func(ctx context.Context, c *Client, p Params) error {
		for _, suffix := range suffixes {
			if _, err := c.CreateTask(ctx, batchTaskRequest(p, p.TaskPath()+suffix)); err != nil {
				return err
			}
		}
		return nil
	}
}

// withSecondTask returns a two-task batch builder: a task with a generated
// name, then one modified by mod. It isolates one kind of task failure, to
// show whether that failure rejects the whole batch or only its own task.
func withSecondTask(mod func(*taskspb.Task)) func(p Params) []*taskspb.CreateTaskRequest {
	return func(p Params) []*taskspb.CreateTaskRequest {
		requests := batchOf(generatedName, generatedName)(p)
		mod(requests[1].GetTask())
		return requests
	}
}

// batchObservations is the full BatchCreateTasks battery. Names are stable
// golden keys - do not rename casually.
func batchObservations() []batchObservation {
	return []batchObservation{
		{
			// The named tasks are deliberately out of lexical order, so a
			// response sorted by name is distinguishable from request order.
			name:  "batch/all-created",
			build: batchOf(suffixedTaskName("-c"), suffixedTaskName("-a"), generatedName, suffixedTaskName("-b")),
		},
		{
			name:  "batch/some-failed",
			setup: createFarOutTasks(""),
			build: batchOf(suffixedTaskName("-b"), variantTaskName, generatedName, suffixedTaskName("-a")),
		},
		{
			name:  "batch/all-failed",
			setup: createFarOutTasks("", "-x"),
			build: batchOf(variantTaskName, suffixedTaskName("-x")),
		},
		{
			name:   "batch/request-id-replay",
			build:  batchOf(generatedName, generatedName),
			replay: true,
		},
		{
			name:    "batch/queue-not-found",
			noQueue: true,
			build:   batchOf(generatedName),
		},
		{
			name:  "batch/queue-recently-deleted",
			setup: deleteQueue,
			build: batchOf(generatedName),
		},

		// One task fails for each reason, beside one valid task: these show
		// which failures reject the whole batch and which only their own task.
		{
			name:  "batch/task-already-exists",
			setup: createFarOutTasks(""),
			build: batchOf(generatedName, variantTaskName),
		},
		{
			name:  "batch/task-invalid-name",
			build: batchOf(generatedName, invalidTaskName),
		},
		{
			name:  "batch/task-queue-mismatch",
			build: batchOf(generatedName, otherQueueTaskName),
		},
		{
			name:  "batch/task-invalid-url",
			build: withSecondTask(func(t *taskspb.Task) { t.GetHttpRequest().Url = "" }),
		},
		{
			name:  "batch/task-deadline-too-short",
			build: withSecondTask(func(t *taskspb.Task) { t.DispatchDeadline = durationpb.New(5 * time.Second) }),
		},
		{
			// A fixed far-future instant, so any interpolation of it into the
			// message is stable across runs.
			name: "batch/task-schedule-too-far",
			build: withSecondTask(func(t *taskspb.Task) {
				t.ScheduleTime = timestamppb.New(time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC))
			}),
		},
	}
}

// RunBatch executes the BatchCreateTasks battery against the client and
// returns one snapshot per observation. Like the other batteries, it never
// aborts on an individual RPC failure - a failure is recorded in the snapshot
// and is itself data.
func RunBatch(ctx context.Context, c *Client, opts RunOptions) []BatchSnapshot {
	obs := batchObservations()
	out := make([]BatchSnapshot, 0, len(obs))
	for i, o := range obs {
		p := opts.paramsFor(i, 0)
		out = append(out, observeBatch(ctx, c, p, o))
	}
	return out
}

func observeBatch(ctx context.Context, c *Client, p Params, obs batchObservation) BatchSnapshot {
	snap := BatchSnapshot{Name: obs.name}

	if !obs.noQueue {
		if err := withStep(ctx, c, p, createQueue); err != nil {
			snap.Err = "setup: " + errTemplate(err, p)
			return snap
		}
		defer func() { _ = withStep(ctx, c, p, deleteQueue) }() // best-effort cleanup
	}
	if obs.setup != nil {
		if err := withStep(ctx, c, p, obs.setup); err != nil {
			snap.Err = "setup: " + errTemplate(err, p)
			return snap
		}
	}

	req := &taskspb.BatchCreateTasksRequest{Parent: p.QueuePath(), Requests: obs.build(p)}
	if obs.replay {
		req.RequestId = newRequestID()
	}

	first, err := sendBatch(ctx, c, req)
	if err != nil {
		snap.Err = errTemplate(err, p)
	} else {
		snap.DoneOnReturn = first.doneOnReturn
		snap.OperationName = templateOperationName(first.operationName, p)
		if first.waitErr != nil {
			snap.WaitErr = errTemplate(first.waitErr, p)
		}
		snap.State = first.metadata.GetState().String()
		snap.Tasks = templateTaskNames(first.response.GetTasks(), p)
		snap.Failed = templateFailedRequests(first.metadata, p)
	}
	snap.Listed = listTaskCount(ctx, c, p)

	if obs.replay && err == nil {
		snap.Replay = &BatchReplay{}
		replayed, err := sendBatch(ctx, c, req)
		if err != nil {
			snap.Replay.Err = strings.ReplaceAll(errTemplate(err, p), req.GetRequestId(), "{request_id}")
		} else {
			snap.Replay.SameOperation = replayed.operationName == first.operationName
			snap.Replay.SameTasks = sameTaskNames(replayed.response.GetTasks(), first.response.GetTasks())
		}
		snap.Replay.Listed = listTaskCount(ctx, c, p)
	}
	return snap
}

// batchOutcome is what one accepted BatchCreateTasks call reported once its
// operation finished. waitErr is set when waiting for the operation failed;
// metadata is still read from the operation in that case.
type batchOutcome struct {
	doneOnReturn  bool
	operationName string
	waitErr       error
	response      *taskspb.BatchCreateTasksResponse
	metadata      *taskspb.BatchCreateTasksMetadata
}

// sendBatch sends req and waits for its operation, within one step's deadline.
// It returns an error only when the RPC itself fails. It records whether the
// operation was already done when the RPC returned, before waiting on it.
func sendBatch(ctx context.Context, c *Client, req *taskspb.BatchCreateTasksRequest) (batchOutcome, error) {
	var out batchOutcome
	err := within(ctx, func(ctx context.Context) error {
		op, err := c.BatchCreateTasks(ctx, req)
		if err != nil {
			return err
		}
		out.doneOnReturn = op.Done()
		out.operationName = op.Name()
		out.response, out.waitErr = op.Wait(ctx)
		out.metadata, _ = op.Metadata()
		return nil
	})
	return out, err
}

// listTaskCount returns how many tasks the variant's queue holds, or -1 when
// listing fails.
func listTaskCount(ctx context.Context, c *Client, p Params) int {
	count := 0
	err := within(ctx, func(ctx context.Context) error {
		it := c.ListTasks(ctx, &taskspb.ListTasksRequest{Parent: p.QueuePath()})
		for {
			_, err := it.Next()
			if err == iterator.Done {
				return nil
			}
			if err != nil {
				return err
			}
			count++
		}
	})
	if err != nil {
		return -1
	}
	return count
}

// errTemplate renders a gRPC error as "Code: template", normalizing the
// run-scoped names in its message.
func errTemplate(err error, p Params) string {
	st := status.Convert(err)
	return fmt.Sprintf("%s: %s", st.Code(), Normalize(st.Message(), p))
}

// templateTaskName normalizes a created task's name. Names the batch chose
// become {task_path}-based templates via Normalize; a name Cloud Tasks
// generated keeps only its queue, as {queue_path}/tasks/{generated}.
func templateTaskName(name string, p Params) string {
	template := Normalize(name, p)
	if strings.HasPrefix(template, "{queue_path}/tasks/") {
		return "{queue_path}/tasks/{generated}"
	}
	return template
}

func templateTaskNames(tasks []*taskspb.Task, p Params) []string {
	names := make([]string, len(tasks))
	for i, task := range tasks {
		names[i] = templateTaskName(task.GetName(), p)
	}
	return names
}

// templateOperationName normalizes an operation name: the location becomes
// {parent} and the trailing ID is replaced by its character class ({digits},
// {hex} or {id}), so the golden records the name's shape, not one run's ID.
func templateOperationName(name string, p Params) string {
	template := Normalize(name, p)
	i := strings.LastIndex(template, "/")
	if i < 0 {
		return template
	}
	return template[:i+1] + idClass(template[i+1:])
}

func idClass(id string) string {
	switch {
	case id == "":
		return ""
	case strings.Trim(id, "0123456789") == "":
		return "{digits}"
	case strings.Trim(strings.ToLower(id), "0123456789abcdef-") == "":
		return "{hex}"
	default:
		return "{id}"
	}
}

// templateFailedRequests renders failed_requests as "index: Code: template"
// entries, by ascending index.
func templateFailedRequests(metadata *taskspb.BatchCreateTasksMetadata, p Params) []string {
	indexes := make([]int, 0, len(metadata.GetFailedRequests()))
	for i := range metadata.GetFailedRequests() {
		indexes = append(indexes, int(i))
	}
	sort.Ints(indexes)

	entries := make([]string, len(indexes))
	for i, index := range indexes {
		st := metadata.GetFailedRequests()[int32(index)]
		entries[i] = fmt.Sprintf("%d: %s", index, errTemplate(status.ErrorProto(st), p))
	}
	return entries
}

func sameTaskNames(a, b []*taskspb.Task) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].GetName() != b[i].GetName() {
			return false
		}
	}
	return true
}

// newRequestID returns a random UUID4, the format BatchCreateTasksRequest's
// request_id requires.
func newRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// SaveBatch writes BatchCreateTasks snapshots to path (see saveGolden).
func SaveBatch(path string, snaps []BatchSnapshot) error {
	return saveGolden(path, snaps, func(s BatchSnapshot) string { return s.Name })
}

// LoadBatch reads a BatchCreateTasks golden keyed by observation name.
func LoadBatch(path string) (map[string]BatchSnapshot, error) {
	return loadGolden(path, func(s BatchSnapshot) string { return s.Name })
}

// CompareBatch checks recorded snapshots against a golden, returning a Diff per
// mismatched field.
func CompareBatch(golden map[string]BatchSnapshot, got []BatchSnapshot) []Diff {
	return compareByName(golden, got,
		func(s BatchSnapshot) string { return s.Name },
		func(want, g BatchSnapshot) []Diff {
			fields := []struct {
				name string
				w, g string
			}{
				{"err", want.Err, g.Err},
				{"doneOnReturn", fmt.Sprint(want.DoneOnReturn), fmt.Sprint(g.DoneOnReturn)},
				{"operationName", want.OperationName, g.OperationName},
				{"waitErr", want.WaitErr, g.WaitErr},
				{"state", want.State, g.State},
				{"tasks", strings.Join(want.Tasks, "\n"), strings.Join(g.Tasks, "\n")},
				{"failed", strings.Join(want.Failed, "\n"), strings.Join(g.Failed, "\n")},
				{"listed", fmt.Sprint(want.Listed), fmt.Sprint(g.Listed)},
				{"replay", formatReplay(want.Replay), formatReplay(g.Replay)},
			}
			var diffs []Diff
			for _, f := range fields {
				if f.w != f.g {
					diffs = append(diffs, Diff{Case: g.Name, Field: f.name, Want: f.w, Got: f.g})
				}
			}
			return diffs
		})
}

func formatReplay(r *BatchReplay) string {
	if r == nil {
		return "(none)"
	}
	return fmt.Sprintf("err=%q sameOperation=%t sameTasks=%t listed=%d", r.Err, r.SameOperation, r.SameTasks, r.Listed)
}
