package conformance

import (
	"context"
	"fmt"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Client is the official Cloud Tasks client; the same type drives both the real
// API and the emulator.
type Client = cloudtasks.Client

// Case is one error scenario exercised against a target. Setup establishes any
// precondition (e.g. create-then-delete to reach "recently deleted"); Invoke
// performs the RPC whose error we want to capture; Teardown is best-effort
// cleanup. Setup/Teardown may be nil.
//
// Each case is run with several Params variants so the recorder can distinguish
// static message text from interpolated request values.
type Case struct {
	Name     string
	RPC      string
	Category string
	Setup    func(ctx context.Context, c *Client, p Params) error
	Invoke   func(ctx context.Context, c *Client, p Params) error
	Teardown func(ctx context.Context, c *Client, p Params) error
}

// httpTask returns a minimal valid task body so CreateTask succeeds during
// setup (Cloud Tasks requires a message type on the task).
func httpTask(name string) *taskspb.Task {
	return &taskspb.Task{
		Name: name,
		MessageType: &taskspb.Task_HttpRequest{
			HttpRequest: &taskspb.HttpRequest{Url: "https://example.com/"},
		},
	}
}

// appEngineTask returns a minimal valid App Engine-target task body.
func appEngineTask(name string) *taskspb.Task {
	return &taskspb.Task{
		Name: name,
		MessageType: &taskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{},
		},
	}
}

// createTaskMod returns an Invoke that creates the task built by mk for the
// variant's task path after applying mod, for probing how Cloud Tasks
// validates task configuration at create time. The offending values are
// static inputs, stable across variants.
func createTaskMod(mk func(string) *taskspb.Task, mod func(*taskspb.Task)) func(ctx context.Context, c *Client, p Params) error {
	return func(ctx context.Context, c *Client, p Params) error {
		task := mk(p.TaskPath())
		mod(task)
		_, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
			Parent: p.QueuePath(),
			Task:   task,
		})
		return err
	}
}

func createQueue(ctx context.Context, c *Client, p Params) error {
	_, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: p.Parent(),
		Queue:  &taskspb.Queue{Name: p.QueuePath()},
	})
	return err
}

// otherLocation returns a valid Cloud Tasks location that differs from loc, for
// building a queue name that is well-formed but does not fall under the request
// parent's location. Both returned values are real locations, so the name and
// the parent each pass their own shape/existence checks and only their
// relationship is wrong.
func otherLocation(loc string) string {
	if loc == "us-central1" {
		return "us-east1"
	}
	return "us-central1"
}

// mismatchedQueueName is the well-formed queue name used by the parent-mismatch
// case: same project as the request parent, but a different (still valid)
// location, so it structurally cannot fall under parent.
func mismatchedQueueName(p Params) string {
	return fmt.Sprintf("projects/%s/locations/%s/queues/%s", p.Project, otherLocation(p.Location), p.QueueID)
}

func deleteQueue(ctx context.Context, c *Client, p Params) error {
	return c.DeleteQueue(ctx, &taskspb.DeleteQueueRequest{Name: p.QueuePath()})
}

func createTask(ctx context.Context, c *Client, p Params) error {
	_, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: p.QueuePath(),
		Task:   httpTask(p.TaskPath()),
	})
	return err
}

// createTaskURL returns an Invoke that creates an HTTP task whose target URL is
// the supplied (typically malformed) string, for probing how Cloud Tasks
// validates HttpRequest.Url at create time. The URL is a static input, so it is
// stable across variants and needs no normalization.
func createTaskURL(url string) func(ctx context.Context, c *Client, p Params) error {
	return func(ctx context.Context, c *Client, p Params) error {
		_, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
			Parent: p.QueuePath(),
			Task: &taskspb.Task{
				Name: p.TaskPath(),
				MessageType: &taskspb.Task_HttpRequest{
					HttpRequest: &taskspb.HttpRequest{Url: url},
				},
			},
		})
		return err
	}
}

// createQueueConfig returns an Invoke that creates a queue carrying the
// supplied rate limits / retry config, for probing how Cloud Tasks validates
// queue configuration at create time. The offending values are static inputs,
// stable across variants.
func createQueueConfig(rl *taskspb.RateLimits, rc *taskspb.RetryConfig) func(ctx context.Context, c *Client, p Params) error {
	return func(ctx context.Context, c *Client, p Params) error {
		_, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
			Parent: p.Parent(),
			Queue:  &taskspb.Queue{Name: p.QueuePath(), RateLimits: rl, RetryConfig: rc},
		})
		return err
	}
}

// Cases is the full error-state battery. Names are stable identifiers used as
// golden keys, so do not rename casually.
func Cases() []Case {
	return []Case{
		// --- Queue lifecycle: not-found vs recently-deleted ---
		{
			Name: "queue/get/not-found", RPC: "GetQueue", Category: "queue-not-found",
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				_, err := c.GetQueue(ctx, &taskspb.GetQueueRequest{Name: p.QueuePath()})
				return err
			},
		},
		{
			Name: "queue/delete/not-found", RPC: "DeleteQueue", Category: "queue-not-found",
			Invoke: deleteQueue,
		},
		{
			Name: "queue/pause/not-found", RPC: "PauseQueue", Category: "queue-not-found",
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				_, err := c.PauseQueue(ctx, &taskspb.PauseQueueRequest{Name: p.QueuePath()})
				return err
			},
		},
		{
			Name: "queue/resume/not-found", RPC: "ResumeQueue", Category: "queue-not-found",
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				_, err := c.ResumeQueue(ctx, &taskspb.ResumeQueueRequest{Name: p.QueuePath()})
				return err
			},
		},
		{
			Name: "queue/purge/not-found", RPC: "PurgeQueue", Category: "queue-not-found",
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				_, err := c.PurgeQueue(ctx, &taskspb.PurgeQueueRequest{Name: p.QueuePath()})
				return err
			},
		},
		{
			Name: "queue/get/recently-deleted", RPC: "GetQueue", Category: "queue-recently-deleted",
			Setup: func(ctx context.Context, c *Client, p Params) error {
				if err := createQueue(ctx, c, p); err != nil {
					return err
				}
				return deleteQueue(ctx, c, p)
			},
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				_, err := c.GetQueue(ctx, &taskspb.GetQueueRequest{Name: p.QueuePath()})
				return err
			},
		},
		{
			Name: "queue/create/recently-deleted", RPC: "CreateQueue", Category: "queue-recently-deleted",
			Setup: func(ctx context.Context, c *Client, p Params) error {
				if err := createQueue(ctx, c, p); err != nil {
					return err
				}
				return deleteQueue(ctx, c, p)
			},
			Invoke: createQueue,
		},
		{
			Name: "queue/create/already-exists", RPC: "CreateQueue", Category: "queue-already-exists",
			Setup:    createQueue,
			Invoke:   createQueue,
			Teardown: deleteQueue,
		},
		{
			Name: "queue/create/invalid-name", RPC: "CreateQueue", Category: "queue-invalid-name",
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				_, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
					Parent: p.Parent(),
					Queue:  &taskspb.Queue{Name: "not-a-valid-queue-name"},
				})
				return err
			},
		},
		{
			Name: "queue/create/invalid-parent", RPC: "CreateQueue", Category: "queue-invalid-parent",
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				// Syntactically malformed parent (not a projects/.../locations/...
				// resource), so it fails request validation rather than being read
				// as a real project and tripping an IAM permission check.
				_, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
					Parent: "not-a-valid-parent",
					Queue:  &taskspb.Queue{Name: p.QueuePath()},
				})
				return err
			},
		},
		{
			// Queue name is well-formed and the parent is well-formed, but the
			// name sits under a different (valid) location than the request
			// parent, so only their relationship is wrong. Probes whether Cloud
			// Tasks requires the queue name to fall under parent (as CreateTask
			// requires the task name to) and with what code/message - the
			// emulator currently performs no such check. Teardown is best-effort
			// and targets the name's own location, in case the create is
			// accepted rather than rejected.
			Name: "queue/create/parent-mismatch", RPC: "CreateQueue", Category: "queue-parent-mismatch",
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				_, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
					Parent: p.Parent(),
					Queue:  &taskspb.Queue{Name: mismatchedQueueName(p)},
				})
				return err
			},
			Teardown: func(ctx context.Context, c *Client, p Params) error {
				return c.DeleteQueue(ctx, &taskspb.DeleteQueueRequest{Name: mismatchedQueueName(p)})
			},
		},

		// --- CreateQueue configuration validation ---
		// Probes how Cloud Tasks rejects out-of-range RateLimits/RetryConfig
		// values at create time (code and message), pinning down the validation
		// the emulator enforces before sizing queue internals from these
		// fields. Teardown deletes best-effort because one case succeeds: real
		// v2 treats max_burst_size as output-only and ignores the client value,
		// so burst-negative creates the queue and records OK.
		{
			Name: "queue/create/rate-negative", RPC: "CreateQueue", Category: "queue-invalid-config",
			Invoke:   createQueueConfig(&taskspb.RateLimits{MaxDispatchesPerSecond: -1}, nil),
			Teardown: deleteQueue,
		},
		{
			Name: "queue/create/rate-too-high", RPC: "CreateQueue", Category: "queue-invalid-config",
			Invoke:   createQueueConfig(&taskspb.RateLimits{MaxDispatchesPerSecond: 501}, nil),
			Teardown: deleteQueue,
		},
		{
			Name: "queue/create/burst-negative", RPC: "CreateQueue", Category: "queue-invalid-config",
			Invoke:   createQueueConfig(&taskspb.RateLimits{MaxBurstSize: -1}, nil),
			Teardown: deleteQueue,
		},
		{
			Name: "queue/create/concurrent-negative", RPC: "CreateQueue", Category: "queue-invalid-config",
			Invoke:   createQueueConfig(&taskspb.RateLimits{MaxConcurrentDispatches: -1}, nil),
			Teardown: deleteQueue,
		},
		{
			Name: "queue/create/concurrent-too-high", RPC: "CreateQueue", Category: "queue-invalid-config",
			Invoke:   createQueueConfig(&taskspb.RateLimits{MaxConcurrentDispatches: 5001}, nil),
			Teardown: deleteQueue,
		},
		{
			Name: "queue/create/max-attempts-below-minus-one", RPC: "CreateQueue", Category: "queue-invalid-config",
			Invoke:   createQueueConfig(nil, &taskspb.RetryConfig{MaxAttempts: -2}),
			Teardown: deleteQueue,
		},
		{
			Name: "queue/create/max-doublings-negative", RPC: "CreateQueue", Category: "queue-invalid-config",
			Invoke:   createQueueConfig(nil, &taskspb.RetryConfig{MaxDoublings: -1}),
			Teardown: deleteQueue,
		},
		{
			Name: "queue/create/min-backoff-negative", RPC: "CreateQueue", Category: "queue-invalid-config",
			Invoke:   createQueueConfig(nil, &taskspb.RetryConfig{MinBackoff: durationpb.New(-time.Second)}),
			Teardown: deleteQueue,
		},
		{
			Name: "queue/create/backoff-order", RPC: "CreateQueue", Category: "queue-invalid-config",
			Invoke: createQueueConfig(nil, &taskspb.RetryConfig{
				MinBackoff: durationpb.New(10 * time.Second),
				MaxBackoff: durationpb.New(5 * time.Second),
			}),
			Teardown: deleteQueue,
		},

		// --- Task lifecycle, inside a real queue ---
		{
			Name: "task/create/queue-not-found", RPC: "CreateTask", Category: "queue-not-found",
			Invoke: createTask,
		},
		{
			Name: "task/create/queue-recently-deleted", RPC: "CreateTask", Category: "queue-recently-deleted",
			Setup: func(ctx context.Context, c *Client, p Params) error {
				if err := createQueue(ctx, c, p); err != nil {
					return err
				}
				return deleteQueue(ctx, c, p)
			},
			Invoke: createTask,
		},
		{
			Name: "task/create/already-exists", RPC: "CreateTask", Category: "task-already-exists",
			Setup: func(ctx context.Context, c *Client, p Params) error {
				if err := createQueue(ctx, c, p); err != nil {
					return err
				}
				return createTask(ctx, c, p)
			},
			Invoke:   createTask,
			Teardown: deleteQueue,
		},
		{
			Name: "task/create/queue-mismatch", RPC: "CreateTask", Category: "task-queue-mismatch",
			Setup: createQueue,
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				// Task name references a different queue than the parent.
				other := p
				other.QueueID = p.QueueID + "-other"
				_, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
					Parent: p.QueuePath(),
					Task:   httpTask(other.TaskPath()),
				})
				return err
			},
			Teardown: deleteQueue,
		},
		{
			Name: "task/create/invalid-name", RPC: "CreateTask", Category: "task-invalid-name",
			Setup: createQueue,
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				_, err := c.CreateTask(ctx, &taskspb.CreateTaskRequest{
					Parent: p.QueuePath(),
					Task:   httpTask(p.QueuePath() + "/tasks/not a valid id"),
				})
				return err
			},
			Teardown: deleteQueue,
		},
		// --- CreateTask URL validation ---
		// Probes whether Cloud Tasks rejects a malformed HttpRequest.Url at
		// create time (and with what code/message/details), or accepts it and
		// only fails at dispatch. Together these pin down the validation rule
		// the emulator should enforce so a doomed task never enters a queue.
		{
			Name: "task/create/invalid-url-empty", RPC: "CreateTask", Category: "task-invalid-url",
			Setup:    createQueue,
			Invoke:   createTaskURL(""),
			Teardown: deleteQueue,
		},
		{
			Name: "task/create/invalid-url-no-scheme", RPC: "CreateTask", Category: "task-invalid-url",
			Setup:    createQueue,
			Invoke:   createTaskURL("example.com/path"),
			Teardown: deleteQueue,
		},
		{
			Name: "task/create/invalid-url-non-http-scheme", RPC: "CreateTask", Category: "task-invalid-url",
			Setup:    createQueue,
			Invoke:   createTaskURL("ftp://example.com/"),
			Teardown: deleteQueue,
		},
		{
			// Invalid percent-escape: unparseable by Go's url.Parse, the input
			// that trips dispatch.go's build-request path.
			Name: "task/create/invalid-url-bad-escape", RPC: "CreateTask", Category: "task-invalid-url",
			Setup:    createQueue,
			Invoke:   createTaskURL("http://example.com/%zz"),
			Teardown: deleteQueue,
		},
		// --- CreateTask configuration validation ---
		// Probes how Cloud Tasks rejects out-of-range task values at create
		// time (code and message): dispatch_deadline must be in [15s, 30m] for
		// HTTP targets and [15s, 24h15s] for App Engine targets, and
		// schedule_time may be at most 30 days in the future. The one-hour
		// App Engine deadline case is expected to record OK - it is legal
		// there and illegal for HTTP, pinning the per-family split. Its
		// schedule time is pushed out an hour so the created task never
		// actually dispatches during the run.
		{
			Name: "task/create/deadline-too-short", RPC: "CreateTask", Category: "task-invalid-config",
			Setup: createQueue,
			Invoke: createTaskMod(httpTask, func(t *taskspb.Task) {
				t.DispatchDeadline = durationpb.New(5 * time.Second)
			}),
			Teardown: deleteQueue,
		},
		{
			Name: "task/create/deadline-too-long", RPC: "CreateTask", Category: "task-invalid-config",
			Setup: createQueue,
			Invoke: createTaskMod(httpTask, func(t *taskspb.Task) {
				t.DispatchDeadline = durationpb.New(31 * time.Minute)
			}),
			Teardown: deleteQueue,
		},
		{
			Name: "task/create/appengine-deadline-too-long", RPC: "CreateTask", Category: "task-invalid-config",
			Setup: createQueue,
			Invoke: createTaskMod(appEngineTask, func(t *taskspb.Task) {
				t.DispatchDeadline = durationpb.New(25 * time.Hour)
			}),
			Teardown: deleteQueue,
		},
		{
			Name: "task/create/appengine-deadline-one-hour", RPC: "CreateTask", Category: "task-invalid-config",
			Setup: createQueue,
			Invoke: createTaskMod(appEngineTask, func(t *taskspb.Task) {
				t.DispatchDeadline = durationpb.New(time.Hour)
				t.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
			}),
			Teardown: deleteQueue,
		},
		{
			// The timestamp is a fixed far-future instant rather than a
			// now-relative one so every variant sends the same value and any
			// interpolation of it into the message stays stable.
			Name: "task/create/schedule-too-far", RPC: "CreateTask", Category: "task-invalid-config",
			Setup: createQueue,
			Invoke: createTaskMod(httpTask, func(t *taskspb.Task) {
				t.ScheduleTime = timestamppb.New(time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC))
			}),
			Teardown: deleteQueue,
		},
		{
			Name: "task/get/not-found", RPC: "GetTask", Category: "task-not-found",
			Setup: createQueue,
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				_, err := c.GetTask(ctx, &taskspb.GetTaskRequest{Name: p.TaskPath()})
				return err
			},
			Teardown: deleteQueue,
		},
		{
			Name: "task/delete/not-found", RPC: "DeleteTask", Category: "task-not-found",
			Setup: createQueue,
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				return c.DeleteTask(ctx, &taskspb.DeleteTaskRequest{Name: p.TaskPath()})
			},
			Teardown: deleteQueue,
		},
		{
			Name: "task/run/not-found", RPC: "RunTask", Category: "task-not-found",
			Setup: createQueue,
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				_, err := c.RunTask(ctx, &taskspb.RunTaskRequest{Name: p.TaskPath()})
				return err
			},
			Teardown: deleteQueue,
		},
		{
			Name: "task/get/recently-deleted", RPC: "GetTask", Category: "task-recently-deleted",
			Setup: func(ctx context.Context, c *Client, p Params) error {
				if err := createQueue(ctx, c, p); err != nil {
					return err
				}
				if err := createTask(ctx, c, p); err != nil {
					return err
				}
				return c.DeleteTask(ctx, &taskspb.DeleteTaskRequest{Name: p.TaskPath()})
			},
			Invoke: func(ctx context.Context, c *Client, p Params) error {
				_, err := c.GetTask(ctx, &taskspb.GetTaskRequest{Name: p.TaskPath()})
				return err
			},
			Teardown: deleteQueue,
		},
	}
}
