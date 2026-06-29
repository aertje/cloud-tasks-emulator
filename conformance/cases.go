package conformance

import (
	"context"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
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

func createQueue(ctx context.Context, c *Client, p Params) error {
	_, err := c.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: p.Parent(),
		Queue:  &taskspb.Queue{Name: p.QueuePath()},
	})
	return err
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
