// Package conformance captures the observable error behaviour of Cloud Tasks
// (real or emulated) by firing a fixed battery of deliberately-malformed and
// edge-case RPCs at a target and recording the resulting gRPC status.
//
// The same battery runs against the real API (to produce a committed golden
// snapshot) and against the emulator (to validate it). Because Cloud Tasks
// interpolates request values into some error messages, results are normalized
// into templates with placeholders before comparison - see normalize.go.
package conformance

import "fmt"

// Params are the request-shaping values for one invocation of a case. The
// runner generates several variants per case with differing QueueID/TaskID so
// normalize.go can tell static message text from interpolated request values.
type Params struct {
	Project  string
	Location string
	QueueID  string
	TaskID   string
}

// Parent is the location resource name (CreateQueue parent, ListQueues parent).
func (p Params) Parent() string {
	return fmt.Sprintf("projects/%s/locations/%s", p.Project, p.Location)
}

// QueuePath is the fully-qualified queue resource name.
func (p Params) QueuePath() string {
	return fmt.Sprintf("%s/queues/%s", p.Parent(), p.QueueID)
}

// TaskPath is the fully-qualified task resource name.
func (p Params) TaskPath() string {
	return fmt.Sprintf("%s/tasks/%s", p.QueuePath(), p.TaskID)
}
