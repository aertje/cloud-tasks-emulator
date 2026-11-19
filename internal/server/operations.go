package server

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
)

// errOperationNotFound reports a GetOperation for a name the registry never
// stored. It is mapped to NotFound by mapErr.
var errOperationNotFound = errors.New("operation not found")

// operationRegistry records the long-running operations the server returns, so
// GetOperation can serve them by name and a retried request carrying the same
// request ID gets the original operation instead of repeating its work.
// Long-running operations are an API-level concept the engine does not model,
// so the registry lives here, holding the operations in their proto form.
// Operations are never evicted; the emulator is short-lived and each one is
// small, so the registry stays bounded in practice.
type operationRegistry struct {
	mu        sync.Mutex
	byName    map[string]*longrunningpb.Operation
	byRequest map[operationRequestKey]*pendingOperation
}

// operationRequestKey scopes a client-supplied request ID to the resource it
// was sent for, so the same ID reused against another queue starts a new
// operation.
type operationRequestKey struct {
	parent    string
	requestID string
}

// pendingOperation is one request's operation, from the moment the registry
// hands it to the request that will run it until that request finishes. Other
// requests carrying the same request ID wait on done instead of repeating the
// work.
type pendingOperation struct {
	key  operationRequestKey
	done chan struct{}
	op   *longrunningpb.Operation
	err  error
}

func newOperationRegistry() *operationRegistry {
	return &operationRegistry{
		byName:    map[string]*longrunningpb.Operation{},
		byRequest: map[operationRequestKey]*pendingOperation{},
	}
}

// start returns the operation for parent and requestID. It reports started
// when the caller must run the work and call finish; otherwise the returned
// operation belongs to an earlier request with the same request ID, and the
// caller should wait for it. An empty requestID always starts a new operation.
// The registry lock is held only to look up and reserve the request ID, never
// while the work runs.
func (r *operationRegistry) start(parent, requestID string) (pending *pendingOperation, started bool) {
	key := operationRequestKey{parent: parent, requestID: requestID}
	pending = &pendingOperation{key: key, done: make(chan struct{})}
	if requestID == "" {
		return pending, true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.byRequest[key]; ok {
		return existing, false
	}
	r.byRequest[key] = pending
	return pending, true
}

// finish records the outcome of an operation returned by start and releases
// every request waiting on it. A failed request is forgotten, so a later retry
// with the same request ID runs again instead of replaying the error.
func (r *operationRegistry) finish(pending *pendingOperation, op *longrunningpb.Operation, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	pending.op, pending.err = op, err
	if err != nil {
		if r.byRequest[pending.key] == pending {
			delete(r.byRequest, pending.key)
		}
	} else {
		r.byName[op.GetName()] = op
	}
	close(pending.done)
}

// get returns the named operation, or errOperationNotFound.
func (r *operationRegistry) get(name string) (*longrunningpb.Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	op, ok := r.byName[name]
	if !ok {
		return nil, errOperationNotFound
	}
	return op, nil
}

// wait blocks until the request running the operation finishes, or ctx ends.
func (p *pendingOperation) wait(ctx context.Context) (*longrunningpb.Operation, error) {
	select {
	case <-p.done:
		return p.op, p.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// newOperationName builds an operation resource name under the location of
// the given queue: projects/<P>/locations/<L>/operations/<ID>. The ID is a
// random decimal number, generated the same way as auto-generated task IDs.
func newOperationName(queueName string) string {
	location := queueName
	if i := strings.LastIndex(queueName, "/queues/"); i >= 0 {
		location = queueName[:i]
	}
	return location + "/operations/" + strconv.FormatUint(rand.Uint64(), 10)
}

// operationsServer is the gRPC google.longrunning.Operations implementation. It
// serves GetOperation from the registry; the other Operations RPCs are not
// implemented.
type operationsServer struct {
	longrunningpb.UnimplementedOperationsServer

	operations *operationRegistry
}

// GetOperation returns a previously returned operation by name.
func (o *operationsServer) GetOperation(ctx context.Context, in *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	op, err := o.operations.get(in.GetName())
	if err != nil {
		return nil, mapErr(err)
	}
	return op, nil
}
