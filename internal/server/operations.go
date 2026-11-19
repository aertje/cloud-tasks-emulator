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

var (
	// errOperationNotFound reports a GetOperation for a name the registry never
	// stored. It is mapped to NotFound by mapErr.
	errOperationNotFound = errors.New("operation not found")

	// errDuplicateRequestID reports a BatchCreateTasks request whose request ID
	// an earlier request for the same queue already used. Real Cloud Tasks
	// rejects it with InvalidArgument rather than returning the original
	// operation (see conformance case batch/request-id-replay); it is mapped in
	// mapErrForBatchCreateTasks, whose message interpolates the ID.
	errDuplicateRequestID = errors.New("duplicate request id")
)

// operationRegistry records the long-running operations the server returns, so
// GetOperation can serve them by name, and the request IDs those requests
// used, so a request reusing one is rejected. Long-running operations are an
// API-level concept the engine does not model, so the registry lives here,
// holding the operations in their proto form. Nothing is ever evicted; the
// emulator is short-lived and each entry is small, so the registry stays
// bounded in practice.
type operationRegistry struct {
	mu         sync.Mutex
	byName     map[string]*longrunningpb.Operation
	requestIDs map[operationRequestKey]bool
}

// operationRequestKey scopes a client-supplied request ID to the queue it was
// sent for. Real Cloud Tasks' scope is not recorded; per queue is the narrower
// assumption.
type operationRequestKey struct {
	parent    string
	requestID string
}

func newOperationRegistry() *operationRegistry {
	return &operationRegistry{
		byName:     map[string]*longrunningpb.Operation{},
		requestIDs: map[operationRequestKey]bool{},
	}
}

// reserveRequestID claims requestID for parent, or reports
// errDuplicateRequestID if an earlier request claimed it. An empty requestID is
// never claimed. The claim is made before the request runs, so of several
// concurrent requests with the same ID exactly one proceeds.
func (r *operationRegistry) reserveRequestID(parent, requestID string) error {
	if requestID == "" {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := operationRequestKey{parent: parent, requestID: requestID}
	if r.requestIDs[key] {
		return errDuplicateRequestID
	}
	r.requestIDs[key] = true
	return nil
}

// releaseRequestID frees a request ID whose request was rejected, so a retry
// with the same ID can run. Whether real Cloud Tasks keeps the ID of a
// rejected request is not recorded.
func (r *operationRegistry) releaseRequestID(parent, requestID string) {
	if requestID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.requestIDs, operationRequestKey{parent: parent, requestID: requestID})
}

// add stores an operation so GetOperation can serve it by name.
func (r *operationRegistry) add(op *longrunningpb.Operation) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.byName[op.GetName()] = op
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

// newOperationName builds an operation resource name under the location of
// the given queue: projects/<P>/locations/<L>/operations/<ID>. Real Cloud
// Tasks' operation IDs are hexadecimal (see the batch/* observations in
// conformance/golden/batch.json); the emulator's is a random 64-bit value.
func newOperationName(queueName string) string {
	location := queueName
	if i := strings.LastIndex(queueName, "/queues/"); i >= 0 {
		location = queueName[:i]
	}
	return location + "/operations/" + strconv.FormatUint(rand.Uint64(), 16)
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
