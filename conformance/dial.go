package conformance

import (
	"context"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewRealClient connects to the production Cloud Tasks API using Application
// Default Credentials (run `gcloud auth application-default login` first). The
// official client sets the routing headers and per-method deadlines itself, so
// what we record is exactly what a real caller using the SDK would see.
func NewRealClient(ctx context.Context) (*cloudtasks.Client, error) {
	return cloudtasks.NewClient(ctx)
}

// NewEmulatorClient points the same official client at a running emulator over
// an insecure local channel, with no credentials.
func NewEmulatorClient(ctx context.Context, addr string) (*cloudtasks.Client, error) {
	return cloudtasks.NewClient(ctx,
		option.WithEndpoint(addr),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
}
