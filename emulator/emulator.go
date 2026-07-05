// Package emulator embeds an in-process Cloud Tasks emulator for use in tests.
//
// It runs the emulator over an in-memory (bufconn) gRPC listener, so no TCP
// port is opened and no external process or container is required. Point the
// official Cloud Tasks client at it via NewClient or DialOptions and exercise
// your code exactly as it would run against real Cloud Tasks.
package emulator

import (
	"context"
	"log"
	"net"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/server"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// bufSize is the in-memory listener buffer. 1 MiB comfortably holds the small
// control-plane messages the emulator exchanges.
const bufSize = 1024 * 1024

// Emulator is an in-process Cloud Tasks emulator backed by an in-memory gRPC
// listener. The zero value is not usable; construct one with Start.
type Emulator struct {
	srv  *server.Server
	grpc *grpc.Server
	lis  *bufconn.Listener
}

// Option configures an Emulator at construction time.
type Option func(*server.ServerOptions)

// WithHardResetOnPurgeQueue mirrors the binary's -hard-reset-on-purge-queue
// flag: PurgeQueue synchronously deletes tasks and releases their name handles.
func WithHardResetOnPurgeQueue(v bool) Option {
	return func(o *server.ServerOptions) { o.HardResetOnPurgeQueue = v }
}

// Start launches an in-process emulator serving on an in-memory listener. The
// caller must call Close to stop the server and release its resources.
func Start(opts ...Option) *Emulator {
	s := server.NewServer()
	for _, opt := range opts {
		opt(&s.Options)
	}

	lis := bufconn.Listen(bufSize)
	gs := grpc.NewServer()
	taskspb.RegisterCloudTasksServer(gs, s)

	// Serve returns nil when Close stops the server via GracefulStop; any
	// other error means the in-memory listener failed, which is unrecoverable
	// here, so log it and let the goroutine exit.
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Printf("cloud-tasks-emulator: in-process gRPC server stopped: %v", err)
		}
	}()

	return &Emulator{srv: s, grpc: gs, lis: lis}
}

// ClientOptions returns the options that point a standard Cloud Tasks client at
// this in-process emulator, with no transport security and no authentication.
// Pass them to cloudtasks.NewClient:
//
//	client, err := cloudtasks.NewClient(ctx, em.ClientOptions()...)
func (e *Emulator) ClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithContextDialer(
			func(ctx context.Context, _ string) (net.Conn, error) {
				return e.lis.DialContext(ctx)
			})),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}

// Close stops the gRPC server and the emulator engine, ensuring no background
// goroutine outlives the Emulator. It is safe to call once.
func (e *Emulator) Close() {
	e.grpc.GracefulStop()
	e.srv.Stop()
}
