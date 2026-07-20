// Package emulator embeds an in-process Cloud Tasks emulator for use in tests.
//
// It runs the emulator over an in-memory (bufconn) gRPC listener, so no TCP
// port is opened and no external process or container is required. Point the
// official Cloud Tasks client at it via NewClient or DialOptions and exercise
// your code exactly as it would run against real Cloud Tasks.
package emulator

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/maybe"
	"github.com/aertje/cloud-tasks-emulator/v2/internal/oidc"
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
// listener. The zero value is not usable; construct one with New, or with
// NewUnstarted followed by Start.
type Emulator struct {
	srv  *server.Server
	grpc *grpc.Server
	lis  *bufconn.Listener
	// logger is this package's diagnostic logger, tagged with a component
	// attribute so its records match the engine's.
	logger *slog.Logger
	// started guards against calling Start more than once.
	started bool
}

// Option configures an Emulator at construction time.
type Option func(*server.ServerOptions)

// WithHardResetOnPurgeQueue mirrors the binary's -hard-reset-on-purge-queue
// flag: PurgeQueue synchronously deletes tasks and releases their name handles.
func WithHardResetOnPurgeQueue(v bool) Option {
	return func(o *server.ServerOptions) { o.HardResetOnPurgeQueue = maybe.Some(v) }
}

// WithInsecureSkipTLSVerify mirrors the binary's -insecure-skip-tls-verify
// flag: task dispatch to HTTPS targets skips TLS certificate verification. It
// is intended for local development against self-signed certificates and has no
// production equivalent; leave it off unless you need it.
func WithInsecureSkipTLSVerify(v bool) Option {
	return func(o *server.ServerOptions) { o.InsecureSkipTLSVerify = maybe.Some(v) }
}

// WithAppEngineEmulatorHost mirrors the binary's -app-engine-emulator-host flag:
// App Engine target tasks route to the given base URL instead of the production
// https://<project>.appspot.com. It is intended for local development against an
// App Engine emulator and has no production equivalent. New and NewUnstarted
// panic if host is not a valid base URL, matching the binary's fail-fast
// behaviour at construction.
func WithAppEngineEmulatorHost(host string) Option {
	return func(o *server.ServerOptions) { o.AppEngineEmulatorHost = maybe.OfNonZero(host) }
}

// WithAppEngineRegionID mirrors the binary's -app-engine-region-id flag: it
// selects the App Engine region ID (e.g. "uc" for us-central1) used in the
// default appspot.com routing, producing the regional host format
// https://<project>.<region>.r.appspot.com that production Cloud Tasks emits.
// Leave it unset for the legacy https://<project>.appspot.com format. It is
// ignored when WithAppEngineEmulatorHost is also set.
func WithAppEngineRegionID(regionID string) Option {
	return func(o *server.ServerOptions) { o.AppEngineRegionID = maybe.OfNonZero(regionID) }
}

// WithOIDCSigningKey mirrors the binary's -openid-signing-key flag: tasks
// dispatched with an OIDC token are signed with the given PEM-encoded RSA private
// key instead of the baked-in development key. Supplying a known key lets tests
// validate dispatched tokens against its public half. The embedded emulator does
// not serve the OpenID discovery/JWKS HTTP endpoints (the binary's -openid-issuer
// flag has no embedded equivalent). WithOIDCSigningKey panics if privateKeyPEM
// does not parse as an RSA private key, matching the binary's fail-fast behaviour
// at construction.
func WithOIDCSigningKey(privateKeyPEM []byte) Option {
	cfg, err := oidc.NewConfig(privateKeyPEM)
	if err != nil {
		panic(fmt.Errorf("emulator.WithOIDCSigningKey: %w", err))
	}
	return func(o *server.ServerOptions) { o.OIDC = maybe.Some(*cfg) }
}

// WithLogger routes the emulator's queue-lifecycle and dispatch diagnostics to
// the given logger. Emulator log records carry a component="cloud-tasks-emulator"
// attribute so they can be filtered from the host application's own output. When
// unset, the emulator logs through slog.Default(); pass
// slog.New(slog.DiscardHandler) to silence it entirely in tests.
func WithLogger(l *slog.Logger) Option {
	return func(o *server.ServerOptions) { o.Logger = l }
}

// New constructs an in-process emulator and starts it serving, ready for
// clients. The caller must call Close to stop the server and release its
// resources.
func New(opts ...Option) *Emulator {
	e := NewUnstarted(opts...)
	e.Start()
	return e
}

// NewUnstarted constructs an in-process emulator and its in-memory listener
// without yet serving on it. Call Start to begin serving and Close to release
// resources. ClientOptions may be called on the returned Emulator before Start;
// a client dial blocks until Start accepts connections.
func NewUnstarted(opts ...Option) *Emulator {
	var so server.ServerOptions
	for _, opt := range opts {
		opt(&so)
	}
	s := server.NewServer(so)

	// Resolve the logger from the same option the engine reads, so both share it.
	logger := so.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "cloud-tasks-emulator")

	lis := bufconn.Listen(bufSize)
	gs := grpc.NewServer()
	taskspb.RegisterCloudTasksServer(gs, s)

	return &Emulator{srv: s, grpc: gs, lis: lis, logger: logger}
}

// Start begins serving on the in-memory listener in a background goroutine. The
// caller must call Close to stop the server and release its resources. Start
// panics if called more than once.
func (e *Emulator) Start() {
	if e.started {
		panic("emulator: Start called more than once")
	}
	e.started = true

	// Serve returns nil when Close stops the server via GracefulStop; any
	// other error means the in-memory listener failed, which is unrecoverable
	// here, so log it and let the goroutine exit.
	go func() {
		if err := e.grpc.Serve(e.lis); err != nil {
			e.logger.Error("in-process gRPC server stopped", "err", err)
		}
	}()
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
