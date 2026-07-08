package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	tasks "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/oidc"
	"github.com/aertje/cloud-tasks-emulator/v2/internal/server"

	"github.com/lmittmann/tint"
	"github.com/peterbourgon/ff/v3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// arrayFlags used for parsing list of potentially repeated flags e.g. -initial-queue $Q1 -initial-queue $Q2
type arrayFlags []string

func (i *arrayFlags) String() string {
	return strings.Join(*i, ", ")
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

// Creates an initial queue on the emulator
func createInitialQueue(emulatorServer *server.Server, name string) {
	slog.Info("creating initial queue", "queue", name)

	r := regexp.MustCompile("/queues/[A-Za-z0-9-]+$")
	parentName := r.ReplaceAllString(name, "")

	queue := &tasks.Queue{Name: name}
	req := &tasks.CreateQueueRequest{
		Parent: parentName,
		Queue:  queue,
	}

	_, err := emulatorServer.CreateQueue(context.TODO(), req)
	if err != nil {
		panic(err)
	}
}

func main() {
	// The emulator is a development tool run in a terminal, so default to
	// tint's colored, human-readable handler on stderr. Library consumers that
	// import the emulator package configure their own logger instead.
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		TimeFormat: "2006-01-02 15:04:05.000",
	})))

	var initialQueues arrayFlags

	fs := flag.NewFlagSet("emulator", flag.ExitOnError)

	host := fs.String("host", "localhost", "The host name")
	port := fs.String("port", "8123", "The port")
	openidIssuer := fs.String("openid-issuer", "", "URL to serve the OpenID configuration on, if required")
	hardResetOnPurgeQueue := fs.Bool("hard-reset-on-purge-queue", false, "Set to force the 'Purge Queue' call to perform a hard reset of all state (differs from production)")
	insecureSkipTLSVerify := fs.Bool("insecure-skip-tls-verify", false, "Skip TLS certificate verification when dispatching to HTTPS targets (development only, e.g. self-signed certs)")

	fs.Var(&initialQueues, "initial-queue", "A queue to create on startup (repeat as required)")

	// Flags may also be set via env vars derived from the flag name, e.g.
	// -openid-issuer <- OPENID_ISSUER, -initial-queue <- INITIAL_QUEUE
	// (comma-separated for multiple). Explicit flags take precedence over env
	// vars.
	if err := ff.Parse(fs, os.Args[1:],
		ff.WithEnvVarNoPrefix(),
		ff.WithEnvVarSplit(","),
	); err != nil {
		panic(err)
	}

	oidcCfg := oidc.DefaultConfig()

	var openIDServer *http.Server
	if *openidIssuer != "" {
		srv, cfg, err := oidc.ConfigureIssuer(*openidIssuer, *oidcCfg)
		if err != nil {
			panic(err)
		}
		openIDServer = srv
		oidcCfg = &cfg
		slog.Info("serving OpenID configuration", "issuer", *openidIssuer, "addr", srv.Addr)
	}

	emulatorServer := server.NewServer(server.ServerOptions{
		HardResetOnPurgeQueue: *hardResetOnPurgeQueue,
		InsecureSkipTLSVerify: *insecureSkipTLSVerify,
		OIDC:                  oidcCfg,
	})

	lis, err := net.Listen("tcp", fmt.Sprintf("%v:%v", *host, *port))
	if err != nil {
		panic(err)
	}

	slog.Info("starting cloud tasks emulator", "host", *host, "port", *port)

	grpcServer := grpc.NewServer()
	tasks.RegisterCloudTasksServer(grpcServer, emulatorServer)

	for _, queueName := range initialQueues {
		createInitialQueue(emulatorServer, queueName)
	}

	if err := serve(grpcServer, lis, openIDServer, emulatorServer); err != nil {
		panic(err)
	}
}

// serve runs the gRPC server and the optional OpenID HTTP server until one of
// them exits or a SIGINT/SIGTERM arrives, then shuts the other down too. It
// also stops the emulator engine so no queue or dispatch goroutine outlives
// the process. It returns the first non-nil error from any server.
func serve(grpcServer *grpc.Server, lis net.Listener, openIDServer *http.Server, emulatorServer *server.Server) error {
	// Cancelled on signal, or by any server goroutine returning (via the deferred
	// cancel), so one server stopping brings the others down with it.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var group errgroup.Group

	group.Go(func() error {
		defer cancel()
		if err := grpcServer.Serve(lis); err != nil {
			return fmt.Errorf("gRPC server: %w", err)
		}
		return nil
	})

	if openIDServer != nil {
		group.Go(func() error {
			defer cancel()
			if err := openIDServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("OpenID server: %w", err)
			}
			return nil
		})
	}

	// Shutdown watcher: wakes on the first server exit or signal and stops the
	// rest, so group.Wait can return.
	group.Go(func() error {
		<-ctx.Done()
		slog.Info("shutting down cloud tasks emulator")
		// Stop accepting RPCs and let in-flight ones drain first, then tear down
		// the engine so its queue and dispatch goroutines don't outlive us.
		grpcServer.GracefulStop()
		emulatorServer.Stop()
		if openIDServer != nil {
			if err := openIDServer.Shutdown(context.Background()); err != nil {
				return fmt.Errorf("OpenID server shutdown: %w", err)
			}
		}
		slog.Info("cloud tasks emulator stopped")
		return nil
	})

	return group.Wait()
}
