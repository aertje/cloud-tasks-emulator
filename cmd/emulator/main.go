package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	tasks "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"

	"github.com/aertje/cloud-tasks-emulator/internal/oidc"
	"github.com/aertje/cloud-tasks-emulator/internal/server"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// arrayFlags used for parsing list of potentially repeated flags e.g. -queue $Q1 -queue $Q2
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
	fmt.Printf("Creating initial queue %s\n", name)

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
	var initialQueues arrayFlags

	host := flag.String("host", "localhost", "The host name")
	port := flag.String("port", "8123", "The port")
	openidIssuer := flag.String("openid-issuer", "", "URL to serve the OpenID configuration on, if required")
	hardResetOnPurgeQueue := flag.Bool("hard-reset-on-purge-queue", false, "Set to force the 'Purge Queue' call to perform a hard reset of all state (differs from production)")

	flag.Var(&initialQueues, "queue", "A queue to create on startup (repeat as required)")

	flag.Parse()

	emulatorServer := server.NewServer()
	emulatorServer.Options.HardResetOnPurgeQueue = *hardResetOnPurgeQueue

	var openIDServer *http.Server
	if *openidIssuer != "" {
		srv, err := oidc.ConfigureIssuer(*openidIssuer, emulatorServer.Options.OIDC)
		if err != nil {
			panic(err)
		}
		openIDServer = srv
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("%v:%v", *host, *port))
	if err != nil {
		panic(err)
	}

	fmt.Printf("Starting cloud tasks emulator, listening on %v:%v\n", *host, *port)

	grpcServer := grpc.NewServer()
	tasks.RegisterCloudTasksServer(grpcServer, emulatorServer)

	for _, queueName := range initialQueues {
		createInitialQueue(emulatorServer, queueName)
	}

	if err := serve(grpcServer, lis, openIDServer); err != nil {
		panic(err)
	}
}

// serve runs the gRPC server and the optional OpenID HTTP server until one of
// them exits or a SIGINT/SIGTERM arrives, then shuts the other down too. It
// returns the first non-nil error from any server.
func serve(grpcServer *grpc.Server, lis net.Listener, openIDServer *http.Server) error {
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
		grpcServer.GracefulStop()
		if openIDServer != nil {
			if err := openIDServer.Shutdown(context.Background()); err != nil {
				return fmt.Errorf("OpenID server shutdown: %w", err)
			}
		}
		return nil
	})

	return group.Wait()
}
