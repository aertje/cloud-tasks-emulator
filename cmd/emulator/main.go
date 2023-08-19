package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/aertje/cloud-tasks-emulator/pkg/cloudtasks/emulator"
	"github.com/aertje/cloud-tasks-emulator/pkg/cloudtasks/server"
	"github.com/aertje/cloud-tasks-emulator/pkg/oidc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

func main() {
	var initialQueues arrayFlags

	emulatorAddress := flag.String("address", "0.0.0.0:8123", "The address (host and port) the emulator will listen on")
	resetOnPurge := flag.Bool("reset-on-purge", false, "Perform a synchronous reset on purge queue (differs from production)")
	flag.Var(&initialQueues, "queue", "Queue(s) to create on startup (repeat as required)")

	oidcEnable := flag.Bool("oidc", false, "Enable OIDC server")
	oidcAddress := flag.String("oidc-address", "0.0.0.0:80", "The address (host and port) the OIDC server will listen on")
	oidcIssuer := flag.String("oidc-issuer", "http://localhost", "The issuer URL for OIDC")

	flag.Parse()

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals)
	defer signal.Stop(osSignals)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	defer g.Wait()

	g.Go(func() error {
		err := startEmulatorServer(ctx, *emulatorAddress, *resetOnPurge, initialQueues)
		if err != nil {
			return fmt.Errorf("unable to run emulator server: %w", err)
		}
		return nil
	})

	if *oidcEnable {
		g.Go(func() error {
			err := startOIDCServer(ctx, *oidcAddress, *oidcIssuer)
			if err != nil {
				return fmt.Errorf("unable to run OIDC server: %w", err)
			}
			return nil
		})
	}

	select {
	case s := <-osSignals:
		log.Printf("Signal received: %v", s.String())
		cancel()
	case <-ctx.Done():
		err := g.Wait()
		if err != nil {
			log.Printf("Error received: %v", err)
		}
	}
}

func startEmulatorServer(ctx context.Context, address string, resetOnPurge bool, initialQueues []string) error {
	log.Printf("Starting cloud tasks emulator server, listening on %v...", address)

	httpClient := &http.Client{}
	oidcTokenCreator := oidc.NewOIDCTokenCreator()
	em := emulator.NewEmulator(oidcTokenCreator, httpClient, resetOnPurge)
	handler := server.NewHandler(em)

	grpcServer := grpc.NewServer()
	taskspb.RegisterCloudTasksServer(grpcServer, &handler)

	err := createQueues(ctx, em, initialQueues)
	if err != nil {
		return fmt.Errorf("unable to create queues: %w", err)
	}

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("error listening on address: %w", err)
	}

	// Run the server in a separate goroutine so we can respond to a context
	// cancellation. The actual terminating of the server is done using a
	// deferred server.Stop
	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		err := grpcServer.Serve(lis)
		if err != nil {
			errChan <- fmt.Errorf("error running GRPC server: %w", err)
		}
	}()

	defer func() {
		log.Printf("Shutting down emulator server...")
		// em.Stop()
		grpcServer.Stop() // Forceful close
	}()

	// Block until context cancellation or server error
	select {
	case <-ctx.Done():
	case e := <-errChan:
		return e
	}

	return nil
}

func startOIDCServer(ctx context.Context, address, issuer string) error {
	log.Printf("Starting OIDC server, listening on %v...", address)

	oidcServer := oidc.NewServer(issuer)
	server := &http.Server{
		Addr:    address,
		Handler: oidcServer,
	}

	// Run the server in a separate goroutine so we can respond to a context
	// cancellation. The actual terminating of the server is done using a
	// deferred server.Close
	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		err := server.ListenAndServe()
		if err != nil {
			errChan <- fmt.Errorf("error running HTTP server: %w", err)
		}
	}()

	defer func() {
		log.Printf("Shutting down OIDC server...")
		server.Close() // Forceful close, ignore error from closing
	}()

	// Block until context cancellation or server error
	select {
	case <-ctx.Done():
	case e := <-errChan:
		return e
	}

	return nil
}

// Creates initial queues on the emulator
func createQueues(ctx context.Context, em *emulator.Emulator, queueNames []string) error {
	for _, qn := range queueNames {
		_, err := em.CreateQueue(qn, emulator.NewDefaultQueueConfig())

		if err != nil {
			return err
		}
	}

	return nil
}
