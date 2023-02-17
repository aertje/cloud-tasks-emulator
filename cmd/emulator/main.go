package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"regexp"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/aertje/cloud-tasks-emulator/pkg/emulator"
	"github.com/aertje/cloud-tasks-emulator/pkg/oidc"
	"google.golang.org/grpc"
)

func main() {
	var initialQueues arrayFlags

	emulatorAddress := flag.String("host", "0.0.0.0:8123", "The address (host and port) the emulator will listen on")
	resetOnPurge := flag.Bool("reset-on-purge", false, "Perform a synchronous reset on purge queue (differs from production)")
	flag.Var(&initialQueues, "queue", "Queue(s) to create on startup (repeat as required)")

	oidcAddress := flag.String("oidc-host", "0.0.0.0:80", "The address (host and port) the OIDC server will listen on")
	oidcIssuer := flag.String("oidc-issuer", "http://localhost", "The issuer URL for OIDC")

	flag.Parse()

	// if *openidIssuer != "" {
	// 	srv, err := configureOpenIdIssuer(*openidIssuer)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer srv.Shutdown(context.Background())
	// }

	startEmulatorServer(*emulatorAddress, *resetOnPurge, initialQueues)
	startOIDCServer(*oidcAddress, *oidcIssuer)
}

func startEmulatorServer(address string, resetOnPurge bool, initialQueues []string) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}

	log.Printf("Starting cloud tasks emulator, listening on %v", address)

	grpcServer := grpc.NewServer()
	emulatorServer := emulator.NewServer(resetOnPurge)
	taskspb.RegisterCloudTasksServer(grpcServer, emulatorServer)

	for _, q := range initialQueues {
		err := createInitialQueue(emulatorServer, q)
		if err != nil {
			panic(err)
		}
	}

	go grpcServer.Serve(lis)

	defer grpcServer.GracefulStop()
}

// Creates an initial queue on the emulator
func createInitialQueue(emulatorServer *emulator.Server, queueName string) error {
	log.Printf("Creating initial queue %s", queueName)

	r, err := regexp.Compile("/queues/[A-Za-z0-9-]+$")
	if err != nil {
		return err
	}

	parent := r.ReplaceAllString(queueName, "")

	queue := &taskspb.Queue{Name: queueName}
	req := &taskspb.CreateQueueRequest{
		Parent: parent,
		Queue:  queue,
	}

	_, err = emulatorServer.CreateQueue(context.TODO(), req)

	return err
}

// TODO: error handling
// TODO: contexts

func startOIDCServer(address, issuer string) {
	// TODO: pass in issuer into server
	server := &http.Server{
		Addr:    address,
		Handler: oidc.GetHandler(),
	}
	go server.ListenAndServe()

	defer server.Shutdown(context.Background())
}
