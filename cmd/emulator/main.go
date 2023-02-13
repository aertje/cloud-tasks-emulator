package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"regexp"

	tasks "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/aertje/cloud-tasks-emulator/pkg/emulator"
	"google.golang.org/grpc"
)

func main() {
	host := flag.String("host", "localhost", "The host name")
	port := flag.String("port", "8123", "The port")
	// openidIssuer := flag.String("openid-issuer", "", "URL to serve the OpenID configuration on, if required")

	var initialQueues arrayFlags
	flag.Var(&initialQueues, "queue", "Queue(s) to create on startup (repeat as required)")

	resetOnPurge := flag.Bool("reset-on-purge", false, "Perform a synchronous reset on purge queue (differs from production)")

	flag.Parse()

	// if *openidIssuer != "" {
	// 	srv, err := configureOpenIdIssuer(*openidIssuer)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer srv.Shutdown(context.Background())
	// }

	lis, err := net.Listen("tcp", fmt.Sprintf("%v:%v", *host, *port))
	if err != nil {
		panic(err)
	}

	print(fmt.Sprintf("Starting cloud tasks emulator, listening on %v:%v\n", *host, *port))

	grpcServer := grpc.NewServer()
	emulatorServer := emulator.NewServer()
	emulatorServer.Options.HardResetOnPurgeQueue = *resetOnPurge
	tasks.RegisterCloudTasksServer(grpcServer, emulatorServer)

	for i := 0; i < len(initialQueues); i++ {
		createInitialQueue(emulatorServer, initialQueues[i])
	}

	grpcServer.Serve(lis)
}

// Creates an initial queue on the emulator
func createInitialQueue(emulatorServer *emulator.Server, name string) {
	print(fmt.Sprintf("Creating initial queue %s\n", name))

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
