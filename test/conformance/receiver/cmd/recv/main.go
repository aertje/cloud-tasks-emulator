// Command recv serves the dispatch-header receiver. Deployed to App Engine (see
// ../../app.yaml) it records the headers real Cloud Tasks attaches to task
// dispatches; the same handler is imported directly by the conformance harness
// to run hermetically in tests.
//
// It listens on $PORT (set by the App Engine runtime, default 8080).
package main

import (
	"log"
	"net/http"
	"os"

	"github.com/aertje/cloud-tasks-emulator/test/conformance/receiver"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("receiver listening on :%s", port)
	if err := http.ListenAndServe(":"+port, receiver.NewHandler()); err != nil {
		log.Fatalf("receiver: %v", err)
	}
}
