// Command record fires the conformance battery at a target (real Cloud Tasks or
// a running emulator) and writes the captured results as JSON.
//
// Run from inside the conformance/ module directory.
//
// Record the golden snapshot from the real API:
//
//	gcloud auth application-default login
//	go run ./cmd/record \
//	  -target=real -project=$PROJECT -location=us-central1 \
//	  -out=golden/realcloud.json
//
// Dump the emulator's current behaviour for ad-hoc comparison:
//
//	go run ./cmd/emulator -port 8123 &   # from repo root
//	go run ./cmd/record -target=emulator -addr=localhost:8123 -out=/tmp/emu.json
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/aertje/cloud-tasks-emulator/conformance"
)

func main() {
	target := flag.String("target", "emulator", "real | emulator")
	project := flag.String("project", "", "GCP project id (real) or placeholder (emulator)")
	location := flag.String("location", "us-central1", "location id")
	addr := flag.String("addr", "localhost:8123", "emulator address (target=emulator)")
	out := flag.String("out", "", "output JSON path (default: stdout)")
	variants := flag.Int("variants", 3, "differing-input variants per case")
	flag.Parse()

	if *project == "" {
		// The emulator ignores the project, but resource names still need one.
		*project = "conformance-test"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	client, err := dial(ctx, *target, *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", *target, err)
		os.Exit(1)
	}
	defer func() {
		if err := client.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close client: %v\n", err)
		}
	}()

	opts := conformance.RunOptions{
		Project:  *project,
		Location: *location,
		Prefix:   fmt.Sprintf("cte-conf-%d", rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1<<31)),
		Variants: *variants,
	}

	fmt.Fprintf(os.Stderr, "recording %s (project=%s location=%s prefix=%s)...\n",
		*target, *project, *location, opts.Prefix)
	results := conformance.Run(ctx, client, opts)

	unstable := 0
	for _, r := range results {
		if !r.Stable {
			unstable++
			fmt.Fprintf(os.Stderr, "  UNSTABLE %s: variants disagreed after normalization (unmodeled interpolation?)\n", r.Name)
		}
	}

	if *out == "" {
		if err := conformance.Save("/dev/stdout", results); err != nil {
			fmt.Fprintf(os.Stderr, "write: %v\n", err)
			os.Exit(1)
		}
	} else if err := conformance.Save(*out, results); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", *out, err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "done: %d cases, %d unstable\n", len(results), unstable)
}

func dial(ctx context.Context, target, addr string) (*conformance.Client, error) {
	switch target {
	case "real":
		return conformance.NewRealClient(ctx)
	case "emulator":
		return conformance.NewEmulatorClient(ctx, addr)
	default:
		return nil, fmt.Errorf("unknown target %q (want real|emulator)", target)
	}
}
