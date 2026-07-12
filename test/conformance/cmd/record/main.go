// Command record fires the conformance battery at a target (real Cloud Tasks or
// a running emulator) and writes the captured results as JSON.
//
// Run from inside the conformance/ module directory.
//
// Record the error-battery golden from the real API:
//
//	gcloud auth application-default login
//	go run ./cmd/record \
//	  -target=real -project=$PROJECT -location=us-central1 \
//	  -out=golden/errors.json
//
// Dump the emulator's current behaviour for ad-hoc comparison:
//
//	go run ./cmd/emulator -port 8123 &   # from repo root
//	go run ./cmd/record -target=emulator -addr=localhost:8123 -out=/tmp/emu.json
//
// Record the happy-path golden (needs cloudtasks.tasks.fullView for the FULL view):
//
//	go run ./cmd/record \
//	  -target=real -kind=happypath -project=$PROJECT -location=us-central1 \
//	  -out=golden/happypath.json
//
// Record the dispatch-headers golden (needs a deployed receiver - see test/conformance/receiver):
//
//	go run ./cmd/record -target=real -kind=dispatch \
//	  -project=$PROJECT -location=us-central1 \
//	  -receiver-url=https://$PROJECT.appspot.com -out=golden/dispatch.json
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/aertje/cloud-tasks-emulator/test/conformance"
)

func main() {
	target := flag.String("target", "emulator", "real | emulator")
	kind := flag.String("kind", "errors", "errors | happypath | dispatch (which battery to record)")
	project := flag.String("project", "", "GCP project id (real) or placeholder (emulator)")
	location := flag.String("location", "us-central1", "location id")
	addr := flag.String("addr", "localhost:8123", "emulator address (target=emulator)")
	out := flag.String("out", "", "output JSON path (default: stdout)")
	variants := flag.Int("variants", 3, "differing-input variants per case (errors battery only)")
	receiverURL := flag.String("receiver-url", "", "receiver base URL, e.g. https://PROJECT.appspot.com (kind=dispatch)")
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

	fmt.Fprintf(os.Stderr, "recording %s battery=%s (project=%s location=%s prefix=%s)...\n",
		*target, *kind, *project, *location, opts.Prefix)

	switch *kind {
	case "errors":
		recordErrors(ctx, client, opts, *out)
	case "happypath":
		recordHappyPath(ctx, client, opts, *out)
	case "dispatch":
		recordDispatch(ctx, client, opts, *receiverURL, *out)
	default:
		fmt.Fprintf(os.Stderr, "unknown kind %q (want errors|happypath|dispatch)\n", *kind)
		os.Exit(1)
	}
}

func recordErrors(ctx context.Context, client *conformance.Client, opts conformance.RunOptions, out string) {
	results := conformance.RunErrors(ctx, client, opts)

	unstable := 0
	for _, r := range results {
		if !r.Stable {
			unstable++
			fmt.Fprintf(os.Stderr, "  UNSTABLE %s: variants disagreed after normalization (unmodeled interpolation?)\n", r.Name)
		}
	}

	writeJSON(out, func(path string) error { return conformance.SaveErrors(path, results) })
	fmt.Fprintf(os.Stderr, "done: %d cases, %d unstable\n", len(results), unstable)
}

func recordHappyPath(ctx context.Context, client *conformance.Client, opts conformance.RunOptions, out string) {
	snaps := conformance.RunHappyPath(ctx, client, opts)

	failed := 0
	for _, s := range snaps {
		if s.CreateFull.Err != "" || s.GetBasic.Err != "" || s.GetFull.Err != "" {
			failed++
			fmt.Fprintf(os.Stderr, "  ERRORS %s: create=%q basic=%q full=%q\n",
				s.Name, s.CreateFull.Err, s.GetBasic.Err, s.GetFull.Err)
		}
	}

	writeJSON(out, func(path string) error { return conformance.SaveHappyPath(path, snaps) })
	fmt.Fprintf(os.Stderr, "done: %d observations, %d with errors\n", len(snaps), failed)
}

func recordDispatch(ctx context.Context, client *conformance.Client, opts conformance.RunOptions, receiverURL, out string) {
	if receiverURL == "" {
		fmt.Fprintln(os.Stderr, "error: -kind=dispatch requires -receiver-url (see test/conformance/receiver)")
		os.Exit(1)
	}

	snaps := conformance.RunDispatch(ctx, client, opts, receiverURL)

	noRetry := 0
	for _, s := range snaps {
		retried := false
		for _, a := range s.Attempts {
			if a.Attempt >= 1 {
				retried = true
				break
			}
		}
		if retried {
			fmt.Fprintf(os.Stderr, "  %s: retry observed (%d attempts)\n", s.Name, len(s.Attempts))
		} else {
			noRetry++
			fmt.Fprintf(os.Stderr, "  NO RETRY observed for %s (%d attempts captured) - is the receiver reachable and deployed?\n",
				s.Name, len(s.Attempts))
		}
	}

	writeJSON(out, func(path string) error { return conformance.SaveDispatch(path, snaps) })
	fmt.Fprintf(os.Stderr, "done: %d cases, %d without an observed retry\n", len(snaps), noRetry)
}

// writeJSON sends the battery output to stdout or the given path.
func writeJSON(out string, save func(path string) error) {
	if out == "" {
		out = "/dev/stdout"
	}
	if err := save(out); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", out, err)
		os.Exit(1)
	}
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
