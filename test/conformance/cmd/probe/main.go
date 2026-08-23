// Command probe adaptively discovers how Cloud Tasks measures the task size
// limit: the exact boundary per target type, which
// fields count toward it, and the exact rejection error. Unlike cmd/record it
// does not produce a golden - it produces knowledge, from which the emulator's
// validation (and then permanent error-battery cases) are written.
//
// Run from inside the conformance/ module directory, against a throwaway
// project with the Cloud Tasks API enabled (control-plane only, nothing
// dispatches):
//
//	gcloud auth application-default login
//	go run ./cmd/probe -project=$PROJECT -location=us-central1 -out=/tmp/sizeprobe.json
//
// Once the emulator enforces the limit, the same tool can sanity-check it:
//
//	go run ./cmd/emulator -port 8123 &   # from repo root
//	go run ./cmd/probe -target=emulator -addr=localhost:8123
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/aertje/cloud-tasks-emulator/test/conformance"
)

func main() {
	target := flag.String("target", "real", "real | emulator")
	project := flag.String("project", "", "GCP project id (real) or placeholder (emulator)")
	location := flag.String("location", "us-central1", "location id")
	addr := flag.String("addr", "localhost:8123", "emulator address (target=emulator)")
	targets := flag.String("targets", "http,appengine", "comma-separated task target types to probe")
	prefix := flag.String("prefix", "", "run-scoped resource-name prefix (default: random). Task size depends on name length, so pin this (with -project) to a previous run's prefix to reproduce its boundaries, e.g. when validating the emulator against a report recorded from real")
	out := flag.String("out", "", "output JSON path (default: stdout)")
	flag.Parse()

	if *project == "" {
		if *target == "real" {
			fmt.Fprintln(os.Stderr, "error: -project is required with -target=real")
			os.Exit(1)
		}
		// The emulator ignores the project, but resource names still need one.
		*project = "conformance-test"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
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

	if *prefix == "" {
		*prefix = fmt.Sprintf("cte-probe-%d", rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1<<31))
	}
	opts := conformance.SizeProbeOptions{
		Project:  *project,
		Location: *location,
		Prefix:   *prefix,
		Targets:  splitTargets(*targets),
		Log:      os.Stderr,
	}

	fmt.Fprintf(os.Stderr, "probing %s (project=%s location=%s prefix=%s targets=%v)...\n",
		*target, *project, *location, opts.Prefix, opts.Targets)

	reports := conformance.RunSizeProbe(ctx, client, opts)

	failed := summarize(reports)
	writeReport(*out, reports)
	if failed {
		os.Exit(1)
	}
}

// summarize prints the human-readable digest to stderr and reports whether any
// target's probe aborted.
func summarize(reports []conformance.TargetSizeReport) bool {
	failed := false
	for _, r := range reports {
		if r.Err != "" {
			failed = true
			fmt.Fprintf(os.Stderr, "\n%s: FAILED after %d calls: %s\n", r.Target, r.Calls, r.Err)
			continue
		}
		fmt.Fprintf(os.Stderr, "\n%s (%d calls): max body=%d; at boundary task proto=%d, message proto=%d, request proto=%d\n",
			r.Target, r.Calls, r.MaxBody, r.TaskProtoSizeAtMax, r.MessageProtoSizeAtMax, r.RequestProtoSizeAtMax)
		fmt.Fprintf(os.Stderr, "  rejection: %s: %s\n", r.RejectCode, r.RejectMessage)
		for _, o := range r.OtherRejections {
			fmt.Fprintf(os.Stderr, "  WARNING other rejection seen during search: %s\n", o)
		}
		for _, p := range r.Perturbations {
			verdict := "counts exactly its proto encoding"
			switch {
			case p.Note != "":
				verdict = p.Note
			case !p.Consistent && p.ActualMaxBody >= 0:
				verdict = fmt.Sprintf("INCONSISTENT: actual max body=%d (predicted %d, weight %+d vs proto)",
					p.ActualMaxBody, p.PredictedMaxBody, p.PredictedMaxBody-p.ActualMaxBody)
			}
			fmt.Fprintf(os.Stderr, "  %-24s proto delta=%-4d %s\n", p.Name, p.ProtoDelta, verdict)
		}
	}
	return failed
}

func writeReport(out string, reports []conformance.TargetSizeReport) {
	if out == "" {
		out = "/dev/stdout"
	}
	data, err := json.MarshalIndent(reports, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal report: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(out, append(data, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", out, err)
		os.Exit(1)
	}
}

func splitTargets(s string) []string {
	var out []string
	for t := range strings.SplitSeq(s, ",") {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	return out
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
