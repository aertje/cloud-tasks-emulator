//go:build conformance
// +build conformance

package conformance_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aertje/cloud-tasks-emulator/conformance"
)

const (
	goldenPath        = "golden/realcloud.json"
	headersGoldenPath = "golden/headers.json"
)

// TestEmulatorMatchesRealCloud builds and starts the emulator, replays the
// conformance battery against it, and asserts every captured code+template
// matches the golden snapshot recorded from the real API.
//
//	go test -tags conformance ./conformance/
//
// Skips if the golden file is absent (record it first - see cmd/record).
func TestEmulatorMatchesRealCloud(t *testing.T) {
	if _, err := os.Stat(goldenPath); os.IsNotExist(err) {
		t.Skipf("no golden snapshot at %s; record it with cmd/record -target=real", goldenPath)
	}
	golden, err := conformance.Load(goldenPath)
	if err != nil {
		t.Fatalf("load golden: %v", err)
	}

	addr := startEmulator(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := conformance.NewEmulatorClient(ctx, addr)
	if err != nil {
		t.Fatalf("dial emulator: %v", err)
	}
	defer client.Close()

	results := conformance.Run(ctx, client, conformance.RunOptions{
		Project:  "conformance-test",
		Location: "us-central1",
		Prefix:   "emu",
		Variants: 2,
	})

	diverged := make(map[string]bool)
	for _, d := range conformance.Compare(golden, results) {
		diverged[d.Case] = true
		if reason, known := conformance.KnownDivergences[d.Case]; known {
			t.Logf("KNOWN divergence %s (%s):\n%s", d.Case, reason, d.String())
			continue
		}
		t.Errorf("%s", d.String())
	}

	// Flag accepted gaps that have started matching - the ledger entry can go.
	for name := range conformance.KnownDivergences {
		if !diverged[name] {
			t.Errorf("case %q is listed in KnownDivergences but now matches real Cloud Tasks; remove it from the ledger", name)
		}
	}
}

// TestEmulatorHeaderSnapshots replays the happy-path header battery against the
// emulator and asserts the headers it echoes back match the golden recorded
// from the real API (see RunHeaderObservations for what each stage probes).
//
//	go test -tags conformance ./conformance/
//
// Skips if the header golden is absent (record it with
// `cmd/record -kind=headers -target=real`).
func TestEmulatorHeaderSnapshots(t *testing.T) {
	if _, err := os.Stat(headersGoldenPath); os.IsNotExist(err) {
		t.Skipf("no header golden at %s; record it with cmd/record -kind=headers -target=real", headersGoldenPath)
	}
	golden, err := conformance.LoadHeaderSnapshots(headersGoldenPath)
	if err != nil {
		t.Fatalf("load header golden: %v", err)
	}

	addr := startEmulator(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := conformance.NewEmulatorClient(ctx, addr)
	if err != nil {
		t.Fatalf("dial emulator: %v", err)
	}
	defer client.Close()

	snaps := conformance.RunHeaderObservations(ctx, client, conformance.RunOptions{
		Project:  "conformance-test",
		Location: "us-central1",
		Prefix:   "emu",
	})

	for _, d := range conformance.CompareHeaderSnapshots(golden, snaps) {
		t.Errorf("%s", d.String())
	}
}

// startEmulator builds the emulator from the repo root, runs it on a free port,
// and returns its address. The process is killed on test cleanup.
func startEmulator(t *testing.T) string {
	t.Helper()

	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}

	bin := filepath.Join(t.TempDir(), "emulator")
	build := exec.Command("go", "build", "-o", bin, "./cmd/emulator")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build emulator: %v\n%s", err, out)
	}

	port := freePort(t)
	cmd := exec.Command(bin, "-port", port)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start emulator: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	addr := "localhost:" + port
	waitForListen(t, addr)
	return addr
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func waitForListen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("emulator did not start listening on %s", addr)
}
