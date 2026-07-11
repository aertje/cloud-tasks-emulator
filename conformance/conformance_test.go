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
	errorsGoldenPath    = "golden/errors.json"
	happyPathGoldenPath = "golden/happypath.json"
)

// TestEmulatorErrors builds and starts the emulator, replays the error battery
// against it, and asserts every captured code+template matches the golden
// snapshot recorded from the real API.
//
//	go test -tags conformance ./conformance/
//
// Skips if the golden file is absent (record it first - see cmd/record).
func TestEmulatorErrors(t *testing.T) {
	if _, err := os.Stat(errorsGoldenPath); os.IsNotExist(err) {
		t.Skipf("no error golden at %s; record it with cmd/record -target=real", errorsGoldenPath)
	}
	golden, err := conformance.LoadErrors(errorsGoldenPath)
	if err != nil {
		t.Fatalf("load error golden: %v", err)
	}

	addr := startEmulator(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := conformance.NewEmulatorClient(ctx, addr)
	if err != nil {
		t.Fatalf("dial emulator: %v", err)
	}
	defer client.Close()

	results := conformance.RunErrors(ctx, client, conformance.RunOptions{
		Project:  "conformance-test",
		Location: "us-central1",
		Prefix:   "emu",
		Variants: 2,
	})

	diverged := make(map[string]bool)
	for _, d := range conformance.CompareErrors(golden, results) {
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

// TestEmulatorHappyPath replays the happy-path battery against the emulator and
// asserts the headers and body it echoes back at each read stage match the
// golden recorded from the real API (see RunHappyPath for what each stage
// probes, including the BASIC/FULL view division of the body).
//
//	go test -tags conformance ./conformance/
//
// Skips if the happy-path golden is absent (record it with
// `cmd/record -kind=happypath -target=real`).
func TestEmulatorHappyPath(t *testing.T) {
	if _, err := os.Stat(happyPathGoldenPath); os.IsNotExist(err) {
		t.Skipf("no happy-path golden at %s; record it with cmd/record -kind=happypath -target=real", happyPathGoldenPath)
	}
	golden, err := conformance.LoadHappyPath(happyPathGoldenPath)
	if err != nil {
		t.Fatalf("load happy-path golden: %v", err)
	}

	addr := startEmulator(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := conformance.NewEmulatorClient(ctx, addr)
	if err != nil {
		t.Fatalf("dial emulator: %v", err)
	}
	defer client.Close()

	snaps := conformance.RunHappyPath(ctx, client, conformance.RunOptions{
		Project:  "conformance-test",
		Location: "us-central1",
		Prefix:   "emu",
	})

	for _, d := range conformance.CompareHappyPath(golden, snaps) {
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
