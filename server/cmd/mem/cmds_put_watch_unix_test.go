//go:build unix

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// AC-005 with a real signal, not a cancelled context: SIGTERM must end the run
// between cycles with no error. The test waits for the watcher's own startup
// line so the handler is installed before the signal is sent.
//
// Tagged unix because signaling a process by pid is not a portable test, and
// skipped under CI because a test that loses the race would take the whole test
// binary with it; the stopped-between-cycles contract itself is covered on every
// platform by TestWatchCancellationExitsZero.
func TestWatchSIGTERMExitsZero(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("signal delivery to a test binary is host-dependent under CI")
	}
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	f.w.interval = 10 * time.Millisecond
	writeFile(t, filepath.Join(f.root, "a.txt"), "a")

	out := &syncBuffer{}
	f.w.cmd.SetOut(out)
	done := make(chan error, 1)
	go func() { done <- f.w.run(context.Background()) }()

	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(out.String(), "watching ") {
		if time.Now().After(deadline) {
			t.Fatal("watcher never reported it was watching")
		}
		time.Sleep(time.Millisecond)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signal self: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("SIGTERM run returned %v, want nil (exit 0)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not stop after SIGTERM")
	}
	if n := len(readLogLines(t, f.w.reportLog)); n < 1 {
		t.Errorf("report log lines = %d, want at least one completed cycle", n)
	}
}
