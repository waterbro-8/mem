//go:build !unix

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// acquireWatchLock takes the per-root lock by creating a file no other process
// may create again.
//
// Recorded limit, not a silent compromise: this platform has no portable
// advisory-lock primitive in the standard library, so exclusivity is claimed by
// the filesystem entry rather than by an open handle. A watcher killed without a
// chance to release leaves that file behind and the next one fails fast; the
// error names the path so an operator can remove it. The unix build uses flock
// and has no such stale-lock case. Watch is exercised on Linux and macOS CI
// only, so this path is compile-verified, not behaviour-tested.
func acquireWatchLock(path string) (release func(), err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create watch lock dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, errWatchLocked
		}
		return nil, fmt.Errorf("open watch lock: %w", err)
	}
	if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("write watch lock: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("close watch lock: %w", err)
	}
	return func() { os.Remove(path) }, nil
}
