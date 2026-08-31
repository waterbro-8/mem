//go:build unix

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// acquireWatchLock takes the advisory lock for one watched root.
//
// POSIX advisory locking is the mechanism, so the lock is released by the kernel
// when the holder exits for any reason, including a kill that never ran a
// cleanup handler. That property is the whole point: a crashed watcher must not
// require an operator to delete a file before the next one can start.
//
// The lock file itself is left in place; an empty file with no holder is not a
// lock.
//
// The lock owns its directory: a first run has no state tree, and a caller that
// only wants to know whether a root is claimed must not have to reproduce the
// setup.
func acquireWatchLock(path string) (release func(), err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create watch lock dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open watch lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errWatchLocked
		}
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	// Best effort: a reader of this file gets a name for the holder. A failed
	// write must not undo a lock we already hold.
	_ = f.Truncate(0)
	_, _ = fmt.Fprintf(f, "%d\n", os.Getpid())
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
