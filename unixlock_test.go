// Copyright (c) 2025 Visvasity LLC

package unixlock

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBasicLocking(t *testing.T) {
	tmpDir := t.TempDir()
	lockPath := filepath.Join(tmpDir, "testlock.sock")

	ctx := context.Background()
	lock := New(lockPath)

	// Try to acquire the lock
	closef, err := lock.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("first TryAcquire failed: %v", err)
	}
	if closef == nil {
		t.Fatal("first TryAcquire returned nil close function")
	}

	// Second lock in same process should fail
	lock2 := New(lockPath)
	if _, err := lock2.TryAcquire(ctx); err == nil {
		t.Fatal("expected second TryAcquire to fail, but it succeeded")
	}

	// Verify GetOwnerPid returns our PID
	pid, err := lock.Owner(ctx)
	if err != nil {
		t.Fatalf("GetOwnerPid failed: %v", err)
	}
	if pid != os.Getpid() {
		t.Fatalf("expected owner pid %d, got %d", os.Getpid(), pid)
	}

	// Release the lock
	closef()

	// Wait a moment for cleanup
	time.Sleep(100 * time.Millisecond)

	// Now second lock should succeed
	closef2, err := lock2.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("second TryAcquire after unlock failed: %v", err)
	}
	closef2()
}
