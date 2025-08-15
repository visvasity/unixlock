// Copyright (c) 2025 Visvasity LLC

// Package unixlock provides inter-process mutual exclusion using Unix domain sockets.
// It creates a cooperative mutex at a specified file path, allowing processes to:
//
//   - Acquire and release locks.
//   - Communicate with the lock owner to request graceful shutdown.
//   - Query the owner's process ID (PID).
//   - Verify if the owner is an ancestor process.
//
// The mutex supports non-blocking and blocking lock acquisition, context-aware
// cancellation, and status reporting between processes.
package unixlock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrShutdown indicates the context was canceled due to a shutdown request.
var ErrShutdown = errors.New("shutdown")

// ErrUnlocked indicates the context was canceled because the lock was closed.
var ErrUnlocked = errors.New("unlocked")

type Mutex struct {
	mu sync.Mutex

	wg sync.WaitGroup

	listener atomic.Pointer[net.UnixListener]

	derivedCancels []context.CancelCauseFunc

	fpath string

	ppid int

	pidMap map[int]bool

	reportCh chan string

	reported bool
}

// New creates a cooperative mutual exclusion lock instance using a Unix domain
// socket at the specified file path. The mutex supports inter-process
// communication for shutdown requests, PID queries, and ancestor checks.
func New(fpath string) *Mutex {
	m := &Mutex{
		fpath:    fpath,
		ppid:     os.Getppid(),
		pidMap:   make(map[int]bool),
		reportCh: make(chan string, 1),
	}
	m.pidMap[os.Getpid()] = true
	return m
}

// Close releases the lock if held by the current process and cancels all derived
// contexts with os.ErrClosed. It waits for all associated goroutines to terminate.
func (m *Mutex) Close() {
	m.stopServer(os.ErrClosed)
	m.wg.Wait()
}

func (m *Mutex) startServer(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.listener.Load() != nil {
		return "", os.ErrExist
	}

	dir := filepath.Dir(m.fpath)
	base := filepath.Base(m.fpath)
	pid := os.Getpid()

	tpath := filepath.Join(dir, fmt.Sprintf("%s.%d", base, pid))
	_ = os.Remove(tpath) // Remove stale file with matching pid (very unlikely)

	addr := &net.UnixAddr{Name: tpath, Net: "unix"}
	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		return "", err
	}

	m.listener.Store(l)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		for l := m.listener.Load(); l != nil; l = m.listener.Load() {
			conn, err := l.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					slog.Error("could not accept incoming connection (ignored)", "err", err)
					continue
				}
				return
			}
			m.handle(conn)
		}
	}()

	return tpath, nil
}

func (m *Mutex) stopServer(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if l := m.listener.Load(); l != nil {
		l.Close()
		m.listener.Store(nil)
		for _, cancel := range m.derivedCancels {
			cancel(err)
		}
		m.derivedCancels = nil
	}
}

func (m *Mutex) handle(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		slog.Warn("could not read from incoming connection", "err", err)
		return
	}
	request := string(buf[:n])
	args := strings.Fields(request)
	cmd := args[0]

	switch cmd {
	case "getpid":
		fmt.Fprintf(conn, "%d", os.Getpid())
		return

	case "shutdown":
		slog.Info("canceling derived contexts due to shutdown message", "socket", m.fpath)
		for _, cancel := range m.derivedCancels {
			cancel(ErrShutdown)
		}
		return

	case "report":
		slog.Debug("received a report", "socket", m.fpath, "args", args)
		if len(args) < 2 {
			fmt.Fprintf(conn, "os.ErrInvalid")
			return
		}
		report := ""
		if len(args) > 2 {
			report = strings.Join(args[2:], " ")
		}
		pid, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || pid != int64(os.Getpid()) {
			if err == nil {
				slog.Warn("report message target is not the current lock owner", "report", report, "target", pid, "socket", m.fpath)
			}
			fmt.Fprintf(conn, "os.ErrInvalid")
			return
		}
		m.reportCh <- report
		return

	case "check-ancestor":
		if len(args) != 3 {
			fmt.Fprintf(conn, "os.ErrInvalid")
			return
		}
		ppid, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			fmt.Fprintf(conn, "os.ErrInvalid")
			return
		}
		pid, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			fmt.Fprintf(conn, "os.ErrInvalid")
			return
		}
		// pidMap requires no lock because it is only accessed by the command
		// handler and it handles one socket at a time, sequentially.
		if m.pidMap[int(ppid)] || m.pidMap[int(pid)] {
			m.pidMap[int(ppid)] = true
			m.pidMap[int(pid)] = true
			return
		}
		fmt.Fprintf(conn, "os.ErrNotExist")
		return

	default:
		fmt.Fprintf(conn, "os.ErrInvalid")
		slog.Warn("invalid/unrecognized input command", "cmd", cmd, "args", args)
		return
	}
}

func (m *Mutex) sendCmd(ctx context.Context, cmd string) (string, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", m.fpath)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if _, err := conn.Write([]byte(cmd)); err != nil {
		return "", err
	}
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	buf = buf[:n]
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return "", err
		}
	}
	return string(buf), nil
}

// Owner returns the PID of the process currently holding the lock.
func (m *Mutex) Owner(ctx context.Context) (int, error) {
	response, err := m.sendCmd(ctx, "getpid")
	if err != nil {
		return -1, err
	}
	pid, err := strconv.ParseInt(response, 10, 64)
	if err != nil {
		return -1, err
	}
	return int(pid), nil
}

// shutdown sends a shutdown request to the lock owner.
func (m *Mutex) shutdown(ctx context.Context) error {
	response, err := m.sendCmd(ctx, "shutdown")
	if err != nil {
		return err
	}
	if len(response) != 0 {
		return errors.New(response)
	}
	return nil
}

// CheckAncestor verifies if the lock owner is a parent or ancestor of the current
// process. It returns nil if the check passes, otherwise an error. The lock owner
// tracks PIDs of processes that pass this check to support deeply nested descendants.
func (m *Mutex) CheckAncestor(ctx context.Context) error {
	pid := os.Getpid()
	cmd := fmt.Sprintf("check-ancestor %d %d", m.ppid, pid)
	response, err := m.sendCmd(ctx, cmd)
	if err != nil {
		return err
	}
	if len(response) != 0 {
		return errors.New(response)
	}
	return nil
}

// TryLock attempts to acquire the lock without waiting. It returns an unlock
// function and nil error on success. It returns os.ErrInvalid if the lock is
// held by the current process, or another error if acquisition fails.
func (m *Mutex) TryLock(ctx context.Context) (unlockf func(), status error) {
	if pid, err := m.Owner(ctx); err == nil {
		if pid == os.Getpid() {
			return nil, os.ErrInvalid
		}
		return nil, fmt.Errorf("locked by another process")
	}

	tmpPath, err := m.startServer(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if status != nil {
			m.stopServer(os.ErrClosed)
		}
	}()

	os.Rename(tmpPath, m.fpath)

	pid, err := m.Owner(ctx)
	if err != nil {
		return nil, err
	}
	if pid != os.Getpid() {
		return nil, fmt.Errorf("lock won by another process")
	}
	return func() { m.stopServer(ErrUnlocked) }, nil
}

// Lock acquires the lock, waiting until it is available or the context expires.
// If shutdown is true, it sends a shutdown request to the owner before waiting.
// It returns an unlock function and nil on success, or an error on failure.
func (m *Mutex) Lock(ctx context.Context, shutdown bool) (unlockf func(), status error) {
	if closef, err := m.TryLock(ctx); err == nil {
		return closef, nil
	}
	if shutdown {
		if err := m.shutdown(ctx); err != nil {
			return nil, err
		}
	}
	for ctx.Err() == nil {
		if closef, err := m.TryLock(ctx); err == nil {
			return closef, nil
		}
		timeout := 50*time.Millisecond + time.Duration(rand.Intn(100))*time.Millisecond
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-time.After(timeout):
		}
	}
	return nil, context.Cause(ctx)
}

// WithLock returns a context that is canceled when the input context is canceled,
// the mutex is unlocked (ErrUnlocked), or a shutdown request is received
// (ErrShutdown). It returns os.ErrInvalid if the mutex is not held by the
// current process. The mutex is not automatically unlocked when the context is
// canceled.
func WithLock(ctx context.Context, m *Mutex) (context.Context, error) {
	pid, err := m.Owner(ctx)
	if err != nil {
		return nil, err
	}
	if pid != os.Getpid() {
		return nil, fmt.Errorf("not locked by this process")
	}

	nctx, ncancel := context.WithCancelCause(ctx)
	m.derivedCancels = append(m.derivedCancels, ncancel)
	return nctx, nil
}

// WaitForReport blocks until a status report is received via the Unix domain
// socket or the context is canceled. It is used by parent processes to wait for
// initialization status from a child process.
func WaitForReport(ctx context.Context, m *Mutex) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case report := <-m.reportCh:
		if len(report) == 0 {
			return nil
		}
		return errors.New(report)
	}
}

// Report sends a status message to the parent process if it is the current
// lock owner. A nil status indicates success. Only one report can be sent per
// mutex; subsequent calls are no-ops. It returns an error if the report cannot
// be sent.
func Report(ctx context.Context, m *Mutex, status error) error {
	if m.reported {
		return nil
	}
	cmd := fmt.Sprintf("report %d", m.ppid)
	if status != nil {
		cmd = fmt.Sprintf("report %d %v", m.ppid, status)
	}
	if _, err := m.sendCmd(ctx, cmd); err != nil {
		if !strings.Contains(err.Error(), ": connection refused") {
			slog.Warn("could not send status report", "status", status, "err", err, "socket", m.fpath)
		}
		return err
	}
	m.reported = true
	return nil
}
