// Copyright (c) 2025 Visvasity LLC

// Package unixlock implements inter-process mutual exclusion using unix domain
// sockets in a given directory. With unix domain sockets, losing processes can
// communicate with the winner to signal for shutdown or other operations as well.

package unixlock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// ErrShutdown indicates context is canceled due to an incoming shutdown message.
var ErrShutdown = errors.New("shutdown")

// ErrUnlocked indicates context is canceled due to the mutex unlock.
var ErrUnlocked = errors.New("unlocked")

type Lock struct {
	wg sync.WaitGroup

	listener net.Listener

	derivedCancels []context.CancelCauseFunc

	fpath string
}

// New creates a mutual exclusion lock instance using an unix domain socket at
// the input path.
func New(fpath string) *Lock {
	return &Lock{fpath: fpath}
}

// Close destroys the lock. It will release the lock if it was acquired by this
// process.
func (v *Lock) Close() {
	v.stopServer(os.ErrClosed)
	v.wg.Wait()
}

func (v *Lock) startServer(ctx context.Context) (string, error) {
	dir := filepath.Dir(v.fpath)
	base := filepath.Base(v.fpath)
	pid := os.Getpid()

	tpath := filepath.Join(dir, fmt.Sprintf("%s.%d", base, pid))
	_ = os.Remove(tpath) // Remove stale file with matching pid (very unlikely)

	l, err := net.Listen("unix", tpath)
	if err != nil {
		return "", err
	}

	v.listener = l

	v.wg.Add(1)
	go func() {
		defer v.wg.Done()

		for {
			conn, err := v.listener.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					slog.Error("could not accept incoming connection (ignored)", "err", err)
					continue
				}
				return
			}
			v.handle(conn)
		}
	}()

	return tpath, nil
}

func (v *Lock) stopServer(err error) {
	if v.listener != nil {
		v.listener.Close()
		v.listener = nil
		for _, cancel := range v.derivedCancels {
			cancel(err)
		}
		v.derivedCancels = nil
	}
}

func (v *Lock) handle(conn net.Conn) error {
	defer conn.Close()

	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	buf = buf[:n]

	switch cmd := string(buf); cmd {
	case "getpid":
		fmt.Fprintf(conn, "%d", os.Getpid())
		return nil

	case "shutdown":
		v.stopServer(ErrShutdown)
		fmt.Fprintf(conn, "ok")
		return nil

	default:
		fmt.Fprintf(conn, "error: os.ErrInvalid")
		return os.ErrInvalid
	}
}

func (v *Lock) sendCmd(ctx context.Context, cmd string) (string, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", v.fpath)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if _, err := conn.Write([]byte(cmd)); err != nil {
		return "", err
	}
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}
	buf = buf[:n]
	return string(buf), nil
}

// Owner returns the pid of the current owner.
func (v *Lock) Owner(ctx context.Context) (int, error) {
	response, err := v.sendCmd(ctx, "getpid")
	if err != nil {
		return -1, err
	}
	pid, err := strconv.ParseInt(response, 10, 64)
	if err != nil {
		return -1, err
	}
	return int(pid), nil
}

// Shutdown sends shutdown message to the current owner.
func (v *Lock) Shutdown(ctx context.Context) error {
	if _, err := v.sendCmd(ctx, "shutdown"); err != nil {
		return err
	}
	return nil
}

// TryAcquire attempts to acquire the lock without waiting. Returns the unlock
// function and nil error on success. Returns os.ErrInvalid if the lock is
// already held by the current process.
func (v *Lock) TryAcquire(ctx context.Context) (closef func(), status error) {
	if pid, err := v.Owner(ctx); err == nil {
		if pid == os.Getpid() {
			return nil, os.ErrInvalid
		}
		return nil, err
	}

	tmpPath, err := v.startServer(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if status != nil {
			v.stopServer(os.ErrClosed)
		}
	}()

	os.Rename(tmpPath, v.fpath)

	pid, err := v.Owner(ctx)
	if err != nil {
		return nil, err
	}
	if pid != os.Getpid() {
		return nil, fmt.Errorf("locked by another")
	}
	return func() { v.stopServer(ErrUnlocked) }, nil
}

// Acquire acquires the lock by waiting until the lock is available or the
// context expires. If force is true, then sends shutdown message once and then
// waits for lock. Returns the unlock function and nil on success.
func (v *Lock) Acquire(ctx context.Context, force bool) (func(), error) {
	if closef, err := v.TryAcquire(ctx); err == nil {
		return closef, nil
	}
	if force {
		if err := v.Shutdown(ctx); err != nil {
			return nil, err
		}
	}
	for ctx.Err() == nil {
		if closef, err := v.TryAcquire(ctx); err == nil {
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

// WithLock returns a context that is canceled when the mutex is unlocked
// (ErrUnlocked) or receives a shutdown request (ErrShutdown). Returns
// os.ErrInvalid if the mutex is not locked.
func WithLock(ctx context.Context, v *Lock) (context.Context, error) {
	pid, err := v.Owner(ctx)
	if err != nil {
		return nil, err
	}
	if pid != os.Getpid() {
		return nil, fmt.Errorf("not locked by this process")
	}

	nctx, ncancel := context.WithCancelCause(ctx)
	v.derivedCancels = append(v.derivedCancels, ncancel)

	return nctx, nil
}
