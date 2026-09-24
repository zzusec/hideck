//go:build linux

package qmi

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestShouldSpawnProxySkipsBusyListener(t *testing.T) {
	if shouldSpawnProxy(syscall.EAGAIN) {
		t.Fatal("EAGAIN should not start another qmi-proxy")
	}
	busy := &net.OpError{Op: "dial", Net: "unix", Err: syscall.EAGAIN}
	if shouldSpawnProxy(busy) {
		t.Fatal("busy unix dial should not start another qmi-proxy")
	}
	if !shouldSpawnProxy(syscall.ECONNREFUSED) {
		t.Fatal("connection refused should start qmi-proxy")
	}
}

func TestStartProxyProcessKeepsASingleLiveChild(t *testing.T) {
	t.Cleanup(StopStartedProxy)
	exe := filepath.Join(t.TempDir(), "qmi-proxy")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := startProxyProcess(exe); err != nil {
		t.Fatal(err)
	}
	if err := startProxyProcess(exe); err != nil {
		t.Fatal(err)
	}
	proxyMu.Lock()
	cmd := liveProxyCmd
	proxyMu.Unlock()
	if cmd == nil || cmd.Process == nil {
		t.Fatal("expected a live qmi-proxy child")
	}
	pid := cmd.Process.Pid
	if err := startProxyProcess(exe); err != nil {
		t.Fatal(err)
	}
	proxyMu.Lock()
	second := liveProxyCmd
	proxyMu.Unlock()
	if second == nil || second.Process == nil || second.Process.Pid != pid {
		t.Fatalf("second start replaced the live proxy")
	}
	StopStartedProxy()
	if err := syscall.Kill(pid, 0); err == nil {
		t.Fatal("StopStartedProxy left the proxy running")
	}
}

func TestProxySocketAddressNormalizesCommonAbstractSocketNames(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "default", in: "", want: "\x00qmi-proxy"},
		{name: "plain", in: "qmi-proxy", want: "\x00qmi-proxy"},
		{name: "at prefix", in: "@qmi-proxy", want: "\x00qmi-proxy"},
		{name: "nul prefix", in: "\x00qmi-proxy", want: "\x00qmi-proxy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := proxySocketAddress(tt.in); got != tt.want {
				t.Fatalf("proxySocketAddress(%q)=%q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestOpenProxyTransportDoesNotSpawnWhenProxyIsBusy(t *testing.T) {
	proxyExecutable := filepath.Join(t.TempDir(), "qmi-proxy")
	if err := os.WriteFile(proxyExecutable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	oldDial := dialProxyHook
	oldStart := startProxyProcessHook
	oldKill := killProxyProcessesHook
	oldRetryDelay := proxyRetryDelay
	t.Cleanup(func() {
		dialProxyHook = oldDial
		startProxyProcessHook = oldStart
		killProxyProcessesHook = oldKill
		proxyRetryDelay = oldRetryDelay
	})
	starts := 0
	dialProxyHook = func(context.Context, string) (qmiTransport, error) {
		return nil, syscall.EAGAIN
	}
	startProxyProcessHook = func(string) error {
		starts++
		return nil
	}
	// No hung proxy to reap, so the busy socket is left alone.
	killProxyProcessesHook = func(string) int { return 0 }
	proxyRetryDelay = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := openProxyTransport(ctx, ClientOptions{ProxyExecutable: proxyExecutable})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want deadline", err)
	}
	if starts != 0 {
		t.Fatalf("starts=%d, want 0 when the proxy socket is busy", starts)
	}
}

// A qmi-proxy that answers dial with EAGAIN is wedged: it owns the abstract
// socket but never accepts connections. It must be reaped and replaced, or the
// modem stays offline forever (worker rebuild keeps failing to open QMI).
func TestOpenProxyTransportRestartsHungProxy(t *testing.T) {
	proxyExecutable := filepath.Join(t.TempDir(), "qmi-proxy")
	if err := os.WriteFile(proxyExecutable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	oldDial := dialProxyHook
	oldStart := startProxyProcessHook
	oldKill := killProxyProcessesHook
	oldRetryDelay := proxyRetryDelay
	oldSettle := proxyRestartSettleDelay
	t.Cleanup(func() {
		dialProxyHook = oldDial
		startProxyProcessHook = oldStart
		killProxyProcessesHook = oldKill
		proxyRetryDelay = oldRetryDelay
		proxyRestartSettleDelay = oldSettle
	})

	starts := 0
	kills := 0
	attempts := 0
	var serverConn net.Conn
	dialProxyHook = func(context.Context, string) (qmiTransport, error) {
		attempts++
		if attempts == 1 {
			return nil, syscall.EAGAIN
		}
		clientConn, conn := net.Pipe()
		serverConn = conn
		return clientConn, nil
	}
	startProxyProcessHook = func(string) error {
		starts++
		return nil
	}
	killProxyProcessesHook = func(string) int {
		kills++
		return 1
	}
	proxyRetryDelay = time.Millisecond
	proxyRestartSettleDelay = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := openProxyTransport(ctx, ClientOptions{ProxyExecutable: proxyExecutable})
	if err != nil {
		t.Fatalf("openProxyTransport() error=%v", err)
	}
	defer conn.Close()
	if serverConn != nil {
		defer serverConn.Close()
	}
	if kills != 1 {
		t.Fatalf("kill attempts=%d, want 1", kills)
	}
	if starts != 1 {
		t.Fatalf("start attempts=%d, want 1 after reaping the hung proxy", starts)
	}
}

func TestIsProxyUnresponsiveError(t *testing.T) {
	if !isProxyUnresponsiveError(syscall.EAGAIN) {
		t.Fatal("EAGAIN should be treated as an unresponsive proxy")
	}
	busy := &net.OpError{Op: "dial", Net: "unix", Err: syscall.EAGAIN}
	if !isProxyUnresponsiveError(busy) {
		t.Fatal("wrapped EAGAIN should be treated as an unresponsive proxy")
	}
	if isProxyUnresponsiveError(syscall.ECONNREFUSED) {
		t.Fatal("ECONNREFUSED means no proxy, not an unresponsive proxy")
	}
	if isProxyUnresponsiveError(nil) {
		t.Fatal("nil error must not be reported as unresponsive")
	}
}

func TestOpenProxyTransportRetriesUntilContextDeadline(t *testing.T) {
	proxyExecutable := filepath.Join(t.TempDir(), "qmi-proxy")
	if err := os.WriteFile(proxyExecutable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	oldDial := dialProxyHook
	oldStart := startProxyProcessHook
	oldRetryDelay := proxyRetryDelay
	t.Cleanup(func() {
		dialProxyHook = oldDial
		startProxyProcessHook = oldStart
		proxyRetryDelay = oldRetryDelay
	})

	attempts := 0
	starts := 0
	dialProxyHook = func(context.Context, string) (qmiTransport, error) {
		attempts++
		return nil, errors.New("proxy socket not ready")
	}
	startProxyProcessHook = func(string) error {
		starts++
		return nil
	}
	proxyRetryDelay = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := openProxyTransport(ctx, ClientOptions{
		ProxyPath:       "@qmi-proxy",
		ProxyExecutable: proxyExecutable,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("openProxyTransport() error=%v, want context deadline exceeded", err)
	}
	if starts != 1 {
		t.Fatalf("start attempts=%d, want 1", starts)
	}
	if attempts < 3 {
		t.Fatalf("dial attempts=%d, want at least 3 retries before deadline", attempts)
	}
}

func TestOpenProxyTransportRetriesUntilProxyIsReady(t *testing.T) {
	proxyExecutable := filepath.Join(t.TempDir(), "qmi-proxy")
	if err := os.WriteFile(proxyExecutable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	oldDial := dialProxyHook
	oldStart := startProxyProcessHook
	oldRetryDelay := proxyRetryDelay
	t.Cleanup(func() {
		dialProxyHook = oldDial
		startProxyProcessHook = oldStart
		proxyRetryDelay = oldRetryDelay
	})

	attempts := 0
	starts := 0
	var serverConn net.Conn
	dialProxyHook = func(context.Context, string) (qmiTransport, error) {
		attempts++
		if attempts < 4 {
			return nil, errors.New("proxy socket not ready")
		}
		clientConn, conn := net.Pipe()
		serverConn = conn
		return clientConn, nil
	}
	startProxyProcessHook = func(string) error {
		starts++
		return nil
	}
	proxyRetryDelay = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := openProxyTransport(ctx, ClientOptions{
		ProxyPath:       "\x00qmi-proxy",
		ProxyExecutable: proxyExecutable,
	})
	if err != nil {
		t.Fatalf("openProxyTransport() error=%v", err)
	}
	defer conn.Close()
	if serverConn != nil {
		defer serverConn.Close()
	}
	if starts != 1 {
		t.Fatalf("start attempts=%d, want 1", starts)
	}
	if attempts != 4 {
		t.Fatalf("dial attempts=%d, want 4", attempts)
	}
}
