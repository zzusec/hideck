//go:build linux

package qmi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	dialProxyHook         = dialProxy
	startProxyProcessHook = startProxyProcess
	// killProxyProcessesHook is overridable so tests can simulate a hung proxy
	// without touching real processes on the host.
	killProxyProcessesHook = killProcessesByComm
	proxyRetryDelay        = 100 * time.Millisecond
	// proxyRestartSettleDelay gives the kernel time to release the abstract
	// socket after a hung qmi-proxy is killed, before a replacement is spawned.
	proxyRestartSettleDelay = 300 * time.Millisecond
)

func openProxyTransport(ctx context.Context, opts ClientOptions) (qmiTransport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	proxyPath := opts.ProxyPath
	if proxyPath == "" {
		proxyPath = defaultProxyPath
	}
	proxyExecutable := opts.ProxyExecutable
	if proxyExecutable == "" {
		proxyExecutable = defaultProxyExecutable
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	conn, firstErr := dialProxyHook(ctx, proxyPath)
	if firstErr == nil {
		return conn, nil
	}

	if proxyExecutable == "" {
		return nil, fmt.Errorf("connect qmi-proxy %q: %w", proxyPath, firstErr)
	}
	if _, err := os.Stat(proxyExecutable); err != nil {
		return nil, fmt.Errorf("connect qmi-proxy %q failed: %w; proxy executable %s is unavailable: %v", proxyPath, firstErr, proxyExecutable, err)
	}

	// A dial that fails with EAGAIN/EWOULDBLOCK means the socket exists but the
	// running qmi-proxy is not accepting connections (listen backlog full / the
	// process is wedged). It can never recover by itself, and spawning a second
	// proxy cannot help because the stuck one still owns the abstract socket.
	// Reap the hung proxy so a fresh one can take over; otherwise the modem is
	// stuck offline forever (worker rebuild keeps failing to open QMI).
	restarted := false
	if isProxyUnresponsiveError(firstErr) && restartUnresponsiveProxy(proxyExecutable) {
		restarted = true
		waitForProxySocketRelease(ctx)
	}

	if shouldSpawnProxy(firstErr) || restarted {
		if err := startProxyProcessHook(proxyExecutable); err != nil && !restarted {
			return nil, fmt.Errorf("connect qmi-proxy %q failed and start %s failed: %w", proxyPath, proxyExecutable, err)
		}
	}

	var lastErr error = firstErr
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("connect qmi-proxy %q after starting %s: last error: %v: %w", proxyPath, proxyExecutable, lastErr, err)
		}
		timer := time.NewTimer(proxyRetryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, fmt.Errorf("connect qmi-proxy %q after starting %s: last error: %v: %w", proxyPath, proxyExecutable, lastErr, ctx.Err())
		case <-timer.C:
		}
		conn, err := dialProxyHook(ctx, proxyPath)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
}

func dialProxy(ctx context.Context, proxyPath string) (qmiTransport, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "unix", proxySocketAddress(proxyPath))
}

func proxySocketAddress(proxyPath string) string {
	if proxyPath == "" {
		proxyPath = defaultProxyPath
	}
	if strings.HasPrefix(proxyPath, "\x00") {
		return proxyPath
	}
	if strings.HasPrefix(proxyPath, "@") {
		return "\x00" + strings.TrimPrefix(proxyPath, "@")
	}
	return "\x00" + proxyPath
}

// isProxyUnresponsiveError reports whether dialing the proxy failed because the
// existing qmi-proxy process is hung rather than because no proxy is running.
func isProxyUnresponsiveError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) && (errno == syscall.EAGAIN || errno == syscall.EWOULDBLOCK) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "resource temporarily unavailable")
}

// restartUnresponsiveProxy terminates the proxy process hideck spawned plus any
// stale qmi-proxy left behind by a previous run. Returns true when at least one
// process was signalled.
func restartUnresponsiveProxy(proxyExecutable string) bool {
	killed := false
	proxyMu.Lock()
	cmd := liveProxyCmd
	liveProxyCmd = nil
	proxyMu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		killed = true
	}
	name := filepath.Base(strings.TrimSpace(proxyExecutable))
	if name == "" || name == "." || name == string(filepath.Separator) {
		name = filepath.Base(defaultProxyExecutable)
	}
	if n := killProxyProcessesHook(name); n > 0 {
		killed = true
	}
	return killed
}

func killProcessesByComm(name string) int {
	if name == "" {
		return 0
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}
	self := os.Getpid()
	killed := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == self || pid <= 1 {
			continue
		}
		comm, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "comm"))
		if err != nil || strings.TrimSpace(string(comm)) != name {
			continue
		}
		proc, err := os.FindProcess(pid)
		if err != nil {
			continue
		}
		if err := proc.Kill(); err == nil {
			killed++
		}
	}
	return killed
}

func waitForProxySocketRelease(ctx context.Context) {
	timer := time.NewTimer(proxyRestartSettleDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func shouldSpawnProxy(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
		return false
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EAGAIN:
			return false
		case syscall.ECONNREFUSED, syscall.ENOENT:
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "resource temporarily unavailable") {
		return false
	}
	if strings.Contains(msg, "connection refused") || strings.Contains(msg, "no such file") {
		return true
	}
	return true
}

var (
	proxyMu      sync.Mutex
	liveProxyCmd *exec.Cmd
)

func startProxyProcess(proxyExecutable string) error {
	proxyMu.Lock()
	defer proxyMu.Unlock()
	if liveProxyAliveLocked() {
		return nil
	}

	cmd := exec.Command(proxyExecutable)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	// Stay in hideck's process tree and systemd cgroup so service stop
	// also tears the proxy down. A separate process group would survive
	// SIGTERM to the main pid and leave /dev/cdc-wdm* busy.
	if err := cmd.Start(); err != nil {
		return err
	}
	liveProxyCmd = cmd
	go reapLiveProxy(cmd)
	return nil
}

func liveProxyAliveLocked() bool {
	if liveProxyCmd == nil || liveProxyCmd.Process == nil {
		return false
	}
	if err := liveProxyCmd.Process.Signal(syscall.Signal(0)); err != nil {
		liveProxyCmd = nil
		return false
	}
	return true
}

func reapLiveProxy(cmd *exec.Cmd) {
	_ = cmd.Wait()
	proxyMu.Lock()
	if liveProxyCmd == cmd {
		liveProxyCmd = nil
	}
	proxyMu.Unlock()
}

// StopStartedProxy terminates the qmi-proxy process hideck spawned, if any.
func StopStartedProxy() {
	proxyMu.Lock()
	cmd := liveProxyCmd
	proxyMu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		proxyMu.Lock()
		gone := liveProxyCmd != cmd
		proxyMu.Unlock()
		if gone {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		proxyMu.Lock()
		gone := liveProxyCmd != cmd
		proxyMu.Unlock()
		if gone {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
