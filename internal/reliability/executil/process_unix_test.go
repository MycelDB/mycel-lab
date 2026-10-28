//go:build unix

package executil

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLocalRunnerCancellationKillsProcessGroup(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "pids")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := (LocalRunner{}).Run(ctx, "bash", "-c", `sleep 60 & child=$!; printf '%s %s\n' "$$" "$child" > "$1"; wait`, "mycel-lab-process-group-test", pidPath)
	if err == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("Run() error=%v context=%v", err, ctx.Err())
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("process-group cancellation took %s", elapsed)
	}
	raw, readErr := os.ReadFile(pidPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, value := range strings.Fields(string(raw)) {
		pid, parseErr := strconv.Atoi(value)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		deadline := time.Now().Add(2 * time.Second)
		for {
			probeErr := syscall.Kill(pid, 0)
			if errors.Is(probeErr, syscall.ESRCH) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("process %d survived group cancellation: %v", pid, probeErr)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
