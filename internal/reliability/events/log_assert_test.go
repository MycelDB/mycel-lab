package events

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
)

func TestLogAssertAbsentScansCompleteBoundedWindowBeyondCapturedTail(t *testing.T) {
	binDir := t.TempDir()
	kubectl := filepath.Join(binDir, "kubectl")
	script := `#!/bin/sh
case "$*" in
  *"--tail=-1"*"--since=20m"*) ;;
  *) echo "missing complete-window arguments: $*" >&2; exit 2 ;;
esac
echo "stale automation invocation update"
i=0
while [ "$i" -lt 6001 ]; do
  echo "ordinary raft log line $i"
  i=$((i + 1))
done
`
	if err := os.WriteFile(kubectl, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake kubectl: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	result, err := executeLogAssertAbsentEvent(context.Background(), env.Environment{Context: "test", Namespace: "test"}, map[string]any{
		"selector": "app=myceld",
		"tail":     -1,
		"since":    "20m",
		"patterns": []any{"stale automation invocation update"},
	})
	if err == nil {
		t.Fatal("executeLogAssertAbsentEvent() error=nil, want forbidden startup line failure")
	}
	if len(result.Matches) != 1 || result.Matches[0] != "stale automation invocation update" {
		t.Fatalf("matches=%v", result.Matches)
	}
	if strings.Contains(result.Stdout, "stale automation invocation update") {
		t.Fatalf("bounded output unexpectedly retained the old startup line: %q", result.Stdout)
	}
	if len(result.Stdout) > 16384 {
		t.Fatalf("bounded output length=%d", len(result.Stdout))
	}
	if result.Timeout != "2m0s" {
		t.Fatalf("timeout=%q", result.Timeout)
	}
}

func TestLogAssertAbsentRejectsEmptyLogWindows(t *testing.T) {
	for _, target := range []map[string]any{{"tail": 0}, {"tail": -2}, {"since": "0s"}, {"since": "not-a-duration"}, {"timeout": "not-a-duration"}} {
		if _, err := executeLogAssertAbsentEvent(context.Background(), env.Environment{}, target); err == nil {
			t.Fatalf("executeLogAssertAbsentEvent(%v) error=nil", target)
		}
	}
}
