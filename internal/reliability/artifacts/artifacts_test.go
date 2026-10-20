package artifacts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSinkWritesJSONTextAndJSONL(t *testing.T) {
	sink, err := NewSink(t.TempDir())
	if err != nil {
		t.Fatalf("NewSink() error=%v", err)
	}
	jsonPath, err := sink.WriteJSON("result.json", map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("WriteJSON() error=%v", err)
	}
	textPath, err := sink.WriteText("summary.md", "# ok\n")
	if err != nil {
		t.Fatalf("WriteText() error=%v", err)
	}
	jsonlPath, err := sink.AppendJSONL("events.jsonl", EventNow("test", "phase", "actor", map[string]bool{"ok": true}))
	if err != nil {
		t.Fatalf("AppendJSONL() error=%v", err)
	}
	for _, path := range []string{jsonPath, textPath, jsonlPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("artifact %s missing: %v", path, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(sink.Root(), "events.jsonl"))
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if !strings.Contains(string(data), `"type":"test"`) {
		t.Fatalf("events missing type: %s", data)
	}
}
