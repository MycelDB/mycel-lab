package artifacts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
)

func TestSinkRejectsPathsOutsideRunRoot(t *testing.T) {
	root := t.TempDir()
	sink, err := artifacts.NewSink(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", "../escape", filepath.Join(string(filepath.Separator), "absolute")} {
		if _, err := sink.WriteText(name, "unsafe"); err == nil {
			t.Fatalf("WriteText(%q) error = nil", name)
		}
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.WriteText("link/escape", "unsafe"); err == nil {
		t.Fatal("WriteText through symlink error = nil")
	}
}

func TestSanitizedSinkRedactsEveryWritePath(t *testing.T) {
	const secret = "opaque-artifact-secret"
	root := t.TempDir()
	sink, err := artifacts.NewSanitizedSink(root, provider.NewRedactor([]string{secret}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.WriteJSON("value.json", map[string]any{"password": "visible", "note": secret}); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.WriteText("value.log", "Bearer abcdefghijklmnop "+secret); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.AppendJSONL("value.jsonl", map[string]any{"apiKey": "sk-1234567890abcdefghijklmnop", "note": secret}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"value.json", "value.log", "value.jsonl"} {
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		if strings.Contains(text, secret) || strings.Contains(text, "visible") || strings.Contains(text, "abcdefghijklmnop") {
			t.Fatalf("%s leaked credential: %s", name, text)
		}
		if !strings.Contains(text, provider.Redacted) {
			t.Fatalf("%s lacks redaction marker: %s", name, text)
		}
	}
}
