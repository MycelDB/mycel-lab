package artifacts

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScopedSinkDisableRejectsLateWritesWithoutDisablingParent(t *testing.T) {
	parent, err := NewSink(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scoped := parent.Scoped()
	if _, err := scoped.WriteText("before.txt", "before\n"); err != nil {
		t.Fatal(err)
	}
	scoped.DisableWrites()
	if _, err := scoped.WriteText("late.txt", "late\n"); err == nil {
		t.Fatal("disabled scoped sink accepted a late write")
	}
	if _, err := parent.WriteText("terminal.txt", "terminal\n"); err != nil {
		t.Fatalf("parent sink was disabled: %v", err)
	}
	if _, err := os.Stat(filepath.Join(parent.Root(), "late.txt")); !os.IsNotExist(err) {
		t.Fatalf("late artifact exists: %v", err)
	}
}
