package app

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("Run(--help) code=%d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "Usage:") || !strings.Contains(stdout.String(), "mycel-lab") {
		t.Fatalf("help output missing usage/name: %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr=%q, want empty", stderr.String())
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"wat"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("Run(unknown) code=0, want nonzero")
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("stderr missing unknown command: %q", stderr.String())
	}
}

func TestSubcommandSkeletons(t *testing.T) {
	scenarioPath := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "raft-3-node-short-outage.yaml")
	tests := [][]string{
		{"db", "migrate"},
		{"db", "status"},
		{"import", "--dry-run", "tests/reliability"},
		{"export", "scenario", "raft-5-node"},
		{"run", "scenario-file", scenarioPath, "--dry-run"},
		{"version"},
	}
	for _, args := range tests {
		var stdout, stderr bytes.Buffer
		if code := Run(args, &stdout, &stderr); code != 0 {
			t.Fatalf("Run(%v) code=%d stderr=%q", args, code, stderr.String())
		}
		if stdout.Len() == 0 {
			t.Fatalf("Run(%v) wrote no stdout", args)
		}
	}
}

func TestRunRequiresTarget(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"run", "scenario"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("Run(run scenario) code=0, want nonzero")
	}
	if !strings.Contains(stderr.String(), "requires") {
		t.Fatalf("stderr missing requires message: %q", stderr.String())
	}
}
