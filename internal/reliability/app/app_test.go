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
	fixtureRoot := filepath.Join("..", "..", "..", "tests", "reliability")
	scenarioPath := filepath.Join(fixtureRoot, "scenarios", "raft-3-node-short-outage.yaml")
	tests := [][]string{
		{"db", "--help"},
		{"import", "--dry-run", fixtureRoot},
		{"export", "--help"},
		{"list", "--help"},
		{"show", "--help"},
		{"delete", "--help"},
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

func TestDBRequiresDatabaseURL(t *testing.T) {
	t.Setenv("MYCEL_LAB_DATABASE_URL", "")
	t.Setenv("MYCEL_RELIABILITY_DATABASE_URL", "")
	t.Setenv("DATABASE_URL", "")
	var stdout, stderr bytes.Buffer
	code := Run([]string{"db", "status"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("Run(db status) code=0, want nonzero")
	}
	if !strings.Contains(stderr.String(), "database URL is required") {
		t.Fatalf("stderr missing database URL requirement: %q", stderr.String())
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
