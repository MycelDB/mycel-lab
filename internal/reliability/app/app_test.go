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
	artifactRoot := t.TempDir()
	tests := [][]string{
		{"db", "--help"},
		{"import", "--dry-run", fixtureRoot},
		{"export", "--help"},
		{"list", "--help"},
		{"show", "--help"},
		{"delete", "--help"},
		{"run", "scenario-file", scenarioPath, "--dry-run", "--artifact-root", artifactRoot},
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

func TestRunRejectsInvalidConsolePortBase(t *testing.T) {
	fixtureRoot := filepath.Join("..", "..", "..", "tests", "reliability")
	scenarioPath := filepath.Join(fixtureRoot, "scenarios", "raft-3-node-short-outage.yaml")
	var stdout, stderr bytes.Buffer
	code := Run([]string{"run", "scenario-file", scenarioPath, "--dry-run", "--console-port-base", "nope"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("Run(invalid console port) code=0, want nonzero")
	}
	if !strings.Contains(stderr.String(), "invalid --console-port-base") {
		t.Fatalf("stderr missing invalid console port: %q", stderr.String())
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

func TestRunOptionsParseEnvironmentOverride(t *testing.T) {
	var stdout, stderr bytes.Buffer
	opts, ok := runOptionsFromArgs([]string{"--environment-driver", "compose", "--environment-namespace", "lab", "--environment-option", "composeFile=/tmp/compose.yml", "--environment-option", "grpcPorts=19091,19092"}, &stdout, &stderr)
	if !ok {
		t.Fatalf("runOptionsFromArgs ok=false stderr=%q", stderr.String())
	}
	if opts.EnvironmentOverride == nil {
		t.Fatal("EnvironmentOverride=nil")
	}
	if opts.EnvironmentOverride.Driver != "compose" || opts.EnvironmentOverride.Namespace != "lab" {
		t.Fatalf("override=%+v", opts.EnvironmentOverride)
	}
	if opts.EnvironmentOverride.Options["composeFile"] != "/tmp/compose.yml" || opts.EnvironmentOverride.Options["grpcPorts"] != "19091,19092" {
		t.Fatalf("options=%+v", opts.EnvironmentOverride.Options)
	}
}

func TestRunOptionsRejectInvalidEnvironmentOption(t *testing.T) {
	var stdout, stderr bytes.Buffer
	_, ok := runOptionsFromArgs([]string{"--environment-option", "not-a-pair"}, &stdout, &stderr)
	if ok {
		t.Fatal("runOptionsFromArgs ok=true, want false")
	}
	if !strings.Contains(stderr.String(), "key=value") {
		t.Fatalf("stderr=%q, want key=value", stderr.String())
	}
}
