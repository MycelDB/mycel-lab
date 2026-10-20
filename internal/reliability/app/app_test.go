package app

import (
	"bytes"
	"os"
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
		{"run", "scenario-file", scenarioPath, "--dry-run", "--image", "myceldb/mycel:test", "--artifact-root", artifactRoot},
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

func TestRunDryRunWritesResolvedProviderArtifact(t *testing.T) {
	fixtureRoot := filepath.Join("..", "..", "..", "tests", "reliability")
	scenarioPath := filepath.Join(fixtureRoot, "scenarios", "raft-3-node-short-outage.yaml")
	profilePath := filepath.Join(fixtureRoot, "provider-profiles", "deterministic-token-basis-v1.yaml")
	artifactRoot := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := Run([]string{"run", "scenario-file", scenarioPath, "--dry-run", "--provider-profile", profilePath, "--artifact-root", artifactRoot}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("Run() code=%d stderr=%q", code, stderr.String())
	}
	var providerArtifact string
	if err := filepath.WalkDir(artifactRoot, func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && filepath.Base(path) == "provider-profile-resolved.json" {
			providerArtifact = path
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if providerArtifact == "" {
		t.Fatal("provider-profile-resolved.json not found")
	}
	raw, err := os.ReadFile(providerArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"sourceSHA256": "sha256:`) || !strings.Contains(string(raw), `"selection": "cli-override"`) {
		t.Fatalf("unexpected provider artifact: %s", raw)
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
	opts, ok := runOptionsFromArgs([]string{"--image", " myceldb/mycel:candidate ", "--environment-driver", "compose", "--environment-namespace", "lab", "--environment-option", "composeFile=/tmp/compose.yml", "--environment-option", "grpcPorts=19091,19092"}, &stdout, &stderr)
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
	if opts.ImageOverride != "myceldb/mycel:candidate" {
		t.Fatalf("ImageOverride=%q", opts.ImageOverride)
	}
}

func TestRunOptionsParseProviderOverride(t *testing.T) {
	var stdout, stderr bytes.Buffer
	opts, ok := runOptionsFromArgs([]string{"--provider-profile", "candidate-profile.yaml"}, &stdout, &stderr)
	if !ok {
		t.Fatalf("runOptionsFromArgs ok=false stderr=%q", stderr.String())
	}
	if opts.ProviderProfileOverride != "candidate-profile.yaml" {
		t.Fatalf("opts=%+v", opts)
	}
}

func TestRunOptionsRejectInvalidImageOverride(t *testing.T) {
	for _, args := range [][]string{{"--image"}, {"--image="}, {"--image", "--dry-run"}, {"--image=--dry-run"}} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			_, ok := runOptionsFromArgs(args, &stdout, &stderr)
			if ok || !strings.Contains(stderr.String(), "invalid --image") {
				t.Fatalf("runOptionsFromArgs(%v) ok=%t stderr=%q", args, ok, stderr.String())
			}
		})
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
