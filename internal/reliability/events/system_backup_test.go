package events

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/executil"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestClusterBackupValidateStagesHostArtifactsToValidationNode(t *testing.T) {
	hostDir := t.TempDir()
	writeFile(t, filepath.Join(hostDir, "backup-set.json"), `{"complete":true,"state":"succeeded","nodes":[{"applied_indexes":{"system":1}}]}`)
	writeFile(t, filepath.Join(hostDir, "myceld-0", "node-0.tar"), "archive-0")
	writeFile(t, filepath.Join(hostDir, "myceld-0", "node-0.manifest.json"), `{}`)
	writeFile(t, filepath.Join(hostDir, "myceld-1", "node-1.tar"), "archive-1")

	driver := &backupValidationDriver{}
	backup := SystemBackupOperationResult{
		Operation:   "cluster-backup-create",
		BackupSetID: "backup-set-test",
		BackupDir:   "/tmp/mycel-system-backups",
		HostDir:     hostDir,
		Artifacts: []SystemBackupArtifact{
			{PodName: "myceld-0", Ordinal: 0, ArchiveName: "node-0.tar", ManifestName: "node-0.manifest.json", LocalArchive: filepath.Join(hostDir, "myceld-0", "node-0.tar"), LocalManifest: filepath.Join(hostDir, "myceld-0", "node-0.manifest.json")},
			{PodName: "myceld-1", Ordinal: 1, ArchiveName: "node-1.tar", LocalArchive: filepath.Join(hostDir, "myceld-1", "node-1.tar")},
		},
	}
	result, err := executeClusterBackupValidate(context.Background(), driver, env.Environment{Name: "test", Driver: "k3d"}, map[string]SystemBackupOperationResult{"default": backup}, map[string]any{"ordinal": 0}, SystemBackupOperationResult{Operation: "cluster-backup-validate", BackupDir: "/tmp/mycel-system-backups"})
	if err != nil {
		t.Fatalf("executeClusterBackupValidate() error=%v", err)
	}
	if result.BackupSetID != "backup-set-test" || len(result.Artifacts) != 2 {
		t.Fatalf("result=%+v, want backup evidence carried forward", result)
	}
	joinedCopies := strings.Join(driver.copies, "\n")
	for _, want := range []string{
		filepath.Join(hostDir, "backup-set.json") + " -> /tmp/mycel-system-backups/backup-set.json",
		filepath.Join(hostDir, "myceld-0", "node-0.tar") + " -> /tmp/mycel-system-backups/node-0.tar",
		filepath.Join(hostDir, "myceld-0", "node-0.manifest.json") + " -> /tmp/mycel-system-backups/node-0.manifest.json",
		filepath.Join(hostDir, "myceld-1", "node-1.tar") + " -> /tmp/mycel-system-backups/node-1.tar",
	} {
		if !strings.Contains(joinedCopies, want) {
			t.Fatalf("missing staged copy %q\n%s", want, joinedCopies)
		}
	}
	joinedExecs := strings.Join(driver.execs, "\n")
	if !strings.Contains(joinedExecs, "mkdir -p '/tmp/mycel-system-backups'") || !strings.Contains(joinedExecs, "admin backup cluster validate --backup-set /tmp/mycel-system-backups") {
		t.Fatalf("missing expected execs:\n%s", joinedExecs)
	}
}

func writeFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

type backupValidationDriver struct {
	execs  []string
	copies []string
}

func (d *backupValidationDriver) Name() string { return "test" }
func (d *backupValidationDriver) Capabilities() env.CapabilitySet {
	return env.NewCapabilitySet(env.CapabilityNodeExec)
}
func (d *backupValidationDriver) Validate(spec.EnvironmentSpec) error                   { return nil }
func (d *backupValidationDriver) Preflight(context.Context, spec.EnvironmentSpec) error { return nil }
func (d *backupValidationDriver) Create(context.Context, spec.ResolvedScenario, spec.EnvironmentSpec) (env.Environment, error) {
	return env.Environment{Name: "test", Driver: "test"}, nil
}
func (d *backupValidationDriver) WaitReady(context.Context, env.Environment) error { return nil }
func (d *backupValidationDriver) Nodes(context.Context, env.Environment) ([]env.Node, error) {
	return []env.Node{{Name: "myceld-0", Ordinal: 0}}, nil
}
func (d *backupValidationDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	return nil, nil
}
func (d *backupValidationDriver) Exec(_ context.Context, _ env.Environment, _ env.NodeRef, req env.ExecRequest) (env.ExecResult, error) {
	cmd := strings.Join(req.Command, " ")
	d.execs = append(d.execs, cmd)
	if strings.Contains(cmd, "admin backup cluster validate") {
		return executil.Result{Command: cmd, Stdout: `{"valid":true,"errors":[]}`}, nil
	}
	return executil.Result{Command: cmd, Stdout: "ok"}, nil
}
func (d *backupValidationDriver) RestartNode(context.Context, env.Environment, env.NodeRef) error {
	return nil
}
func (d *backupValidationDriver) RollingRestart(context.Context, env.Environment) error { return nil }
func (d *backupValidationDriver) Delete(context.Context, env.Environment) error         { return nil }
func (d *backupValidationDriver) CaptureState(context.Context, env.Environment, *artifacts.Sink) error {
	return nil
}
func (d *backupValidationDriver) CopyFromNode(context.Context, env.Environment, env.NodeRef, string, string) (env.ExecResult, error) {
	return env.ExecResult{}, nil
}
func (d *backupValidationDriver) CopyToNode(_ context.Context, _ env.Environment, _ env.NodeRef, hostPath, containerPath string) (env.ExecResult, error) {
	d.copies = append(d.copies, hostPath+" -> "+containerPath)
	return env.ExecResult{Command: "copy"}, nil
}
