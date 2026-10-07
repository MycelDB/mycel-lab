package events

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/executil"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestClusterBackupCreateUsesAsyncStartWait(t *testing.T) {
	hostDir := t.TempDir()
	driver := &clusterBackupCreateDriver{}
	result, err := executeClusterBackupCreate(context.Background(), driver, env.Environment{Name: "test", Driver: "k3d"}, spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 1}}, map[string]any{"backupDir": "/tmp/mycel-system-backups", "hostDir": hostDir, "archiveFormat": "tar", "reason": "test backup", "waitTimeout": "15m", "convergenceTimeout": "10m", "idempotencyKey": "retry-key"}, SystemBackupOperationResult{Operation: "cluster-backup-create", BackupDir: "/tmp/mycel-system-backups", HostDir: hostDir})
	if err != nil {
		t.Fatalf("executeClusterBackupCreate() error=%v", err)
	}
	if result.BackupSetID != "backup-set-test" || len(result.Artifacts) != 1 {
		t.Fatalf("result=%+v, want backup-set-test with one artifact", result)
	}
	joinedExecs := strings.Join(driver.execs, "\n")
	for _, want := range []string{"admin backup cluster start", "--wait", "--timeout 15m0s", "--convergence-timeout 10m0s", "--idempotency-key retry-key"} {
		if !strings.Contains(joinedExecs, want) {
			t.Fatalf("missing async start command fragment %q\n%s", want, joinedExecs)
		}
	}
	if strings.Contains(joinedExecs, "admin backup cluster trigger") {
		t.Fatalf("must not use removed trigger command:\n%s", joinedExecs)
	}
	if _, err := os.Stat(filepath.Join(hostDir, "backup-set.json")); err != nil {
		t.Fatalf("backup-set.json not staged: %v", err)
	}
	if _, err := os.Stat(filepath.Join(hostDir, "myceld-0", "node-0.tar")); err != nil {
		t.Fatalf("archive not staged: %v", err)
	}
}

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

func TestClusterRestoreVerifyRetriesUntilRestoredAuthIsReady(t *testing.T) {
	driver := &restoreVerifyDriver{failuresRemaining: 2}
	resources := &provision.ScenarioResources{Assignments: []provision.ActorAssignment{{ActorID: "writer-0", Username: "writer", Password: "writer-pass", SpaceID: "space", DomainID: "domain"}}}
	result, err := executeClusterRestoreVerify(context.Background(), driver, env.Environment{Name: "test", Driver: "test"}, resources, nil, map[string]any{"actorId": "writer-0", "verifyTimeout": "200ms", "verifyInterval": "1ms"}, SystemBackupOperationResult{Operation: "cluster-restore-verify"})
	if err != nil {
		t.Fatalf("executeClusterRestoreVerify() error=%v", err)
	}
	if result.VerifiedPods != 1 || driver.execCalls != 3 {
		t.Fatalf("verifiedPods=%d execCalls=%d, want 1 pod after retries", result.VerifiedPods, driver.execCalls)
	}
}

func TestRestoreCLICompatibilityErrorExplainsStaleImage(t *testing.T) {
	err := restoreCLICompatibilityError(errors.New("kubectl exec failed"), env.ExecResult{Stderr: "unknown flag: --backup-set"})
	if err == nil || !strings.Contains(err.Error(), "rebuild/import an image") || !strings.Contains(err.Error(), "MycelDB/mycel#156") {
		t.Fatalf("restoreCLICompatibilityError() = %v, want stale image hint", err)
	}
}

func TestClusterRestoreApplyUsesRestorePlanAndRestoreLocalPrimitives(t *testing.T) {
	driver := &restoreApplyDriver{}
	hostDir := t.TempDir()
	writeFile(t, filepath.Join(hostDir, "backup-set.json"), `{"complete":true,"state":"succeeded","nodes":[{"applied_indexes":{"system":1}}]}`)
	backup := SystemBackupOperationResult{BackupDir: "/tmp/mycel-system-backups", HostDir: hostDir, Artifacts: []SystemBackupArtifact{
		{PodName: "myceld-0", Ordinal: 0, ArchiveName: "node-0.tar", ManifestName: "node-0.manifest.json", LocalArchive: "/tmp/node-0.tar", LocalManifest: "/tmp/node-0.manifest.json"},
		{PodName: "myceld-1", Ordinal: 1, ArchiveName: "node-1.tar", ManifestName: "node-1.manifest.json", LocalArchive: "/tmp/node-1.tar", LocalManifest: "/tmp/node-1.manifest.json"},
	}}
	scenario := spec.ResolvedScenario{
		Environment: spec.EnvironmentSpec{Namespace: "test-ns"},
		Cluster:     spec.ClusterSpec{Nodes: 2, Image: "myceldb/mycel:latest", Raft: spec.RaftSpec{NodeCount: 2, PartitionCount: 8, ReplicaFactor: 2}},
	}
	result, err := executeClusterRestoreApply(context.Background(), driver, env.Environment{Name: "test", Driver: "k3d", Namespace: "test-ns", Context: "k3d-test"}, scenario, map[string]SystemBackupOperationResult{"default": backup}, map[string]any{"statefulSet": "myceld"}, SystemBackupOperationResult{Operation: "cluster-restore-apply"})
	if err != nil {
		t.Fatalf("executeClusterRestoreApply() error=%v", err)
	}
	if len(result.OldPVCUIDs) != 2 || len(result.NewPVCUIDs) != 2 {
		t.Fatalf("result PVC evidence old=%v new=%v", result.OldPVCUIDs, result.NewPVCUIDs)
	}
	if !strings.Contains(result.RestorePlan, "restore plan") || len(result.RestoreResults) != 2 {
		t.Fatalf("missing restore-plan/results evidence: plan=%q results=%+v", result.RestorePlan, result.RestoreResults)
	}
	joined := strings.Join(driver.ops, "\n")
	for _, want := range []string{
		"exec:mycel --output json admin backup cluster restore-plan --backup-set /tmp/mycel-system-backups",
		"pvcuids:myceld:2",
		"reset-namespace",
		"apply-yaml",
		"wait-pvcs:myceld:2",
		"scale:myceld:0",
		"restore-local:data-myceld-0:0:myceldb/mycel:latest:2",
		"restore-local:data-myceld-1:1:myceldb/mycel:latest:2",
		"scale:myceld:2",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("restore operations missing %q\n%s", want, joined)
		}
	}
	if strings.Index(joined, "wait-pvcs:myceld:2") > strings.Index(joined, "scale:myceld:0") {
		t.Fatalf("replacement PVC wait must happen before scale-down:\n%s", joined)
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

type clusterBackupCreateDriver struct {
	execs []string
}

func (d *clusterBackupCreateDriver) Name() string { return "test" }
func (d *clusterBackupCreateDriver) Capabilities() env.CapabilitySet {
	return env.NewCapabilitySet(env.CapabilityNodeExec)
}
func (d *clusterBackupCreateDriver) Validate(spec.EnvironmentSpec) error { return nil }
func (d *clusterBackupCreateDriver) Preflight(context.Context, spec.EnvironmentSpec) error {
	return nil
}
func (d *clusterBackupCreateDriver) Create(context.Context, spec.ResolvedScenario, spec.EnvironmentSpec) (env.Environment, error) {
	return env.Environment{Name: "test", Driver: "test"}, nil
}
func (d *clusterBackupCreateDriver) WaitReady(context.Context, env.Environment) error { return nil }
func (d *clusterBackupCreateDriver) Nodes(context.Context, env.Environment) ([]env.Node, error) {
	return []env.Node{{Name: "myceld-0", Ordinal: 0}}, nil
}
func (d *clusterBackupCreateDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	return nil, nil
}
func (d *clusterBackupCreateDriver) Exec(_ context.Context, _ env.Environment, _ env.NodeRef, req env.ExecRequest) (env.ExecResult, error) {
	cmd := strings.Join(req.Command, " ")
	d.execs = append(d.execs, cmd)
	if strings.Contains(cmd, "admin backup cluster start") {
		return executil.Result{Command: cmd, Stdout: `{"status":{"backup_set_id":"backup-set-test","state":"succeeded","expected_nodes":1,"manifest_uri":"file:///tmp/mycel-system-backups/backup-set.json","raft_barriers":{"system":1},"nodes":[{"pod_name":"myceld-0","ordinal":0,"archive_name":"node-0.tar","manifest_name":"node-0.manifest.json","applied_indexes":{"system":1}}]}}`}, nil
	}
	return executil.Result{Command: cmd, Stdout: "ok"}, nil
}
func (d *clusterBackupCreateDriver) RestartNode(context.Context, env.Environment, env.NodeRef) error {
	return nil
}
func (d *clusterBackupCreateDriver) RollingRestart(context.Context, env.Environment) error {
	return nil
}
func (d *clusterBackupCreateDriver) Delete(context.Context, env.Environment) error { return nil }
func (d *clusterBackupCreateDriver) CaptureState(context.Context, env.Environment, *artifacts.Sink) error {
	return nil
}
func (d *clusterBackupCreateDriver) CopyFromNode(_ context.Context, _ env.Environment, _ env.NodeRef, containerPath, hostPath string) (env.ExecResult, error) {
	content := "archive"
	if strings.HasSuffix(containerPath, "backup-set.json") {
		content = `{"complete":true,"state":"succeeded","nodes":[{"applied_indexes":{"system":1}}]}`
	} else if strings.HasSuffix(containerPath, ".manifest.json") {
		content = `{}`
	}
	if err := os.MkdirAll(filepath.Dir(hostPath), 0o755); err != nil {
		return env.ExecResult{}, err
	}
	return env.ExecResult{Command: "copy"}, os.WriteFile(hostPath, []byte(content), 0o600)
}
func (d *clusterBackupCreateDriver) CopyToNode(context.Context, env.Environment, env.NodeRef, string, string) (env.ExecResult, error) {
	return env.ExecResult{Command: "copy"}, nil
}

type restoreVerifyDriver struct {
	failuresRemaining int
	execCalls         int
}

func (d *restoreVerifyDriver) Name() string { return "test" }
func (d *restoreVerifyDriver) Capabilities() env.CapabilitySet {
	return env.NewCapabilitySet(env.CapabilityNodeExec)
}
func (d *restoreVerifyDriver) Validate(spec.EnvironmentSpec) error { return nil }
func (d *restoreVerifyDriver) Preflight(context.Context, spec.EnvironmentSpec) error {
	return nil
}
func (d *restoreVerifyDriver) Create(context.Context, spec.ResolvedScenario, spec.EnvironmentSpec) (env.Environment, error) {
	return env.Environment{Name: "test", Driver: "test"}, nil
}
func (d *restoreVerifyDriver) WaitReady(context.Context, env.Environment) error { return nil }
func (d *restoreVerifyDriver) Nodes(context.Context, env.Environment) ([]env.Node, error) {
	return []env.Node{{Name: "myceld-0", Ordinal: 0}}, nil
}
func (d *restoreVerifyDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	return nil, nil
}
func (d *restoreVerifyDriver) Exec(context.Context, env.Environment, env.NodeRef, env.ExecRequest) (env.ExecResult, error) {
	d.execCalls++
	if d.failuresRemaining > 0 {
		d.failuresRemaining--
		return env.ExecResult{}, errors.New("login principal: invalid credentials")
	}
	return env.ExecResult{Stdout: `{"rows":[{"count":1}]}`}, nil
}
func (d *restoreVerifyDriver) RestartNode(context.Context, env.Environment, env.NodeRef) error {
	return nil
}
func (d *restoreVerifyDriver) RollingRestart(context.Context, env.Environment) error {
	return nil
}
func (d *restoreVerifyDriver) Delete(context.Context, env.Environment) error { return nil }
func (d *restoreVerifyDriver) CaptureState(context.Context, env.Environment, *artifacts.Sink) error {
	return nil
}

type restoreApplyDriver struct {
	ops      []string
	uidCalls int
}

func (d *restoreApplyDriver) Name() string { return "test" }
func (d *restoreApplyDriver) Capabilities() env.CapabilitySet {
	return env.NewCapabilitySet(env.CapabilityVolumeReplacement)
}
func (d *restoreApplyDriver) Validate(spec.EnvironmentSpec) error                   { return nil }
func (d *restoreApplyDriver) Preflight(context.Context, spec.EnvironmentSpec) error { return nil }
func (d *restoreApplyDriver) Create(context.Context, spec.ResolvedScenario, spec.EnvironmentSpec) (env.Environment, error) {
	return env.Environment{Name: "test", Driver: "test"}, nil
}
func (d *restoreApplyDriver) WaitReady(context.Context, env.Environment) error { return nil }
func (d *restoreApplyDriver) Nodes(context.Context, env.Environment) ([]env.Node, error) {
	return []env.Node{{Name: "myceld-0", Ordinal: 0}, {Name: "myceld-1", Ordinal: 1}}, nil
}
func (d *restoreApplyDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	return nil, nil
}
func (d *restoreApplyDriver) Exec(_ context.Context, _ env.Environment, _ env.NodeRef, req env.ExecRequest) (env.ExecResult, error) {
	cmd := strings.Join(req.Command, " ")
	d.ops = append(d.ops, "exec:"+cmd)
	if strings.Contains(cmd, "admin backup cluster restore-plan") {
		return env.ExecResult{Command: cmd, Stdout: `{"backup_set_id":"backup-set-test","warnings":["restore plan"],"nodes":[]}`}, nil
	}
	return env.ExecResult{Command: cmd}, nil
}
func (d *restoreApplyDriver) RestartNode(context.Context, env.Environment, env.NodeRef) error {
	return nil
}
func (d *restoreApplyDriver) RollingRestart(context.Context, env.Environment) error { return nil }
func (d *restoreApplyDriver) Delete(context.Context, env.Environment) error         { return nil }
func (d *restoreApplyDriver) CaptureState(context.Context, env.Environment, *artifacts.Sink) error {
	return nil
}
func (d *restoreApplyDriver) PVCUIDs(_ context.Context, _ env.Environment, statefulSet string, count int) (map[string]string, error) {
	d.uidCalls++
	d.ops = append(d.ops, "pvcuids:"+statefulSet+":"+strconv.Itoa(count))
	prefix := "old-"
	if d.uidCalls > 1 {
		prefix = "new-"
	}
	out := map[string]string{}
	for i := 0; i < count; i++ {
		name := "data-" + statefulSet + "-" + strconv.Itoa(i)
		out[name] = prefix + name
	}
	return out, nil
}
func (d *restoreApplyDriver) WaitPVCs(_ context.Context, _ env.Environment, statefulSet string, count int) error {
	d.ops = append(d.ops, "wait-pvcs:"+statefulSet+":"+strconv.Itoa(count))
	return nil
}
func (d *restoreApplyDriver) ResetNamespace(context.Context, env.Environment) error {
	d.ops = append(d.ops, "reset-namespace")
	return nil
}
func (d *restoreApplyDriver) ApplyYAML(context.Context, env.Environment, string) (env.ExecResult, error) {
	d.ops = append(d.ops, "apply-yaml")
	return env.ExecResult{}, nil
}
func (d *restoreApplyDriver) ScaleStatefulSet(_ context.Context, _ env.Environment, statefulSet string, replicas int) error {
	d.ops = append(d.ops, "scale:"+statefulSet+":"+strconv.Itoa(replicas))
	return nil
}
func (d *restoreApplyDriver) DeletePVC(_ context.Context, _ env.Environment, pvcName string) error {
	d.ops = append(d.ops, "delete-pvc:"+pvcName)
	return nil
}

func (d *restoreApplyDriver) RestoreArchiveToPVC(_ context.Context, _ env.Environment, pvcName, archivePath string) error {
	d.ops = append(d.ops, "restore-archive:"+pvcName+":"+archivePath)
	return nil
}

func (d *restoreApplyDriver) RestoreBackupSetOrdinalToPVC(_ context.Context, _ env.Environment, req env.RestoreBackupSetRequest) (env.ExecResult, error) {
	d.ops = append(d.ops, "restore-local:"+req.PVCName+":"+strconv.Itoa(req.Ordinal)+":"+req.Image+":"+strconv.Itoa(len(req.Artifacts)))
	return env.ExecResult{Stdout: `{"data_dir":"/data/mycel"}`}, nil
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
