package events

import (
	"context"
	"encoding/json"
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
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
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

func TestClusterBackupCreateSupportsRepositoryBackedStart(t *testing.T) {
	driver := &clusterBackupCreateDriver{}
	result, err := executeClusterBackupCreate(context.Background(), driver, env.Environment{Name: "test", Driver: "compose"}, spec.ResolvedScenario{Cluster: spec.ClusterSpec{Nodes: 1}}, map[string]any{"repositoryId": "repo-minio", "archiveFormat": "tar", "captureProvider": "filesystem_immutable_copy", "quiesceReadProfile": "control_only", "waitTimeout": "15m"}, SystemBackupOperationResult{Operation: "cluster-backup-create", RepositoryID: "repo-minio", RepositoryBucket: "mycel-compose-backups", RepositoryPrefix: "runs/test"})
	if err != nil {
		t.Fatalf("executeClusterBackupCreate() error=%v", err)
	}
	if result.BackupSetID != "backup-set-test" || result.RepositoryID != "repo-minio" || result.RepositoryBackupSetPrefix != "runs/test/backup-sets/backup-set-test" {
		t.Fatalf("result=%+v, want repository-backed backup evidence", result)
	}
	joinedExecs := strings.Join(driver.execs, "\n")
	for _, want := range []string{"admin backup cluster start", "--repository-id repo-minio", "--capture-provider filesystem_immutable_copy", "--quiesce-read-profile control_only"} {
		if !strings.Contains(joinedExecs, want) {
			t.Fatalf("missing repository start fragment %q\n%s", want, joinedExecs)
		}
	}
	if strings.Contains(joinedExecs, "--output-dir") || strings.Contains(joinedExecs, "rm -rf") {
		t.Fatalf("repository-backed start must not require local output-dir staging:\n%s", joinedExecs)
	}
}

func TestClusterBackupValidateStagesHostArtifactsToValidationNode(t *testing.T) {
	hostDir := t.TempDir()
	writeFile(t, filepath.Join(hostDir, "backup-set.json"), `{"complete":true,"state":"succeeded","nodes":[{"applied_indexes":{"system":1}}]}`)
	writeFile(t, filepath.Join(hostDir, "COMPLETE"), `{"schema":"mycel.backup_complete","state":"COMPLETE"}`)
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
		filepath.Join(hostDir, "COMPLETE") + " -> /tmp/mycel-system-backups/COMPLETE",
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

func TestClusterBackupValidateSupportsNegativeFixtureExpectations(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "missing complete marker", body: `{"valid":false,"errors":["missing COMPLETE marker"]}`, want: "missing COMPLETE"},
		{name: "manifest checksum mismatch", body: `{"valid":false,"validation":{"errors":["manifest checksum mismatch"]}}`, want: "manifest checksum"},
		{name: "corrupt required artifact", body: `{"valid":false,"validation":{"blockers":[{"reason":"artifact_corrupt","detail":"required artifact checksum mismatch"}]}}`, want: "artifact_corrupt"},
		{name: "unsupported required feature", body: `{"valid":false,"validation":{"blockers":[{"reason":"unsupported_required_feature","detail":"future feature missing"}]}}`, want: "unsupported_required_feature"},
		{name: "topology mismatch", body: `{"valid":false,"validation":{"blockers":[{"reason":"topology_mismatch","detail":"expected 3 nodes got 2"}]}}`, want: "topology_mismatch"},
		{name: "raw secret artifact evidence", body: `{"valid":false,"validation":{"errors":["raw secret-like material detected in artifact evidence"]}}`, want: "raw secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hostDir := t.TempDir()
			writeFile(t, filepath.Join(hostDir, "backup-set.json"), `{"schema":"mycel.backup_set","manifest_version":1}`)
			writeFile(t, filepath.Join(hostDir, "myceld-0", "node-0.tar"), "archive-0")
			driver := &backupValidationDriver{validateStdout: tc.body}
			backup := SystemBackupOperationResult{Operation: "cluster-backup-create", BackupSetID: "backup-set-negative", BackupDir: "/tmp/mycel-system-backups", HostDir: hostDir, Artifacts: []SystemBackupArtifact{{PodName: "myceld-0", Ordinal: 0, ArchiveName: "node-0.tar", LocalArchive: filepath.Join(hostDir, "myceld-0", "node-0.tar")}}}
			result, err := executeClusterBackupValidate(context.Background(), driver, env.Environment{Name: "test", Driver: "k3d"}, map[string]SystemBackupOperationResult{"default": backup}, map[string]any{"ordinal": 0, "expectValid": false}, SystemBackupOperationResult{Operation: "cluster-backup-validate", BackupDir: "/tmp/mycel-system-backups"})
			if err != nil {
				t.Fatalf("executeClusterBackupValidate() error=%v", err)
			}
			if result.BackupSetID != "backup-set-negative" || len(result.ValidationErrors) == 0 || !strings.Contains(strings.Join(result.ValidationErrors, "; "), tc.want) {
				t.Fatalf("result=%+v, want validation error containing %q", result, tc.want)
			}
		})
	}
}

func TestClusterBackupValidateFailsWhenNegativeFixtureUnexpectedlyPasses(t *testing.T) {
	hostDir := t.TempDir()
	writeFile(t, filepath.Join(hostDir, "backup-set.json"), `{"complete":true,"state":"succeeded","nodes":[{"applied_indexes":{"system":1}}]}`)
	writeFile(t, filepath.Join(hostDir, "myceld-0", "node-0.tar"), "archive-0")
	driver := &backupValidationDriver{validateStdout: `{"valid":true,"errors":[]}`}
	backup := SystemBackupOperationResult{Operation: "cluster-backup-create", BackupSetID: "backup-set-positive", BackupDir: "/tmp/mycel-system-backups", HostDir: hostDir, Artifacts: []SystemBackupArtifact{{PodName: "myceld-0", Ordinal: 0, ArchiveName: "node-0.tar", LocalArchive: filepath.Join(hostDir, "myceld-0", "node-0.tar")}}}
	_, err := executeClusterBackupValidate(context.Background(), driver, env.Environment{Name: "test", Driver: "k3d"}, map[string]SystemBackupOperationResult{"default": backup}, map[string]any{"expectValid": false}, SystemBackupOperationResult{Operation: "cluster-backup-validate", BackupDir: "/tmp/mycel-system-backups"})
	if err == nil || !strings.Contains(err.Error(), "unexpectedly validated") {
		t.Fatalf("error=%v, want unexpected pass rejection", err)
	}
}

func TestClusterRestoreVerifyRetriesUntilRestoredAuthIsReady(t *testing.T) {
	driver := &restoreVerifyDriver{failuresRemaining: 2}
	resources := &provision.ScenarioResources{Assignments: []provision.ActorAssignment{{ActorID: "writer-0", Username: "writer", Password: "writer-pass", SpaceID: "space", DomainID: "domain"}}}
	fixture := &userBackupFixtureResult{nodeIDs: []string{"a", "b", "blob"}, edgeID: "edge", edgeFrom: "a", edgeTo: "b"}
	state := map[string]SystemBackupOperationResult{"default": {fixture: fixture}}
	counterCalls := 0
	result, err := executeClusterRestoreVerifyWithCounter(context.Background(), driver, env.Environment{Name: "test", Driver: "test"}, resources, state, map[string]any{"actorId": "writer-0", "verifyTimeout": "200ms", "verifyInterval": "1ms", "expectedNodes": 3, "expectedEdges": 1}, SystemBackupOperationResult{Operation: "cluster-restore-verify"}, func(context.Context, provision.ActorAssignment, int, userBackupFixtureResult) (int64, int64, error) {
		counterCalls++
		return 3, 1, nil
	})
	if err != nil {
		t.Fatalf("executeClusterRestoreVerify() error=%v", err)
	}
	if result.VerifiedPods != 1 || driver.execCalls != 3 || driver.deadlineCalls != 3 || !result.FixtureProved || result.FixtureNodes != 3 || result.FixtureEdges != 1 || counterCalls != 1 {
		t.Fatalf("result=%+v execCalls=%d counterCalls=%d, want endpoint retries and exact fixture proof", result, driver.execCalls, counterCalls)
	}
}

func TestClusterRestoreVerifyRejectsFixtureCountMismatch(t *testing.T) {
	driver := &restoreVerifyDriver{}
	resources := &provision.ScenarioResources{Assignments: []provision.ActorAssignment{{ActorID: "writer-0", Username: "writer", Password: "writer-pass", SpaceID: "space", DomainID: "domain"}}}
	fixture := &userBackupFixtureResult{nodeIDs: []string{"a", "b", "blob"}, edgeID: "edge", edgeFrom: "a", edgeTo: "b"}
	state := map[string]SystemBackupOperationResult{"default": {fixture: fixture}}
	_, err := executeClusterRestoreVerifyWithCounter(context.Background(), driver, env.Environment{Name: "test", Driver: "test"}, resources, state, map[string]any{"actorId": "writer-0", "verifyTimeout": "200ms", "verifyInterval": "1ms", "expectedNodes": 3, "expectedEdges": 1}, SystemBackupOperationResult{Operation: "cluster-restore-verify"}, func(context.Context, provision.ActorAssignment, int, userBackupFixtureResult) (int64, int64, error) {
		return 4, 1, nil
	})
	if err == nil || !strings.Contains(err.Error(), "counts do not match") {
		t.Fatalf("error=%v, want exact backed-up fixture count rejection", err)
	}
}

func TestSystemBackupFixtureCheckpointDoesNotMarshalIdentities(t *testing.T) {
	result := SystemBackupOperationResult{Operation: "system-backup-fixture", FixtureNodes: 3, FixtureEdges: 1, fixture: &userBackupFixtureResult{nodeIDs: []string{"secret-node"}, edgeID: "secret-edge", edgeFrom: "secret-from", edgeTo: "secret-to"}}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	if !strings.Contains(text, `"fixtureNodes":3`) || !strings.Contains(text, `"fixtureEdges":1`) || strings.Contains(text, "secret-") {
		t.Fatalf("unsafe or incomplete fixture evidence: %s", text)
	}
}

func TestSystemBackupFixtureIdentityRequiresExactNodesAndEdge(t *testing.T) {
	fixture := userBackupFixtureResult{nodeIDs: []string{"a", "b", "blob"}, edgeID: "edge", edgeFrom: "a", edgeTo: "b"}
	nodes := []*clientv1.Node{{NodeId: "a"}, {NodeId: "b"}, {NodeId: "blob"}}
	edge := &clientv1.Edge{EdgeId: "edge", FromNodeId: "a", ToNodeId: "b"}
	if !systemBackupFixtureIdentityMatches(fixture, nodes, edge) {
		t.Fatal("exact restored fixture identity rejected")
	}
	edge.ToNodeId = "other"
	if systemBackupFixtureIdentityMatches(fixture, nodes, edge) {
		t.Fatal("changed restored edge identity accepted")
	}
}

func TestRestoreCLICompatibilityErrorExplainsStaleImage(t *testing.T) {
	err := restoreCLICompatibilityError(errors.New("kubectl exec failed"), env.ExecResult{Stderr: "unknown flag: --backup-set"})
	if err == nil || !strings.Contains(err.Error(), "rebuild/import an image") || !strings.Contains(err.Error(), "MycelDB/mycel#156") {
		t.Fatalf("restoreCLICompatibilityError() = %v, want stale image hint", err)
	}
}

func TestRepositoryNegativeChecksCoverRepositoryTruthChaos(t *testing.T) {
	driver := &repositoryNegativeDriver{}
	backup := SystemBackupOperationResult{
		BackupSetID:               "backup-set-good",
		RepositoryID:              "repo-minio",
		RepositoryBucket:          "mycel-backups",
		RepositoryPrefix:          "runs/test",
		RepositoryBackupSetPrefix: "runs/test/backup-sets/backup-set-good",
		Artifacts: []SystemBackupArtifact{
			{PodName: "myceld-0", Ordinal: 0, ArchiveName: "node-0.tar", ArchiveURI: "s3://mycel-backups/runs/test/backup-sets/backup-set-good/nodes/myceld-0/node-0.tar", ManifestName: "node-0.manifest.json", ManifestURI: "s3://mycel-backups/runs/test/backup-sets/backup-set-good/nodes/myceld-0/node-0.manifest.json"},
			{PodName: "myceld-1", Ordinal: 1, ArchiveName: "node-1.tar", ArchiveURI: "s3://mycel-backups/runs/test/backup-sets/backup-set-good/nodes/myceld-1/node-1.tar", ManifestName: "node-1.manifest.json", ManifestURI: "s3://mycel-backups/runs/test/backup-sets/backup-set-good/nodes/myceld-1/node-1.manifest.json"},
		},
	}
	checks, err := executeRepositoryNegativeChecks(context.Background(), driver, env.Environment{Name: "test", Driver: "test"}, backup, map[string]any{"ordinal": 0})
	if err != nil {
		t.Fatalf("executeRepositoryNegativeChecks() error=%v", err)
	}
	wantNames := map[string]bool{"missing-complete": true, "corrupt-required-manifest": true, "corrupt-complete-marker": true, "interrupted-publish-after-node-artifacts": true, "partial-node-upload": true}
	if len(checks) != len(wantNames) {
		t.Fatalf("checks=%+v, want %d checks", checks, len(wantNames))
	}
	for _, check := range checks {
		if !wantNames[check.Name] {
			t.Fatalf("unexpected check name %q in %+v", check.Name, checks)
		}
		delete(wantNames, check.Name)
		if check.Listed || check.Valid || len(check.Errors) == 0 || check.FixturePrefix == "" || len(check.FixtureObjects) == 0 {
			t.Fatalf("check=%+v, want unlisted invalid fixture with diagnostics", check)
		}
	}
	if len(wantNames) != 0 {
		t.Fatalf("missing checks: %+v", wantNames)
	}
	joinedObjects := strings.Join(driver.objectCommands, "\n")
	for _, want := range []string{"missing-complete", "corrupt-manifest", "corrupt-complete", "interrupted-publish", "partial-node-upload", "mc rm --recursive --force"} {
		if !strings.Contains(joinedObjects, want) {
			t.Fatalf("object-store commands missing %q\n%s", want, joinedObjects)
		}
	}
	if driver.listCalls != len(checks) || driver.validateCalls != len(checks) {
		t.Fatalf("listCalls=%d validateCalls=%d checks=%d", driver.listCalls, driver.validateCalls, len(checks))
	}
}

func TestClusterRestoreApplyUsesRestorePlanAndRestoreLocalPrimitives(t *testing.T) {
	driver := &restoreApplyDriver{}
	hostDir := t.TempDir()
	writeFile(t, filepath.Join(hostDir, "backup-set.json"), `{"complete":true,"state":"succeeded","nodes":[{"applied_indexes":{"system":1}}]}`)
	fixture := &userBackupFixtureResult{nodeIDs: []string{"a", "b", "blob"}, edgeID: "edge", edgeFrom: "a", edgeTo: "b"}
	backup := SystemBackupOperationResult{BackupDir: "/tmp/mycel-system-backups", HostDir: hostDir, fixture: fixture, Artifacts: []SystemBackupArtifact{
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
	if result.HostDir != hostDir || result.BackupDir != backup.BackupDir || result.fixture != fixture {
		t.Fatalf("restore did not retain cleanup/checkpoint state: host=%q backup=%q fixture=%p", result.HostDir, result.BackupDir, result.fixture)
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
		"restore-local:data-myceld-0:0:myceldb/mycel:latest:2",
		"restore-local:data-myceld-1:1:myceldb/mycel:latest:2",
		"scale:myceld:2",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("restore operations missing %q\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "scale:myceld:0") {
		t.Fatalf("reset-namespace restore should not create then scale down a fresh StatefulSet before PVC restore:\n%s", joined)
	}
	firstRestore := strings.Index(joined, "restore-local:data-myceld-0")
	lastApply := strings.LastIndex(joined, "apply-yaml")
	if firstRestore < 0 || lastApply < 0 || firstRestore > lastApply {
		t.Fatalf("full manifest apply must happen after PVC restore so fresh daemons cannot initialize clean PVCs first:\n%s", joined)
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
	if strings.Contains(cmd, "admin backup cluster start") && strings.Contains(cmd, "--repository-id") {
		return executil.Result{Command: cmd, Stdout: `{"status":{"backup_set_id":"backup-set-test","state":"succeeded","expected_nodes":1,"repository_id":"repo-minio","manifest_uri":"s3://mycel-compose-backups/runs/test/backup-sets/backup-set-test/backup-set.json","raft_barriers":{"system":1},"nodes":[{"pod_name":"myceld-0","ordinal":0,"archive_name":"node-0.tar","archive_uri":"s3://mycel-compose-backups/runs/test/backup-sets/backup-set-test/nodes/myceld-0/node-0.tar","manifest_name":"node-0.manifest.json","manifest_uri":"s3://mycel-compose-backups/runs/test/backup-sets/backup-set-test/nodes/myceld-0/node-0.manifest.json","applied_indexes":{"system":1}}]}}`}, nil
	}
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
	deadlineCalls     int
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
func (d *restoreVerifyDriver) Exec(ctx context.Context, _ env.Environment, _ env.NodeRef, _ env.ExecRequest) (env.ExecResult, error) {
	d.execCalls++
	if _, ok := ctx.Deadline(); ok {
		d.deadlineCalls++
	}
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
	execs          []string
	copies         []string
	validateStdout string
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
		stdout := d.validateStdout
		if strings.TrimSpace(stdout) == "" {
			stdout = `{"valid":true,"errors":[]}`
		}
		return executil.Result{Command: cmd, Stdout: stdout}, nil
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

type repositoryNegativeDriver struct {
	objectCommands []string
	listCalls      int
	validateCalls  int
}

func (d *repositoryNegativeDriver) Name() string { return "test" }
func (d *repositoryNegativeDriver) Capabilities() env.CapabilitySet {
	return env.NewCapabilitySet(env.CapabilityNodeExec, env.CapabilityObjectStoreFixture)
}
func (d *repositoryNegativeDriver) Validate(spec.EnvironmentSpec) error                   { return nil }
func (d *repositoryNegativeDriver) Preflight(context.Context, spec.EnvironmentSpec) error { return nil }
func (d *repositoryNegativeDriver) Create(context.Context, spec.ResolvedScenario, spec.EnvironmentSpec) (env.Environment, error) {
	return env.Environment{Name: "test", Driver: "test"}, nil
}
func (d *repositoryNegativeDriver) WaitReady(context.Context, env.Environment) error { return nil }
func (d *repositoryNegativeDriver) Nodes(context.Context, env.Environment) ([]env.Node, error) {
	return []env.Node{{Name: "myceld-0", Ordinal: 0}}, nil
}
func (d *repositoryNegativeDriver) Endpoints(context.Context, env.Environment) ([]env.Endpoint, error) {
	return nil, nil
}
func (d *repositoryNegativeDriver) Exec(_ context.Context, _ env.Environment, _ env.NodeRef, req env.ExecRequest) (env.ExecResult, error) {
	cmd := strings.Join(req.Command, " ")
	if strings.Contains(cmd, "admin backup cluster list") {
		d.listCalls++
		return env.ExecResult{Command: cmd, Stdout: `{"backup_sets":[]}`}, nil
	}
	if strings.Contains(cmd, "admin backup cluster validate") {
		d.validateCalls++
		return env.ExecResult{Command: cmd, Stdout: `{"valid":false,"validation":{"errors":["injected repository chaos fixture is invalid"]}}`}, nil
	}
	return env.ExecResult{Command: cmd}, nil
}
func (d *repositoryNegativeDriver) RestartNode(context.Context, env.Environment, env.NodeRef) error {
	return nil
}
func (d *repositoryNegativeDriver) RollingRestart(context.Context, env.Environment) error {
	return nil
}
func (d *repositoryNegativeDriver) Delete(context.Context, env.Environment) error { return nil }
func (d *repositoryNegativeDriver) CaptureState(context.Context, env.Environment, *artifacts.Sink) error {
	return nil
}
func (d *repositoryNegativeDriver) ExecObjectStore(_ context.Context, _ env.Environment, command string) (env.ExecResult, error) {
	d.objectCommands = append(d.objectCommands, command)
	return env.ExecResult{Command: command, Stdout: "ok"}, nil
}
