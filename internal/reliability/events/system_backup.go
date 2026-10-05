package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/deploy"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type SystemBackupOperationResult struct {
	Operation    string                 `json:"operation"`
	BackupSetID  string                 `json:"backupSetId,omitempty"`
	BackupDir    string                 `json:"backupDir,omitempty"`
	HostDir      string                 `json:"hostDir,omitempty"`
	Artifacts    []SystemBackupArtifact `json:"artifacts,omitempty"`
	OldPVCUIDs   map[string]string      `json:"oldPvcUids,omitempty"`
	NewPVCUIDs   map[string]string      `json:"newPvcUids,omitempty"`
	VerifiedPods int                    `json:"verifiedPods,omitempty"`
	Stdout       string                 `json:"stdout,omitempty"`
	Stderr       string                 `json:"stderr,omitempty"`
}

type SystemBackupArtifact struct {
	PodName        string `json:"podName"`
	Ordinal        int    `json:"ordinal"`
	ArchiveName    string `json:"archiveName"`
	ManifestName   string `json:"manifestName,omitempty"`
	ChecksumSHA256 string `json:"checksumSha256,omitempty"`
	LocalArchive   string `json:"localArchive"`
	LocalManifest  string `json:"localManifest,omitempty"`
}

type clusterBackupStatusResponse struct {
	Status struct {
		BackupSetID   string                       `json:"backup_set_id"`
		State         string                       `json:"state"`
		ExpectedNodes int                          `json:"expected_nodes"`
		ManifestURI   string                       `json:"manifest_uri"`
		Error         string                       `json:"error"`
		FailedPhase   string                       `json:"failed_phase"`
		RaftBarriers  map[string]uint64            `json:"raft_barriers"`
		Blockers      []clusterBackupStatusBlocker `json:"blockers"`
		Nodes         []struct {
			PodName        string            `json:"pod_name"`
			Ordinal        int               `json:"ordinal"`
			ArchiveName    string            `json:"archive_name"`
			ManifestName   string            `json:"manifest_name"`
			ChecksumSHA256 string            `json:"checksum_sha256"`
			AppliedIndexes map[string]uint64 `json:"applied_indexes"`
		} `json:"nodes"`
	} `json:"status"`
}

type clusterBackupStatusBlocker struct {
	NodeName     string `json:"node_name"`
	NodeID       string `json:"node_id"`
	RaftNodeID   uint64 `json:"raft_node_id"`
	RaftGroup    string `json:"raft_group"`
	Reason       string `json:"reason"`
	AppliedIndex uint64 `json:"applied_index"`
	CommitIndex  uint64 `json:"commit_index"`
	Detail       string `json:"detail"`
}

type clusterBackupValidateResponse struct {
	Valid  bool     `json:"valid"`
	Errors []string `json:"errors"`
}

func executeSystemBackupEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, resources *provision.ScenarioResources, eventType string, target map[string]any, state map[string]SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	result := SystemBackupOperationResult{Operation: eventType, BackupDir: firstNonEmpty(stringTarget(target, "backupDir"), "/tmp/mycel-system-backups"), HostDir: expandTargetPath(firstNonEmpty(stringTarget(target, "hostDir"), filepath.Join(os.TempDir(), "{runID}-system-backup")), environment, resources)}
	switch eventType {
	case "system-backup-fixture":
		_, _, _, err := executeUserBackupFixture(ctx, driver, environment, env.NodeRef{Ordinal: intTarget(target, "ordinal", 0)}, resources, target)
		return result, err
	case "cluster-backup-create":
		return executeClusterBackupCreate(ctx, driver, environment, scenario, target, result)
	case "cluster-backup-validate":
		return executeClusterBackupValidate(ctx, driver, environment, state, target, result)
	case "cluster-restore-apply":
		return executeClusterRestoreApply(ctx, driver, environment, scenario, state, target, result)
	case "cluster-restore-verify":
		return executeClusterRestoreVerify(ctx, driver, environment, resources, state, target, result)
	default:
		return result, fmt.Errorf("unsupported system backup event type %q", eventType)
	}
}

func executeClusterBackupCreate(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, target map[string]any, result SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	stager, ok := driver.(env.NodeFileStager)
	if !ok {
		return result, fmt.Errorf("driver %q does not support node file staging", driver.Name())
	}
	nodes, err := driver.Nodes(ctx, environment)
	if err != nil {
		return result, err
	}
	if len(nodes) == 0 {
		return result, fmt.Errorf("no nodes available for cluster backup")
	}
	for _, node := range nodes {
		if _, err := driver.Exec(ctx, environment, env.NodeRef{Name: node.Name, Ordinal: node.Ordinal}, env.ExecRequest{Command: []string{"sh", "-ec", "rm -rf " + shellQuote(result.BackupDir) + " && mkdir -p " + shellQuote(result.BackupDir)}}); err != nil {
			return result, err
		}
	}
	reason := firstNonEmpty(stringTarget(target, "reason"), "native mycel-lab system backup/restore test")
	format := firstNonEmpty(stringTarget(target, "archiveFormat"), "tar")
	cmd := append(adminCLIBase(), "admin", "backup", "cluster", "start", "--output-dir", result.BackupDir, "--archive-format", format, "--reason", reason, "--wait")
	if waitTimeout := durationTarget(target, "waitTimeout", 0); waitTimeout > 0 {
		cmd = append(cmd, "--timeout", waitTimeout.String())
	}
	if convergenceTimeout := durationTarget(target, "convergenceTimeout", 0); convergenceTimeout > 0 {
		cmd = append(cmd, "--convergence-timeout", convergenceTimeout.String())
	}
	if idempotencyKey := stringTarget(target, "idempotencyKey"); idempotencyKey != "" {
		cmd = append(cmd, "--idempotency-key", idempotencyKey)
	}
	execResult, err := driver.Exec(ctx, environment, env.NodeRef{Name: nodes[0].Name, Ordinal: nodes[0].Ordinal}, env.ExecRequest{Command: cmd})
	result.Stdout = bounded(execResult.Stdout, 8192)
	result.Stderr = bounded(execResult.Stderr, 8192)
	if err != nil {
		return result, err
	}
	var response clusterBackupStatusResponse
	if err := json.Unmarshal([]byte(execResult.Stdout), &response); err != nil {
		return result, fmt.Errorf("decode cluster backup status response: %w", err)
	}
	status := response.Status
	result.BackupSetID = status.BackupSetID
	if strings.ToLower(status.State) != "succeeded" {
		return result, fmt.Errorf("cluster backup state %q phase=%s error=%s blockers=%s", status.State, status.FailedPhase, status.Error, formatClusterBackupBlockers(status.Blockers))
	}
	if want := scenario.Cluster.Nodes; want > 0 && (status.ExpectedNodes != want || len(status.Nodes) != want) {
		return result, fmt.Errorf("cluster backup expected %d nodes, status expected=%d artifacts=%d", want, status.ExpectedNodes, len(status.Nodes))
	}
	if len(status.RaftBarriers) == 0 {
		return result, fmt.Errorf("cluster backup status missing raft barrier evidence")
	}
	backupSetPath := filepath.Join(result.HostDir, "backup-set.json")
	if _, err := stager.CopyFromNode(ctx, environment, env.NodeRef{Name: nodes[0].Name, Ordinal: nodes[0].Ordinal}, filepath.Join(result.BackupDir, "backup-set.json"), backupSetPath); err != nil {
		return result, err
	}
	if err := validateBackupSetFile(backupSetPath); err != nil {
		return result, err
	}
	byPod := map[string]env.Node{}
	for _, node := range nodes {
		byPod[node.Name] = node
	}
	for _, node := range status.Nodes {
		pod := node.PodName
		if pod == "" {
			pod = fmt.Sprintf("myceld-%d", node.Ordinal)
		}
		envNode, ok := byPod[pod]
		if !ok {
			envNode = env.Node{Name: pod, Ordinal: node.Ordinal}
		}
		podDir := filepath.Join(result.HostDir, pod)
		archivePath := filepath.Join(podDir, node.ArchiveName)
		if _, err := stager.CopyFromNode(ctx, environment, env.NodeRef{Name: envNode.Name, Ordinal: envNode.Ordinal}, filepath.Join(result.BackupDir, node.ArchiveName), archivePath); err != nil {
			return result, err
		}
		if node.ChecksumSHA256 != "" {
			if err := verifyFileSHA256(archivePath, node.ChecksumSHA256); err != nil {
				return result, err
			}
		}
		artifact := SystemBackupArtifact{PodName: pod, Ordinal: node.Ordinal, ArchiveName: node.ArchiveName, ManifestName: node.ManifestName, ChecksumSHA256: node.ChecksumSHA256, LocalArchive: archivePath}
		if node.ManifestName != "" {
			manifestPath := filepath.Join(podDir, node.ManifestName)
			if _, err := stager.CopyFromNode(ctx, environment, env.NodeRef{Name: envNode.Name, Ordinal: envNode.Ordinal}, filepath.Join(result.BackupDir, node.ManifestName), manifestPath); err != nil {
				return result, err
			}
			artifact.LocalManifest = manifestPath
		}
		result.Artifacts = append(result.Artifacts, artifact)
	}
	sort.Slice(result.Artifacts, func(i, j int) bool { return result.Artifacts[i].Ordinal < result.Artifacts[j].Ordinal })
	return result, nil
}

func executeClusterBackupValidate(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, state map[string]SystemBackupOperationResult, target map[string]any, result SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	backup := state[systemBackupKey(target)]
	if backup.BackupDir != "" {
		result.BackupDir = backup.BackupDir
	}
	if backup.HostDir != "" {
		result.HostDir = backup.HostDir
	}
	if len(backup.Artifacts) > 0 {
		if err := stageClusterBackupForValidation(ctx, driver, environment, backup, env.NodeRef{Ordinal: intTarget(target, "ordinal", 0)}); err != nil {
			return result, err
		}
	}
	cmd := append(adminCLIBase(), "admin", "backup", "cluster", "validate", "--backup-set", result.BackupDir)
	execResult, err := driver.Exec(ctx, environment, env.NodeRef{Ordinal: intTarget(target, "ordinal", 0)}, env.ExecRequest{Command: cmd})
	result.Stdout = bounded(execResult.Stdout, 8192)
	result.Stderr = bounded(execResult.Stderr, 8192)
	if err != nil {
		return result, err
	}
	var response clusterBackupValidateResponse
	if err := json.Unmarshal([]byte(execResult.Stdout), &response); err != nil {
		return result, fmt.Errorf("decode cluster backup validate response: %w", err)
	}
	if !response.Valid {
		return result, fmt.Errorf("cluster backup set invalid: %s", strings.Join(response.Errors, "; "))
	}
	result.BackupSetID = backup.BackupSetID
	result.Artifacts = backup.Artifacts
	return result, nil
}

func stageClusterBackupForValidation(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, backup SystemBackupOperationResult, node env.NodeRef) error {
	stager, ok := driver.(env.NodeFileStager)
	if !ok {
		return nil
	}
	if backup.BackupDir == "" || backup.HostDir == "" {
		return nil
	}
	if _, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: []string{"sh", "-ec", "mkdir -p " + shellQuote(backup.BackupDir)}}); err != nil {
		return fmt.Errorf("prepare validation backup dir: %w", err)
	}
	backupSetPath := filepath.Join(backup.HostDir, "backup-set.json")
	if _, err := stager.CopyToNode(ctx, environment, node, backupSetPath, filepath.Join(backup.BackupDir, "backup-set.json")); err != nil {
		return fmt.Errorf("stage backup-set.json for validation: %w", err)
	}
	for _, artifact := range backup.Artifacts {
		if artifact.LocalArchive != "" && artifact.ArchiveName != "" {
			if _, err := stager.CopyToNode(ctx, environment, node, artifact.LocalArchive, filepath.Join(backup.BackupDir, artifact.ArchiveName)); err != nil {
				return fmt.Errorf("stage archive %s for validation: %w", artifact.PodName, err)
			}
		}
		if artifact.LocalManifest != "" && artifact.ManifestName != "" {
			if _, err := stager.CopyToNode(ctx, environment, node, artifact.LocalManifest, filepath.Join(backup.BackupDir, artifact.ManifestName)); err != nil {
				return fmt.Errorf("stage manifest %s for validation: %w", artifact.PodName, err)
			}
		}
	}
	return nil
}

func executeClusterRestoreApply(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, state map[string]SystemBackupOperationResult, target map[string]any, result SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	backup := state[systemBackupKey(target)]
	if len(backup.Artifacts) == 0 {
		return result, fmt.Errorf("no cluster backup artifacts recorded for %q", systemBackupKey(target))
	}
	vr, ok := driver.(env.VolumeReplacementDriver)
	if !ok {
		return result, fmt.Errorf("driver %q does not support volume replacement", driver.Name())
	}
	statefulSet := firstNonEmpty(stringTarget(target, "statefulSet"), "myceld")
	oldUIDs, err := vr.PVCUIDs(ctx, environment, statefulSet, scenario.Cluster.Nodes)
	if err != nil {
		return result, err
	}
	result.OldPVCUIDs = oldUIDs
	manifests, err := deploy.RenderKubernetesManifests(scenario)
	if err != nil {
		return result, err
	}
	if err := vr.ResetNamespace(ctx, environment); err != nil {
		return result, err
	}
	if _, err := vr.ApplyYAML(ctx, environment, manifests.YAML); err != nil {
		return result, err
	}
	if err := vr.WaitPVCs(ctx, environment, statefulSet, scenario.Cluster.Nodes); err != nil {
		return result, err
	}
	if err := vr.ScaleStatefulSet(ctx, environment, statefulSet, 0); err != nil {
		return result, err
	}
	newUIDs, err := vr.PVCUIDs(ctx, environment, statefulSet, scenario.Cluster.Nodes)
	if err != nil {
		return result, err
	}
	result.NewPVCUIDs = newUIDs
	if err := verifyPVCReplacement(oldUIDs, newUIDs); err != nil {
		return result, err
	}
	for _, artifact := range backup.Artifacts {
		pvcName := fmt.Sprintf("data-%s-%d", statefulSet, artifact.Ordinal)
		if err := vr.RestoreArchiveToPVC(ctx, environment, pvcName, artifact.LocalArchive); err != nil {
			return result, err
		}
	}
	if err := vr.ScaleStatefulSet(ctx, environment, statefulSet, scenario.Cluster.Nodes); err != nil {
		return result, err
	}
	result.BackupSetID = backup.BackupSetID
	result.Artifacts = backup.Artifacts
	return result, nil
}

func executeClusterRestoreVerify(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, state map[string]SystemBackupOperationResult, target map[string]any, result SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	actorID := firstNonEmpty(stringTarget(target, "actorId"), stringTarget(target, "sourceActorId"))
	assignment, ok := findAssignment(resources, actorID)
	if !ok {
		return result, fmt.Errorf("actor assignment %q not found", actorID)
	}
	nodes, err := driver.Nodes(ctx, environment)
	if err != nil {
		return result, err
	}
	verifyTimeout := durationTarget(target, "verifyTimeout", 5*time.Minute)
	verifyInterval := durationTarget(target, "verifyInterval", 5*time.Second)
	if verifyInterval <= 0 {
		verifyInterval = 5 * time.Second
	}
	verifyCtx, cancel := context.WithTimeout(ctx, verifyTimeout)
	defer cancel()
	verified := map[string]bool{}
	var lastErr error
	for len(verified) < len(nodes) {
		for _, node := range nodes {
			if verified[node.Name] {
				continue
			}
			cmd := append(principalCLIBase(assignment.Username, assignment.Password), "query", "gql", "--space-id", assignment.SpaceID, "--domain-id", assignment.DomainID, "MATCH (n) RETURN count(n)")
			if _, err := driver.Exec(verifyCtx, environment, env.NodeRef{Name: node.Name, Ordinal: node.Ordinal}, env.ExecRequest{Command: cmd}); err != nil {
				lastErr = fmt.Errorf("restored workload query on %s: %w", node.Name, err)
				continue
			}
			verified[node.Name] = true
		}
		if len(verified) == len(nodes) {
			break
		}
		select {
		case <-verifyCtx.Done():
			if lastErr != nil {
				return result, fmt.Errorf("restored workload query did not converge after %s: %w", verifyTimeout, lastErr)
			}
			return result, fmt.Errorf("restored workload query did not converge after %s: %w", verifyTimeout, verifyCtx.Err())
		case <-time.After(verifyInterval):
		}
	}
	result.VerifiedPods = len(verified)
	return result, nil
}

func systemBackupKey(target map[string]any) string {
	return firstNonEmpty(stringTarget(target, "backup"), stringTarget(target, "name"), "default")
}

func verifyPVCReplacement(oldUIDs, newUIDs map[string]string) error {
	if len(oldUIDs) == 0 || len(newUIDs) == 0 {
		return fmt.Errorf("missing PVC UID evidence old=%d new=%d", len(oldUIDs), len(newUIDs))
	}
	for name, oldUID := range oldUIDs {
		newUID := newUIDs[name]
		if strings.TrimSpace(oldUID) == "" || strings.TrimSpace(newUID) == "" {
			return fmt.Errorf("missing PVC UID for %s old=%q new=%q", name, oldUID, newUID)
		}
		if oldUID == newUID {
			return fmt.Errorf("PVC %s was not replaced; UID remained %s", name, oldUID)
		}
	}
	return nil
}

func durationTarget(target map[string]any, key string, fallback time.Duration) time.Duration {
	if target == nil {
		return fallback
	}
	switch value := target[key].(type) {
	case time.Duration:
		return value
	case string:
		if strings.TrimSpace(value) == "" {
			return fallback
		}
		parsed, err := time.ParseDuration(strings.TrimSpace(value))
		if err == nil {
			return parsed
		}
	case int:
		if value > 0 {
			return time.Duration(value) * time.Second
		}
	case int64:
		if value > 0 {
			return time.Duration(value) * time.Second
		}
	case float64:
		if value > 0 {
			return time.Duration(value) * time.Second
		}
	}
	return fallback
}

func formatClusterBackupBlockers(blockers []clusterBackupStatusBlocker) string {
	if len(blockers) == 0 {
		return ""
	}
	parts := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		label := firstNonEmpty(blocker.RaftGroup, blocker.NodeName, blocker.NodeID)
		if blocker.Reason != "" && label != "" {
			parts = append(parts, label+":"+blocker.Reason+":"+blocker.Detail)
			continue
		}
		parts = append(parts, firstNonEmpty(blocker.Detail, blocker.Reason, label))
	}
	return strings.Join(parts, "; ")
}

func validateBackupSetFile(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lower := strings.ToLower(string(raw))
	for _, forbidden := range []string{"password", "access_token", "refresh_token", "auth_session", "secret"} {
		if strings.Contains(lower, forbidden) {
			return fmt.Errorf("backup-set metadata contains forbidden secret/session material")
		}
	}
	var doc struct {
		Complete bool   `json:"complete"`
		State    string `json:"state"`
		Nodes    []struct {
			RaftFreeze     any               `json:"raft_freeze"`
			AppliedIndexes map[string]uint64 `json:"applied_indexes"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}
	if !doc.Complete || strings.ToLower(doc.State) != "succeeded" || len(doc.Nodes) == 0 {
		return fmt.Errorf("backup-set metadata is not complete/succeeded")
	}
	for i, node := range doc.Nodes {
		if node.RaftFreeze == nil && len(node.AppliedIndexes) == 0 {
			return fmt.Errorf("backup-set node %d missing raft freeze/checkpoint evidence", i)
		}
	}
	return nil
}

func verifyFileSHA256(path, want string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(got, want) {
		return fmt.Errorf("sha256 mismatch for %s: got %s want %s", path, got, want)
	}
	return nil
}
