package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/deploy"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SystemBackupOperationResult struct {
	Operation                 string                               `json:"operation"`
	BackupSetID               string                               `json:"backupSetId,omitempty"`
	BackupDir                 string                               `json:"backupDir,omitempty"`
	HostDir                   string                               `json:"hostDir,omitempty"`
	RepositoryID              string                               `json:"repositoryId,omitempty"`
	RepositoryBucket          string                               `json:"repositoryBucket,omitempty"`
	RepositoryPrefix          string                               `json:"repositoryPrefix,omitempty"`
	RepositoryBackupSetPrefix string                               `json:"repositoryBackupSetPrefix,omitempty"`
	ManifestURI               string                               `json:"manifestUri,omitempty"`
	CompleteMarkerURI         string                               `json:"completeMarkerUri,omitempty"`
	RepositoryKeys            []string                             `json:"repositoryKeys,omitempty"`
	RepositoryListCount       int                                  `json:"repositoryListCount,omitempty"`
	RepositoryGetVerified     bool                                 `json:"repositoryGetVerified,omitempty"`
	RepositoryValidationState string                               `json:"repositoryValidationState,omitempty"`
	RepositoryValidationError string                               `json:"repositoryValidationError,omitempty"`
	RepositoryBlockers        []string                             `json:"repositoryBlockers,omitempty"`
	NegativeChecks            []SystemBackupRepositoryNegativeCase `json:"negativeChecks,omitempty"`
	Artifacts                 []SystemBackupArtifact               `json:"artifacts,omitempty"`
	OldPVCUIDs                map[string]string                    `json:"oldPvcUids,omitempty"`
	NewPVCUIDs                map[string]string                    `json:"newPvcUids,omitempty"`
	RestorePlan               string                               `json:"restorePlan,omitempty"`
	RestoreOperationID        string                               `json:"restoreOperationId,omitempty"`
	RestoreResults            []SystemBackupRestoreResult          `json:"restoreResults,omitempty"`
	VerifiedPods              int                                  `json:"verifiedPods,omitempty"`
	FixtureNodes              int64                                `json:"fixtureNodes,omitempty"`
	FixtureEdges              int64                                `json:"fixtureEdges,omitempty"`
	FixtureProved             bool                                 `json:"fixtureProved,omitempty"`
	Stdout                    string                               `json:"stdout,omitempty"`
	Stderr                    string                               `json:"stderr,omitempty"`
	ValidationErrors          []string                             `json:"validationErrors,omitempty"`
	fixture                   *userBackupFixtureResult
}

// SystemBackupRepositoryNegativeCase captures repository fail-closed checks without exposing object-store credentials.
type SystemBackupRepositoryNegativeCase struct {
	Name           string   `json:"name"`
	BackupSetID    string   `json:"backupSetId,omitempty"`
	FixturePrefix  string   `json:"fixturePrefix,omitempty"`
	FixtureObjects []string `json:"fixtureObjects,omitempty"`
	Listed         bool     `json:"listed"`
	Valid          bool     `json:"valid"`
	Errors         []string `json:"errors,omitempty"`
}

const backupSetManifestName = "backup-set.json"

// SystemBackupRestoreResult captures one restore-local invocation for an ordinal PVC.
type SystemBackupRestoreResult struct {
	Ordinal int    `json:"ordinal"`
	PVCName string `json:"pvcName"`
	Stdout  string `json:"stdout,omitempty"`
	Stderr  string `json:"stderr,omitempty"`
}

type SystemBackupArtifact struct {
	PodName        string `json:"podName"`
	Ordinal        int    `json:"ordinal"`
	ArchiveName    string `json:"archiveName"`
	ArchiveURI     string `json:"archiveUri,omitempty"`
	ManifestName   string `json:"manifestName,omitempty"`
	ManifestURI    string `json:"manifestUri,omitempty"`
	ChecksumSHA256 string `json:"checksumSha256,omitempty"`
	LocalArchive   string `json:"localArchive,omitempty"`
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
		RepositoryID  string                       `json:"repository_id"`
		Nodes         []struct {
			PodName        string            `json:"pod_name"`
			Ordinal        int               `json:"ordinal"`
			ArchiveName    string            `json:"archive_name"`
			ArchiveURI     string            `json:"archive_uri"`
			ManifestName   string            `json:"manifest_name"`
			ManifestURI    string            `json:"manifest_uri"`
			ChecksumSHA256 string            `json:"checksum_sha256"`
			AppliedIndexes map[string]uint64 `json:"applied_indexes"`
		} `json:"nodes"`
	} `json:"status"`
}

type clusterBackupListResponse struct {
	BackupSets []struct {
		BackupSetID  string `json:"backup_set_id"`
		RepositoryID string `json:"repository_id"`
		State        string `json:"state"`
		ManifestURI  string `json:"manifest_uri"`
		Manifest     struct {
			CompleteMarkerURI string `json:"complete_marker_uri"`
		} `json:"manifest"`
	} `json:"backup_sets"`
}

type clusterRestorePlanResponse struct {
	Plan struct {
		RestorePlanID string `json:"restore_plan_id"`
		RepositoryID  string `json:"repository_id"`
		BackupSetID   string `json:"backup_set_id"`
		Restorable    bool   `json:"restorable"`
		Blockers      []struct {
			Reason string `json:"reason"`
			Detail string `json:"detail"`
		} `json:"blockers"`
	} `json:"plan"`
}

type repositoryObjectListItem struct {
	Key  string `json:"key"`
	Type string `json:"type,omitempty"`
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
	Valid      bool     `json:"valid"`
	Errors     []string `json:"errors"`
	Validation struct {
		Errors   []string `json:"errors"`
		Blockers []struct {
			Reason    string `json:"reason"`
			Detail    string `json:"detail"`
			FieldPath string `json:"field_path"`
		} `json:"blockers"`
	} `json:"validation"`
}

func executeSystemBackupEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, resources *provision.ScenarioResources, eventType string, target map[string]any, state map[string]SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	result := SystemBackupOperationResult{Operation: eventType, BackupDir: firstNonEmpty(stringTarget(target, "backupDir"), "/tmp/mycel-system-backups"), HostDir: expandTargetPath(firstNonEmpty(stringTarget(target, "hostDir"), filepath.Join(os.TempDir(), "{runID}-system-backup")), environment, resources)}
	switch eventType {
	case "system-backup-fixture":
		fixture, _, _, err := executeUserBackupFixture(ctx, driver, environment, env.NodeRef{Ordinal: intTarget(target, "ordinal", 0)}, resources, target)
		result.FixtureNodes, result.FixtureEdges, result.fixture = int64(fixture.NodesCreated), int64(fixture.EdgesCreated), &fixture
		return result, err
	case "cluster-backup-repository-create":
		previous := state[systemBackupKey(target)]
		result.fixture = previous.fixture
		return executeClusterBackupRepositoryCreate(ctx, driver, environment, resources, target, result)
	case "cluster-backup-create":
		previous := state[systemBackupKey(target)]
		result.fixture = previous.fixture
		copyRepositoryEvidence(&result, previous)
		return executeClusterBackupCreate(ctx, driver, environment, scenario, target, result)
	case "cluster-backup-validate":
		return executeClusterBackupValidate(ctx, driver, environment, state, target, result)
	case "cluster-backup-repository-assert":
		return executeClusterBackupRepositoryAssert(ctx, driver, environment, resources, state, target, result)
	case "cluster-restore-plan":
		return executeClusterRestorePlanEvent(ctx, driver, environment, state, target, result)
	case "cluster-restore-start":
		previous := state[systemBackupKey(target)]
		copyRepositoryEvidence(&result, previous)
		return executeClusterRestoreStart(ctx, driver, environment, resources, state, target, result)
	case "cluster-restore-apply":
		return executeClusterRestoreApply(ctx, driver, environment, scenario, state, target, result)
	case "cluster-restore-verify":
		return executeClusterRestoreVerify(ctx, driver, environment, resources, state, target, result)
	case "cluster-backup-cleanup":
		backup := state[systemBackupKey(target)]
		if strings.TrimSpace(backup.HostDir) == "" {
			return result, fmt.Errorf("no cluster backup host directory recorded for %q", systemBackupKey(target))
		}
		if !safeSystemBackupHostDir(backup.HostDir) {
			return result, errors.New("cluster backup host cleanup path is outside the managed temporary root")
		}
		if err := os.RemoveAll(backup.HostDir); err != nil {
			return result, errors.New("remove cluster backup host directory")
		}
		if _, err := os.Stat(backup.HostDir); !os.IsNotExist(err) {
			return result, errors.New("cluster backup host directory still exists after cleanup")
		}
		delete(state, systemBackupKey(target))
		result.HostDir = ""
		return result, nil
	default:
		return result, fmt.Errorf("unsupported system backup event type %q", eventType)
	}
}

func safeSystemBackupHostDir(path string) bool {
	root, err := filepath.Abs(os.TempDir())
	if err != nil {
		return false
	}
	candidate, err := filepath.Abs(strings.TrimSpace(path))
	if err != nil || candidate == root {
		return false
	}
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))
}

func copyRepositoryEvidence(dst *SystemBackupOperationResult, src SystemBackupOperationResult) {
	if dst == nil {
		return
	}
	if dst.RepositoryID == "" {
		dst.RepositoryID = src.RepositoryID
	}
	if dst.RepositoryBucket == "" {
		dst.RepositoryBucket = src.RepositoryBucket
	}
	if dst.RepositoryPrefix == "" {
		dst.RepositoryPrefix = src.RepositoryPrefix
	}
	if dst.RepositoryBackupSetPrefix == "" {
		dst.RepositoryBackupSetPrefix = src.RepositoryBackupSetPrefix
	}
	if dst.ManifestURI == "" {
		dst.ManifestURI = src.ManifestURI
	}
	if dst.CompleteMarkerURI == "" {
		dst.CompleteMarkerURI = src.CompleteMarkerURI
	}
}

func executeClusterBackupRepositoryCreate(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, target map[string]any, result SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	repositoryID := firstNonEmpty(stringTarget(target, "repositoryId"), stringTarget(target, "repositoryID"), "mycel-lab-minio")
	bucket := firstNonEmpty(stringTarget(target, "bucket"), "mycel-compose-backups")
	prefix := strings.Trim(expandTargetPath(firstNonEmpty(stringTarget(target, "prefix"), "mycel-lab/{runID}"), environment, resources), "/")
	region := firstNonEmpty(stringTarget(target, "region"), "us-east-1")
	endpoint := firstNonEmpty(stringTarget(target, "endpoint"), "http://minio:9000")
	result.RepositoryID = repositoryID
	result.RepositoryBucket = bucket
	result.RepositoryPrefix = prefix
	if bucket == "" {
		return result, errors.New("repository bucket is required")
	}
	if executor, ok := driver.(env.ObjectStoreExecutor); ok {
		if _, err := objectStoreCommand(ctx, executorDriver{EnvironmentDriver: driver, ObjectStoreExecutor: executor}, environment, fmt.Sprintf("mc mb --ignore-existing %s", shellQuote("local/"+bucket))); err != nil {
			return result, fmt.Errorf("create backup repository bucket: %w", err)
		}
	} else if boolTarget(target, "requireObjectStoreFixture", true) {
		return result, fmt.Errorf("driver %q does not support object-store execution", driver.Name())
	}
	admin, err := dialBackupAdmin(ctx, resources, "mycel-lab-backup-repository")
	if err != nil {
		return result, err
	}
	defer admin.Close()
	repo := &adminv1.BackupRepository{
		RepositoryId: repositoryID,
		DisplayName:  firstNonEmpty(stringTarget(target, "displayName"), "Mycel Lab MinIO backup repository"),
		Provider:     adminv1.BackupRepositoryProvider_BACKUP_REPOSITORY_PROVIDER_S3_COMPATIBLE,
		S3: &adminv1.S3CompatibleRepositoryConfig{
			Bucket:         bucket,
			Prefix:         prefix,
			Region:         region,
			EndpointUri:    endpoint,
			ForcePathStyle: boolTarget(target, "forcePathStyle", true),
			DisableTls:     boolTarget(target, "disableTLS", true),
		},
		CredentialRef: &adminv1.SecretReference{Scheme: adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_ENV, Keys: map[string]string{
			"access_key_id":     firstNonEmpty(stringTarget(target, "accessKeyEnv"), "AWS_ACCESS_KEY_ID"),
			"secret_access_key": firstNonEmpty(stringTarget(target, "secretKeyEnv"), "AWS_SECRET_ACCESS_KEY"),
		}},
		Encryption: &adminv1.EncryptionPolicy{Mode: adminv1.EncryptionMode_ENCRYPTION_MODE_NONE},
		DevOnly:    boolTarget(target, "devOnly", true),
	}
	if _, err := admin.Backup.CreateBackupRepository(ctx, &adminv1.CreateBackupRepositoryRequest{Repository: repo}); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return result, fmt.Errorf("create backup repository: %w", err)
		}
		if _, err := admin.Backup.UpdateBackupRepository(ctx, &adminv1.UpdateBackupRepositoryRequest{Repository: repo}); err != nil {
			return result, fmt.Errorf("update existing backup repository: %w", err)
		}
	}
	validation, err := admin.Backup.ValidateBackupRepository(ctx, &adminv1.ValidateBackupRepositoryRequest{RepositoryId: repositoryID})
	if err != nil {
		return result, fmt.Errorf("validate backup repository: %w", err)
	}
	validationSummary := validation.GetValidation()
	result.RepositoryValidationState = validationSummary.GetState().String()
	result.RepositoryValidationError = bounded(validationSummary.GetSanitizedError(), 2048)
	result.RepositoryBlockers = repositoryValidationBlockers(validationSummary)
	expectValid := boolTarget(target, "expectValid", true)
	if validationSummary.GetState() != adminv1.BackupValidationState_BACKUP_VALIDATION_STATE_VALID {
		if !expectValid {
			return result, nil
		}
		return result, fmt.Errorf("backup repository validation state %s", validationSummary.GetState().String())
	}
	if !expectValid {
		return result, errors.New("backup repository unexpectedly validated successfully")
	}
	return result, nil
}

func repositoryValidationBlockers(validation *adminv1.BackupRepositoryValidationSummary) []string {
	if validation == nil {
		return nil
	}
	blockers := make([]string, 0, len(validation.GetBlockers()))
	for _, blocker := range validation.GetBlockers() {
		reason := strings.TrimSpace(blocker.GetReason())
		if reason == "" {
			reason = "unknown"
		}
		field := strings.TrimSpace(blocker.GetFieldPath())
		if field != "" {
			reason += ":" + field
		}
		blockers = append(blockers, reason)
	}
	return blockers
}

// executorDriver adapts a previously asserted ObjectStoreExecutor while preserving the EnvironmentDriver identity.
type executorDriver struct {
	env.EnvironmentDriver
	env.ObjectStoreExecutor
}

func dialBackupAdmin(ctx context.Context, resources *provision.ScenarioResources, clientName string) (*mycel.AdminClient, error) {
	if resources == nil || strings.TrimSpace(resources.AdminAddr) == "" {
		return nil, errors.New("admin endpoint resources are required for backup repository operations")
	}
	return mycel.DialAdmin(ctx, mycel.Config{Addr: resources.AdminAddr, Username: firstNonEmpty(resources.AdminUsername, provision.DefaultAdminUsername), Password: firstNonEmpty(resources.AdminPassword, provision.DefaultAdminPassword), ClientName: clientName})
}

func executeClusterBackupCreate(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, scenario spec.ResolvedScenario, target map[string]any, result SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	repositoryID := firstNonEmpty(stringTarget(target, "repositoryId"), stringTarget(target, "repositoryID"), result.RepositoryID)
	repositoryMode := strings.TrimSpace(repositoryID) != ""
	var stager env.NodeFileStager
	if !repositoryMode {
		var ok bool
		stager, ok = driver.(env.NodeFileStager)
		if !ok {
			return result, fmt.Errorf("driver %q does not support node file staging", driver.Name())
		}
	}
	nodes, err := driver.Nodes(ctx, environment)
	if err != nil {
		return result, err
	}
	if len(nodes) == 0 {
		return result, fmt.Errorf("no nodes available for cluster backup")
	}
	if !repositoryMode {
		for _, node := range nodes {
			if _, err := driver.Exec(ctx, environment, env.NodeRef{Name: node.Name, Ordinal: node.Ordinal}, env.ExecRequest{Command: []string{"sh", "-ec", "rm -rf " + shellQuote(result.BackupDir) + " && mkdir -p " + shellQuote(result.BackupDir)}}); err != nil {
				return result, err
			}
		}
	}
	reason := firstNonEmpty(stringTarget(target, "reason"), "native mycel-lab system backup/restore test")
	format := firstNonEmpty(stringTarget(target, "archiveFormat"), "tar")
	cmd := append(adminCLIBase(), "admin", "backup", "cluster", "start", "--archive-format", format, "--reason", reason, "--wait")
	if repositoryMode {
		cmd = append(cmd, "--repository-id", repositoryID)
		if captureProvider := stringTarget(target, "captureProvider"); captureProvider != "" {
			cmd = append(cmd, "--capture-provider", captureProvider)
		}
		if quiesceReadProfile := stringTarget(target, "quiesceReadProfile"); quiesceReadProfile != "" {
			cmd = append(cmd, "--quiesce-read-profile", quiesceReadProfile)
		}
	} else {
		cmd = append(cmd, "--output-dir", result.BackupDir)
	}
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
	expectFailure := boolTarget(target, "expectFailure", false)
	if err != nil {
		if expectFailure && backupFailureMatchesExpectation(target, err.Error(), result.Stdout, result.Stderr) {
			return result, nil
		}
		return result, err
	}
	var response clusterBackupStatusResponse
	if err := json.Unmarshal([]byte(execResult.Stdout), &response); err != nil {
		return result, fmt.Errorf("decode cluster backup status response: %w", err)
	}
	status := response.Status
	result.BackupSetID = status.BackupSetID
	result.ManifestURI = status.ManifestURI
	if repositoryMode {
		result.RepositoryID = firstNonEmpty(status.RepositoryID, repositoryID)
		if result.RepositoryBackupSetPrefix == "" {
			result.RepositoryBackupSetPrefix = backupSetPrefixFromManifestURI(result.ManifestURI, result.RepositoryBucket, result.BackupSetID)
		}
	}
	if strings.ToLower(status.State) != "succeeded" {
		if expectFailure && backupFailureMatchesExpectation(target, status.State, status.FailedPhase, status.Error, formatClusterBackupBlockers(status.Blockers), result.Stdout, result.Stderr) {
			return result, nil
		}
		return result, fmt.Errorf("cluster backup state %q phase=%s error=%s blockers=%s", status.State, status.FailedPhase, status.Error, formatClusterBackupBlockers(status.Blockers))
	}
	if expectFailure {
		return result, errors.New("cluster backup unexpectedly succeeded")
	}
	if want := scenario.Cluster.Nodes; want > 0 && (status.ExpectedNodes != want || len(status.Nodes) != want) {
		return result, fmt.Errorf("cluster backup expected %d nodes, status expected=%d artifacts=%d", want, status.ExpectedNodes, len(status.Nodes))
	}
	if len(status.RaftBarriers) == 0 {
		return result, fmt.Errorf("cluster backup status missing raft barrier evidence")
	}
	if !repositoryMode {
		backupSetPath := filepath.Join(result.HostDir, "backup-set.json")
		if _, err := stager.CopyFromNode(ctx, environment, env.NodeRef{Name: nodes[0].Name, Ordinal: nodes[0].Ordinal}, filepath.Join(result.BackupDir, "backup-set.json"), backupSetPath); err != nil {
			return result, err
		}
		if err := validateBackupSetFile(backupSetPath); err != nil {
			return result, err
		}
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
		artifact := SystemBackupArtifact{PodName: pod, Ordinal: node.Ordinal, ArchiveName: node.ArchiveName, ArchiveURI: node.ArchiveURI, ManifestName: node.ManifestName, ManifestURI: node.ManifestURI, ChecksumSHA256: node.ChecksumSHA256}
		if !repositoryMode {
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
			artifact.LocalArchive = archivePath
			if node.ManifestName != "" {
				manifestPath := filepath.Join(podDir, node.ManifestName)
				if _, err := stager.CopyFromNode(ctx, environment, env.NodeRef{Name: envNode.Name, Ordinal: envNode.Ordinal}, filepath.Join(result.BackupDir, node.ManifestName), manifestPath); err != nil {
					return result, err
				}
				artifact.LocalManifest = manifestPath
			}
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
	copyRepositoryEvidence(&result, backup)
	repositoryID := firstNonEmpty(stringTarget(target, "repositoryId"), stringTarget(target, "repositoryID"), result.RepositoryID)
	backupSetID := firstNonEmpty(stringTarget(target, "backupSetId"), stringTarget(target, "backupSetID"), backup.BackupSetID)
	repositoryMode := strings.TrimSpace(repositoryID) != "" || (strings.TrimSpace(backupSetID) != "" && strings.TrimSpace(result.BackupDir) == "")
	if !repositoryMode && len(backup.Artifacts) > 0 {
		if err := stageClusterBackupForValidation(ctx, driver, environment, backup, env.NodeRef{Ordinal: intTarget(target, "ordinal", 0)}); err != nil {
			return result, err
		}
	}
	cmd := append(adminCLIBase(), "admin", "backup", "cluster", "validate")
	if repositoryMode {
		if repositoryID == "" || backupSetID == "" {
			return result, errors.New("repository_id and backup_set_id are required for repository validation")
		}
		cmd = append(cmd, "--repository-id", repositoryID, "--backup-set-id", backupSetID)
	} else {
		cmd = append(cmd, "--backup-set", result.BackupDir)
	}
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
	result.ValidationErrors = clusterBackupValidationErrors(response)
	expectValid := boolTarget(target, "expectValid", true)
	if !response.Valid {
		if !expectValid {
			result.BackupSetID = backupSetID
			result.Artifacts = backup.Artifacts
			result.RepositoryID = repositoryID
			return result, nil
		}
		return result, fmt.Errorf("cluster backup set invalid: %s", strings.Join(result.ValidationErrors, "; "))
	}
	if !expectValid {
		return result, errors.New("cluster backup set unexpectedly validated successfully")
	}
	result.BackupSetID = backupSetID
	result.Artifacts = backup.Artifacts
	result.RepositoryID = repositoryID
	return result, nil
}

func executeClusterBackupRepositoryAssert(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, state map[string]SystemBackupOperationResult, target map[string]any, result SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	backup := state[systemBackupKey(target)]
	result.BackupSetID = firstNonEmpty(stringTarget(target, "backupSetId"), stringTarget(target, "backupSetID"), backup.BackupSetID)
	copyRepositoryEvidence(&result, backup)
	result.RepositoryID = firstNonEmpty(stringTarget(target, "repositoryId"), stringTarget(target, "repositoryID"), result.RepositoryID)
	result.RepositoryBucket = firstNonEmpty(stringTarget(target, "bucket"), result.RepositoryBucket)
	if result.RepositoryID == "" || result.BackupSetID == "" {
		return result, errors.New("repository assertion requires repository_id and backup_set_id")
	}
	if result.RepositoryBackupSetPrefix == "" {
		result.RepositoryBackupSetPrefix = joinS3Key(result.RepositoryPrefix, "backup-sets", result.BackupSetID)
	}

	listed, listStdout, err := repositoryBackupSetListed(ctx, driver, environment, result.RepositoryID, result.BackupSetID, intTarget(target, "ordinal", 0))
	result.Stdout = bounded(listStdout, 8192)
	if err != nil {
		return result, err
	}
	if !listed {
		return result, fmt.Errorf("repository backup set %s was not listed", result.BackupSetID)
	}
	result.RepositoryListCount = 1

	admin, err := dialBackupAdmin(ctx, resources, "mycel-lab-backup-repository-assert")
	if err != nil {
		return result, err
	}
	defer admin.Close()
	getResponse, err := admin.Backup.GetClusterBackupSet(ctx, &adminv1.GetClusterBackupSetRequest{RepositoryId: result.RepositoryID, BackupSetId: result.BackupSetID})
	if err != nil {
		return result, fmt.Errorf("get repository backup set: %w", err)
	}
	if getResponse.GetBackupSet().GetBackupSetId() != result.BackupSetID {
		return result, fmt.Errorf("repository get returned backup_set_id %q, want %q", getResponse.GetBackupSet().GetBackupSetId(), result.BackupSetID)
	}
	result.RepositoryGetVerified = true
	result.CompleteMarkerURI = getResponse.GetBackupSet().GetManifest().GetCompleteMarkerUri()

	validateResult, err := executeClusterBackupValidate(ctx, driver, environment, map[string]SystemBackupOperationResult{systemBackupKey(target): backup}, map[string]any{"repositoryId": result.RepositoryID, "backupSetId": result.BackupSetID, "ordinal": intTarget(target, "ordinal", 0)}, SystemBackupOperationResult{Operation: "cluster-backup-validate"})
	if err != nil {
		return result, err
	}
	result.ValidationErrors = validateResult.ValidationErrors

	plan, err := executeRepositoryClusterRestorePlan(ctx, driver, environment, result.RepositoryID, result.BackupSetID, target)
	if err != nil {
		return result, err
	}
	result.RestorePlan = plan

	keys, err := verifyRepositoryBackupObjects(ctx, driver, environment, result.RepositoryBucket, result.RepositoryBackupSetPrefix)
	if err != nil {
		return result, err
	}
	result.RepositoryKeys = keys

	if boolTarget(target, "negativeChecks", true) {
		checks, err := executeRepositoryNegativeChecks(ctx, driver, environment, result, target)
		result.NegativeChecks = checks
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func executeClusterRestorePlanEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, state map[string]SystemBackupOperationResult, target map[string]any, result SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	backup := state[systemBackupKey(target)]
	copyRepositoryEvidence(&result, backup)
	result.BackupSetID = firstNonEmpty(stringTarget(target, "backupSetId"), stringTarget(target, "backupSetID"), backup.BackupSetID)
	result.RepositoryID = firstNonEmpty(stringTarget(target, "repositoryId"), stringTarget(target, "repositoryID"), result.RepositoryID)
	if result.RepositoryID != "" {
		plan, err := executeRepositoryClusterRestorePlan(ctx, driver, environment, result.RepositoryID, result.BackupSetID, target)
		result.RestorePlan = plan
		return result, err
	}
	plan, err := executeClusterRestorePlan(ctx, driver, environment, backup, env.NodeRef{Ordinal: intTarget(target, "ordinal", 0)})
	result.RestorePlan = plan
	return result, err
}

func executeClusterRestoreStart(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, state map[string]SystemBackupOperationResult, target map[string]any, result SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	backup := state[systemBackupKey(target)]
	copyRepositoryEvidence(&result, backup)
	result.BackupSetID = firstNonEmpty(stringTarget(target, "backupSetId"), stringTarget(target, "backupSetID"), backup.BackupSetID)
	result.RepositoryID = firstNonEmpty(stringTarget(target, "repositoryId"), stringTarget(target, "repositoryID"), result.RepositoryID)
	if result.RepositoryID == "" || result.BackupSetID == "" {
		return result, errors.New("repository_id and backup_set_id are required for repository restore start")
	}
	if len(backup.Artifacts) == 0 {
		return result, errors.New("repository restore start requires backup artifact evidence from cluster-backup-create")
	}
	executor := env.NodeRef{Ordinal: intTarget(target, "ordinal", 0)}
	targetRoot := expandTargetPath(firstNonEmpty(stringTarget(target, "targetRoot"), "/tmp/mycel-lab-restore-target/{runID}"), environment, resources)
	stagingRoot := expandTargetPath(firstNonEmpty(stringTarget(target, "stagingRoot"), "/tmp/mycel-lab-restore-staging/{runID}"), environment, resources)
	if targetRoot == "" || stagingRoot == "" {
		return result, errors.New("restore targetRoot and stagingRoot are required")
	}
	if _, err := driver.Exec(ctx, environment, executor, env.ExecRequest{Command: []string{"sh", "-ec", "rm -rf " + shellQuote(targetRoot) + " " + shellQuote(stagingRoot) + " && mkdir -p " + shellQuote(targetRoot) + " " + shellQuote(stagingRoot)}}); err != nil {
		return result, fmt.Errorf("prepare restore target/staging dirs: %w", err)
	}
	artifacts := append([]SystemBackupArtifact(nil), backup.Artifacts...)
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Ordinal < artifacts[j].Ordinal })
	restoreTarget := &adminv1.ClusterRestoreTarget{ExpectedNodeCount: int32(len(artifacts))}
	for _, artifact := range artifacts {
		dir := filepath.Join(targetRoot, fmt.Sprintf("ordinal-%d", artifact.Ordinal))
		restoreTarget.Nodes = append(restoreTarget.Nodes, &adminv1.ClusterRestoreNodeMapping{Ordinal: int32(artifact.Ordinal), SourcePodName: artifact.PodName, TargetPodName: artifact.PodName, TargetDataDir: dir})
	}
	restoreStaging := &adminv1.ClusterRestoreStagingLocation{StagingUri: stagingRoot, CleanBeforeUse: true}
	admin, err := dialBackupAdmin(ctx, resources, "mycel-lab-cluster-restore-start")
	if err != nil {
		return result, err
	}
	defer admin.Close()
	plan, err := admin.Backup.PlanClusterRestore(ctx, &adminv1.PlanClusterRestoreRequest{RepositoryId: result.RepositoryID, BackupSetId: result.BackupSetID, TargetMode: adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_NEW_CLUSTER_BOOTSTRAP, IdentityMode: adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_PRESERVE_BACKUP_ID, AuthSessionPolicy: adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_PRESERVE_RESTORED_SESSIONS, Target: restoreTarget, Staging: restoreStaging})
	if err != nil {
		return result, fmt.Errorf("plan repository restore with execution target: %w", err)
	}
	if !plan.GetPlan().GetRestorable() {
		return result, fmt.Errorf("repository restore plan %q is not restorable", plan.GetPlan().GetRestorePlanId())
	}
	result.RestorePlan = plan.GetPlan().GetRestorePlanId()
	preconditions := &adminv1.ClusterRestorePreconditionEvidence{CleanTarget: &adminv1.ClusterRestoreCleanTargetProof{ProofId: firstNonEmpty(stringTarget(target, "cleanTargetProofId"), "mycel-lab-clean-target"), CheckedAt: time.Now().UTC().Format(time.RFC3339Nano), TargetEmpty: true, CheckedNodes: restoreTarget.Nodes, OperatorAttestation: firstNonEmpty(stringTarget(target, "operatorAttestation"), "mycel-lab temp target dirs were removed and recreated before restore start")}, SourceFencing: &adminv1.ClusterRestoreSourceFencingEvidence{SourceClusterFenced: boolTarget(target, "sourceClusterFenced", false), FencedAt: time.Now().UTC().Format(time.RFC3339Nano), FenceTokenRef: stringTarget(target, "fenceTokenRef")}}
	start, err := admin.Backup.StartClusterRestore(ctx, &adminv1.StartClusterRestoreRequest{RestorePlanId: plan.GetPlan().GetRestorePlanId(), PlanFreshnessToken: plan.GetPlan().GetRestorePlanId(), RepositoryId: result.RepositoryID, BackupSetId: result.BackupSetID, TargetMode: adminv1.ClusterRestoreTargetMode_CLUSTER_RESTORE_TARGET_MODE_NEW_CLUSTER_BOOTSTRAP, IdentityMode: adminv1.ClusterRestoreIdentityMode_CLUSTER_RESTORE_IDENTITY_MODE_PRESERVE_BACKUP_ID, AuthSessionPolicy: adminv1.ClusterRestoreAuthSessionPolicy_CLUSTER_RESTORE_AUTH_SESSION_POLICY_PRESERVE_RESTORED_SESSIONS, IdempotencyKey: firstNonEmpty(stringTarget(target, "idempotencyKey"), "mycel-lab-repository-restore-start-v1"), Target: restoreTarget, Staging: restoreStaging, Preconditions: preconditions})
	if err != nil {
		return result, fmt.Errorf("start repository restore: %w", err)
	}
	op := start.GetOperation()
	if op == nil {
		return result, errors.New("start repository restore returned no operation")
	}
	result.RestoreOperationID = op.GetRestoreOperationId()
	deadline := time.Now().Add(durationTarget(target, "waitTimeout", 5*time.Minute))
	poll := durationTarget(target, "pollInterval", 2*time.Second)
	if poll <= 0 {
		poll = 2 * time.Second
	}
	for {
		if op.GetState() == adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_SUCCEEDED {
			break
		}
		if clusterRestoreStateTerminal(op.GetState()) {
			return result, fmt.Errorf("repository restore operation %s ended in %s: %s", op.GetRestoreOperationId(), op.GetState().String(), op.GetError())
		}
		if time.Now().After(deadline) {
			return result, fmt.Errorf("repository restore operation %s did not finish before timeout; last state %s", op.GetRestoreOperationId(), op.GetState().String())
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
		status, err := admin.Backup.GetClusterRestoreStatus(ctx, &adminv1.GetClusterRestoreStatusRequest{RestoreOperationId: op.GetRestoreOperationId()})
		if err != nil {
			return result, fmt.Errorf("get repository restore status: %w", err)
		}
		op = status.GetOperation()
		if op == nil {
			return result, errors.New("repository restore status returned no operation")
		}
	}
	progress := op.GetProgress()
	if progress == nil || int(progress.GetVerifiedNodes()) != len(artifacts) {
		return result, fmt.Errorf("repository restore verified_nodes=%d want %d", progress.GetVerifiedNodes(), len(artifacts))
	}
	result.Stdout = fmt.Sprintf("restore_operation_id=%s state=%s verified_nodes=%d", op.GetRestoreOperationId(), op.GetState().String(), progress.GetVerifiedNodes())
	return result, nil
}

func clusterRestoreStateTerminal(state adminv1.ClusterRestoreState) bool {
	switch state {
	case adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_SUCCEEDED, adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_CANCELED, adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_FAILED, adminv1.ClusterRestoreState_CLUSTER_RESTORE_STATE_FAILED_REQUIRES_OPERATOR:
		return true
	default:
		return false
	}
}

func repositoryBackupSetListed(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, repositoryID, backupSetID string, ordinal int) (bool, string, error) {
	cmd := append(adminCLIBase(), "admin", "backup", "cluster", "list", "--repository-id", repositoryID)
	execResult, err := driver.Exec(ctx, environment, env.NodeRef{Ordinal: ordinal}, env.ExecRequest{Command: cmd})
	if err != nil {
		return false, execResult.Stdout, err
	}
	var response clusterBackupListResponse
	if err := json.Unmarshal([]byte(execResult.Stdout), &response); err != nil {
		return false, execResult.Stdout, fmt.Errorf("decode repository backup-set list response: %w", err)
	}
	for _, backupSet := range response.BackupSets {
		if backupSet.BackupSetID == backupSetID {
			return true, execResult.Stdout, nil
		}
	}
	return false, execResult.Stdout, nil
}

func executeRepositoryClusterRestorePlan(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, repositoryID, backupSetID string, target map[string]any) (string, error) {
	if repositoryID == "" || backupSetID == "" {
		return "", errors.New("repository_id and backup_set_id are required for repository restore planning")
	}
	cmd := append(adminCLIBase(), "admin", "backup", "cluster", "restore-plan", "--repository-id", repositoryID, "--backup-set-id", backupSetID)
	cmd = append(cmd, "--target-mode", firstNonEmpty(stringTarget(target, "targetMode"), "new_cluster_bootstrap"))
	cmd = append(cmd, "--identity-mode", firstNonEmpty(stringTarget(target, "identityMode"), "preserve_backup_id"))
	cmd = append(cmd, "--auth-session-policy", firstNonEmpty(stringTarget(target, "authSessionPolicy"), "preserve_restored_sessions"))
	execResult, err := driver.Exec(ctx, environment, env.NodeRef{Ordinal: intTarget(target, "ordinal", 0)}, env.ExecRequest{Command: cmd})
	if err != nil {
		return bounded(execResult.Stdout, 8192), err
	}
	var response clusterRestorePlanResponse
	if err := json.Unmarshal([]byte(execResult.Stdout), &response); err != nil {
		return bounded(execResult.Stdout, 8192), fmt.Errorf("decode repository restore-plan response: %w", err)
	}
	if !response.Plan.Restorable {
		return bounded(execResult.Stdout, 8192), fmt.Errorf("repository restore plan %q is not restorable: %v", response.Plan.RestorePlanID, response.Plan.Blockers)
	}
	return bounded(execResult.Stdout, 32768), nil
}

func verifyRepositoryBackupObjects(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, bucket, backupSetPrefix string) ([]string, error) {
	if bucket == "" || backupSetPrefix == "" {
		return nil, errors.New("repository bucket and backup-set prefix are required")
	}
	for _, name := range []string{"backup-set.json", "COMPLETE"} {
		if _, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc stat %s >/dev/null", shellQuote("local/"+bucket+"/"+joinS3Key(backupSetPrefix, name)))); err != nil {
			return nil, fmt.Errorf("repository object %s missing: %w", name, err)
		}
	}
	res, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc ls --recursive --json %s", shellQuote("local/"+bucket+"/"+backupSetPrefix)))
	if err != nil {
		return nil, fmt.Errorf("list repository backup-set objects: %w", err)
	}
	keys := []string{}
	for _, line := range strings.Split(res.Stdout, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var item repositoryObjectListItem
		if json.Unmarshal([]byte(line), &item) == nil && item.Key != "" && !strings.HasSuffix(item.Key, "/") {
			keys = append(keys, item.Key)
		}
	}
	sort.Strings(keys)
	if len(keys) < 2 {
		return keys, fmt.Errorf("repository backup-set object list too small: %d", len(keys))
	}
	return keys, nil
}

func executeRepositoryNegativeChecks(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, backup SystemBackupOperationResult, target map[string]any) ([]SystemBackupRepositoryNegativeCase, error) {
	checks := []SystemBackupRepositoryNegativeCase{}
	if strings.TrimSpace(backup.RepositoryBucket) == "" || strings.TrimSpace(backup.RepositoryBackupSetPrefix) == "" || strings.TrimSpace(backup.RepositoryID) == "" || strings.TrimSpace(backup.BackupSetID) == "" {
		return checks, errors.New("repository negative checks require repository_id, backup_set_id, bucket, and backup-set prefix evidence")
	}
	ordinal := intTarget(target, "ordinal", 0)
	addCheck := func(name, suffix string, create func(prefix string) ([]string, error)) error {
		backupSetID := backup.BackupSetID + "-" + suffix
		prefix := siblingBackupSetPrefix(backup.RepositoryBackupSetPrefix, backupSetID)
		defer cleanupRepositoryPrefix(context.Background(), driver, environment, backup.RepositoryBucket, prefix)
		objects, err := create(prefix)
		if err != nil {
			return fmt.Errorf("create %s negative fixture: %w", name, err)
		}
		listed, _, err := repositoryBackupSetListed(ctx, driver, environment, backup.RepositoryID, backupSetID, ordinal)
		if err != nil {
			return err
		}
		validation, err := validateRepositoryNegativeBackupSet(ctx, driver, environment, backup.RepositoryID, backupSetID, target)
		validation.Name = name
		validation.BackupSetID = backupSetID
		validation.FixturePrefix = prefix
		validation.FixtureObjects = append([]string(nil), objects...)
		validation.Listed = listed
		checks = append(checks, validation)
		if err != nil {
			return err
		}
		if listed || validation.Valid {
			return fmt.Errorf("%s repository fixture listed=%t valid=%t", name, listed, validation.Valid)
		}
		return nil
	}

	if err := addCheck("missing-complete", "missing-complete", func(prefix string) ([]string, error) {
		object := joinS3Key(prefix, "backup-set.json")
		_, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc cp %s %s", shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, joinS3Key(backup.RepositoryBackupSetPrefix, "backup-set.json"))), shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, object))))
		return []string{object}, err
	}); err != nil {
		return checks, err
	}
	if err := addCheck("corrupt-required-manifest", "corrupt-manifest", func(prefix string) ([]string, error) {
		manifestObject := joinS3Key(prefix, "backup-set.json")
		completeObject := joinS3Key(prefix, "COMPLETE")
		cmd := fmt.Sprintf("tmp=$(mktemp) && printf 'not-json' > $tmp && mc cp $tmp %s && mc cp %s %s && rm -f $tmp", shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, manifestObject)), shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, joinS3Key(backup.RepositoryBackupSetPrefix, "COMPLETE"))), shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, completeObject)))
		_, err := objectStoreCommand(ctx, driver, environment, cmd)
		return []string{manifestObject, completeObject}, err
	}); err != nil {
		return checks, err
	}
	if err := addCheck("corrupt-complete-marker", "corrupt-complete", func(prefix string) ([]string, error) {
		manifestObject := joinS3Key(prefix, "backup-set.json")
		completeObject := joinS3Key(prefix, "COMPLETE")
		cmd := fmt.Sprintf("tmp=$(mktemp) && printf 'not-json' > $tmp && mc cp %s %s && mc cp $tmp %s && rm -f $tmp", shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, joinS3Key(backup.RepositoryBackupSetPrefix, "backup-set.json"))), shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, manifestObject)), shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, completeObject)))
		_, err := objectStoreCommand(ctx, driver, environment, cmd)
		return []string{manifestObject, completeObject}, err
	}); err != nil {
		return checks, err
	}
	if len(backup.Artifacts) > 0 {
		if err := addCheck("interrupted-publish-after-node-artifacts", "interrupted-publish", func(prefix string) ([]string, error) {
			return copyRepositoryArtifactSubset(ctx, driver, environment, backup, prefix, backup.Artifacts[:1], false)
		}); err != nil {
			return checks, err
		}
		if err := addCheck("partial-node-upload", "partial-node-upload", func(prefix string) ([]string, error) {
			objects := []string{joinS3Key(prefix, "backup-set.json"), joinS3Key(prefix, "COMPLETE")}
			if _, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc cp %s %s", shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, joinS3Key(backup.RepositoryBackupSetPrefix, "backup-set.json"))), shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, objects[0])))); err != nil {
				return objects, err
			}
			if _, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc cp %s %s", shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, joinS3Key(backup.RepositoryBackupSetPrefix, "COMPLETE"))), shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, objects[1])))); err != nil {
				return objects, err
			}
			artifactObjects, err := copyRepositoryArtifactSubset(ctx, driver, environment, backup, prefix, backup.Artifacts[:1], true)
			objects = append(objects, artifactObjects...)
			return objects, err
		}); err != nil {
			return checks, err
		}
	}
	return checks, nil
}

func validateRepositoryNegativeBackupSet(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, repositoryID, backupSetID string, target map[string]any) (SystemBackupRepositoryNegativeCase, error) {
	result, err := executeClusterBackupValidate(ctx, driver, environment, map[string]SystemBackupOperationResult{systemBackupKey(target): {RepositoryID: repositoryID, BackupSetID: backupSetID}}, map[string]any{"repositoryId": repositoryID, "backupSetId": backupSetID, "expectValid": false, "ordinal": intTarget(target, "ordinal", 0)}, SystemBackupOperationResult{Operation: "cluster-backup-validate"})
	check := SystemBackupRepositoryNegativeCase{BackupSetID: backupSetID, Valid: len(result.ValidationErrors) == 0, Errors: result.ValidationErrors}
	return check, err
}

func copyRepositoryArtifactSubset(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, backup SystemBackupOperationResult, targetPrefix string, artifacts []SystemBackupArtifact, includeManifests bool) ([]string, error) {
	objects := []string{}
	for _, artifact := range artifacts {
		pod := firstNonEmpty(artifact.PodName, fmt.Sprintf("myceld-%d", artifact.Ordinal))
		if strings.TrimSpace(artifact.ArchiveName) != "" {
			dest := joinS3Key(targetPrefix, "nodes", pod, artifact.ArchiveName)
			source := repositoryArtifactLocalPath(backup, artifact.ArchiveURI, pod, artifact.ArchiveName)
			if _, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc cp %s %s", shellQuote(source), shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, dest)))); err != nil {
				return objects, err
			}
			objects = append(objects, dest)
		}
		if includeManifests && strings.TrimSpace(artifact.ManifestName) != "" {
			dest := joinS3Key(targetPrefix, "nodes", pod, artifact.ManifestName)
			source := repositoryArtifactLocalPath(backup, artifact.ManifestURI, pod, artifact.ManifestName)
			if _, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc cp %s %s", shellQuote(source), shellQuote(repositoryLocalObjectPath(backup.RepositoryBucket, dest)))); err != nil {
				return objects, err
			}
			objects = append(objects, dest)
		}
	}
	return objects, nil
}

func repositoryArtifactLocalPath(backup SystemBackupOperationResult, uri, podName, objectName string) string {
	if key := repositoryObjectKeyFromURI(backup.RepositoryBucket, uri); key != "" {
		return repositoryLocalObjectPath(backup.RepositoryBucket, key)
	}
	return repositoryLocalObjectPath(backup.RepositoryBucket, joinS3Key(backup.RepositoryBackupSetPrefix, "nodes", podName, objectName))
}

func repositoryObjectKeyFromURI(bucket, raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "s3" || strings.TrimSpace(u.Host) == "" {
		return ""
	}
	if strings.TrimSpace(bucket) != "" && u.Host != strings.TrimSpace(bucket) {
		return ""
	}
	return strings.Trim(u.Path, "/")
}

func repositoryLocalObjectPath(bucket, key string) string {
	return "local/" + strings.Trim(strings.TrimSpace(bucket), "/") + "/" + strings.Trim(strings.TrimSpace(key), "/")
}

func cleanupRepositoryPrefix(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, bucket, prefix string) {
	if bucket == "" || prefix == "" {
		return
	}
	_, _ = objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc rm --recursive --force %s >/dev/null 2>&1 || true", shellQuote("local/"+bucket+"/"+prefix)))
}

func backupSetPrefixFromManifestURI(manifestURI, bucket, backupSetID string) string {
	manifestURI = strings.TrimSpace(manifestURI)
	if manifestURI == "" {
		return ""
	}
	u, err := url.Parse(manifestURI)
	if err != nil || u.Scheme != "s3" {
		return ""
	}
	if bucket != "" && u.Host != bucket {
		return ""
	}
	key := strings.Trim(u.Path, "/")
	if strings.HasSuffix(key, "/backup-set.json") {
		return strings.TrimSuffix(key, "/backup-set.json")
	}
	if backupSetID != "" {
		needle := "backup-sets/" + backupSetID
		if idx := strings.Index(key, needle); idx >= 0 {
			return key[:idx+len(needle)]
		}
	}
	return ""
}

func siblingBackupSetPrefix(prefix, backupSetID string) string {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	parent := strings.TrimSuffix(prefix, "/"+filepath.Base(prefix))
	return joinS3Key(parent, backupSetID)
}

func joinS3Key(parts ...string) string {
	out := []string{}
	for _, part := range parts {
		part = strings.Trim(strings.TrimSpace(part), "/")
		if part != "" && part != "." {
			out = append(out, part)
		}
	}
	return strings.Join(out, "/")
}

func clusterBackupValidationErrors(response clusterBackupValidateResponse) []string {
	seen := make(map[string]struct{})
	var out []string
	appendError := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	for _, err := range response.Errors {
		appendError(err)
	}
	for _, err := range response.Validation.Errors {
		appendError(err)
	}
	for _, blocker := range response.Validation.Blockers {
		detail := firstNonEmpty(blocker.Detail, blocker.FieldPath, blocker.Reason)
		if blocker.Reason != "" && detail != blocker.Reason {
			appendError(blocker.Reason + ": " + detail)
			continue
		}
		appendError(detail)
	}
	return out
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
	completePath := filepath.Join(backup.HostDir, "COMPLETE")
	if _, err := os.Stat(completePath); err == nil {
		if _, err := stager.CopyToNode(ctx, environment, node, completePath, filepath.Join(backup.BackupDir, "COMPLETE")); err != nil {
			return fmt.Errorf("stage COMPLETE marker for validation: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat COMPLETE marker for validation: %w", err)
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
	result.BackupDir, result.HostDir, result.fixture = backup.BackupDir, backup.HostDir, backup.fixture
	copyRepositoryEvidence(&result, backup)
	if len(backup.Artifacts) == 0 {
		return result, fmt.Errorf("no cluster backup artifacts recorded for %q", systemBackupKey(target))
	}
	vr, ok := driver.(env.VolumeReplacementDriver)
	if !ok {
		return result, fmt.Errorf("driver %q does not support volume replacement", driver.Name())
	}
	statefulSet := firstNonEmpty(stringTarget(target, "statefulSet"), "myceld")
	repositoryMode := strings.TrimSpace(backup.RepositoryID) != "" || strings.TrimSpace(result.RepositoryID) != ""
	if !repositoryMode {
		clusterRestore, ok := driver.(env.ClusterRestoreDriver)
		if !ok {
			return result, fmt.Errorf("driver %q does not support cluster restore CLI primitives", driver.Name())
		}
		planNode := env.NodeRef{Ordinal: intTarget(target, "planOrdinal", intTarget(target, "ordinal", 0))}
		if plan, err := executeClusterRestorePlan(ctx, driver, environment, backup, planNode); err != nil {
			return result, err
		} else {
			result.RestorePlan = plan
		}
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
		// Create only replacement PVCs before restore. Applying the StatefulSet
		// here would start fresh daemons, initialize the new volumes, and trip the
		// fail-closed empty-PVC restore proof before archives are applied.
		if _, err := vr.ApplyYAML(ctx, environment, renderRestorePVCs(environment.Namespace, statefulSet, scenario)); err != nil {
			return result, err
		}
		if err := vr.WaitPVCs(ctx, environment, statefulSet, scenario.Cluster.Nodes); err != nil {
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
		backupSetPath := filepath.Join(backup.HostDir, backupSetManifestName)
		for _, artifact := range backup.Artifacts {
			pvcName := fmt.Sprintf("data-%s-%d", statefulSet, artifact.Ordinal)
			restoreResult, err := clusterRestore.RestoreBackupSetOrdinalToPVC(ctx, environment, env.RestoreBackupSetRequest{PVCName: pvcName, BackupSetPath: backupSetPath, Artifacts: restoreArtifactsForEnv(backup.Artifacts), Ordinal: artifact.Ordinal, Image: scenario.Cluster.Image})
			result.RestoreResults = append(result.RestoreResults, SystemBackupRestoreResult{Ordinal: artifact.Ordinal, PVCName: pvcName, Stdout: bounded(restoreResult.Stdout, 8192), Stderr: bounded(restoreResult.Stderr, 8192)})
			if err != nil {
				return result, err
			}
		}
		if _, err := vr.ApplyYAML(ctx, environment, manifests.YAML); err != nil {
			return result, err
		}
		if err := vr.ScaleStatefulSet(ctx, environment, statefulSet, scenario.Cluster.Nodes); err != nil {
			return result, err
		}
		result.BackupSetID = backup.BackupSetID
		result.Artifacts = backup.Artifacts
		return result, nil
	}

	repositoryRestore, ok := driver.(env.RepositoryClusterRestoreDriver)
	if !ok {
		return result, fmt.Errorf("driver %q does not support repository cluster restore PVC helpers", driver.Name())
	}
	oldUIDs, err := vr.PVCUIDs(ctx, environment, statefulSet, scenario.Cluster.Nodes)
	if err != nil {
		return result, err
	}
	result.OldPVCUIDs = oldUIDs
	if err := vr.ScaleStatefulSet(ctx, environment, statefulSet, 0); err != nil {
		return result, err
	}
	for ordinal := 0; ordinal < scenario.Cluster.Nodes; ordinal++ {
		if err := vr.DeletePVC(ctx, environment, fmt.Sprintf("data-%s-%d", statefulSet, ordinal)); err != nil {
			return result, err
		}
	}
	if _, err := vr.ApplyYAML(ctx, environment, renderRestorePVCs(environment.Namespace, statefulSet, scenario)); err != nil {
		return result, err
	}
	if err := vr.WaitPVCs(ctx, environment, statefulSet, scenario.Cluster.Nodes); err != nil {
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
	restoreResults, err := repositoryRestore.RestoreRepositoryBackupSetToPVCs(ctx, environment, env.RestoreRepositoryBackupSetRequest{StatefulSet: statefulSet, Bucket: firstNonEmpty(stringTarget(target, "bucket"), result.RepositoryBucket), RepositoryPrefix: result.RepositoryPrefix, BackupSetPrefix: result.RepositoryBackupSetPrefix, BackupSetID: backup.BackupSetID, EndpointURI: firstNonEmpty(stringTarget(target, "endpoint"), "http://minio:9000"), Region: firstNonEmpty(stringTarget(target, "region"), "us-east-1"), AccessKeyEnv: "AWS_ACCESS_KEY_ID", SecretKeyEnv: "AWS_SECRET_ACCESS_KEY", AccessKeyValue: firstNonEmpty(stringTarget(target, "accessKey"), "mycelminio"), SecretKeyValue: firstNonEmpty(stringTarget(target, "secretKey"), "mycelminiosecret"), Artifacts: restoreArtifactsForEnv(backup.Artifacts), Image: scenario.Cluster.Image})
	for i, restoreResult := range restoreResults {
		ordinal := i
		if i < len(backup.Artifacts) {
			ordinal = backup.Artifacts[i].Ordinal
		}
		result.RestoreResults = append(result.RestoreResults, SystemBackupRestoreResult{Ordinal: ordinal, PVCName: fmt.Sprintf("data-%s-%d", statefulSet, ordinal), Stdout: bounded(restoreResult.Stdout, 8192), Stderr: bounded(restoreResult.Stderr, 8192)})
	}
	if err != nil {
		return result, err
	}
	if err := vr.ScaleStatefulSet(ctx, environment, statefulSet, scenario.Cluster.Nodes); err != nil {
		return result, err
	}
	result.BackupSetID = backup.BackupSetID
	result.Artifacts = backup.Artifacts
	return result, nil
}

func renderRestorePVCs(namespace, statefulSet string, scenario spec.ResolvedScenario) string {
	if namespace == "" {
		namespace = "mycel-lab"
	}
	if statefulSet == "" {
		statefulSet = "myceld"
	}
	storageSize := scenario.Cluster.Storage.Size
	if strings.TrimSpace(storageSize) == "" {
		storageSize = "10Gi"
	}
	var b strings.Builder
	for ordinal := 0; ordinal < scenario.Cluster.Nodes; ordinal++ {
		if ordinal > 0 {
			b.WriteString("---\n")
		}
		fmt.Fprintf(&b, "apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: data-%s-%d\n  namespace: %s\n  labels:\n    app: myceld\nspec:\n  accessModes: [\"ReadWriteOnce\"]\n", statefulSet, ordinal, namespace)
		if scenario.Cluster.Storage.ClassName != "" {
			fmt.Fprintf(&b, "  storageClassName: %s\n", scenario.Cluster.Storage.ClassName)
		}
		fmt.Fprintf(&b, "  resources:\n    requests:\n      storage: %s\n", storageSize)
	}
	return b.String()
}

func executeClusterRestorePlan(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, backup SystemBackupOperationResult, node env.NodeRef) (string, error) {
	if backup.BackupDir == "" {
		return "", fmt.Errorf("backup dir is required for restore planning")
	}
	if err := stageClusterBackupForValidation(ctx, driver, environment, backup, node); err != nil {
		return "", err
	}
	cmd := append(offlineCLIBase(), "admin", "backup", "cluster", "restore-plan", "--backup-set", backup.BackupDir)
	execResult, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: cmd})
	if err != nil {
		return bounded(execResult.Stdout, 8192), restoreCLICompatibilityError(err, execResult)
	}
	return bounded(execResult.Stdout, 32768), nil
}

func restoreCLICompatibilityError(err error, execResult env.ExecResult) error {
	combined := strings.ToLower(execResult.Stdout + "\n" + execResult.Stderr + "\n" + err.Error())
	if strings.Contains(combined, "unknown flag: --backup-set") || strings.Contains(combined, "unknown command") || strings.Contains(combined, "unknown shorthand flag") {
		return fmt.Errorf("offline cluster restore CLI is unavailable in the deployed mycel image; rebuild/import an image from mycel develop containing MycelDB/mycel#156, then rerun: %w", err)
	}
	return err
}

func restoreArtifactsForEnv(artifacts []SystemBackupArtifact) []env.RestoreArtifact {
	out := make([]env.RestoreArtifact, 0, len(artifacts))
	for _, artifact := range artifacts {
		out = append(out, env.RestoreArtifact{Ordinal: artifact.Ordinal, PodName: artifact.PodName, ArchiveName: artifact.ArchiveName, ArchiveURI: artifact.ArchiveURI, ManifestName: artifact.ManifestName, ManifestURI: artifact.ManifestURI, ChecksumSHA256: artifact.ChecksumSHA256, LocalArchive: artifact.LocalArchive, LocalManifest: artifact.LocalManifest})
	}
	return out
}

type systemBackupFixtureCounter func(context.Context, provision.ActorAssignment, int, userBackupFixtureResult) (int64, int64, error)

func executeClusterRestoreVerify(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, state map[string]SystemBackupOperationResult, target map[string]any, result SystemBackupOperationResult) (SystemBackupOperationResult, error) {
	return executeClusterRestoreVerifyWithCounter(ctx, driver, environment, resources, state, target, result, querySystemBackupFixtureCounts)
}

func executeClusterRestoreVerifyWithCounter(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, state map[string]SystemBackupOperationResult, target map[string]any, result SystemBackupOperationResult, countFixture systemBackupFixtureCounter) (SystemBackupOperationResult, error) {
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
	execTimeout := durationTarget(target, "execTimeout", 30*time.Second)
	if verifyInterval <= 0 {
		verifyInterval = 5 * time.Second
	}
	if execTimeout <= 0 {
		execTimeout = 30 * time.Second
	}
	if verifyTimeout > 0 && execTimeout > verifyTimeout {
		execTimeout = verifyTimeout
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
			attemptCtx, attemptCancel := context.WithTimeout(verifyCtx, execTimeout)
			_, execErr := driver.Exec(attemptCtx, environment, env.NodeRef{Name: node.Name, Ordinal: node.Ordinal}, env.ExecRequest{Command: cmd})
			attemptCancel()
			if execErr != nil {
				err := execErr
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
	expectedNodes := intTarget(target, "expectedNodes", 3)
	expectedEdges := intTarget(target, "expectedEdges", 1)
	if expectedNodes < 1 || expectedNodes > 100 || expectedEdges < 0 || expectedEdges > 100 {
		return result, errors.New("restored system-backup fixture expectations are invalid")
	}
	fixture := state[systemBackupKey(target)].fixture
	if fixture == nil || len(fixture.nodeIDs) != expectedNodes || fixture.edgeID == "" {
		return result, errors.New("system-backup fixture identity checkpoint is unavailable")
	}
	nodeCount, edgeCount, err := countFixture(verifyCtx, assignment, expectedEdges, *fixture)
	if err != nil {
		return result, errors.New("restored system-backup fixture count verification failed")
	}
	result.FixtureNodes, result.FixtureEdges = nodeCount, edgeCount
	if nodeCount != int64(expectedNodes) || edgeCount != int64(expectedEdges) {
		return result, fmt.Errorf("restored system-backup fixture counts do not match expected nodes=%d edges=%d", expectedNodes, expectedEdges)
	}
	result.FixtureProved = true
	return result, nil
}

func querySystemBackupFixtureCounts(ctx context.Context, assignment provision.ActorAssignment, expectedEdges int, fixture userBackupFixtureResult) (int64, int64, error) {
	client, err := mycel.Dial(ctx, mycel.Config{Addr: assignment.DaemonAddr, Username: assignment.Username, Password: assignment.Password, ClientName: "mycel-lab-system-backup-verify"})
	if err != nil {
		return 0, 0, err
	}
	defer client.Close()
	nodeResult, err := client.QueryGQLReadOnly(ctx, assignment.SpaceID, assignment.DomainID, "MATCH (n) RETURN count(n) FETCH FIRST 1 ROW ONLY", 1)
	if err != nil {
		return 0, 0, err
	}
	nodeCount, err := systemBackupCountFromResult(nodeResult)
	if err != nil {
		return 0, 0, err
	}
	sessionID, err := client.OpenSession(ctx, assignment.SpaceID, assignment.DomainID)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = client.CloseSession(ctx, sessionID) }()
	transactionID, err := client.BeginReadOnlyTransaction(ctx, sessionID)
	if err != nil {
		return 0, 0, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = client.CloseTransaction(ctx, transactionID)
		}
	}()
	observedNodes := make([]*clientv1.Node, 0, len(fixture.nodeIDs))
	for _, nodeID := range fixture.nodeIDs {
		node, getErr := client.GetNode(ctx, transactionID, nodeID)
		if getErr != nil {
			return 0, 0, errors.New("restored system-backup fixture node identity mismatch")
		}
		observedNodes = append(observedNodes, node)
	}
	callCtx, cancel := client.AuthCallContext(ctx)
	edgeResponse, edgeErr := client.Graph.GetEdge(callCtx, &clientv1.GetEdgeRequest{TransactionId: transactionID, EdgeId: fixture.edgeID})
	cancel()
	if edgeErr != nil || !systemBackupFixtureIdentityMatches(fixture, observedNodes, edgeResponse.GetEdge()) {
		return 0, 0, errors.New("restored system-backup fixture identity mismatch")
	}
	var edgeCount int64
	pageToken := ""
	seenTokens := map[string]struct{}{}
	for edgeCount <= int64(expectedEdges) {
		callCtx, cancel := client.AuthCallContext(ctx)
		response, listErr := client.Graph.ListEdges(callCtx, &clientv1.ListEdgesRequest{TransactionId: transactionID, PageSize: 100, PageToken: pageToken})
		cancel()
		if listErr != nil {
			return 0, 0, listErr
		}
		edgeCount += int64(len(response.GetEdges()))
		pageToken = response.GetNextPageToken()
		if pageToken == "" {
			break
		}
		if _, duplicate := seenTokens[pageToken]; duplicate {
			return 0, 0, errors.New("restored system-backup fixture edge pagination repeated")
		}
		seenTokens[pageToken] = struct{}{}
	}
	if err := client.CloseTransaction(ctx, transactionID); err != nil {
		return 0, 0, err
	}
	closed = true
	return nodeCount, edgeCount, nil
}

func systemBackupFixtureIdentityMatches(fixture userBackupFixtureResult, nodes []*clientv1.Node, edge *clientv1.Edge) bool {
	if len(nodes) != len(fixture.nodeIDs) || edge == nil || edge.GetEdgeId() != fixture.edgeID || edge.GetFromNodeId() != fixture.edgeFrom || edge.GetToNodeId() != fixture.edgeTo {
		return false
	}
	for index, nodeID := range fixture.nodeIDs {
		if nodes[index] == nil || nodes[index].GetNodeId() != nodeID {
			return false
		}
	}
	return true
}

func systemBackupCountFromResult(result *clientv1.QueryResult) (int64, error) {
	if len(result.GetRows()) != 1 {
		return 0, errors.New("restored system-backup count query returned an invalid row count")
	}
	for _, value := range result.GetRows()[0].GetFields() {
		if scalar := value.GetScalar(); scalar != nil {
			number := scalar.GetNumberValue()
			if math.Trunc(number) == number && number >= 0 {
				return int64(number), nil
			}
		}
	}
	return 0, errors.New("restored system-backup count query did not return a nonnegative integer")
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

func backupFailureMatchesExpectation(target map[string]any, fragments ...string) bool {
	expected := strings.TrimSpace(firstNonEmpty(stringTarget(target, "expectedFailureContains"), stringTarget(target, "expectedErrorContains"), stringTarget(target, "expectedBlocker")))
	if expected == "" {
		return true
	}
	haystack := strings.ToLower(strings.Join(fragments, "\n"))
	return strings.Contains(haystack, strings.ToLower(expected))
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
