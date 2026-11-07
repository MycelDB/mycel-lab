package events

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
)

const (
	defaultBlobSnapshotBucket       = "mycel-compose-blobs"
	defaultBlobSnapshotBlocker      = "blob snapshot payload unavailable"
	defaultBlobSnapshotWaitTimeout  = 2 * time.Minute
	defaultBlobSnapshotPollInterval = 500 * time.Millisecond
)

type BlobSnapshotRecoveryResult struct {
	Operation         string                       `json:"operation"`
	ActorID           string                       `json:"actorId,omitempty"`
	SpaceID           string                       `json:"spaceId,omitempty"`
	DomainID          string                       `json:"domainId,omitempty"`
	BlobObjectBucket  string                       `json:"blobObjectBucket,omitempty"`
	BlobObjectKey     string                       `json:"blobObjectKey,omitempty"`
	RecoveryObjectKey string                       `json:"recoveryObjectKey,omitempty"`
	SpaceDeleted      bool                         `json:"spaceDeleted,omitempty"`
	ObjectDeleted     bool                         `json:"objectDeleted,omitempty"`
	InitialReadiness  BlobSnapshotReadiness        `json:"initialReadiness"`
	RepairedReadiness BlobSnapshotReadiness        `json:"repairedReadiness"`
	RecoveryVerified  bool                         `json:"recoveryVerified"`
	Snapshot          *RaftSnapshotOperationResult `json:"snapshot,omitempty"`
}

type BlobSnapshotReadiness struct {
	Ready                  bool     `json:"ready"`
	ClientReady            bool     `json:"clientReady"`
	ProcessReady           bool     `json:"processReady"`
	MetadataApplied        bool     `json:"metadataApplied"`
	MetadataValidated      bool     `json:"metadataValidated"`
	MetadataReady          bool     `json:"metadataReady"`
	PartitionGroupsStarted bool     `json:"partitionGroupsStarted"`
	RaftReady              bool     `json:"raftReady"`
	ReadReady              bool     `json:"readReady"`
	WriteReady             bool     `json:"writeReady"`
	Blockers               []string `json:"blockers,omitempty"`
}

type blobSnapshotObject struct {
	Key  string `json:"key"`
	Type string `json:"type,omitempty"`
}

type blobSnapshotClusterStatus struct {
	Readiness struct {
		ClientReady            bool     `json:"client_ready"`
		ProcessReady           bool     `json:"process_ready"`
		MetadataApplied        bool     `json:"metadata_applied"`
		MetadataValidated      bool     `json:"metadata_validated"`
		MetadataReady          bool     `json:"metadata_ready"`
		PartitionGroupsStarted bool     `json:"partition_groups_started"`
		RaftReady              bool     `json:"raft_ready"`
		ReadReady              bool     `json:"read_ready"`
		WriteReady             bool     `json:"write_ready"`
		Blockers               []string `json:"readiness_blockers"`
	} `json:"readiness"`
}

// executeBlobSnapshotRecoveryEvent reproduces the missing object-store payload
// path and then restores the exact Mycel-encoded object to prove recovery. The
// object-store command runs inside the isolated Compose MinIO helper service;
// credentials and raw command output never enter the result artifact.
func executeBlobSnapshotRecoveryEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, target map[string]any) (BlobSnapshotRecoveryResult, error) {
	result := BlobSnapshotRecoveryResult{Operation: "blob-snapshot-recovery"}
	if driver.Name() != "compose" {
		return result, fmt.Errorf("blob-snapshot-recovery requires the Compose driver")
	}
	actorID := firstNonEmpty(stringTarget(target, "actorId"), stringTarget(target, "sourceActorId"))
	assignment, ok := findAssignment(resources, actorID)
	if !ok {
		return result, fmt.Errorf("actor assignment %q not found", actorID)
	}
	result.ActorID, result.SpaceID, result.DomainID = actorID, assignment.SpaceID, assignment.DomainID
	bucket := firstNonEmpty(stringTarget(target, "bucket"), defaultBlobSnapshotBucket)
	result.BlobObjectBucket = bucket

	objectKey, err := findBlobSnapshotObject(ctx, driver, environment, bucket, assignment.SpaceID, assignment.DomainID, target)
	if err != nil {
		return result, err
	}
	result.BlobObjectKey = objectKey
	recoveryObjectKey := objectKey + ".mycel-lab-issue312-recovery"
	result.RecoveryObjectKey = recoveryObjectKey
	if _, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc cp %s %s", shellQuote("local/"+bucket+"/"+objectKey), shellQuote("local/"+bucket+"/"+recoveryObjectKey))); err != nil {
		return result, fmt.Errorf("backup Mycel object-store payload: %w", err)
	}

	if boolTarget(target, "deleteSpace", false) {
		cmd := append(adminCLIBase(), "space", "delete", assignment.SpaceID)
		if _, err := driver.Exec(ctx, environment, env.NodeRef{Ordinal: intTarget(target, "nodeOrdinal", 0)}, env.ExecRequest{Command: cmd}); err != nil {
			return result, fmt.Errorf("delete disposable space %s: %w", assignment.SpaceID, err)
		}
		result.SpaceDeleted = true
	}
	if boolTarget(target, "deleteObject", true) {
		if _, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc rm --force %s", shellQuote("local/"+bucket+"/"+objectKey))); err != nil {
			return result, fmt.Errorf("delete Mycel object-store payload: %w", err)
		}
		result.ObjectDeleted = true
	}

	nodeOrdinal := intTarget(target, "nodeOrdinal", 0)
	if err := driver.RestartNode(ctx, environment, env.NodeRef{Ordinal: nodeOrdinal}); err != nil {
		return result, fmt.Errorf("restart ordinal %d with missing object-store payload: %w", nodeOrdinal, err)
	}
	initial, initialErr := awaitBlobSnapshotReadiness(ctx, driver, environment, nodeOrdinal, target, false)
	result.InitialReadiness = initial

	// Always restore the exact encoded object before returning. This keeps the
	// disposable cluster recoverable even when the reproduction assertion fails.
	restoreErr := restoreBlobSnapshotObject(ctx, driver, environment, bucket, objectKey, recoveryObjectKey)
	if restoreErr == nil {
		if err := driver.RestartNode(ctx, environment, env.NodeRef{Ordinal: nodeOrdinal}); err != nil {
			restoreErr = fmt.Errorf("restart ordinal %d after object restore: %w", nodeOrdinal, err)
		} else {
			result.RepairedReadiness, restoreErr = awaitBlobSnapshotReadiness(ctx, driver, environment, nodeOrdinal, target, true)
			result.RecoveryVerified = restoreErr == nil && result.RepairedReadiness.Ready
		}
	}
	if cleanupErr := deleteBlobSnapshotRecoveryObject(ctx, driver, environment, bucket, recoveryObjectKey); restoreErr == nil && cleanupErr != nil {
		restoreErr = cleanupErr
	}
	if initialErr != nil {
		return result, errors.Join(initialErr, restoreErr)
	}
	if restoreErr != nil {
		return result, restoreErr
	}
	return result, nil
}

func findBlobSnapshotObject(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, bucket, spaceID, domainID string, target map[string]any) (string, error) {
	prefix := firstNonEmpty(stringTarget(target, "objectPrefix"), fmt.Sprintf("spaces/%s/domains/%s/objects/", spaceID, domainID))
	deadline := time.Now().Add(durationTarget(target, "objectWaitTimeout", 30*time.Second))
	for {
		res, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc ls --recursive --json %s", shellQuote("local/"+bucket)))
		if err == nil {
			scanner := bufio.NewScanner(strings.NewReader(res.Stdout))
			var matches []string
			for scanner.Scan() {
				var object blobSnapshotObject
				if json.Unmarshal([]byte(scanner.Text()), &object) == nil && strings.HasPrefix(object.Key, prefix) && object.Key != "" && !strings.HasSuffix(object.Key, "/") {
					matches = append(matches, object.Key)
				}
			}
			if len(matches) == 1 {
				return matches[0], nil
			}
			if len(matches) > 1 {
				return "", fmt.Errorf("expected one Mycel blob object under prefix %q, found %d", prefix, len(matches))
			}
		}
		if !time.Now().Before(deadline) {
			return "", fmt.Errorf("Mycel blob object under prefix %q was not visible before timeout", prefix)
		}
		if err := sleepContext(ctx, defaultBlobSnapshotPollInterval); err != nil {
			return "", err
		}
	}
}

func objectStoreCommand(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, command string) (env.ExecResult, error) {
	executor, ok := driver.(env.ObjectStoreExecutor)
	if !ok {
		return env.ExecResult{}, fmt.Errorf("driver %q does not support object-store execution", driver.Name())
	}
	wrapped := fmt.Sprintf("mc alias set local http://minio:9000 %s %s >/dev/null && %s", shellQuote("mycelminio"), shellQuote("mycelminiosecret"), command)
	return executor.ExecObjectStore(ctx, environment, wrapped)
}

func restoreBlobSnapshotObject(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, bucket, objectKey, recoveryObjectKey string) error {
	if _, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc cp %s %s", shellQuote("local/"+bucket+"/"+recoveryObjectKey), shellQuote("local/"+bucket+"/"+objectKey))); err != nil {
		return fmt.Errorf("restore Mycel object-store payload: %w", err)
	}
	return nil
}

func deleteBlobSnapshotRecoveryObject(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, bucket, recoveryObjectKey string) error {
	if _, err := objectStoreCommand(ctx, driver, environment, fmt.Sprintf("mc rm --force %s", shellQuote("local/"+bucket+"/"+recoveryObjectKey))); err != nil {
		return fmt.Errorf("remove object-store recovery copy: %w", err)
	}
	return nil
}

func awaitBlobSnapshotReadiness(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, ordinal int, target map[string]any, wantReady bool) (BlobSnapshotReadiness, error) {
	timeout := durationTarget(target, "readinessTimeout", defaultBlobSnapshotWaitTimeout)
	interval := durationTarget(target, "readinessInterval", defaultBlobSnapshotPollInterval)
	deadline := time.Now().Add(timeout)
	var last BlobSnapshotReadiness
	var lastErr error
	for {
		res, err := driver.Exec(ctx, environment, env.NodeRef{Ordinal: ordinal}, env.ExecRequest{Command: append(adminCLIBase(), "cluster", "status")})
		if err == nil {
			last, lastErr = parseBlobSnapshotReadiness(res.Stdout)
			if lastErr == nil && last.Ready == wantReady {
				if !wantReady {
					blocker := strings.ToLower(firstNonEmpty(stringTarget(target, "expectedBlocker"), defaultBlobSnapshotBlocker))
					joined := strings.ToLower(strings.Join(last.Blockers, "\n"))
					if !strings.Contains(joined, blocker) {
						lastErr = fmt.Errorf("unready node did not report expected blocker %q", blocker)
					} else {
						return last, nil
					}
				} else {
					return last, nil
				}
			}
		} else {
			lastErr = err
		}
		if !time.Now().Before(deadline) {
			if lastErr == nil {
				lastErr = fmt.Errorf("readiness remained ready=%t, want %t", last.Ready, wantReady)
			}
			return last, lastErr
		}
		if err := sleepContext(ctx, interval); err != nil {
			return last, err
		}
	}
}

func parseBlobSnapshotReadiness(raw string) (BlobSnapshotReadiness, error) {
	var status blobSnapshotClusterStatus
	if err := json.Unmarshal([]byte(raw), &status); err != nil {
		return BlobSnapshotReadiness{}, fmt.Errorf("decode cluster status: %w", err)
	}
	r := status.Readiness
	return BlobSnapshotReadiness{
		Ready:                  r.ClientReady && r.ProcessReady && r.MetadataApplied && r.MetadataValidated && r.MetadataReady && r.PartitionGroupsStarted && r.RaftReady && r.ReadReady && r.WriteReady,
		ClientReady:            r.ClientReady,
		ProcessReady:           r.ProcessReady,
		MetadataApplied:        r.MetadataApplied,
		MetadataValidated:      r.MetadataValidated,
		MetadataReady:          r.MetadataReady,
		PartitionGroupsStarted: r.PartitionGroupsStarted,
		RaftReady:              r.RaftReady,
		ReadReady:              r.ReadReady,
		WriteReady:             r.WriteReady,
		Blockers:               append([]string(nil), r.Blockers...),
	}, nil
}
