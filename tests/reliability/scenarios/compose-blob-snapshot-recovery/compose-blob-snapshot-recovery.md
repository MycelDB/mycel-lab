# compose-blob-snapshot-recovery

## Purpose

Reproduce MycelDB/mycel#312 with a deterministic, credential-free Compose cluster and an isolated MinIO object store. The scenario creates a Mycel-managed blob, captures a retained Raft snapshot, deletes the disposable space and its object-store payload, restarts one daemon from its retained volume, records the readiness blocker, restores the exact Mycel object-store payload, and proves that the daemon can recover.

This is a diagnostic/reproduction scenario, not a release-gate claim. It records the current daemon behavior so the daemon fix can be developed against a stable oracle.

## What this proves

- The Compose fixture uses the object-store blob backend and an isolated MinIO bucket.
- The blob payload is captured through Mycel and the exact object-store key is discovered without retaining raw payload bytes.
- A retained Raft snapshot is created before destructive object-store manipulation.
- The restarted daemon reports the expected sanitized blocker when the payload is unavailable.
- Restoring the valid Mycel-encoded object to its original key allows the daemon to become ready again.
- The final cluster converges and cleanup remains owned by the Lab runner.

The scenario deliberately preserves the encoded object before deletion. Restoring the raw fixture text instead would not prove a valid Mycel object-store repair.

## Scenario phases

1. **create-blob-fixture** — creates one disposable graph/blob fixture through the provisioned actor.
2. **capture-retained-snapshot** — compacts local Raft snapshots on ordinal 0.
3. **remove-payload-restart-and-repair** — backs up the encoded MinIO object, deletes the space and object, restarts ordinal 0, requires the `blob snapshot payload unavailable` readiness blocker, restores the encoded object, restarts again, and requires readiness.
4. **final-cluster-recovery** — verifies stable cluster readiness after repair.

## Scope and limitations

- Uses local MinIO as an S3-compatible backend; AWS S3-specific behavior still requires separate confirmation.
- The current scenario covers the observed missing-payload path. A follow-up variant should omit manual object deletion to isolate whether normal API space deletion alone leaves an unsafe retained-snapshot reference.
- The scenario does not implement the daemon repair; it provides the reproduction and recovery oracle for MycelDB/mycel#312.
- The destructive run requires fresh approval and `--confirm-destructive`.

## Evidence

- `events.jsonl` records fixture, snapshot, object-store, readiness, and repair phases.
- The `blobSnapshotRecovery` result records only bounded IDs, object key metadata, readiness dimensions, and sanitized blockers.
- Raw object payloads, credentials, and MinIO command output are not persisted as scenario results.
- `result.json`, `summary.md`, and final cleanup/absence evidence are required before treating a run as valid.

## Run

```text
mycel-lab run scenario compose-blob-snapshot-recovery --dry-run
```

Real execution is destructive and requires fresh approval:

```text
mycel-lab run scenario compose-blob-snapshot-recovery \
  --confirm-destructive \
  --image myceldb/mycel:<exact-candidate>
```

## Related issue

- [MycelDB/mycel#312](https://github.com/MycelDB/mycel/issues/312)
- [MycelDB/mycel-lab#105](https://github.com/MycelDB/mycel-lab/issues/105)
