# Native transition inventory for wrapper suites

## Status

NT0 inventory for [mycel-lab#12](https://github.com/MycelDB/mycel-lab/issues/12).

This document captures the parity requirements for replacing transitional Mycel
Lab wrapper suites with native Mycel Lab implementation. The current wrappers
use `host-command` events to invoke legacy MycelDB scripts/harnesses while Mycel
Lab owns run entrypoints, dry-run planning, and artifact roots.

## Goals

- Identify what each wrapper currently proves.
- Define native Mycel Lab actors, events, operations, assertions, and artifacts
  needed for equivalent or better coverage.
- Sequence implementation so wrappers can be retired safely.
- Keep destructive actions explicit via `--confirm-destructive`.

## Non-goals

- Removing wrapper suites before parity exists.
- Adding generic remote Kubernetes or cloud drivers.
- Moving fast Go/package tests out of `mycel`.
- Reintroducing shell-specific behavior into the core runner except where a
  wrapper is intentionally transitional.

## Current wrapper suites

| Suite | Wrapper scenario | Legacy entrypoint | Purpose |
| --- | --- | --- | --- |
| `compose-user-backup-restore` | `compose-user-backup-restore-harness` | `../mycel/scripts/testComposeUserBackupRestore.sh` | Principal-scoped backup export/import, graph/blob restore, and safety checks on Compose. |
| `k3d-system-backup-restore` | `k3d-system-backup-restore-harness` | `go run ./cmd/mycel-system-backuptest ...` | Full-cluster backup/restore on disposable k3d/K3s, including PVC wipe/restore evidence. |
| `k3d-raft-restart-soak` | `k3d-raft-restart-soak-harness` | `go run ./cmd/mycel-raft-disrupttest --profile restart-soak-1h ...` | One-hour moderate restart/write soak using edge workload. |
| `k3d-raft-restart-hard-soak` | `k3d-raft-restart-hard-soak-harness` | `go run ./cmd/mycel-raft-disrupttest --profile restart-soak-hard-1h ...` | One-hour harder restart/write soak using multi-space workload. |

## NT1: native k3d restart soak parity

### Legacy behavior to preserve

| Legacy profile | Duration | Writers | Rate | Restart cadence | Workload | Current wrapper suite |
| --- | ---: | ---: | ---: | --- | --- | --- |
| `restart-soak-1h` | 1h | 4 | 2/s | rotating single-pod restart every 3m | `edges` | `k3d-raft-restart-soak` |
| `restart-soak-hard-1h` | 1h | 6 | 3/s | rotating single-pod restart every 1m | `multi-space` | `k3d-raft-restart-hard-soak` |

Legacy disruption harness assertions include:

- successful writes are counted separately from ambiguous/transient/permanent
  failures;
- committed/read-index checks have no permanent failures;
- final client-observed and per-node counts converge;
- transient exhausted retries during intentional restarts are warnings only when
  final convergence succeeds;
- permanent write failures are zero;
- cluster identity does not diverge;
- artifacts include setup details, workload events, disruption/restart evidence,
  final summary, and failure diagnostics.

### Native Mycel Lab requirements

| Requirement | Current support | Gap |
| --- | --- | --- |
| Long-running phases | Supported by phase durations. | Need documented soak profiles and operator guidance. |
| Rotating node restarts | `node-restart` exists; sequential phases work. | Need compact schedule syntax or generated scenario support for 1h/3m and 1h/1m cadences to avoid huge YAML. |
| Edge workload | Graph actor supports edges through `graph-committer` operation mix. | Need an explicit edge-heavy actor profile matching legacy `edges` workload counts. |
| Multi-space workload | Provisioning supports per-actor spaces/domains. | Need actor/profile support for a legacy-style three-scope multi-space workload and final per-scope counts. |
| Successful/ambiguous/transient/permanent write counters | Actor events record acknowledged/failed writes. | Need a soak summary that separates ambiguous vs transient vs permanent classes and treats expected degradation correctly. |
| Final convergence | Final oracle checks graph counts by actor. | Need per-node/per-endpoint convergence checks, not only assignment endpoint checks. |
| Cluster identity health | Implemented final cluster assertions. | Need repeated/per-phase health assertions during soak recovery windows. |
| Artifacts | Current run artifacts and environment capture. | Need concise soak summary comparable to legacy harness output. |

### Proposed native deliverables

1. Add explicit actor profiles:
   - `raft-edge-soak-writer`;
   - `raft-multi-space-soak-writer`.
2. Add a restart schedule model, either:
   - phase-level `repeat`/`interval` support; or
   - generated checked-in scenarios with clear names.
3. Add per-endpoint final graph convergence checks.
4. Add `oracle/soak-summary.json` and Markdown summary.
5. Replace wrapper suites with native suite definitions:
   - `k3d-raft-restart-soak`;
   - `k3d-raft-restart-hard-soak`.

## NT2: native Compose user backup/restore parity

### Legacy behavior to preserve

The legacy script `scripts/testComposeUserBackupRestore.sh` currently:

1. builds a local Mycel image unless disabled;
2. resets the Compose cluster;
3. validates source cluster identity;
4. creates multiple users:
   - empty user;
   - one-space user;
   - complex user;
5. creates spaces/domains;
6. writes graph fixtures with:
   - plain nodes;
   - edges;
   - blob-backed nodes;
7. exports one backup archive per principal;
8. validates backup archives;
9. wipes and recreates the Compose cluster;
10. imports backups into the fresh cluster with new passwords;
11. verifies restored users, spaces, domains, graph counts, edge counts, and blob
    payloads through every Compose service;
12. asserts safety properties, including no active secret/session leakage and
    source passwords no longer authenticating for restored users;
13. retains temporary artifacts on failure when requested.

### Native Mycel Lab requirements

| Requirement | Current support | Gap |
| --- | --- | --- |
| Compose lifecycle reset | Compose driver supports reset/create/delete. | Need explicit mid-scenario environment reset/recreate operation or suite split with artifact handoff. |
| Multi-user provisioning | Provisioner creates actor users/spaces/domains. | Need declarative fixture users not tied only to actor assignments. |
| Graph fixture with blobs | Graph actor writes nodes/edges. | Need blob-node creation actor/operation and blob payload verification. |
| User backup export/import | Not native. | Need admin operation event(s): `user-backup-export`, `user-backup-validate`, `user-backup-import`. |
| Backup archive staging | Not native. | Need artifact path model and driver file copy/staging for Compose services. |
| Fresh cluster restore | Compose driver can delete/create once per scenario. | Need reset/recreate event, or two-scenario suite with artifact handoff. |
| Restored data verification | Partial graph count checks exist. | Need per-user restored space/domain/blob assertions across all endpoints. |
| Secret/session safety assertions | Not native. | Need backup manifest/content assertion and login-negative assertion. |

### Proposed native deliverables

1. Add backup operation model under a new package such as
   `internal/reliability/backupops` or `internal/reliability/operations`.
2. Add scenario events:
   - `fixture-user-create`;
   - `fixture-graph-blob-write`;
   - `user-backup-export`;
   - `user-backup-validate`;
   - `environment-reset` for Compose;
   - `user-backup-import`;
   - `restored-user-verify`.
3. Add artifact routing for backup archives:
   - `backup/user/<username>.tar.zst`;
   - validation JSON/Markdown;
   - restore import reports.
4. Add assertions:
   - restored user exists;
   - restored spaces/domains exist;
   - restored graph/blob payloads match expected fixture;
   - source password rejected after restore with new password;
   - forbidden secret/session material absent.
5. Replace `compose-user-backup-restore-harness` with native Compose scenario(s).

## NT3: native k3d system backup/restore parity

### Legacy behavior to preserve

The legacy `cmd/mycel-system-backuptest` currently validates:

1. disposable k3d/K3s cluster creation;
2. graph workload writes through normal APIs;
3. pre-backup per-pod count convergence;
4. coordinated cluster system backup;
5. raft freeze/checkpoint evidence in `backup-set.json`;
6. namespace/PVC wipe;
7. ordinal archive restore into fresh PVCs;
8. StatefulSet restart;
9. restored cluster health;
10. per-pod local consistency counts;
11. restored workload GQL read through a session-capable pod;
12. PVC UID change evidence;
13. artifacts under `artifacts/system-backup-restore/<timestamp>-<cluster>/`.

Legacy profiles:

| Profile | Writes | Default workload |
| --- | ---: | --- |
| `backup-smoke` | 40 | `edges` |
| `backup-small` | 200 | operator-selected |
| `backup-multi-space` | 90 | operator-selected |

### Native Mycel Lab requirements

| Requirement | Current support | Gap |
| --- | --- | --- |
| k3d lifecycle | Supported. | None for baseline lifecycle. |
| Workload writes | Graph actors support writes. | Need profile parity for `nodes`, `edges`, `multi-space` and count expectations. |
| Coordinated cluster backup | Not native. | Need admin cluster backup operation. |
| Backup metadata validation | Not native. | Need `backup-set.json` parser/assertions for raft freeze/checkpoint evidence. |
| PVC wipe/restore | Not native. | Need `volume-replacement`/PVC operation in k3d driver and archive placement by ordinal. |
| StatefulSet restore restart | Rolling restart exists. | Need restore-specific ordering and readiness gates. |
| Restored local consistency counts | Not native. | Need per-node consistency-report assertions. |
| Restored GQL read through session-capable pod | Partially supported by query actors/oracle. | Need post-restore session recreation and restored workload read assertion. |
| PVC UID changed evidence | Not native. | Need k3d/Kubernetes PVC metadata capture before/after restore. |

### Proposed native deliverables

1. Add `CapabilityVolumeReplacement` implementation to `K3DDriver` for controlled
   PVC/data replacement only in disposable k3d environments.
2. Add cluster backup/restore operations:
   - `cluster-backup-create`;
   - `cluster-backup-validate`;
   - `cluster-restore-stage-ordinal`;
   - `cluster-restore-apply`.
3. Add backup metadata assertions for:
   - backup-set shape;
   - raft freeze evidence;
   - graph/checkpoint evidence;
   - expected ordinal archives.
4. Add PVC evidence artifacts:
   - pre-wipe PVC list/UIDs;
   - post-restore PVC list/UIDs;
   - restore placement log.
5. Add restored workload assertions:
   - per-node local consistency counts;
   - session-capable GQL read;
   - cluster identity and health.
6. Replace `k3d-system-backup-restore-harness` with native k3d scenario(s).

## NT4: wrapper retirement criteria

A wrapper suite can be retired when all of the following are true:

1. A native suite exists with the same public suite name.
2. The native suite has dry-run validation coverage.
3. The native suite has at least one successful destructive operator run recorded
   with artifacts.
4. Legacy assertions are mapped to native assertions or intentionally documented
   as superseded.
5. Mycel compatibility Make target delegates to the native suite without invoking
   the legacy script/binary.
6. Documentation has been updated in both `mycel-lab` and `mycel`.
7. The old harness/script remains available only as a temporary manual fallback
   or is explicitly deprecated.

## Recommended implementation sequence

1. NT1a: add soak profile documentation and edge-heavy/multi-space actor profiles.
2. NT1b: add restart schedule support or generated long-running native restart
   soak scenarios.
3. NT1c: add per-endpoint final convergence and soak summary artifacts.
4. NT1d: replace k3d restart-soak wrapper scenarios with native scenarios.
5. NT2a: add native user-backup export/validate/import operations.
6. NT2b: add fixture graph/blob writer and restored-data assertions.
7. NT2c: implement Compose environment reset/handoff for restore.
8. NT2d: replace Compose user backup/restore wrapper.
9. NT3a: implement k3d `volume-replacement`/PVC evidence capability.
10. NT3b: implement cluster backup metadata assertions.
11. NT3c: implement native system restore operation and restored workload checks.
12. NT3d: replace k3d system backup/restore wrapper.
13. NT4: update compatibility targets and retire wrapper scenarios.

## Tracking checklist

- [x] NT0 wrapper parity inventory.
- [ ] NT1 native k3d restart-soak suites.
- [ ] NT2 native Compose user backup/restore suite.
- [ ] NT3 native k3d system backup/restore suite.
- [ ] NT4 wrapper retirement/deprecation.
