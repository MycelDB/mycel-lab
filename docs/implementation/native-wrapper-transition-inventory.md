# Native transition inventory for wrapper suites

## Status

NT0 inventory for [mycel-lab#12](https://github.com/MycelDB/mycel-lab/issues/12).

This document captures the parity requirements for replacing transitional Mycel
Lab wrapper suites with native Mycel Lab implementation. Earlier transitional
suites used `host-command` events to invoke legacy MycelDB scripts/harnesses
while Mycel Lab owned run entrypoints, dry-run planning, and artifact roots. The
public wrapper suites have now been replaced by native scenarios.

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
| `compose-user-backup-restore` | native `compose-user-backup-restore` | Native Mycel Lab Compose scenario | Principal-scoped backup export/import, graph/blob restore, fresh-cluster reset, and safety checks on Compose. |
| `k3d-system-backup-restore` | native `k3d-system-backup-restore` | Native Mycel Lab k3d scenario | Full-cluster backup/restore on disposable k3d, including PVC replacement/restore evidence. |
| `k3d-raft-restart-soak` | native `k3d-raft-restart-soak` | Native Mycel Lab k3d scenario | One-hour moderate restart/write soak using rotating single-node restarts. |
| `k3d-raft-restart-hard-soak` | native `k3d-raft-restart-hard-soak` | Native Mycel Lab k3d scenario | One-hour harder restart/write soak using frequent rotating single-node restarts. |

## NT1: native k3d restart soak parity

Status: implemented with native Mycel Lab scenarios and repeatable rotating
`node-restart` events.

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

Status: implemented with a native Compose scenario. The suite now creates source
fixtures, exports and validates principal backup archives, stages archives across
a fresh Compose reset, imports into restored principals, verifies restored graph
and blob counts across Compose endpoints, and asserts archive/login safety.

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
| Compose lifecycle reset | Native `environment-reset` support for Compose. | Destructive operator run still required before NT4 retirement. |
| Multi-user provisioning | Native scenario covers empty, one-space, and complex source principals via actor assignments. | Declarative fixture-user syntax remains future ergonomics work. |
| Graph fixture with blobs | Native `user-backup-fixture` creates graph nodes, an edge, and a blob-backed node. | Broader fixture shapes can be added later. |
| User backup export/import | Native operation events exist: `user-backup-export`, `user-backup-validate`, `user-backup-import`. | None for NT2 parity. |
| Backup archive staging | Compose driver supports node-to-host and host-to-node archive staging. | Artifact-root routing can replace `/tmp/{runID}-...` staging later. |
| Fresh cluster restore | Native scenario resets Compose mid-run and imports staged archives into the fresh cluster. | None for NT2 parity. |
| Restored data verification | Native `user-backup-verify-restored` checks imported graph/blob counts and restored queries across Compose nodes. | Payload-by-payload diff remains future hardening. |
| Secret/session safety assertions | Native `user-backup-assert-safety` checks forbidden archive text and source-password login rejection. | Additional structured manifest/content assertions can be added later. |

### Proposed native deliverables

1. Native backup operation events are available:
   - `user-backup-fixture`;
   - `user-backup-export`;
   - `user-backup-validate`;
   - `environment-reset`;
   - `user-backup-import`;
   - `user-backup-verify-restored`;
   - `user-backup-assert-safety`.
2. The public `compose-user-backup-restore` suite now points at a native Compose
   scenario instead of `compose-user-backup-restore-harness`.
3. Destructive validation evidence is still required by NT4 before deleting or
   deprecating fallback harness files.

## NT3: native k3d system backup/restore parity

Status: implemented with a native k3d scenario. The suite creates a workload
fixture, triggers and validates coordinated cluster backup metadata, captures
node archives, replaces fresh PVCs with UID evidence, restores archives by
ordinal, restarts the StatefulSet, and verifies the restored workload through
all k3d pods.

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
| Workload writes | Native `system-backup-fixture` creates graph nodes, an edge, and a blob-backed node. | Broader workload profiles remain future hardening. |
| Coordinated cluster backup | Native `cluster-backup-create` event triggers Admin cluster backup and captures node archives. | None for NT3 parity. |
| Backup metadata validation | Native `cluster-backup-create` and `cluster-backup-validate` validate status, raft barrier/checkpoint evidence, checksums, and backup-set safety. | None for NT3 parity. |
| PVC wipe/restore | K3D driver implements `volume-replacement` primitives and `cluster-restore-apply` restores archives by ordinal. | None for NT3 parity. |
| StatefulSet restore restart | Native restore scales the StatefulSet down, restores PVC contents, scales up, and waits for readiness. | None for NT3 parity. |
| Restored local consistency counts | Final cluster assertions and restored workload queries cover restored availability. | Dedicated per-node consistency-report artifact remains future hardening. |
| Restored GQL read through session-capable pod | Native `cluster-restore-verify` queries the restored workload through every pod. | None for NT3 parity. |
| PVC UID changed evidence | Native restore captures and compares PVC UIDs before/after replacement. | None for NT3 parity. |

### Proposed native deliverables

1. K3D driver now advertises `volume-replacement` and implements PVC UID,
   namespace reset, YAML apply, StatefulSet scale, and ordinal archive restore
   primitives.
2. Native system backup/restore events are available:
   - `system-backup-fixture`;
   - `cluster-backup-create`;
   - `cluster-backup-validate`;
   - `cluster-restore-apply`;
   - `cluster-restore-verify`.
3. The public `k3d-system-backup-restore` suite now points at a native k3d
   scenario instead of `k3d-system-backup-restore-harness`.

## NT4: wrapper retirement criteria

Wrapper retirement status: public wrapper scenarios have been removed from suite
routing. Destructive operator evidence is still required before deleting legacy
manual fallback scripts/binaries from the Mycel daemon repo.

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
5. NT2a: add native user-backup export/validate/import operations. Done.
6. NT2b: add fixture graph/blob writer and restored-data assertions. Done.
7. NT2c: implement Compose environment reset/handoff for restore. Done.
8. NT2d: replace Compose user backup/restore wrapper. Done.
9. NT3a: implement k3d `volume-replacement`/PVC evidence capability. Done.
10. NT3b: implement cluster backup metadata assertions. Done.
11. NT3c: implement native system restore operation and restored workload checks. Done.
12. NT3d: replace k3d system backup/restore wrapper. Done.
13. NT4: update compatibility targets and retire wrapper scenarios. Done for Mycel Lab suite routing; legacy daemon harnesses remain manual fallbacks until destructive evidence is recorded.

## Tracking checklist

- [x] NT0 wrapper parity inventory.
- [x] NT1 native k3d restart-soak suites.
- [x] NT2 native Compose user backup/restore suite.
- [x] NT3 native k3d system backup/restore suite.
- [x] NT4 wrapper retirement/deprecation in Mycel Lab suite routing.
