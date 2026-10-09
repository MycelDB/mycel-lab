# MycelDB system integration migration inventory

## Status

SIM0 inventory for [mycel-lab#7](https://github.com/MycelDB/mycel-lab/issues/7).

Source repository audited: `mycel` on `develop`.

This inventory maps current MycelDB system-integration entrypoints to proposed
Mycel Lab scenarios and suites. It is intentionally planning-only: no legacy
scripts or Make targets are changed in this tranche.

## Executive summary

Current `mycel` CI runs `make test`, `make build`, and a strict public-surface
check. Destructive system-integration targets are documented and manually invoked
through Make targets; GitHub Actions does not currently run Compose/k3d system
flows by default.

Migration should proceed in this order:

1. Compose cluster validation (`SIM1`) because the `compose` environment driver
   already covers lifecycle, endpoints, logs, and restart behavior.
2. k3d cluster validation (`SIM2`) while explicitly separating PVC replacement
   because it needs a durable volume/PVC operation contract.
3. Raft disruption and soak profiles (`SIM3`/`SIM5`) after workload/profile
   modeling is explicit in Mycel Lab.
4. Backup/restore flows (`SIM4`) because they require additional operation and
   assertion support around backup archives, restore execution, and safety
   checks.
5. Release-gate compatibility targets after their component suites exist.

## Current entrypoints

### Make targets in `mycel/Makefile`

| Target | Current behavior | Classification |
| --- | --- | --- |
| `test-integration-cluster-identity` | Runs focused Go tests for cluster identity behavior. | Keep in `mycel`; fast/in-process. |
| `test-integration-daemon-cluster` | Runs focused daemon cluster Go tests. | Keep in `mycel`; fast/in-process. |
| `test-integration-raft-subsystems` | Runs focused raft subsystem Go tests. | Keep in `mycel`; fast/in-process. |
| `test-integration-routing` | Runs focused routing Go tests. | Keep in `mycel`; fast/in-process. |
| `test-integration-client-admin` | Runs focused client/admin Go tests. | Keep in `mycel`; fast/in-process. |
| `test-integration-graph-consistency` | Runs focused graph consistency Go tests. | Keep in `mycel`; fast/in-process. |
| `test-cluster-identity` | Compatibility alias for `test-integration-cluster-identity`. | Keep alias in `mycel`. |
| `test-phase-a` | Compatibility alias for `test-integration-daemon-cluster`. | Keep alias in `mycel`. |
| `test-phase-d` | Compatibility alias for `test-integration-raft-subsystems`. | Keep alias in `mycel`. |
| `test-phase-e` | Compatibility alias for `test-integration-routing`. | Keep alias in `mycel`. |
| `test-phase-f` | Compatibility alias for `test-integration-client-admin`. | Keep alias in `mycel`. |
| `test-phase-g` | Compatibility alias for `test-integration-graph-consistency`. | Keep alias in `mycel`. |
| `test-compose-cluster` | Builds `local/mycel:dev`, resets the Compose fixture, validates identity/data plane, restarts all daemon services, then revalidates identity/data plane and file-source identity. | Migrate to Mycel Lab `compose` suite in SIM1; keep alias. |
| `test-k3s-cluster` | Runs `scripts/testK3sCluster.sh` to create/reset disposable k3d/K3s resources, validate identity/data plane, perform rolling restart, replace one PVC, and revalidate. | Migrate k3d-compatible parts in SIM2; PVC replacement needs additional capability. |
| `test-k3s-raft-disruption-smoke` | Builds `myceldb/mycel:raft-disrupt-local`; runs `cmd/mycel-raft-disrupttest` smoke profile on disposable k3d/K3s. | Migrate to Mycel Lab disruption suite in SIM3. |
| `test-k3s-raft-disruption` | Runs raft disruption small profile with all-pod restarts. | Migrate to SIM3. |
| `test-k3s-raft-disruption-edges` | Runs raft disruption small profile using edge workload. | Migrate to SIM3. |
| `test-k3s-raft-restart-soak` | Runs one-hour moderate edge workload with rotating restarts. | Migrate to SIM5 soak suite after SIM3 profile modeling. |
| `test-k3s-raft-restart-hard-soak` | Runs one-hour harder multi-space workload with frequent rotating restarts. | Migrate to SIM5 soak suite after SIM3 profile modeling. |
| `test-k3s-system-backup-restore` | Builds `myceldb/mycel:system-backup-restore-local`; runs `cmd/mycel-system-backuptest` backup-smoke profile on disposable k3d/K3s. | Migrate to SIM4 after backup/restore operation support. |
| `test-compose-user-backup-restore` | Runs `scripts/testComposeUserBackupRestore.sh` against the Compose fixture. | Migrate to SIM4; Compose first. |
| `test-cluster-soak` | Runs `scripts/testClusterSoak.sh` against the Compose fixture with repeated validation and restarts. | Migrate to SIM5 after SIM1 scenarios exist. |
| `test-cluster-release-gate` | Aggregates `make test`, focused integration bundles, Compose cluster validation, k3d cluster validation, and k3d system backup/restore. | Keep in `mycel`; later delegate destructive parts to Mycel Lab suites. |
| `test-cluster-raft-sensitive-gate` | Aggregates `make test`, focused integration bundles, and disruption smoke/edge targets. | Keep in `mycel`; later delegate destructive parts to Mycel Lab suites. |

Current image/fixture defaults:

| Variable | Default | Used by |
| --- | --- | --- |
| `MYCEL_COMPOSE_IMAGE` | `local/mycel:dev` | `test-compose-cluster`. |
| `MYCEL_COMPOSE_FILE` | `$(CURDIR)/tests/compose/cluster/compose.yml` | Compose validation and backup/restore scripts. |
| `MYCEL_COMPOSE_ROOT` | `$(CURDIR)/tests/compose/cluster` | Compose lifecycle targets. |
| `MYCEL_COMPOSE_SERVICES` | `myceld-a,myceld-b,myceld-c` | Compose validators. |
| `MYCEL_RAFT_DISRUPT_IMAGE` | `myceldb/mycel:raft-disrupt-local` | Raft disruption targets. |
| `MYCEL_SYSTEM_BACKUP_RESTORE_IMAGE` | `myceldb/mycel:system-backup-restore-local` | k3d system backup/restore target. |

### Scripts

| Script | Current behavior | Proposed disposition |
| --- | --- | --- |
| `scripts/validateComposeClusterIdentity.sh` | Polls Compose services, checks `cluster status`, `cluster health`, metadata files, shared cluster ID, admitted clustered state, and optional file-source identity. | Replace with Mycel Lab cluster identity assertion/oracle used by `compose-cluster-validation`. Keep script until alias replacement is stable. |
| `scripts/validateComposeClusterDataPlane.sh` | Creates graph data via Compose service CLI and verifies reads/queries/consistency through services, with state persisted across restart revalidation. | Replace with Mycel Lab graph actor plus convergence/data-plane assertions. Needs state carryover between phases. |
| `scripts/testComposeUserBackupRestore.sh` | Resets Compose cluster, creates multi-principal fixtures with graph/blob payloads, exports user backups, wipes/recreates cluster, imports backups, and verifies restored users/data on every node. | Migrate in SIM4. Needs backup/export/import operations and per-node restored-data assertions. |
| `scripts/testClusterSoak.sh` | Repeatedly runs Compose identity/data-plane validators and periodically restarts daemon services. Snapshot/PVC replacement options fail closed. | Migrate in SIM5 using Compose suite with repeated phases and rolling restarts. |
| `scripts/testK3sCluster.sh` | Builds/loads local image into k3d, applies Kubernetes resources, validates identity/data-plane, performs rolling restart, forces raft snapshot, deletes/replaces one PVC, and revalidates. | Split in SIM2: k3d identity/data-plane/rolling-restart first; PVC replacement later when capability exists. |
| `scripts/validateK3sClusterIdentity.sh` | Polls pods through `kubectl exec`, checks cluster status/health and shared cluster identity. | Replace with Mycel Lab identity assertion/oracle through `k3d` driver. |
| `scripts/validateK3sClusterDataPlane.sh` | Creates and verifies graph data through pods using CLI and consistency reports. | Replace with Mycel Lab graph actor plus convergence/data-plane assertions. |
| `scripts/startClusterNode.sh` | Local single-node clustered development helper. | Keep in `mycel`; not a system-integration migration candidate. |

### Harness commands

| Command package | Current behavior | Proposed disposition |
| --- | --- | --- |
| `cmd/mycel-raft-disrupttest` | Disposable k3d/K3s pod-restart pressure harness with `smoke`, `small`, `medium`, `soak`, `restart-soak-1h`, and `restart-soak-hard-1h` profiles and `nodes`, `edges`, `multi-space` workloads. | SIM3 decides whether to fully supersede, wrap, or temporarily invoke it from Mycel Lab. Long-running profiles feed SIM5. |
| `cmd/mycel-system-backuptest` | Disposable k3d/K3s full-cluster backup/restore harness with backup metadata validation, PVC wipe/restore, and workload verification. | SIM4 migrates behavior or wraps temporarily after Mycel Lab has backup/restore operations. |

### Documentation

| Source doc | Current command(s) | Proposed Mycel Lab doc update |
| --- | --- | --- |
| `docs/system_integration/compose-cluster-validation.md` | `make test-compose-cluster` | Point to `mycel-lab run suite compose-cluster-validation` after SIM1. |
| `docs/system_integration/compose-user-backup-restore.md` | `make test-compose-user-backup-restore` | Point to `mycel-lab run suite compose-user-backup-restore` after SIM4. |
| `docs/system_integration/k3s-cluster-validation.md` | `make test-k3s-cluster` | Point to `mycel-lab run suite k3d-cluster-validation` after SIM2; document PVC replacement status separately. |
| `docs/system_integration/k3s-system-backup-restore.md` | `make test-k3s-system-backup-restore` | Point to `mycel-lab run suite k3d-system-backup-restore` after SIM4. |
| `docs/system_integration/raft-disruption-test-harness.md` | Raft disruption Make targets and direct harness invocations. | Point to Mycel Lab disruption/soak suites after SIM3/SIM5. |
| `docs/system_integration/cluster-soak.md` | `make test-cluster-soak` | Point to Mycel Lab Compose soak suite after SIM5. |
| `docs/system_integration/cluster-release-gate.md` | `make test-cluster-release-gate` | Keep aggregate command in `mycel`; update destructive substeps after SIM1/SIM2/SIM4. |
| `docs/system_integration/cluster-raft-sensitive-gate.md` | `make test-cluster-raft-sensitive-gate` | Keep aggregate command in `mycel`; update destructive substeps after SIM3. |
| `docs/system_integration/README.md` | Summary of all system flows. | Update incrementally after each migrated suite is stable. |

### CI references

Current `mycel/.github/workflows/ci.yml` runs:

- `make test`;
- `make build`;
- `scripts/check-public-surface.sh --workspace "$GITHUB_WORKSPACE" --strict`.

No current GitHub Actions workflow invokes the destructive Compose/k3d system
integration targets. Initial migration PRs should not add destructive CI runs by
default. CI can safely add dry-run catalog/scenario validation in `mycel-lab`
without provisioning external environments.

## Migration matrix

| Existing entrypoint | Validates | Destructive resources | Proposed driver | Proposed Mycel Lab scenario/suite | Needed Mycel Lab work | Compatibility alias strategy |
| --- | --- | --- | --- | --- | --- | --- |
| `make test-compose-cluster` | Compose raft bootstrap, shared identity, health, graph write/read/query, restart stability, persisted file-source identity. | Docker Compose project, local volumes, local image. | `compose` | Suite `compose-cluster-validation`; scenarios `compose-cluster-identity`, `compose-graph-data-plane`, `compose-rolling-restart`. | Cluster identity assertion; graph data-plane phase state carryover; optional file-source identity assertion via driver exec/log artifact. | Keep target; after SIM1 delegate to `mycel-lab run suite compose-cluster-validation --confirm-destructive` with image/compose-file overrides. |
| `scripts/validateComposeClusterIdentity.sh` | Per-service status/health, shared cluster ID, admitted clustered state, metadata file-source identity. | Reads Compose services; no lifecycle by itself. | `compose` | Assertion/oracle used by `compose-cluster-identity`. | Reusable cluster identity oracle over driver endpoints and optional driver exec for metadata files. | Keep script until delegated target is stable; then mark legacy. |
| `scripts/validateComposeClusterDataPlane.sh` | Graph writes, reads, GQL/query behavior, consistency reports, pre/post restart continuity. | Writes graph data to Compose cluster. | `compose` | Scenario `compose-graph-data-plane`. | Actor/assertion support for the same data-plane invariants and state persisted between phases. | Keep script until Mycel Lab scenario matches coverage. |
| `make test-k3s-cluster` / `scripts/testK3sCluster.sh` | k3d/K3s bootstrap, identity, data plane, rolling restart, raft snapshot, one-PVC replacement/rejoin. | k3d cluster, Kubernetes namespace/resources, PVCs. | `k3d` | Suite `k3d-cluster-validation`; scenario `k3d-cluster-baseline`; later `k3d-pvc-rejoin`. | Existing k3d lifecycle plus identity/data-plane oracle; `volume-replacement`/PVC operation for full parity. | Keep target; delegate baseline parts after SIM2 and retain legacy PVC replacement path until parity exists. |
| `scripts/validateK3sClusterIdentity.sh` | Per-pod status/health and shared cluster identity. | Reads pods through `kubectl exec`. | `k3d` | Assertion/oracle used by `k3d-cluster-baseline`. | Same identity oracle as Compose, endpoint-backed and/or driver exec. | Keep script until replacement suite is stable. |
| `scripts/validateK3sClusterDataPlane.sh` | Graph writes/reads/queries and consistency reports through pods. | Writes graph data to k3d cluster. | `k3d` | Scenario `k3d-graph-data-plane`. | Data-plane actor/assertion parity with legacy shell script. | Keep script until replacement suite is stable. |
| `make test-k3s-raft-disruption-smoke` | Smoke graph workload under a pod restart. | k3d cluster, Kubernetes resources, pod deletion/restart. | `k3d` | Suite/scenario `k3d-raft-disruption-smoke`. | Profile/workload model; final convergence oracle; restart event scheduling. | Keep target; after SIM3 delegate to equivalent suite. |
| `make test-k3s-raft-disruption` | Small workload with all-pod restarts. | k3d cluster/resources. | `k3d` | Suite `k3d-raft-disruption`. | Same as smoke, plus all-node restart sequence. | Keep target; after SIM3 delegate. |
| `make test-k3s-raft-disruption-edges` | Edge workload under restart pressure. | k3d cluster/resources. | `k3d` | Scenario `k3d-raft-disruption-edges`. | Edge workload actor and convergence oracle. | Keep target; after SIM3 delegate. |
| `make test-k3s-raft-restart-soak` | One-hour moderate edge workload with rotating restarts. | k3d cluster/resources. | `k3d` | Suite `k3d-raft-restart-soak`. | Long-duration profile controls, restart interval scheduling, soak artifact summaries. | Keep target; after SIM5 delegate. |
| `make test-k3s-raft-restart-hard-soak` | One-hour multi-space workload with frequent rotating restarts. | k3d cluster/resources. | `k3d` | Suite `k3d-raft-restart-hard-soak`. | Multi-space workload actor, long-duration controls. | Keep target; after SIM5 delegate. |
| `cmd/mycel-raft-disrupttest` direct invocations | Disposable disruption harness profiles/workloads. | k3d cluster/resources. | `k3d` | Underlying scenarios/suites above. | Decide whether Mycel Lab supersedes or wraps the binary. | Keep binary until all documented direct profiles have Mycel Lab equivalents. |
| `make test-compose-user-backup-restore` | Principal-scoped backup export/import, safety filters, graph/blob restore through every node. | Compose project/volumes; backup temp files. | `compose` | Suite `compose-user-backup-restore`. | Backup/export/import operation support; blob fixture verification; secret-leak assertions. | Keep target; after SIM4 delegate or wrap. |
| `make test-k3s-system-backup-restore` | Coordinated full-cluster backup, raft freeze/checkpoint evidence, PVC wipe/restore, restored workload verification. | k3d cluster/resources/PVCs; backup archives. | `k3d` | Suite `k3d-system-backup-restore`. | Backup/restore operations; PVC wipe/restore or controlled volume replacement; backup metadata oracle. | Keep target; after SIM4 delegate only when parity exists. |
| `cmd/mycel-system-backuptest` direct invocations | Full-cluster backup/restore profiles and workloads. | k3d cluster/resources/PVCs. | `k3d` | Backup/restore suites above. | Decide whether Mycel Lab supersedes or wraps the binary. | Keep binary until direct profile coverage has replacements. |
| `make test-cluster-soak` / `scripts/testClusterSoak.sh` | Repeated Compose identity/data-plane validation with periodic restarts. | Compose project/volumes. | `compose` | Suite `compose-cluster-soak`. | Repetition/iteration modeling; rolling restart phase; soak summary. | Keep target; after SIM5 delegate to Mycel Lab soak suite. |
| `make test-cluster-release-gate` | Full gate combining fast tests, focused in-process bundles, Compose, k3d cluster validation, and k3d backup/restore. | Mixed; includes Compose/k3d. | Mixed (`compose`, `k3d`) | Eventually `mycel-lab` release-gate suite for destructive parts, while `mycel` keeps fast prerequisites. | Component suites from SIM1/SIM2/SIM4; gate orchestration policy. | Keep target in `mycel`; later run fast prerequisites then delegate destructive suites. |
| `make test-cluster-raft-sensitive-gate` | Fast tests plus disruption smoke/edge workloads. | k3d cluster/resources. | `k3d` | Eventually `k3d-raft-sensitive-gate` suite for destructive parts. | Component disruption suites from SIM3. | Keep target in `mycel`; later run fast prerequisites then delegate destructive suite. |

## Flows that stay in `mycel`

The following are intentionally not migrated to Mycel Lab:

- `make test`, `make test-verbose`, `make test-watch`.
- `test-integration-cluster-identity`.
- `test-integration-daemon-cluster`.
- `test-integration-raft-subsystems`.
- `test-integration-routing`.
- `test-integration-client-admin`.
- `test-integration-graph-consistency`.
- Historical aliases `test-cluster-identity` and `test-phase-*`.
- Build, coverage, proto generation, public-surface, docs, and daemon-only
  checks.
- Local development helpers such as `scripts/startClusterNode.sh`.

These are fast enough or tightly coupled to Go package boundaries and should
remain in the daemon repository.

## Feature gaps before full parity

| Gap | Needed for | Notes |
| --- | --- | --- |
| Cluster identity oracle/assertion | SIM1, SIM2, gates | Should work through driver-discovered endpoints and optionally driver exec for metadata files. |
| Data-plane stateful assertion parity | SIM1, SIM2, soak | Needs to preserve fixture IDs/count expectations across phases and restarts. |
| Edge and multi-space workload profiles | SIM3, SIM5, backup/restore | Existing graph actor may need richer workload shapes. |
| Volume/PVC replacement capability | Full `test-k3s-cluster`, system backup/restore | Do not add generic Kubernetes driver; add capability to `k3d` only if required. |
| Backup/export/import operations | SIM4 | Must include safety assertions for user backup and raft freeze/checkpoint metadata for system backup. |
| Backup archive staging/capture | SIM4 | Needs explicit artifact paths and per-node archive handling. |
| Long-running soak controls | SIM5 | Need profile durations, restart intervals, and concise summaries without adding destructive CI by default. |
| Compatibility target delegation | Each tranche | `mycel` targets need a stable way to locate/run `mycel-lab` from sibling checkout or installed binary. |

## Recommended next implementation order

1. SIM1a: add reusable cluster identity assertion/oracle to Mycel Lab.
2. SIM1b: port Compose cluster identity and data-plane validation into a
   `compose-cluster-validation` suite.
3. SIM1c: update `mycel` compatibility target `test-compose-cluster` to delegate
   or print the replacement command.
4. SIM2a: port k3d identity/data-plane/rolling-restart validation excluding PVC
   replacement.
5. SIM2b: decide and implement `volume-replacement` capability for k3d, then add
   PVC replacement/rejoin parity if still required.
6. SIM3: model disruption profiles/workloads and migrate raft disruption smoke,
   small, and edge flows.
7. SIM4: migrate Compose user backup/restore, then k3d system backup/restore.
8. SIM5: migrate soak/release-gate aggregations.
