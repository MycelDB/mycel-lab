# MycelDB system integration migration implementation plan

## Status

Proposed for [mycel-lab#7](https://github.com/MycelDB/mycel-lab/issues/7).

This plan migrates MycelDB destructive/system/reliability test flows from the
`mycel` repository into Mycel Lab. The goal is to make `mycel-lab` the canonical
runner for multi-node, environment-backed validation while keeping fast unit,
package, and in-process integration tests in `mycel`.

## Goals

- Move destructive and environment-backed MycelDB validation into Mycel Lab.
- Preserve existing developer and CI entrypoints with compatibility aliases
  during migration.
- Convert shell-centric system flows into versioned Mycel Lab scenarios/suites
  with artifacts, repeatable environment selection, and explicit destructive
  confirmation.
- Use the existing `dry-run`, `compose`, and `k3d` environment drivers only.
- Keep migrated scenarios runnable in dry-run mode for catalog and planning
  validation.
- Keep migration incremental so `develop` remains usable after each PR.

## Non-goals

- Moving Go unit tests or fast package/in-process integration tests out of
  `mycel`.
- Adding a generic remote `kubernetes` driver.
- Adding cloud drivers.
- Adding a local multi-process driver.
- Implementing the REST-first runner/control plane.
- Removing legacy `mycel` Make targets before replacement Mycel Lab flows are
  stable.

## Current source inventory

Initial migration candidates in `mycel` include:

- Compose cluster validation:
  - `scripts/validateComposeClusterIdentity.sh`
  - `scripts/validateComposeClusterDataPlane.sh`
  - `tests/compose/cluster/Makefile`
  - `tests/compose/cluster/compose.yml`
  - `docs/system_integration/compose-cluster-validation.md`
- k3s/k3d-style local Kubernetes validation:
  - `scripts/testK3sCluster.sh`
  - `scripts/validateK3sClusterIdentity.sh`
  - `scripts/validateK3sClusterDataPlane.sh`
  - `docs/system_integration/k3s-cluster-validation.md`
- Soak and disruption validation:
  - `scripts/testClusterSoak.sh`
  - `cmd/mycel-raft-disrupttest/`
  - `internal/clustering/disrupttest/`
  - `docs/system_integration/cluster-soak.md`
  - `docs/system_integration/raft-disruption-test-harness.md`
- Backup/restore system validation:
  - `scripts/testComposeUserBackupRestore.sh`
  - related k3s backup/restore scripts/docs where present
  - `docs/system_integration/compose-user-backup-restore.md`
  - `docs/system_integration/k3s-system-backup-restore.md`
  - `docs/system_integration/cluster-raft-sensitive-gate.md`
  - `docs/system_integration/cluster-release-gate.md`

The first implementation PR should refresh this inventory against current
`mycel/develop` and record exact Make targets, CI references, scripts, docs, and
required environment assumptions before migrating behavior.

## Classification policy

Keep in `mycel`:

- Go unit tests.
- Package tests and in-process integration tests.
- Fast API/daemon behavior checks run by `go test`.
- Static, docs, generated-code, and public-surface checks.

Move to `mycel-lab` over time:

- Tests that create, reset, or destroy Compose/k3d environments.
- Multi-node cluster lifecycle checks.
- Restart, outage, disruption, quorum, and recovery scenarios.
- Soak scenarios.
- Backup/restore system scenarios.
- Tests that need durable run artifacts, event timelines, and environment state
  capture.

## Migration approach

Each migrated flow should have:

1. A Mycel Lab scenario or suite definition under `tests/reliability/`.
2. Dry-run validation coverage for planning/catalog import.
3. Driver-backed destructive execution using `compose` or `k3d`.
4. Artifact coverage for resolved scenario, events, environment state, nodes,
   endpoints, logs/state capture, and final result.
5. Documentation explaining how to run the replacement.
6. A compatibility alias in `mycel` for existing Make targets/scripts where
   developers or CI may still depend on the old name.
7. A clear removal/deprecation path for shell-only behavior once replacement
   Mycel Lab scenarios are stable.

## Tranche SIM0 — Inventory and compatibility map

### Tasks

1. Audit current `mycel/develop` for system integration scripts, Make targets,
   docs, and CI jobs.
2. Produce a migration matrix with columns:
   - old command/target/script;
   - behavior being validated;
   - destructive resources used;
   - proposed Mycel Lab driver;
   - proposed scenario/suite name;
   - required new Mycel Lab capability, if any;
   - compatibility alias strategy.
3. Identify flows that should remain in `mycel` because they are fast/in-process.
4. Identify flows that require Mycel Lab feature work before migration.
5. Update this plan with the audited matrix if material differences are found.

### Deliverables

- A migration matrix document, either appended to this plan or added as a
  companion implementation note.
- No behavior migration yet.

### Validation

```sh
python3 scripts/checkDocs.py
make test
```

## Tranche SIM1 — Compose cluster validation

### Tasks

1. Port Compose identity validation into one or more Mycel Lab assertions,
   actors, or scenario phases.
2. Port Compose data-plane validation into a Mycel Lab scenario using the
   `compose` driver.
3. Reuse the existing Compose fixture by reference where practical; avoid
   copying large Compose definitions unless a Mycel Lab-owned fixture is needed.
4. Add a suite such as `compose-cluster-validation` that groups the replacement
   scenarios.
5. Ensure dry-run execution works for each new scenario and suite.
6. Update Mycel Lab operations docs with the new Compose validation command.
7. Add/update `mycel` compatibility aliases so existing Compose validation
   targets delegate to Mycel Lab or clearly announce the replacement command.

### Required capabilities

- Existing `compose` driver lifecycle.
- Per-node endpoints.
- Logs/state capture.
- Additional assertion/oracle hooks if existing shell checks cannot be expressed
  through current actors and final oracle behavior.

### Validation

```sh
python3 scripts/checkDocs.py
make test
go run ./cmd/mycel-lab import --dry-run tests/reliability/
go run ./cmd/mycel-lab run suite compose-cluster-validation --dry-run
```

Destructive validation, run explicitly by an operator:

```sh
go run ./cmd/mycel-lab run suite compose-cluster-validation --confirm-destructive
```

## Tranche SIM2 — k3d local cluster validation

### Tasks

1. Translate local k3s/k3d identity validation into Mycel Lab scenarios using
   the `k3d` driver.
2. Translate data-plane validation into driver-backed actor/assertion flows.
3. Add a suite such as `k3d-cluster-validation`.
4. Preserve old `mycel` command names as compatibility aliases.
5. Update MycelDB and Mycel Lab docs to point operators at the new suite.

### Required capabilities

- Existing `k3d` driver lifecycle.
- Per-node endpoints and optional console endpoint forwarding.
- Node restart / rolling restart only if the legacy validation includes them.

### Validation

```sh
python3 scripts/checkDocs.py
make test
go run ./cmd/mycel-lab run suite k3d-cluster-validation --dry-run
```

Destructive validation, run explicitly by an operator:

```sh
go run ./cmd/mycel-lab run suite k3d-cluster-validation --confirm-destructive
```

## Tranche SIM3 — Restart, outage, and disruption scenarios

### Tasks

1. Map existing disruption harness scenarios to Mycel Lab phases/events.
2. Prefer logical event names such as `node-restart`, `node-stop`, and
   `rolling-restart`; keep old `pod-*` event compatibility only where needed.
3. Add any missing driver operation semantics through the environment driver
   abstraction rather than by adding backend-specific runner logic.
4. Add suites for short disruption, raft-sensitive, and release-gate disruption
   coverage.
5. Decide whether the existing `mycel-raft-disrupttest` binary becomes:
   - a deprecated compatibility wrapper;
   - a helper invoked by Mycel Lab; or
   - fully superseded by Mycel Lab scenarios.

### Required capabilities

- `node-restart` and `rolling-restart` are already modeled.
- Durable `node-stop` semantics may require a driver contract refinement if the
  current restart/delete-pod mapping is insufficient.
- Volume replacement and partition/network fault behavior remain future work
  unless required by a migrated scenario.

### Validation

```sh
python3 scripts/checkDocs.py
make test
go run ./cmd/mycel-lab run suite <disruption-suite> --dry-run
```

Destructive validation must remain explicit with `--confirm-destructive`.

## Tranche SIM4 — Backup/restore system scenarios

### Tasks

1. Inventory backup/restore scripts and their durable storage assumptions.
2. Port Compose backup/restore first if it can use the existing Compose object
   store fixture.
3. Model backup/restore operations as explicit phases with clear artifacts.
4. Add assertions for restored resources and cluster health.
5. Preserve old backup/restore Make targets as compatibility aliases.

### Required capabilities

- Object-store fixture metadata for Compose.
- Additional actor/assertion support for backup initiation, restore initiation,
  and post-restore verification.
- Possibly durable artifact capture for backup manifests/results.

### Validation

```sh
python3 scripts/checkDocs.py
make test
go run ./cmd/mycel-lab run suite <backup-restore-suite> --dry-run
```

Destructive validation must remain explicit with `--confirm-destructive`.

## Tranche SIM5 — Soak and release-gate suites

### Tasks

1. Port soak profiles into long-duration Mycel Lab suites.
2. Define short, CI-friendly smoke variants separately from operator-run soak
   variants.
3. Compose release-gate suites from migrated cluster, disruption, and
   backup/restore scenarios.
4. Update MycelDB release documentation to use Mycel Lab suite names while
   preserving legacy aliases.
5. Ensure artifacts are sufficient to diagnose failures without shell-specific
   logs.

### Validation

```sh
python3 scripts/checkDocs.py
make test
go run ./cmd/mycel-lab run suite <release-gate-suite> --dry-run
```

Long-running destructive/soak validation remains manual unless CI is explicitly
configured to allocate disposable environments.

## Compatibility strategy

During migration, existing `mycel` Make targets should remain available. A
compatibility target may either:

- delegate to `mycel-lab` with the equivalent scenario/suite; or
- print a clear replacement command while exiting nonzero only when no safe
  delegation exists.

Compatibility aliases should be removed only after:

1. the Mycel Lab replacement has been stable for at least one release cycle;
2. docs and CI references have been updated;
3. operators have a documented migration path.

## Safety requirements

- Destructive `compose` and `k3d` runs require `--confirm-destructive`.
- `--dry-run` must remain safe and non-mutating.
- No generic remote Kubernetes, cloud, or production-cluster driver is introduced
  by this migration.
- Scenarios must use disposable namespaces/projects/clusters by default.
- Compatibility aliases must not silently target a developer's production or
  shared environment.

## Documentation updates

Each migration tranche should update the relevant docs:

- Mycel Lab operations docs for new scenarios/suites.
- MycelDB system integration docs for replacement commands.
- MycelDB build/test procedure docs for compatibility aliases.
- Scenario/suite comments where additional operator prerequisites are needed.

## Tracking checklist

- [ ] SIM0 inventory and compatibility map.
- [ ] SIM1 Compose cluster validation migration.
- [ ] SIM2 k3d local cluster validation migration.
- [ ] SIM3 restart/outage/disruption migration.
- [ ] SIM4 backup/restore system migration.
- [ ] SIM5 soak and release-gate suite migration.
