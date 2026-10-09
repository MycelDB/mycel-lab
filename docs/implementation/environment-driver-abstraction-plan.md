# Environment driver abstraction implementation plan

## Status

Implemented for [mycel-lab#1](https://github.com/MycelDB/mycel-lab/issues/1).

Design source: [Environment driver abstraction](../design/environment-drivers.md).

This plan records the completed implementation tranches and validation shape.

## Constraints

- Initial drivers are only `dry-run`, `k3d`, and `compose`.
- Do not add a generic remote `kubernetes` driver in this issue.
- Do not add cloud provider drivers.
- Do not add a local multi-process driver.
- Do not implement REST/API server work in this issue.
- CLI may gain local override flags, but the existing CLI direct-run path remains
  acceptable until the separate REST-first runner work begins.
- Destructive environment operations require explicit confirmation.
- Unit/package/in-process tests stay in `go test`; destructive backend tests are
  explicit/manual unless a safe dry-run/fake runner can cover behavior.

## Existing baseline

Current code already has:

- `spec.EnvironmentSpec` with `driver`, `namespace`, and `keepOnFailure`.
- `env.EnvironmentDriver` with `Preflight`, `Create`, `Delete`, and
  `CaptureState`.
- `env.DryRunDriver`.
- `env.K3DDriver` that creates a disposable k3d cluster and applies Kubernetes
  manifests.
- Kubernetes-specific event runtime for pod restart / rolling restart.
- Runner wiring that chooses `DryRunDriver` for `--dry-run` and `K3DDriver` for
  `k3d`.
- Port-forward based endpoint generation for Kubernetes-backed actors.

The implementation should evolve this code rather than replacing it wholesale.

## Tranche ED0 — Schema and docs scaffolding

### Tasks

1. Extend `spec.EnvironmentSpec` with:
   - `Options map[string]any`;
   - `Capabilities CapabilityRequirements`;
   - `CapabilityRequirements.Required []string`.
2. Preserve defaulting behavior:
   - no explicit driver still defaults to `k3d` for non-dry-run scenario YAML;
   - `--dry-run` still forces the dry-run driver at runtime.
3. Add validation for known driver names:
   - `dry-run`;
   - `k3d`;
   - `compose`.
4. Add scenario YAML examples for `dry-run`, `k3d`, and `compose`.
5. Link the environment-driver design from README and operations docs.

### Tests

- Existing spec parsing tests continue to pass.
- New spec tests cover:
  - `options` round-trip;
  - required capabilities round-trip;
  - default driver behavior;
  - unknown driver validation failure.

### Validation

```sh
go test ./internal/reliability/spec
python3 scripts/checkDocs.py
```

## Tranche ED1 — Capability model and driver registry

### Tasks

1. Add capability constants under `internal/reliability/env`.
2. Add `CapabilitySet` helpers:
   - construction from strings;
   - `Has` / `Missing` checks;
   - stable sorted output for errors/artifacts.
3. Add driver metadata interface or expand the driver interface:
   - `Name() string`;
   - `Capabilities() CapabilitySet`;
   - `Validate(spec.EnvironmentSpec) error`.
4. Add a driver registry/factory:
   - selects driver by effective `EnvironmentSpec.Driver`;
   - applies dry-run override;
   - returns clear errors for unknown drivers;
   - rejects generic Kubernetes/cloud drivers until separately designed.
5. Move `defaultEnvironmentDriver` logic out of `runner` into the registry.
6. Validate required scenario capabilities before `Preflight` or `Create`.
7. Write `environment/capabilities.json` to run artifacts.

### Tests

- Registry selects dry-run when `Options.DryRun` is true.
- Registry selects k3d for `driver: k3d`.
- Registry selects compose for `driver: compose` once the stub exists.
- Unknown driver fails with a clear error.
- Required unsupported capability fails before driver create.
- Capability error output includes supported and missing capability names.

### Validation

```sh
go test ./internal/reliability/env ./internal/reliability/runner
```

## Tranche ED2 — Environment handle, nodes, and endpoints

### Tasks

1. Extend `env.Environment` with portable metadata when needed.
2. Add `env.Node` and `env.Endpoint` structs.
3. Add driver methods or side interfaces for:
   - `WaitReady`;
   - `Nodes`;
   - `Endpoints`.
4. Update `DryRunDriver` to synthesize nodes and endpoints for the cluster node
   count.
5. Update `K3DDriver` to discover nodes from Kubernetes pods and return
   port-forward endpoint metadata.
6. Update runner provisioning to use driver-discovered endpoints rather than
   hard-coded `ConsoleEndpointsForNodeCount` when the driver supports
   `per-node-endpoints`.
7. Continue writing `environment/console-endpoints.json` for compatibility if
   `--console-endpoints` is requested, but also write:
   - `environment/nodes.json`;
   - `environment/endpoints.json`.

### Tests

- Dry-run run artifacts include nodes and endpoints.
- K3D driver node discovery is covered with a fake command runner.
- Runner passes discovered endpoint addresses into provisioning.
- Existing dry-run runner tests continue to pass.

### Validation

```sh
go test ./internal/reliability/env ./internal/reliability/runner ./internal/reliability/provision
make test
```

## Tranche ED3 — Driver-backed event runtime

### Tasks

1. Replace runner event-runtime selection with an environment-driver-backed
   runtime for backend operations.
2. Add driver methods or an operation adapter for:
   - `RestartNode`;
   - `RollingRestart`;
   - optional `Exec` if needed by a current scenario.
3. Keep existing `pod-stop`, `pod-restart`, and `rolling-restart` event names
   working.
4. Add logical aliases if desired:
   - `node-restart` -> `RestartNode`;
   - `node-stop` -> same initial semantics as `pod-stop` where supported.
5. Ensure event validation checks required driver capabilities before running
   the phase.
6. Move Kubernetes-specific event behavior behind `K3DDriver` operations.

### Tests

- Dry-run events record start/completion without destructive operations.
- Fake driver proves `pod-restart` dispatches to `RestartNode`.
- Fake driver proves `rolling-restart` dispatches to `RollingRestart`.
- Unsupported event/capability fails with a clear error before mutation.
- Existing Kubernetes event tests are adapted to driver operation tests.

### Validation

```sh
go test ./internal/reliability/events ./internal/reliability/runner ./internal/reliability/env
```

## Tranche ED4 — Compose driver foundation

### Tasks

1. Add `env.ComposeDriver` with typed options:
   - `composeFile`;
   - `projectName` or `projectNamePrefix`;
   - `serviceNames` / daemon service names;
   - optional `grpcPorts` when ports cannot be discovered safely.
2. Add preflight checks:
   - Docker CLI available;
   - Docker Compose available (`docker compose version`);
   - compose file exists and is local/readable.
3. Implement lifecycle:
   - `Create` / reset project;
   - `WaitReady`;
   - `Delete` / project down with volumes when appropriate.
4. Implement node discovery:
   - map logical nodes to Compose services;
   - preserve deterministic ordinal ordering.
5. Implement endpoint discovery:
   - use host-published gRPC ports;
   - fail closed if ports cannot be resolved.
6. Implement log/state capture:
   - `environment/state.json`;
   - `environment/compose-ps.txt`;
   - `environment/compose-config.yaml` where safe;
   - `environment/logs/<service>.log`.
7. Advertise initial Compose capabilities:
   - `per-node-endpoints`;
   - `node-restart`;
   - `rolling-restart`;
   - `logs`;
   - `object-store-fixture` only when configured/confirmed.

### Tests

Use fake command runners for normal unit tests:

- Preflight checks call expected commands.
- Create builds expected `docker compose` command line.
- Delete builds expected cleanup command line.
- Node discovery maps service names to logical nodes.
- Endpoint discovery parses expected port output or typed `grpcPorts` options.
- Capture writes expected artifact names.

Manual validation when Docker is available:

```sh
mycel-lab run scenario-file tests/reliability/scenarios/compose-smoke/compose-smoke.yaml --confirm-destructive
```

The manual scenario should be skipped/not required in normal `make test` until
it is safe and deterministic on all developer machines.

### Validation

```sh
go test ./internal/reliability/env ./internal/reliability/runner
```

## Tranche ED5 — Compose smoke scenario and catalog examples

### Tasks

1. Add a small Compose-backed smoke scenario under `tests/reliability/scenarios/`.
2. Keep the scenario intentionally minimal:
   - create/reset Compose environment;
   - wait for readiness;
   - discover endpoints;
   - run a tiny graph actor or no-op actor depending on fixture maturity;
   - collect artifacts.
3. Add an actor/profile or environment options only if existing profiles are not
   sufficient.
4. Document required local paths and Docker assumptions.
5. Ensure dry-run of the Compose scenario works without Docker:

```sh
mycel-lab run scenario-file tests/reliability/scenarios/compose-smoke/compose-smoke.yaml --dry-run
```

### Tests

- Catalog import/resolve accepts the Compose scenario.
- Dry-run run writes expected resolved environment/options artifacts.
- Capability validation passes for the scenario's required capabilities.

### Validation

```sh
mycel-lab import --dry-run tests/reliability/
mycel-lab run scenario-file tests/reliability/scenarios/compose-smoke/compose-smoke.yaml --dry-run
go test ./...
```

Manual destructive validation:

```sh
mycel-lab run scenario-file tests/reliability/scenarios/compose-smoke/compose-smoke.yaml --confirm-destructive
```

## Tranche ED6 — CLI override hooks

### Tasks

1. Add CLI flags:
   - `--environment-driver <driver>`;
   - repeated `--environment-option key=value`;
   - optional `--environment-namespace <namespace>`.
2. Apply precedence:

```text
CLI override > scenario EnvironmentSpec > default EnvironmentSpec
```

3. Store the effective environment spec in resolved run artifacts.
4. Keep this as local CLI wiring only; REST-first runner architecture remains a
   separate issue.

### Tests

- CLI parser accepts environment driver override.
- CLI parser accepts repeated environment options.
- Override wins over scenario YAML.
- Dry-run still forces dry-run driver while preserving the requested effective
  scenario/environment in artifacts if useful for planning.

### Validation

```sh
go test ./internal/reliability/app ./internal/reliability/runner ./internal/reliability/spec
```

## Tranche ED7 — Documentation and migration cleanup

### Tasks

1. Update `README.md` command examples.
2. Update `docs/operations.md` with:
   - driver selection;
   - driver capabilities;
   - dry-run/k3d/compose examples;
   - destructive confirmation and cleanup behavior.
3. Update `docs/design/reliability-harness.md` to point to the dedicated driver
   design instead of embedding stale k3d-only assumptions.
4. Update tests and examples that still imply only k3d exists.
5. Add a short follow-up list for generic Kubernetes, cloud, volume replacement,
   backup/restore, and raft-disruption parity.

### Validation

```sh
python3 scripts/checkDocs.py
go test ./...
```

## Rollout strategy

1. Land schema/registry/capability changes first with no behavior change for
   existing k3d and dry-run scenarios.
2. Move endpoint discovery and event execution behind driver operations while
   keeping the k3d path green.
3. Add Compose as an additive driver with fake-runner unit coverage.
4. Add one Compose smoke scenario and keep destructive execution manual.
5. Add CLI overrides after the driver behavior is stable.

## Acceptance mapping

| Issue acceptance criterion | Planned tranche |
| --- | --- |
| `EnvironmentSpec` supports driver-specific options without breaking scenarios. | ED0 |
| Runner uses common abstraction for `dry-run`, `k3d`, and `compose`. | ED1-ED4 |
| Existing dry-run scenarios still pass. | ED0-ED7 validation |
| Existing k3d scenarios still pass. | ED1-ED4 validation |
| Compose-backed smoke scenario can create/reset, wait, expose endpoints, collect artifacts. | ED4-ED5 |
| Capability checks fail early. | ED1, ED3 |
| Documentation explains model and drivers. | ED7 plus this design/plan |
| Tests cover driver selection, override precedence hooks, and capability validation. | ED1, ED6 |

## Risks and mitigations

- **Compose fixture path coupling**: keep path configurable and fail closed if the
  compose file cannot be resolved.
- **Accidental destructive cleanup**: use unique project/cluster names and keep
  destructive confirmation mandatory.
- **Endpoint assumptions leaking into actors**: route all actor/provisioning
  endpoint usage through driver-discovered endpoints.
- **Kubernetes-specific event names**: keep old `pod-*` names as compatibility
  aliases, but implement them through logical driver methods.
- **Over-generalized interface too early**: implement only methods needed by the
  initial drivers and scenario operations; leave advanced backup/volume APIs for
  follow-up issues.

## Explicit follow-ups outside this issue

- Generic remote `kubernetes` driver.
- Cloud-provider drivers.
- Local multi-process driver.
- Volume/PVC replacement parity.
- Backup/restore scenario operations.
- Raft disruption harness parity.
- REST daemon/control plane (`mycel-lab serve`, `POST /v1/runs`, etc.).
