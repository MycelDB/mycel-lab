# Structured Reliability Harness Implementation Plan

## Status

Proposed. This plan implements the draft design in
[Structured reliability harness](../../design/testing/reliability-harness.md).

The reliability harness is a deterministic, long-running test system for
MycelDB clusters. It creates configurable clusters, runs simulated users through
public SDK/gRPC APIs, injects planned deterministic failure conditions, records
structured evidence, and persists definitions plus run history in Postgres.

This plan is intentionally split into small phases. Early phases build the
catalog/database and runner foundation before adding Kubernetes deployment,
actor execution, and disruption behavior.

## Goals

- Add a reusable `mycel-reliability` command and `internal/reliability` package
  tree.
- Support YAML authoring/import/export for cluster profiles, actor profiles,
  scenarios, and suites.
- Store reliability definitions and run history in Postgres as the operational
  source of truth.
- Enforce mutable-until-used definition lifecycle: definitions can be edited or
  deleted until referenced by a run; used definitions become immutable and edits
  create new versions.
- Store a resolved scenario snapshot for every run.
- Support one MycelDB cluster per scenario with arbitrary initial daemon pod
  count and raft configuration.
- Support actor profiles and scenario actor groups, including per-instance
  identity/data scope and group-total rates.
- Implement the first graph transaction actor with operation budget, weighted
  operation mix, per-operation constraints, fallback to `createNode`, bounded
  retries, and sampled read-after-write checks.
- Implement deterministic phase-based events, initially by pod name.
- Record both Postgres rows and JSONL artifacts for run events, actor events,
  graph transactions, read checks, metrics, assertions, logs, and Kubernetes
  state.
- Keep the design dashboard-ready without requiring a dashboard in the first
  implementation.

## Non-goals

- Do not add the long-running reliability harness to ordinary `make test`.
- Do not run destructive cluster operations without explicit confirmation.
- Do not implement random chaos testing in the first pass.
- Do not implement matrix expansion in the first pass.
- Do not support multiple MycelDB clusters per scenario.
- Do not implement dynamic raft membership until the daemon supports it.
- Do not implement a full graph shadow model in the first pass.
- Do not require TimescaleDB initially.
- Do not require the dashboard/control plane initially.

## Safety requirements

- Require an explicit destructive confirmation flag for cluster create/delete,
  pod stop/restart, rolling restart, or namespace cleanup.
- Generate unique run IDs, namespaces, test spaces, domains, principals, and
  artifact directories.
- Do not target production namespaces by default.
- Support `--keep-environment-on-failure` only as an explicit debugging option,
  and print retained cluster/namespace details prominently.
- Never log plaintext admin passwords, user passwords, access tokens, refresh
  tokens, provider credentials, kubeconfig secrets, or object-store credentials
  in artifacts or database rows.
- Treat correctness divergence as forensic/read-only: the harness reports and
  preserves evidence but does not repair data.
- Write final run status and artifacts even on cancellation where possible.

## Proposed command and package layout

```text
cmd/mycel-reliability/
  main.go

internal/reliability/
  spec/          # YAML schema, Go structs, validation, defaulting
  store/         # Postgres persistence and queries
  migrations/    # DB schema migrations
  catalog/       # import/export/versioning/profile resolution
  runner/        # scenario/suite execution and phase state machine
  env/           # k3d/k8s drivers
  deploy/        # MycelDB manifest rendering/deployment
  actors/        # actor profiles and runtime actor instances
  events/        # pod-stop, pod-restart, rolling-restart
  oracle/        # operation log, counters, convergence checks
  metrics/       # metric samples and aggregation
  artifacts/     # JSONL, logs, k8s snapshots, artifact index
  report/        # summary.md / result.json / future HTML
```

YAML examples should live under:

```text
tests/reliability/
  clusters/
  actor-profiles/
  scenarios/
  suites/
```

## Configuration

Initial command-level configuration should support:

- `--database-url` or `MYCEL_RELIABILITY_DATABASE_URL`;
- `--artifact-root`;
- `--confirm-destructive`;
- `--keep-environment-on-failure`;
- `--profile-dir` for additional cluster/actor/scenario/suite definitions;
- `--dry-run` for import and run planning;
- `--output json|text` for catalog commands.

Database migrations should be explicit:

```sh
mycel-reliability db migrate
mycel-reliability db status
```

## Phases

## RH0: Design document, issue, and project skeleton

### Scope

Keep this phase documentation-only plus command/package skeleton. No database or
cluster operations yet.

### Tasks

1. Add the design document:
   - `docs/design/testing/reliability-harness.md`.
2. Add this implementation plan.
3. Add command skeleton:
   - `cmd/mycel-reliability/main.go`;
   - top-level command parsing and help text;
   - no-op subcommands for `db`, `import`, `export`, and `run`.
4. Add package skeletons under `internal/reliability` with package docs.
5. Add an explicit Makefile target for build-only validation, not long-running
   execution:

   ```sh
   make build-mycel-reliability
   ```

### Tests

- Command help renders.
- Command rejects missing subcommands cleanly.
- Package skeleton builds.

### Acceptance

```sh
go test ./internal/reliability/... ./cmd/mycel-reliability -count=1
go run ./cmd/mycel-reliability --help
git diff --check
```

## RH1: Spec structs, YAML parsing, validation, and resolution

### Scope

Implement the in-memory spec model and YAML parsing for ClusterProfile,
ActorProfile, Scenario, and Suite. No database yet.

### Tasks

1. Add Go structs for:
   - `ClusterProfile`;
   - `ActorProfile`;
   - `Scenario`;
   - `Suite`;
   - shared `Metadata`, duration, rate, retry, artifact, assertion, and event
     types.
2. Parse YAML into typed specs.
3. Validate required fields:
   - `apiVersion` and `kind`;
   - metadata name;
   - cluster node count and raft settings;
   - actor profile behavior type;
   - actor group `profileRef`, count, and rate;
   - phase names and durations;
   - supported events and assertions.
4. Implement profile reference resolution:
   - by path;
   - by name from configured profile directories.
5. Implement cluster and actor overrides.
6. Implement deterministic resolved scenario output.
7. Add sample YAML definitions under `tests/reliability`.

### Tests

- Unit: parse valid cluster/actor/scenario/suite YAML.
- Unit: reject missing metadata or unsupported kind.
- Unit: resolve actor profile refs by name and path.
- Unit: apply cluster and actor group overrides.
- Unit: emit stable resolved scenario for the same input.

### Acceptance

```sh
go test ./internal/reliability/spec ./internal/reliability/catalog -count=1
go run ./cmd/mycel-reliability run scenario-file tests/reliability/scenarios/example.yaml --dry-run
git diff --check
```

## RH2: Postgres store and migrations

### Scope

Add the Postgres schema and persistence layer for definitions and run metadata.
No cluster execution yet.

### Tasks

1. Add database migration support under `internal/reliability/migrations`.
2. Create definition tables:
   - `cluster_profiles`;
   - `actor_profiles`;
   - `scenarios`;
   - `suites`.
3. Create run tables:
   - `test_runs`;
   - `test_run_actor_profiles`;
   - `test_run_phases`;
   - `test_run_events`;
   - `test_run_metrics`;
   - `test_run_artifacts`.
4. Store definitions as JSONB with `spec_hash`, `name`, `version`, timestamps,
   and `used_by_run_count`.
5. Add store interfaces and Postgres implementation.
6. Add integration test support using disposable Postgres. Prefer testcontainers
   if acceptable; otherwise support `MYCEL_RELIABILITY_TEST_DATABASE_URL` and
   skip integration tests when unset.
7. Add migration commands:

   ```sh
   mycel-reliability db migrate
   mycel-reliability db status
   ```

### Tests

- Unit: migration files are ordered and embedded.
- Integration: migrate empty database.
- Integration: insert/list/get definition records.
- Integration: create run with resolved spec snapshot.
- Integration: append events, metrics, and artifact rows.

### Acceptance

```sh
go test ./internal/reliability/store ./internal/reliability/migrations -count=1
MYCEL_RELIABILITY_TEST_DATABASE_URL=postgres://... go test ./internal/reliability/store -run Integration -count=1
mycel-reliability db migrate --database-url "$MYCEL_RELIABILITY_TEST_DATABASE_URL"
git diff --check
```

## RH3: Catalog import/export and definition lifecycle

### Scope

Implement YAML import/export against Postgres and enforce the definition
lifecycle: mutable until used, immutable after first run.

### Tasks

1. Implement `import` command:

   ```sh
   mycel-reliability import tests/reliability/
   mycel-reliability import --dry-run tests/reliability/
   ```

2. Implement default import behavior:
   - create version 1 when name is new;
   - no-op when latest spec hash matches;
   - update latest in place when latest has never been used;
   - create next version when latest has been used and content changed.
3. Implement `export` command:

   ```sh
   mycel-reliability export scenario raft-5-node-long-outage --version 3
   mycel-reliability export actor-profile graph-committer --version latest
   ```

4. Implement list/show commands for definitions.
5. Enforce `can_edit = used_by_run_count == 0` and
   `can_delete = used_by_run_count == 0` in the catalog layer.
6. Add deletion for unused definitions only.
7. Persist resolved scenario snapshots for imported scenarios where profile refs
   are resolvable.

### Tests

- Unit/integration: first import creates version 1.
- Unit/integration: repeated identical import is no-op.
- Unit/integration: unused changed definition updates in place.
- Unit/integration: used changed definition creates new version.
- Unit/integration: used definition cannot be deleted.
- Unit/integration: exported YAML round-trips.

### Acceptance

```sh
go test ./internal/reliability/catalog ./internal/reliability/spec ./internal/reliability/store -count=1
mycel-reliability import --dry-run tests/reliability/
mycel-reliability import tests/reliability/
mycel-reliability export scenario <name> --version latest >/tmp/scenario.yaml
git diff --check
```

## RH4: Runner skeleton, phase engine, artifacts, and reporting

### Scope

Add run execution without creating a real MycelDB cluster. This validates the
run lifecycle, phase state machine, event stream, metrics, artifacts, and
summary generation.

### Tasks

1. Implement `run scenario` and `run suite` command skeletons.
2. Load scenario from database by name/version or from a scenario file.
3. Create `test_runs` row with resolved spec snapshot.
4. Increment/derive definition usage counts transactionally when a run is
   created.
5. Implement phase state machine:
   - pending;
   - running;
   - passed;
   - failed;
   - aborted.
6. Write JSONL artifacts and corresponding DB rows for:
   - run events;
   - phase events;
   - metrics;
   - assertions.
7. Generate `result.json` and `summary.md`.
8. Support cancellation and best-effort finalization.
9. Implement sequential suite execution with `stopOnFailure`.

### Tests

- Unit: phase transitions are valid.
- Unit: finalization writes terminal status.
- Unit: suite executes scenarios sequentially.
- Unit/integration: run rows, phase rows, events, artifacts, and summary are
  created for a dry-run scenario.

### Acceptance

```sh
go test ./internal/reliability/runner ./internal/reliability/artifacts ./internal/reliability/report -count=1
mycel-reliability run scenario-file tests/reliability/scenarios/example.yaml --dry-run
mycel-reliability run suite <suite-name> --dry-run
git diff --check
```

## RH5: Environment driver and MycelDB cluster deployment

### Scope

Create a disposable k3d/Kubernetes environment and deploy a MycelDB StatefulSet
using arbitrary initial node count and raft settings from the resolved cluster
profile.

### Tasks

1. Define environment driver interfaces:

   ```go
   type EnvironmentDriver interface {
       Preflight(ctx context.Context) error
       Create(ctx context.Context, spec ResolvedScenario) (Environment, error)
       Delete(ctx context.Context, env Environment) error
       CaptureState(ctx context.Context, env Environment, sink ArtifactSink) error
   }
   ```

2. Implement k3d/Kubernetes driver:
   - create disposable k3d cluster;
   - create namespace;
   - optionally load local image;
   - delete cluster/namespace during cleanup.
3. Render manifests for arbitrary initial node count:
   - namespace;
   - secrets;
   - ConfigMap;
   - headless service;
   - client service;
   - StatefulSet;
   - PVC templates.
4. Wire raft configuration from cluster profile.
5. Wait for pods to become ready.
6. Capture Kubernetes state and pod logs on success/failure.
7. Preserve `--keep-environment-on-failure` behavior.

### Tests

- Unit: rendered manifests reflect node count, partition count, replica factor,
  raft node addresses, resources, and storage.
- Unit: destructive operations require confirmation.
- Unit: cleanup is invoked on failure.
- Manual/integration: create and delete disposable k3d cluster.

### Acceptance

```sh
go test ./internal/reliability/env ./internal/reliability/deploy -count=1
mycel-reliability run scenario-file tests/reliability/scenarios/one-node-smoke.yaml --confirm-destructive
# Manual when k3d is available:
kubectl --context <context> -n <namespace> get pods
git diff --check
```

## RH6: Public client setup and actor runtime framework

### Scope

Add SDK/gRPC client wiring, per-instance setup, and actor runtime lifecycle, but
start with a no-op/test actor before graph transactions.

### Tasks

1. Add client wrapper for public MycelDB APIs.
2. Connect to cluster service via port-forward or service endpoint.
3. Bootstrap admin/login flow using generated credentials.
4. Implement actor runtime interfaces:

   ```go
   type Actor interface {
       Start(ctx context.Context) error
       UpdateRate(ctx context.Context, rate RateSpec) error
       Stop(ctx context.Context) error
   }
   ```

5. Implement actor group scheduler:
   - fixed count for scenario;
   - group-total rate distribution;
   - phase-level rate override;
   - deterministic per-instance seed derivation.
6. Implement per-instance identity/data setup hooks:
   - principal creation;
   - session/login;
   - space creation;
   - domain creation.
7. Add a no-op actor for testing runner scheduling and rate updates.

### Tests

- Unit: group-total rate divides across instances.
- Unit: deterministic per-instance seeds are stable.
- Unit: phase rate overrides apply and restore as expected.
- Integration/manual: actors connect to disposable cluster and create
  per-instance spaces/domains.

### Acceptance

```sh
go test ./internal/reliability/actors ./internal/reliability/runner -count=1
mycel-reliability run scenario-file tests/reliability/scenarios/actor-noop.yaml --confirm-destructive
git diff --check
```

## RH7: Graph transaction actor and operation log

### Scope

Implement the first real actor: graph transactions with operation budget,
weighted operation mix, bounded retries, and structured operation logging.

### Tasks

1. Implement `graph-transaction` actor profile.
2. Generate deterministic operations per transaction:
   - `createNode`;
   - `updateNode`;
   - `createEdge`.
3. Enforce operation budget and per-operation constraints.
4. Fallback to `createNode` when selected operation is impossible.
5. Keep per-instance known-state index for nodes and edges.
6. Execute graph transactions through public SDK/gRPC APIs.
7. Implement bounded retries and error classification:
   - transient;
   - unavailable;
   - permanent.
8. Write graph transaction events to DB and JSONL artifacts.
9. Record only daemon-acknowledged transactions as must-converge operations.

### Tests

- Unit: operation generation is deterministic from seed and seq.
- Unit: operation mix respects weights statistically within broad tolerance.
- Unit: impossible `createEdge` falls back to `createNode`.
- Unit: no delete operations are generated in v1.
- Unit: acknowledged transactions update expected counters.
- Integration/manual: actor commits graph transactions against disposable cluster.

### Acceptance

```sh
go test ./internal/reliability/actors ./internal/reliability/oracle -count=1
mycel-reliability run scenario-file tests/reliability/scenarios/graph-actor-smoke.yaml --confirm-destructive
git diff --check
```

## RH8: Read-after-write and final convergence oracle

### Scope

Implement sampled read-after-write checks and final convergence checks using
acknowledged graph transaction counts.

### Tasks

1. Implement read-after-write sampling for graph actor:
   - sample every N acknowledged commits;
   - query a created/updated node;
   - bounded retry.
2. Store read check events in Postgres and JSONL.
3. Implement expected count derivation:
   - expected nodes = acknowledged `createNode` operations;
   - expected edges = acknowledged `createEdge` operations.
4. Query counts through public APIs for each actor instance space/domain.
5. Check convergence across reachable pods/endpoints.
6. Implement final assertions:
   - require healthy cluster;
   - require converged counts;
   - require no acknowledged graph transaction loss.
7. Add clear failure summaries with actor group/instance, expected count,
   observed count, endpoint, and phase.

### Tests

- Unit: expected counts derive correctly from operation logs.
- Unit: read-check failures are classified correctly during normal and expected
  degradation phases.
- Integration/manual: graph actor smoke scenario converges.

### Acceptance

```sh
go test ./internal/reliability/oracle ./internal/reliability/actors -count=1
mycel-reliability run scenario-file tests/reliability/scenarios/graph-convergence-smoke.yaml --confirm-destructive
git diff --check
```

## RH9: Deterministic pod lifecycle events and expected degradation

### Scope

Implement the first deterministic events and classify actor failures during
expected-degradation phases.

### Tasks

1. Implement event runtime interfaces.
2. Implement `pod-stop` by pod name for a configured duration.
3. Implement `pod-restart` by pod name.
4. Implement `rolling-restart` with batch size and pause.
5. Attach events to phase start.
6. Record event start/completion/failure in DB and JSONL.
7. Mark phase availability as expected degradation when configured.
8. Classify actor failures during expected-degradation phases as expected
   degradation when they match transient/unavailable classes.
9. Ensure correctness failures still fail scenarios.
10. Capture Kubernetes state before and after events.

### Tests

- Unit: event schedule starts at phase boundary.
- Unit: expected degradation does not mask correctness assertion failures.
- Unit: pod-name targeting is deterministic.
- Integration/manual: pod-stop scenario completes and recovers.

### Acceptance

```sh
go test ./internal/reliability/events ./internal/reliability/runner -count=1
mycel-reliability run scenario-file tests/reliability/scenarios/three-node-short-outage.yaml --confirm-destructive
mycel-reliability run scenario-file tests/reliability/scenarios/five-node-long-outage.yaml --confirm-destructive
git diff --check
```

## RH10: Metrics, trend queries, and run comparison foundation

### Scope

Add aggregate metrics and query surfaces needed for future dashboard/trending.

### Tasks

1. Record metric samples for:
   - commits/sec;
   - transaction latency p50/p95/p99;
   - read-check latency p50/p95/p99;
   - transient/permanent error counts;
   - expected-degradation counts;
   - pod restart counts;
   - leader changes if available.
2. Add aggregation at phase and run level.
3. Add CLI commands:

   ```sh
   mycel-reliability runs list
   mycel-reliability runs show <run-id>
   mycel-reliability runs compare <run-a> <run-b>
   ```

4. Add basic trend query by scenario/profile name.
5. Keep raw metric rows in Postgres and JSONL.

### Tests

- Unit: metric aggregations compute correctly.
- Integration: completed run has expected aggregate metrics.
- Integration: compare command reports deltas.

### Acceptance

```sh
go test ./internal/reliability/metrics ./internal/reliability/report ./internal/reliability/store -count=1
mycel-reliability runs list
mycel-reliability runs show <run-id>
git diff --check
```

## RH11: Baseline scenario library and documentation

### Scope

Add initial reusable profiles/scenarios/suites and document local operation.

### Tasks

1. Add cluster profiles:
   - `single-node`;
   - `raft-3-node`;
   - `raft-5-node`.
2. Add actor profiles:
   - `graph-committer`;
   - `gql-reader`.
3. Add scenarios:
   - one-node graph smoke;
   - three-node graph smoke;
   - three-node short named-pod outage;
   - five-node long named-pod outage.
4. Add suite:
   - `raft-reliability-baseline`.
5. Add operations docs:
   - database setup;
   - import/export;
   - running a scenario;
   - running a suite;
   - interpreting result summaries;
   - retaining environments for debugging;
   - safety notes.
6. Update design docs with any implementation-driven schema adjustments.

### Tests

- Import all baseline definitions.
- Dry-run all baseline scenarios.
- Run at least smoke scenario in disposable environment.

### Acceptance

```sh
mycel-reliability import --dry-run tests/reliability/
mycel-reliability import tests/reliability/
mycel-reliability run suite raft-reliability-baseline --dry-run
mycel-reliability run scenario one-node-graph-smoke --confirm-destructive
python3 scripts/checkDocs.py docs/ tests/reliability/
git diff --check
```

## Suggested first PR breakdown

To keep review sizes manageable:

1. PR 1: design + implementation plan + command/package skeleton.
2. PR 2: spec structs, YAML parsing, validation, sample definitions.
3. PR 3: Postgres migrations and store.
4. PR 4: catalog import/export/lifecycle.
5. PR 5: runner skeleton/artifacts/reporting.
6. PR 6: k3d/Kubernetes deployment.
7. PR 7: actor runtime + graph actor.
8. PR 8: oracle/read checks/convergence.
9. PR 9: pod events/expected degradation.
10. PR 10: baseline scenarios/docs/trend queries.

## Open questions

- Should v1 support only disposable k3d clusters, or also attach to existing
  Kubernetes namespaces?
- Should a local Docker Compose driver be included early or left as future work?
- Should actor clients run in-process initially, or should actor workers run as
  separate pods from the beginning?
- Which Postgres migration library should be used for this repo?
- Should the reliability database be external-only, or should the harness be
  able to launch a disposable Postgres for local development?
- Should artifact storage integrate with the planned `object_store`/MinIO support
  immediately or only after local filesystem artifacts are stable?
