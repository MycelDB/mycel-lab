# Structured reliability harness

## Status

Draft design. This document defines the target shape for a long-running MycelDB
reliability test system. It is not an implementation plan yet.

## Overview

MycelDB needs a deterministic, long-running reliability test system that can
create configurable clusters, run realistic client activity, apply controlled
failure conditions, and persist enough evidence to understand correctness,
availability, and reliability trends over time.

The harness is scenario-driven. A scenario creates exactly one MycelDB cluster,
starts one or more simulated actor groups through the public SDK/gRPC API,
executes deterministic phase-based events such as pod outages or restarts, and
records structured events, metrics, artifacts, and assertion results. See
[Mycel Lab model](model.md) for the full concept glossary covering scenarios,
suites, cluster profiles, actor profiles, phases, events, runs, and artifacts.

YAML is the portable authoring and import/export format. Postgres is the
operational source of truth for definitions, runs, events, metrics, trends, and
artifact indexes.

## Goals

- Support deterministic, reproducible long-running reliability scenarios.
- Allow arbitrary initial MycelDB daemon pod counts: one, three, five, or more.
- Let scenarios control raft topology and tuning, including partition count,
  replica factor, election/heartbeat ticks, and compaction/snapshot settings.
- Run simulated users through public SDK/gRPC APIs, not private in-process hooks.
- Keep actor activity running through planned disruptions unless a scenario says
  otherwise.
- Distinguish hard failures from expected degradation during planned outages.
- Persist definition data, run history, metrics, trends, events, and artifacts in
  Postgres.
- Store a fully resolved scenario snapshot for every run so historical runs stay
  interpretable even after profile definitions change.
- Keep JSONL artifacts alongside database rows for debugging and portability.
- Design for future dashboard/control-plane use without requiring the dashboard
  in the first implementation.

## Non-goals for the first implementation

- Random chaos testing. Initial scenarios are deterministic.
- Matrix expansion. Single scenarios and sequential suites come first.
- Multiple MycelDB clusters in one scenario.
- Dynamic raft membership. MycelDB currently uses fixed raft membership, though
  the harness should be designed to support dynamic scale events later.
- Full graph-state shadow modeling. Initial correctness uses operation logs,
  count convergence, and sampled read-after-write checks.
- Native Azure Blob support. Object-store testing can target S3-compatible
  stores such as MinIO first.
- TimescaleDB. Plain Postgres is sufficient initially.

## Concepts

### Scenario

A `Scenario` is a concrete reliability experiment. It references a cluster
profile, instantiates actor groups, defines sequential phases, attaches events to
phases, and declares final assertions and artifact policy.

One scenario creates and tests one MycelDB cluster.

### Suite

A `Suite` is a sequential list of scenarios. Suites run scenarios one at a time
and may stop on the first failure.

Parallel suite execution is intentionally deferred.

### ClusterProfile

A `ClusterProfile` is a reusable cluster topology/configuration definition. It
answers: what shape of MycelDB cluster should this scenario create?

A cluster profile defines MycelDB daemon pod count and raft settings, including:

- initial node count;
- partition count;
- replica factor;
- election and heartbeat ticks;
- send timeout;
- compaction and snapshot settings;
- pod resources and storage settings.

Cluster profiles should not define scenario-specific disruptions or actors.

### ActorProfile

An `ActorProfile` is a reusable behavior template. It answers: what kind of
simulated user/client behavior is this?

Examples:

- graph committer;
- GQL reader;
- blob checksum user;
- semantic search user;
- backup operator.

Actor profiles define behavior, operation mix, retry policy, data-scope policy,
and checks. Actor profiles do not define count, scenario duration, cluster
shape, or disruption schedule.

### ActorGroup

An `ActorGroup` is a scenario-specific instantiation of an actor profile. It
answers: how many instances of this behavior should run in this scenario and at
what pace?

Actor group count is fixed for v1. Phase overrides may adjust rate but should
not change count in the initial implementation.

### ActorInstance

An `ActorInstance` is one concrete runtime simulated user/client. In v1 each
actor instance uses a distinct MycelDB space and domain.

### Phase

A `Phase` is a named sequential stage in a scenario. Phases have durations,
optional events, optional actor group rate overrides, expected-degradation
policy, and assertions.

Events initially run at phase start. Relative timing inside a phase, such as
`after: 10m`, is a future extension.

### Event

An `Event` is a deterministic condition applied by the harness. Initial event
examples:

- stop a named pod for a duration;
- restart a named pod;
- rolling restart.

Initial event targeting is by explicit pod name, for example `myceld-2`. Role-
based targeting such as leader/follower or seeded random targeting can come
later.

### Expected degradation

A phase may mark availability degradation as expected. During such a phase,
read/write failures caused by the planned condition are recorded as expected
degradation, not hard failures. Correctness failures still fail the scenario.

### Hard failure

A hard failure violates correctness, final recovery, cluster health, or a
scenario assertion. Hard failures fail the scenario.

## Authoring layout

YAML definitions should be easy to review and import. A recommended repository
layout is:

```text
tests/reliability/
  clusters/
    single-node.yaml
    raft-3-node.yaml
    raft-5-node.yaml
    raft-5-node-snapshot.yaml

  actor-profiles/
    graph-committer.yaml
    gql-reader.yaml
    bulk-graph-importer.yaml

  scenarios/
    raft-3-node-short-outage.yaml
    raft-5-node-long-outage.yaml

  suites/
    raft-baseline.yaml
```

## Cluster profile schema example

```yaml
apiVersion: myceldb.io/reliability/v1
kind: ClusterProfile

metadata:
  name: raft-5-node
  description: Five-node raft cluster with local-path storage.

cluster:
  nodes: 5
  image: myceldb/mycel:latest

  raft:
    nodeCount: 5
    partitionCount: 64
    replicaFactor: 3
    localNodeIDMode: ordinal
    electionTick: 25
    heartbeatTick: 1
    sendTimeout: 500ms

    compaction:
      mode: off
      snapshotEntries: 0
      snapshotInterval: 0
      snapshotMaxLogBytes: 0
      snapshotMinRetainEntries: 0

  resources:
    requests:
      cpu: "500m"
      memory: "512Mi"
    limits:
      cpu: "2"
      memory: "2Gi"

  storage:
    size: "10Gi"
    className: local-path
```

## Actor profile model

Actor profiles are behavior templates. They do not specify how many actor
instances run. Count and rate live in scenario actor groups.

Initial actor profiles should support per-instance identity and per-instance
data scope. The schema includes identity to exercise auth/session/domain
boundaries from the start, though an early implementation may initially use an
admin bootstrap path internally to create principals and seed spaces/domains.

### Graph transaction actor strategy

The first graph actor should generate graph transactions using:

- an operation budget per transaction;
- a weighted operation mix;
- per-operation constraints;
- fallback to `createNode` when the selected operation is not currently valid;
- no delete operations in v1;
- a per-actor local known-state index;
- count-based convergence and sampled read-after-write checks.

This approach gives deterministic variation without requiring a full graph
shadow model.

The operation unit is a graph transaction. Initial operation types:

- `createNode`;
- `updateNode`;
- `createEdge`.

Each actor instance tracks its own known nodes and edges because each instance
uses a separate space/domain in v1.

### Graph actor profile example

```yaml
apiVersion: myceldb.io/reliability/v1
kind: ActorProfile

metadata:
  name: graph-committer
  description: Creates and updates graph data using graph transactions.

behavior:
  type: graph-transaction

  identity:
    mode: per-instance
    principalPrefix: graph-user

  dataScope:
    space:
      mode: per-instance
      prefix: journal
    domain:
      mode: per-instance
      schema: pkm-lite
      prefix: journal-domain

  transaction:
    commitUnit: graph-transaction

    operationsPerTransaction:
      min: 1
      max: 20

    operationMix:
      createNode:
        weight: 60
        maxPerTransaction: 10

      updateNode:
        weight: 20
        maxPerTransaction: 5
        requiresExistingNode: true

      createEdge:
        weight: 20
        maxPerTransaction: 10
        requiresExistingNodes: 2

    fallbackOperation: createNode

    dataModel:
      nodeLabels:
        - Note
        - Page
        - Task

      edgeTypes:
        - LINKS_TO
        - CONTAINS
        - REFERENCES

      properties:
        text:
          mode: generated-text
          minBytes: 32
          maxBytes: 512
        ordinal:
          mode: sequence
        actor:
          mode: actor-id
        transactionSeq:
          mode: transaction-seq

  retry:
    maxAttempts: 4
    initialBackoff: 100ms
    maxBackoff: 2s
    retryOn:
      - transient
      - unavailable

  checks:
    readAfterWrite:
      enabled: true
      sampleEveryCommits: 10
      maxAttempts: 4
      backoff: 100ms

    countConvergence:
      enabled: true
```

### GQL reader actor profile example

```yaml
apiVersion: myceldb.io/reliability/v1
kind: ActorProfile

metadata:
  name: gql-reader
  description: Runs read-only GQL queries against actor-owned spaces/domains.

behavior:
  type: gql-read

  querySet:
    - name: count-nodes
      gql: "MATCH (n) RETURN count(n)"

    - name: recent-nodes
      gql: "MATCH (n) RETURN n ORDER BY n.properties.createdAt DESC FETCH FIRST 25 ROWS ONLY"

  retry:
    maxAttempts: 3
    initialBackoff: 100ms
    maxBackoff: 1s
```

## Scenario schema example

```yaml
apiVersion: myceldb.io/reliability/v1
kind: Scenario

metadata:
  name: raft-5-node-long-outage
  description: Five-node raft cluster with short and long pod outages.

seed: 12345

environment:
  driver: k3d
  namespace: mycel-lab
  keepOnFailure: false

clusterRef: raft-5-node

clusterOverrides:
  raft:
    partitionCount: 64
    replicaFactor: 3

actorGroups:
  - name: journal-users
    profileRef: graph-committer
    count: 8
    rate:
      mode: group-total
      commitsPerSecond: 20

  - name: readers
    profileRef: gql-reader
    count: 4
    target:
      actorGroups:
        - journal-users
    rate:
      mode: group-total
      queriesPerSecond: 10

phases:
  - name: warmup
    duration: 5m

  - name: short-outage
    duration: 5m
    events:
      - type: pod-stop
        target:
          pod: myceld-2
        duration: 30s
    outcome:
      availability: expected-degradation

  - name: pressure
    duration: 30m
    actorGroupOverrides:
      journal-users:
        rate:
          mode: group-total
          commitsPerSecond: 50
      readers:
        rate:
          mode: group-total
          queriesPerSecond: 25

  - name: long-outage
    duration: 1h
    events:
      - type: pod-stop
        target:
          pod: myceld-3
        duration: 45m
    outcome:
      availability: expected-degradation

  - name: recovery
    duration: 10m
    assertions:
      requireHealthyCluster: true
      requireConvergedCounts: true
      requireNoAcknowledgedGraphTransactionLoss: true

assertions:
  final:
    requireHealthyCluster: true
    requireConvergedCounts: true
    requireNoAcknowledgedGraphTransactionLoss: true

artifacts:
  collectLogs: true
  collectKubernetesState: true
  collectActorEvents: true
  collectReadChecks: true
  collectScenarioSpec: true
```

## Suite schema example

```yaml
apiVersion: myceldb.io/reliability/v1
kind: Suite

metadata:
  name: raft-reliability-baseline
  description: Baseline raft reliability scenarios.

scenarios:
  - path: ../scenarios/one-node-smoke.yaml
  - path: ../scenarios/three-node-short-outage.yaml
  - path: ../scenarios/five-node-long-outage.yaml

execution:
  mode: sequential
  stopOnFailure: true
```

## Rate semantics

Actor group rates default to `group-total`. This means the declared rate is the
total rate across every actor instance in the group.

Example:

```yaml
count: 8
rate:
  mode: group-total
  commitsPerSecond: 20
```

The group produces about 20 commits/second total, or about 2.5 commits/second
per instance.

A future mode may support per-instance rates:

```yaml
rate:
  mode: per-instance
  commitsPerSecond: 5
```

## Phase and actor lifecycle

For v1:

- actor group count is fixed for the scenario;
- actor groups continue running across phases;
- phase overrides may adjust actor group rate;
- phase overrides do not start or stop actor instances.

Future versions may support phase-level count changes and actor group start/stop
conditions.

## Event model

Supported event types include:

```yaml
type: pod-stop
type: pod-restart
type: node-stop
type: node-restart
type: rolling-restart
type: environment-reset
type: user-backup-fixture
type: user-backup-export
type: user-backup-validate
type: user-backup-import
type: user-backup-verify-restored
type: user-backup-assert-safety
type: system-backup-fixture
type: cluster-backup-create
type: cluster-backup-validate
type: cluster-restore-apply
type: cluster-restore-verify
```

Pod and node events use deterministic pod/name/ordinal targeting:

```yaml
target:
  pod: myceld-2
```

Future targeting modes:

```yaml
target:
  role: leader
```

```yaml
target:
  role: follower
```

```yaml
target:
  selector: random
  seed: 12345
```

## Correctness oracle

The v1 oracle should use:

- actor operation logs;
- acknowledged graph transaction logs;
- expected node and edge counts;
- sampled read-after-write checks;
- final count convergence across live pods/endpoints.

Because v1 graph actors do not delete data and each actor instance has its own
space/domain, expected counts can be derived from acknowledged operations:

```text
expected nodes = acknowledged createNode operations
expected edges = acknowledged createEdge operations
```

A full graph-state shadow model is deferred.

## Runtime event examples

Acknowledged transaction:

```json
{
  "ts": "2026-09-11T12:00:00Z",
  "runId": "...",
  "actorGroup": "journal-users",
  "actorInstance": "journal-users-3",
  "kind": "graph.transaction.acknowledged",
  "seq": 1842,
  "operationCount": 12,
  "latencyMs": 42,
  "spaceId": "...",
  "domainId": "..."
}
```

Failed transaction during expected degradation:

```json
{
  "ts": "2026-09-11T12:05:00Z",
  "actorGroup": "journal-users",
  "actorInstance": "journal-users-3",
  "kind": "graph.transaction.failed",
  "seq": 1850,
  "attempt": 4,
  "errorClass": "transient",
  "expectedDegradation": true
}
```

Read-after-write result:

```json
{
  "ts": "2026-09-11T12:06:00Z",
  "actorGroup": "journal-users",
  "actorInstance": "journal-users-3",
  "kind": "graph.read_after_write.passed",
  "seq": 1840,
  "attempt": 2,
  "latencyMs": 21
}
```

## Database as source of truth

Postgres is the operational source of truth for definitions and execution
history. YAML is an authoring/import/export format.

Definition records include:

- cluster profiles;
- actor profiles;
- scenarios;
- suites.

Execution records include:

- test runs;
- phases;
- events;
- metrics;
- artifacts;
- assertion results;
- summaries.

### Definition lifecycle

Definitions are mutable until used by a run.

Once a definition has been used by at least one run:

- it becomes immutable;
- edits create a new version;
- deletion is not allowed, except possible future archival/soft-delete.

If a definition has never been used:

- it can be edited in place;
- it can be deleted;
- import can update it.

### Import behavior

Default import behavior:

```text
if name does not exist:
  create version 1

if name exists and latest version has identical spec hash:
  no-op

if name exists and latest version has never been used:
  update latest version in place

if name exists and latest version has been used and content changed:
  create next version
```

YAML import should not destructively overwrite used definitions.

### Run reproducibility

Every run stores a full resolved scenario snapshot in Postgres. This includes:

- cluster profile contents;
- actor profile contents;
- scenario overrides;
- resolved phase/event/assertion config;
- seed;
- image/tag;
- environment config.

Historical runs must remain interpretable even if profiles change later.

## Database schema sketch

### Definition tables

Definition tables can initially store specs as JSONB for flexibility.

```sql
cluster_profiles (
  id uuid primary key,
  name text not null,
  version int not null,
  description text,
  spec jsonb not null,
  spec_hash text not null,
  used_by_run_count int not null default 0,
  created_at timestamptz not null,
  updated_at timestamptz not null,
  created_by text,
  updated_by text,
  deleted_at timestamptz,
  unique (name, version)
)
```

Equivalent tables should exist for `actor_profiles`, `scenarios`, and `suites`.

### Runs

```sql
test_runs (
  id uuid primary key,
  suite_id uuid null,
  scenario_id uuid not null,
  cluster_profile_id uuid not null,
  status text not null,
  result text,
  seed bigint,
  resolved_spec jsonb not null,
  started_at timestamptz,
  ended_at timestamptz,
  duration_ms bigint,
  artifact_root text,
  summary jsonb,
  created_at timestamptz not null
)
```

Actor profile usage per run:

```sql
test_run_actor_profiles (
  run_id uuid references test_runs(id),
  actor_profile_id uuid references actor_profiles(id),
  actor_group_name text not null,
  primary key (run_id, actor_group_name)
)
```

### Phases

```sql
test_run_phases (
  id uuid primary key,
  run_id uuid references test_runs(id),
  name text not null,
  status text not null,
  started_at timestamptz,
  ended_at timestamptz,
  duration_ms bigint,
  outcome text
)
```

### Events

```sql
test_run_events (
  id bigserial primary key,
  run_id uuid references test_runs(id),
  phase_id uuid references test_run_phases(id),
  ts timestamptz not null,
  kind text not null,
  severity text,
  actor_group text,
  actor_instance text,
  pod text,
  data jsonb not null
)
```

### Metrics

```sql
test_run_metrics (
  id bigserial primary key,
  run_id uuid references test_runs(id),
  ts timestamptz not null,
  name text not null,
  value double precision not null,
  unit text,
  labels jsonb
)
```

### Artifacts

```sql
test_run_artifacts (
  id uuid primary key,
  run_id uuid references test_runs(id),
  kind text not null,
  path text not null,
  content_type text,
  size_bytes bigint,
  created_at timestamptz not null
)
```

## Artifacts

Artifacts should be stored as files initially and indexed in Postgres. Future
artifact storage should support object storage, preferably the same `object_store`
/ MinIO path planned for blob payloads.

A run artifact directory should include:

```text
artifacts/reliability/<run-id>/
  scenario.original.yaml
  scenario.resolved.yaml
  result.json
  summary.md

  events.jsonl
  phase-events.jsonl
  actor-events.jsonl
  graph-transactions.jsonl
  read-checks.jsonl
  metrics.jsonl
  assertions.json

  k8s/
    pods.txt
    statefulset.txt
    events.txt
    describe/

  logs/
    myceld-0.log
    myceld-1.log
    myceld-2.log
```

High-volume raw event streams should be written to both Postgres and JSONL
artifacts.

## CLI shape

Initial CLI examples:

```bash
mycel-lab import tests/reliability/
mycel-lab import --dry-run tests/reliability/

mycel-lab run scenario raft-5-node-long-outage --version latest
mycel-lab run scenario-file tests/reliability/scenarios/raft-5-node-long-outage.yaml
mycel-lab run suite raft-reliability-baseline --version latest

mycel-lab export scenario raft-5-node-long-outage --version 3 > scenario.yaml
```

Potential code location:

```text
cmd/mycel-lab/
internal/reliability/
```

## Internal architecture

```text
cmd/mycel-lab/
  main.go

internal/reliability/
  spec/          # YAML schema, Go structs, validation, defaulting
  store/         # Postgres persistence
  migrations/    # DB schema migrations
  catalog/       # import/export/versioning/profile resolution
  runner/        # scenario/suite execution
  env/           # dry-run, k3d, compose, and future environment drivers
  deploy/        # manifests and cluster deployment
  actors/        # actor profiles and runtime actor instances
  events/        # pod-stop, pod-restart, rolling-restart
  oracle/        # operation log, counters, convergence checks
  metrics/       # metric samples and aggregation
  artifacts/     # JSONL, logs, k8s snapshots, artifact index
  report/        # summary.md / future HTML
```

## Initial implementation phases

### Phase 1: spec and catalog foundation

- Define Go structs for ClusterProfile, ActorProfile, Scenario, and Suite.
- Implement YAML parsing, validation, defaulting, and reference resolution.
- Add Postgres schema migrations for definition tables.
- Implement import, dry-run import, and export.
- Enforce mutable-until-used definition lifecycle.

### Phase 2: runner skeleton and artifacts

- Add run creation in Postgres.
- Store resolved scenario snapshots.
- Implement run/phase status transitions.
- Write JSONL event artifacts and matching database event rows.
- Generate summary.md and result.json.

### Phase 3: cluster environment and deployment

See [Environment driver abstraction](environment-drivers.md) for the current driver model.

- Implement dry-run, k3d, and Compose environment drivers behind one runner abstraction.
- Render/deploy MycelDB StatefulSet for k3d and Compose-equivalent resources for Compose.
- Support raft settings from cluster profile.
- Collect backend-specific logs and environment state snapshots.

### Phase 4: graph actor and oracle

- Implement graph transaction actor profile.
- Implement per-instance space/domain setup.
- Implement operation-budget/weighted-mix transaction generation.
- Implement bounded retry and read-after-write sampling.
- Implement count convergence and no acknowledged transaction loss checks.

### Phase 5: deterministic events

- Implement pod-stop by pod name.
- Implement pod-restart by pod name.
- Implement rolling restart.
- Classify actor errors during expected-degradation phases.

### Phase 6: suites and trend queries

- Run sequential suites.
- Add basic aggregate metrics and trend queries.
- Prepare data model for dashboard consumption.

## Future work

- Dynamic raft membership and runtime scale-up/scale-down events.
- Phase-level actor count changes.
- Random chaos mode with seed-based reproducibility.
- Role-based event targeting: leader/follower/random.
- Network partitions and latency injection.
- Disk/persistence faults and PVC/data-dir loss.
- Upgrade and rollback scenarios.
- Blob/object_store actors, especially MinIO-backed durability checks.
- Backup/restore actors and events.
- Semantic, lexical, and hybrid search actors.
- Full graph shadow model.
- Matrix expansion.
- Dashboard/control plane for creating, scheduling, and comparing runs.
- Object-store artifact storage.
- Postgres partitioning or TimescaleDB if metric volume requires it.

## Open questions

- Should v1 support only disposable k3d clusters, or also attach to existing
  Kubernetes namespaces?
- Should a local Docker Compose driver be included early or left as future work?
- Should the first implementation keep all actor clients in-process, or should
  actors eventually run as separate worker pods?
- What schema vocabulary should actor data models use for reusable realistic
  graph shapes beyond `pkm-lite`?
