# Mycel Lab model

## Status

Current conceptual model for Mycel Lab reliability and system-test authoring.

This document explains the domain terms used by scenario YAML, suites, the
runner, artifacts, and future REST APIs. It is intentionally product-facing: use
it to decide which concept to model when adding new tests or runner features.

## Concept map

| Concept | YAML kind / location | Meaning |
| --- | --- | --- |
| Test | `Scenario` | One concrete reliability or system experiment. |
| Test suite | `Suite` | Ordered collection of scenarios executed as one validation bundle. |
| Cluster config | `ClusterProfile` | Reusable Mycel cluster topology and daemon/runtime settings. |
| Actor role/template | `ActorProfile` | Reusable workload actor definition. |
| Actor group | `Scenario.actorGroups[]` | Scenario-local group of actors sharing one profile and rate. |
| Concrete actor | `ActorInstance` runtime concept | One running actor created from an actor group. |
| Stage | `Phase` | Time-bounded section of a scenario. |
| Disruption | `Event` | Planned phase-time operation such as restart or outage. |
| Expected outcome | `Assertion` / oracle | Conditions that decide whether the run passed. |
| Execution record | `Run` | One execution of one resolved scenario. |
| Logs/results | `Artifacts` | Files emitted by a run for debugging and evidence. |
| Environment | `EnvironmentSpec` | Backend selection/configuration for running a scenario. |
| Environment driver | Go runner implementation | Backend implementation such as `dry-run`, `k3d`, or `compose`. |

## Test: `Scenario`

A `Scenario` is the canonical Mycel Lab test. It describes one concrete
experiment:

- which environment backend to use;
- which Mycel cluster profile to instantiate;
- which actor groups to run;
- which phases and disruptions occur;
- which assertions and artifacts are required.

Scenarios should express **test intent**, not backend mechanics. For example, a
scenario can say that a three-node raft cluster runs graph writers during a node
restart. The selected environment driver decides whether that means deleting a
Kubernetes pod, restarting a Compose service, or only recording a dry-run plan.

Typical file location:

```text
tests/reliability/scenarios/<name>/<name>.yaml
tests/reliability/scenarios/<name>/<name>.md
```

Each scenario directory contains the executable YAML and a same-named Markdown
guide. The Markdown guide documents the scenario purpose, topology, phase flow,
tunable parameters, evidence artifacts, common failure modes, and related
issues. It should include at least one Mermaid topology diagram so destructive
or long-running tests can be reviewed before execution.

Minimal shape:

```yaml
apiVersion: myceldb.io/reliability/v1
kind: Scenario
metadata:
  name: three-node-graph-smoke
seed: 12345
environment:
  driver: k3d
clusterRef: raft-3-node
actorGroups: []
phases:
  - name: smoke
    duration: 30s
assertions:
  final:
    requireHealthyCluster: true
```

## Test suite: `Suite`

A `Suite` groups scenarios into an ordered validation bundle. Suites are useful
for release gates, baseline reliability checks, and repeated smoke runs.

Typical file location:

```text
tests/reliability/suites/<name>.yaml
```

A suite references scenarios by file path or, in future catalog-backed flows, by
catalog reference:

```yaml
apiVersion: myceldb.io/reliability/v1
kind: Suite
metadata:
  name: raft-reliability-baseline
scenarios:
  - path: ../scenarios/one-node-smoke/one-node-smoke.yaml
  - path: ../scenarios/three-node-short-outage/three-node-short-outage.yaml
execution:
  mode: sequential
  stopOnFailure: true
```

## Cluster config: `ClusterProfile`

A `ClusterProfile` is a reusable Mycel cluster topology and runtime config. It
answers questions such as:

- how many daemon nodes exist;
- which image is deployed;
- how raft partitioning is configured;
- which resources, storage, ports, and environment variables are used.

Typical file location:

```text
tests/reliability/clusters/<name>.yaml
```

Scenarios reference a profile with `clusterRef` and can apply
`clusterOverrides` for scenario-specific changes. Prefer reusable profiles for
stable topologies and use overrides sparingly for experiment-specific pressure.

## Actor role/template: `ActorProfile`

An `ActorProfile` describes a reusable workload role. It is a template, not a
running worker. Examples:

- no-op actor for runner smoke tests;
- graph transaction writer;
- GQL reader/checker.

Typical file location:

```text
tests/reliability/actor-profiles/<name>.yaml
```

Profiles define behavior type and behavior-specific settings. Scenario actor
groups decide how many instances of the profile to run and at what rate.

## Actor group

An actor group is a scenario-local declaration that instantiates an
`ActorProfile` N times with scenario-specific rate and target settings.

```yaml
actorGroups:
  - name: writers
    profileRef: graph-committer
    count: 8
    rate:
      mode: group-total
      commitsPerSecond: 20
```

Actor group names are important because phases, readers, and assertions can
refer to them.

## Concrete actor / actor instance

A concrete actor is created by the runner from an actor group. If a group has
`count: 8`, the runner creates eight actor instances. Each instance has a stable
runtime identity, assignment, seed, credentials/resources where applicable, and
its own event/metric stream.

Concrete actors are runtime records rather than top-level YAML definitions.
They appear in run artifacts and future run APIs.

## Stage: `Phase`

A phase is a named, time-bounded section of a scenario. Phases let a scenario
separate warmup, disruption, pressure, and recovery behavior.

```yaml
phases:
  - name: warmup
    duration: 1m
  - name: restart-one-node
    duration: 2m
    events:
      - type: pod-restart
        target:
          pod: myceld-1
```

Phases may also override actor-group rates or add phase-local assertions.

## Disruption: `Event`

An event is a planned operation during a phase. Current examples include:

- `pod-stop` / `pod-restart` compatibility names;
- `rolling-restart`;
- future logical `node-*` event names.

Events are executed through the selected environment driver. This keeps scenario
intent stable across `k3d`, `compose`, and future drivers.

## Expected outcome: assertions and oracle

Assertions describe what must be true for a run to pass. The oracle turns actor
records, read checks, final live checks, and scenario-specific assertions into a
pass/fail result.

Examples of expected outcomes:

- cluster is healthy at the end;
- graph counts converge;
- no acknowledged graph transaction is lost;
- transient failures are allowed only during explicitly degraded phases;
- correctness failures always fail the scenario.

Assertions may be scenario-wide or phase-local. The exact assertion schema can
expand over time, but assertions should remain declarative.

## Execution record: `Run`

A run is one execution of one resolved scenario. It captures:

- run ID;
- scenario name/version;
- seed;
- status;
- phase statuses;
- resolved scenario snapshot;
- actor/profile snapshots;
- artifact root;
- start and finish timestamps.

Runs are stored in memory or Postgres depending on runner configuration and are
mirrored to artifacts for portable evidence.

## Logs/results: artifacts

Artifacts are the durable files emitted by a run. They are the primary debugging
and review evidence for local execution and CI.

Common artifacts include:

```text
resolved-scenario.json
events.jsonl
result.json
summary.md
environment/state.json
environment/nodes.json
environment/endpoints.json
environment/scenario-resources.json
actors/assignments.json
oracle/final-report.json
oracle/final-report.md
```

Backend-specific artifacts are allowed, for example Kubernetes pod descriptions
or Compose service logs.

## Environment and driver

`EnvironmentSpec` selects where and how a scenario runs. The current intended
drivers are:

- `dry-run` — validate planning, rendering, phases, actors, and artifacts
  without external mutation;
- `k3d` — disposable local Kubernetes/K3s execution;
- `compose` — local Docker Compose execution.

The environment driver implements backend mechanics such as create/reset,
readiness, endpoint discovery, node restart, rolling restart, logs, artifacts,
and cleanup.

See [Environment driver abstraction](environment-drivers.md) for the detailed
driver design.

## Catalog and versioning

The catalog stores imported definitions and resolved snapshots. Definitions are
versioned when imported so a run can point back to the exact scenario/profile
content it used.

Catalog-backed workflows should preserve these rules:

- authored YAML is the source of truth for definitions;
- resolved run snapshots are immutable evidence;
- used definition versions should not be mutated in place;
- suites should resolve to scenario versions before execution.

## Resource provisioning model

Some actors require MycelDB resources before they can run, such as spaces,
domains, users, credentials, or access tokens. Provisioning is derived from the
resolved scenario, actor groups, and discovered environment endpoints.

Provisioning outputs are recorded in:

```text
environment/scenario-resources.json
actors/assignments.json
```

Resource cleanup is separate from environment cleanup. A scenario may clean up
MycelDB resources but retain the external environment for debugging, or a failed
environment create may skip MycelDB resource cleanup because no resources were
provisioned.

## How to choose the right concept

- Use a **Scenario** when adding one new test case.
- Use a **Suite** when composing existing scenarios into a gate or baseline.
- Use a **ClusterProfile** when multiple scenarios share topology/runtime config.
- Use an **ActorProfile** when multiple scenarios share workload behavior.
- Use an **ActorGroup** when one scenario needs a specific count/rate/target for
  a profile.
- Use a **Phase** when behavior changes over time.
- Use an **Event** when the environment should be disrupted or changed during a
  phase.
- Use an **Assertion** when the runner/oracle must decide pass/fail from
  expected outcomes.
- Use **Artifacts** when evidence must survive beyond the process.
