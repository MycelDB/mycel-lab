# five-node-long-outage

## Purpose

Five-node graph smoke with deterministic pod restart events.

This document explains the scenario intent, runtime topology, tunable parameters, and evidence to inspect when the test passes or fails.

## Topology

```mermaid
flowchart LR
  actor0["graph-writers actors"] --> entry["Mycel endpoint"]
  entry --> svc["myceld-client Service"]
  svc --> pod0["myceld-0 Pod"]
  svc --> pod1["myceld-1 Pod"]
  svc --> pod2["myceld-2 Pod"]
  svc --> pod3["myceld-3 Pod"]
  svc --> pod4["myceld-4 Pod"]
  pod0 <--> pod1
  pod1 <--> pod2
  pod2 <--> pod3
  pod3 <--> pod4
  pod0 <--> pod4
```

## Scenario phases

1. **warmup**. Duration: `1s`.
2. **long-outage**. Duration: `1s`. Events: pod-stop, pod-restart.
3. **recovery**. Duration: `1s`. Events: rolling-restart.

## Actors

| Actor group | Profile | Count | Rate knobs |
|---|---|---:|---|
| `graph-writers` | `graph-committer` | `2` | commitsPerSecond=20 |

## Tunable parameters

| Parameter | Default/source | Effect |
|---|---|---|
| `seed` | `7373` | Reproducible scheduling and actor randomness. |
| `environment.driver` | `k3d` | Selects the environment driver used by the scenario. |
| `environment.namespace` | `mycel-lab` | Kubernetes namespace or logical environment scope. |
| `clusterRef` | `raft-5-node` | Cluster profile used as the base topology. |
| `actorGroups[].count` | YAML actor group values | Number of concurrent actors per group. |
| `actorGroups[].rate` | YAML actor group rates | Workload throughput for commit/query actors. |
| `phases[].duration` | YAML phase durations | How long each workload/disruption phase runs. |
| `phases[].events[].target` | event-specific | Tweaks event behavior for `pod-restart, pod-stop, rolling-restart`. |

## Evidence and artifacts

- `result.json` records phase status and terminal pass/fail state.
- `events.jsonl` records phase events, disruption operations, and correctness failures.
- `summary.md` gives a human-readable run summary.
- `resolved-scenario.json` captures the fully resolved scenario, including profile overrides.
- Environment-specific captures under `environment/` show Kubernetes/Compose state, logs, and endpoints when enabled.

## Common failure modes

- Environment setup failures usually indicate missing local prerequisites or stale Kubernetes/Compose resources.
- Actor failures usually indicate data-plane, authentication, or endpoint-routing issues.
- Final assertion failures should be debugged with `events.jsonl`, `oracle/`, and environment captures before rerunning.
- For destructive scenarios, confirm the namespace/project is disposable before using `--confirm-destructive`.

## Related files

- YAML: [`five-node-long-outage.yaml`](./five-node-long-outage.yaml)
- Suites referencing this scenario live under `tests/reliability/suites/`.
