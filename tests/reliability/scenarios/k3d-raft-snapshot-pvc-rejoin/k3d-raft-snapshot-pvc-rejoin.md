# k3d-raft-snapshot-pvc-rejoin

## Purpose

Native k3d forced raft snapshot catch-up after same-raft-ID PVC replacement.

This document explains the scenario intent, runtime topology, tunable parameters, and evidence to inspect when the test passes or fails.

## Topology

```mermaid
flowchart LR
  actor0["graph-writers actors"] --> entry["Mycel endpoint"]
  actor1["readers actors"] --> entry["Mycel endpoint"]
  entry --> svc["myceld-client Service"]
  svc --> pod0["myceld-0 Pod"]
  svc --> pod1["myceld-1 Pod"]
  svc --> pod2["myceld-2 Pod"]
  pod0 <--> pod1
  pod1 <--> pod2
  pod0 <--> pod2
```

## Scenario phases

1. **bootstrap-and-write**. Duration: `5s`.
2. **replace-last-pvc**. Duration: `1s`. Events: raft-pvc-replace-node.
3. **post-rejoin-validation**. Duration: `5s`.
4. **verify-rejoined-snapshot-indexes**. Duration: `1s`. Events: raft-snapshot-verify-rejoined.

## Actors

| Actor group | Profile | Count | Rate knobs |
|---|---|---:|---|
| `graph-writers` | `graph-committer` | `1` | commitsPerSecond=8 |
| `readers` | `gql-reader` | `1` | queriesPerSecond=4 |

## Tunable parameters

| Parameter | Default/source | Effect |
|---|---|---|
| `seed` | `30303` | Reproducible scheduling and actor randomness. |
| `environment.driver` | `k3d` | Selects the environment driver used by the scenario. |
| `environment.namespace` | `mycel-raft-snapshot-pvc-rejoin` | Kubernetes namespace or logical environment scope. |
| `clusterRef` | `raft-3-node` | Cluster profile used as the base topology. |
| `actorGroups[].count` | YAML actor group values | Number of concurrent actors per group. |
| `actorGroups[].rate` | YAML actor group rates | Workload throughput for commit/query actors. |
| `phases[].duration` | YAML phase durations | How long each workload/disruption phase runs. |
| `phases[].events[].target` | event-specific | Tweaks event behavior for `raft-pvc-replace-node, raft-snapshot-verify-rejoined`. |
| `clusterOverrides` | YAML overrides | Scenario-specific cluster settings layered on the referenced profile. |

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

- YAML: [`k3d-raft-snapshot-pvc-rejoin.yaml`](./k3d-raft-snapshot-pvc-rejoin.yaml)
- Suites referencing this scenario live under `tests/reliability/suites/`.
