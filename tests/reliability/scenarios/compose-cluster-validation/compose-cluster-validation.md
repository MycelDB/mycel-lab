# compose-cluster-validation

## Purpose

Validates the primary Compose cluster contract: all nodes share one cluster identity, the graph data plane works through multiple endpoints, and a rolling restart does not lose committed graph data. This is the fast Compose analogue of the k3d cluster validation scenario.

## What this test proves

- Compose nodes form one healthy Mycel cluster.
- Graph writers and readers can use the cluster before and after a rolling restart.
- Final oracle checks can reconcile actor-observed writes against live cluster state.

## Topology

```mermaid
flowchart LR
  actor0["graph-writers actors"] --> entry["Mycel endpoint"]
  actor1["readers actors"] --> entry["Mycel endpoint"]
  entry --> svcA["myceld-a Compose service"]
  entry --> svcB["myceld-b Compose service"]
  entry --> svcC["myceld-c Compose service"]
  svcA <--> svcB
  svcB <--> svcC
  svcA <--> svcC
```

## Scenario phases

1. **bootstrap-and-write**. Duration: `2s`. This phase establishes baseline actor data before disruption or verification.
2. **rolling-restart**. Duration: `1s`. Event(s): `rolling-restart`. This phase executes `rolling-restart` while preserving workload/oracle evidence.
3. **post-restart-validation**. Duration: `2s`. This phase creates the planned availability disturbance and records recovery evidence.

## Actors

| Actor group | Profile | Count | Rate knobs | Role in this scenario |
|---|---|---:|---|---|
| `graph-writers` | `graph-committer` | `3` | `commitsPerSecond` = `12` | writes graph transactions |
| `readers` | `gql-reader` | `3` | `queriesPerSecond` = `6` | reads and validates graph observations |

## Tunable parameters

| Parameter | Current value/source | Why tune it |
|---|---|---|
| `seed` | `17171` | Change only when intentionally exploring a different deterministic schedule. |
| `environment.driver` | `compose` | Selects the infrastructure backend; changing it changes failure semantics. |
| `environment.namespace` | `mycel-lab` | Use a unique namespace/project when running concurrent destructive tests. |
| `clusterRef` | `raft-3-node` | Changes node count, raft placement, image, resources, and storage defaults. |
| `actorGroups[].count` | YAML actor group values | Increase for more concurrency pressure; decrease for faster local debugging. |
| `actorGroups[].rate` | YAML actor group rates | Increase to stress write/read paths; decrease to isolate environment failures. |
| `phases[].duration` | YAML phase durations | Lengthen to catch timing-sensitive bugs; shorten for smoke/debug loops. |
| `phases[].events[].target` | `rolling-restart` targets | Adjust the disrupted node, restart cadence, backup path, snapshot options, or verification timeout. |

## Evidence and artifacts

- `result.json` is the first stop: it records phase status, duration, and terminal pass/fail state.
- `events.jsonl` shows disruption/backup/snapshot events and their structured payloads.
- `resolved-scenario.json` confirms the exact cluster profile, actor profiles, and overrides used for the run.
- `summary.md` gives a short human-readable run report suitable for PR comments.
- `environment/compose-config.yaml`, `environment/compose-ps.txt`, and service logs explain Compose-side failures.
- `oracle/` artifacts, when present, contain final consistency and cluster-health evidence.

## Common failure modes

- Docker Compose unavailable, stale projects, or port collisions prevent environment creation.
- Service logs or `environment/compose-config.yaml` disagree with the expected service/port mapping.
- Actor authentication or provisioning failures usually point to bootstrap credentials, principal setup, or endpoint routing.
- Graph correctness failures should be debugged from `actors/`, `events.jsonl`, and `oracle/` before rerunning.
- If a destructive run fails during cleanup, confirm disposable resources were removed before starting another run.

## When to run

Run when changing Compose manifests, cluster identity, endpoint routing, graph actors, or release-gate composition.

## Related files

- YAML: [`compose-cluster-validation.yaml`](./compose-cluster-validation.yaml)
- Suites referencing this scenario live under `tests/reliability/suites/`.
