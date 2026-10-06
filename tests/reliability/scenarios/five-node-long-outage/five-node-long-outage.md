# five-node-long-outage

## Purpose

Validates that a five-node raft-backed graph workload survives deterministic node outage events. It increases quorum and placement coverage beyond the default three-node topology and is useful for catching assumptions about node count, peer lists, or restart ordering.

## What this test proves

- Five-node cluster profiles render and boot correctly.
- Graph writes survive planned pod restarts in a larger raft topology.
- The final cluster remains healthy and converged after the outage window.

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

1. **warmup**. Duration: `1s`. This phase runs the configured actors at the phase rate and records progress evidence.
2. **long-outage**. Duration: `1s`. Event(s): `pod-stop, pod-restart`. This phase executes `pod-stop, pod-restart` while preserving workload/oracle evidence.
3. **recovery**. Duration: `1s`. Event(s): `rolling-restart`. This phase executes `rolling-restart` while preserving workload/oracle evidence.

## Actors

| Actor group | Profile | Count | Rate knobs | Role in this scenario |
|---|---|---:|---|---|
| `graph-writers` | `graph-committer` | `2` | `commitsPerSecond` = `20` | writes graph transactions |

## Tunable parameters

| Parameter | Current value/source | Why tune it |
|---|---|---|
| `seed` | `7373` | Change only when intentionally exploring a different deterministic schedule. |
| `environment.driver` | `k3d` | Selects the infrastructure backend; changing it changes failure semantics. |
| `environment.namespace` | `mycel-lab` | Use a unique namespace/project when running concurrent destructive tests. |
| `clusterRef` | `raft-5-node` | Changes node count, raft placement, image, resources, and storage defaults. |
| `actorGroups[].count` | YAML actor group values | Increase for more concurrency pressure; decrease for faster local debugging. |
| `actorGroups[].rate` | YAML actor group rates | Increase to stress write/read paths; decrease to isolate environment failures. |
| `phases[].duration` | YAML phase durations | Lengthen to catch timing-sensitive bugs; shorten for smoke/debug loops. |
| `phases[].events[].target` | `pod-restart, pod-stop, rolling-restart` targets | Adjust the disrupted node, restart cadence, backup path, snapshot options, or verification timeout. |

## Evidence and artifacts

- `result.json` is the first stop: it records phase status, duration, and terminal pass/fail state.
- `events.jsonl` shows disruption/backup/snapshot events and their structured payloads.
- `resolved-scenario.json` confirms the exact cluster profile, actor profiles, and overrides used for the run.
- `summary.md` gives a short human-readable run report suitable for PR comments.
- `manifests/myceld.yaml`, `environment/kubernetes-resources.txt`, `environment/pods-describe.txt`, and pod logs explain k3d/Kubernetes failures.
- `oracle/` artifacts, when present, contain final consistency and cluster-health evidence.

## Common failure modes

- Missing k3d/kubectl prerequisites, stale clusters, or namespace cleanup problems prevent environment creation.
- Pod readiness, PVC, or service rendering errors appear in `environment/kubernetes-resources.txt` and `environment/pods-describe.txt`.
- Actor authentication or provisioning failures usually point to bootstrap credentials, principal setup, or endpoint routing.
- Graph correctness failures should be debugged from `actors/`, `events.jsonl`, and `oracle/` before rerunning.
- If a destructive run fails during cleanup, confirm disposable resources were removed before starting another run.

## When to run

Run after raft topology, k3d rendering, cluster membership, or multi-node routing changes.

## Related files

- YAML: [`five-node-long-outage.yaml`](./five-node-long-outage.yaml)
- Suites referencing this scenario live under `tests/reliability/suites/`.
