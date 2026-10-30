# k3d-graph-commit-restart-qualification

## Purpose

Qualifies graph commit reconciliation before a release-candidate number is assigned. It validates identity formation, graph data-plane behavior, and same-transaction commit recovery across a rolling restart on a fresh k3d cluster.

## What this test proves

- k3d manifests create a healthy three-node Mycel cluster.
- Graph writers/readers operate correctly before and after rolling restarts.
- Cluster identity and final oracle checks agree across all nodes.
- The run uses a complete named digest-pinned daemon image with clean daemon and harness provenance.
- No ambiguous graph commit remains unresolved after bounded same-identity commit retries.

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

1. **bootstrap-and-write**. Duration: `2s`. This phase must acknowledge at least one graph transaction and complete at least one successful read before disruption.
2. **rolling-restart**. Duration: `1s`. Event(s): `rolling-restart`. This phase executes `rolling-restart` with expected availability degradation while preserving workload/oracle evidence.
3. **post-restart-validation**. Duration: `2s`. This phase must again acknowledge a graph transaction, complete a successful read, and converge before final verification.

## Actors

| Actor group | Profile | Count | Rate knobs | Role in this scenario |
|---|---|---:|---|---|
| `graph-writers` | `graph-committer` | `3` | `commitsPerSecond` = `12` | writes graph transactions |
| `readers` | `gql-reader` | `3` | `queriesPerSecond` = `6` | reads and validates graph observations |

## Tunable parameters

| Parameter | Current value/source | Why tune it |
|---|---|---|
| `seed` | `18181` | Change only when intentionally exploring a different deterministic schedule. |
| `environment.driver` | `k3d` | Selects the infrastructure backend; changing it changes failure semantics. |
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
- `manifests/myceld.yaml`, `environment/kubernetes-resources.txt`, `environment/pods-describe.txt`, and bounded Kubernetes events explain k3d/Kubernetes failures; closed qualification evidence excludes pod logs.
- `oracle/` artifacts, when present, contain final consistency and cluster-health evidence.

## Common failure modes

- Missing k3d/kubectl prerequisites, stale clusters, or namespace cleanup problems prevent environment creation.
- Pod readiness, PVC, or service rendering errors appear in `environment/kubernetes-resources.txt` and `environment/pods-describe.txt`.
- Actor authentication or provisioning failures usually point to bootstrap credentials, principal setup, or endpoint routing.
- Graph correctness failures should be debugged from `actors/`, `events.jsonl`, and `oracle/` before rerunning.
- If a destructive run fails during cleanup, confirm disposable resources were removed before starting another run.

## When to run

Run three consecutive approved destructive members, each with a fresh cluster and separate artifact root, before candidate construction. All three must use the exact same reviewed Lab, workflow, harness, and named daemon digest identities. Any relevant identity or policy change resets the sequence; qualification transfers no gate result or approval.

## Related files

- YAML: [`k3d-graph-commit-restart-qualification.yaml`](./k3d-graph-commit-restart-qualification.yaml)
- Suites referencing this scenario live under `tests/reliability/suites/`.
