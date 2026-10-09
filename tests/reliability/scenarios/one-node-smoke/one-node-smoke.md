# one-node-smoke

## Purpose

Validates one-node deployment plumbing with no-op actors. It is a low-risk k3d smoke test for manifest rendering, pod readiness, environment capture, phase execution, and cleanup without graph or raft workload complexity.

## What this test proves

- The smallest deployment can be created and observed by the k3d driver.
- No-op actors and phases complete successfully against a live environment.
- Artifact capture works for simple runs.

## Topology

```mermaid
flowchart LR
  actor0["noop-users actors"] --> entry["Mycel endpoint"]
  entry --> svc["myceld-client Service"]
  svc --> pod0["myceld-0 Pod"]
  svc --> pod1["myceld-1 Pod"]
  svc --> pod2["myceld-2 Pod"]
  pod0 <--> pod1
  pod1 <--> pod2
  pod0 <--> pod2
```

## Scenario phases

1. **smoke**. Duration: `1s`. This phase keeps the workload intentionally small while proving the path is functional.

## Actors

| Actor group | Profile | Count | Rate knobs | Role in this scenario |
|---|---|---:|---|---|
| `noop-users` | `noop` | `1` | `commitsPerSecond` = `1` | exercises scheduler/runtime behavior |

## Tunable parameters

| Parameter | Current value/source | Why tune it |
|---|---|---|
| `seed` | `13579` | Change only when intentionally exploring a different deterministic schedule. |
| `environment.driver` | `k3d` | Selects the infrastructure backend; changing it changes failure semantics. |
| `environment.namespace` | `mycel-lab` | Use a unique namespace/project when running concurrent destructive tests. |
| `clusterRef` | `raft-3-node` | Changes node count, raft placement, image, resources, and storage defaults. |
| `actorGroups[].count` | YAML actor group values | Increase for more concurrency pressure; decrease for faster local debugging. |
| `actorGroups[].rate` | YAML actor group rates | Increase to stress write/read paths; decrease to isolate environment failures. |
| `phases[].duration` | YAML phase durations | Lengthen to catch timing-sensitive bugs; shorten for smoke/debug loops. |
| `clusterOverrides` | YAML overrides | Scenario-specific cluster settings layered on the referenced profile. |

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

Run first after local environment, manifest rendering, or k3d driver changes.

## Related files

- YAML: [`one-node-smoke.yaml`](./one-node-smoke.yaml)
- Suites referencing this scenario live under `tests/reliability/suites/`.
