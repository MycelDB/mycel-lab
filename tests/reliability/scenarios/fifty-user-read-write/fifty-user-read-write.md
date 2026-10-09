# fifty-user-read-write

## Purpose

Applies concurrent user-like graph pressure to a three-node k3d cluster. The goal is to catch scheduler, provisioning, authentication, endpoint-routing, and graph consistency issues that only appear when many independent actors write and read at once.

## What this test proves

- Mycel Lab can provision and schedule many actor identities in one run.
- The daemon handles concurrent graph commits and reads through the cluster endpoint.
- Final artifacts preserve enough actor evidence for aggregate debugging.

## Topology

```mermaid
flowchart LR
  actor0["active-users actors"] --> entry["Mycel endpoint"]
  entry --> svc["myceld-client Service"]
  svc --> pod0["myceld-0 Pod"]
  svc --> pod1["myceld-1 Pod"]
  svc --> pod2["myceld-2 Pod"]
  pod0 <--> pod1
  pod1 <--> pod2
  pod0 <--> pod2
```

## Scenario phases

1. **steady-user-traffic**. Duration: `2m`. This phase runs the configured actors at the phase rate and records progress evidence.

## Actors

| Actor group | Profile | Count | Rate knobs | Role in this scenario |
|---|---|---:|---|---|
| `active-users` | `graph-committer` | `50` | `commitsPerSecond` = `25`, `queriesPerSecond` = `5` | writes graph transactions |

## Tunable parameters

| Parameter | Current value/source | Why tune it |
|---|---|---|
| `seed` | `5050` | Change only when intentionally exploring a different deterministic schedule. |
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

Run after changes to actor provisioning, graph write/read paths, authentication, rate limiting, or scheduler concurrency.

## Related files

- YAML: [`fifty-user-read-write.yaml`](./fifty-user-read-write.yaml)
- Suites referencing this scenario live under `tests/reliability/suites/`.
