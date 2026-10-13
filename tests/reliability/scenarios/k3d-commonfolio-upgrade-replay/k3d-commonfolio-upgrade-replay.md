# k3d CommonFolio upgrade replay

This scenario is an end-to-end guardrail for the CommonFolio staging failure shape described in MycelDB/mycel-lab#39.

It starts a three-node k3d MycelDB cluster on `myceldb/mycel:v0.19.2`, provisions per-writer users/spaces/domains, installs CommonFolio-like page-summary automation procedures and bindings, runs a CommonFolio-like graph workload, restarts a pod, and then upgrades `statefulset/myceld` to `myceldb/mycel:v0.19.3`.

## Phases

1. `configure-commonfolio-automation` — install a page-summary-style procedure and per-writer graph automation bindings.
2. `seed-commonfolio-graph-and-invocations` — write CommonFolio-like page/page-entry graph data to trigger automation invocation state.
3. `restart-with-commonfolio-state` — restart one pod with automation and graph state present.
4. `commonfolio-style-db-image-upgrade` — perform a StatefulSet image rollout and wait for readiness.
5. `post-upgrade-commonfolio-validation` — continue graph reads/writes and require convergence.
6. `collect-replay-diagnostics` — collect automation procedure/binding/invocation diagnostics and fail if pod logs contain known replay blockers.

## Determinism

This is intentionally an operational guardrail rather than a deterministic replacement for daemon unit tests. It provisions realistic automation and graph state, then exercises restart and upgrade behavior. It does not force the exact stale/superseded replay tail that appeared in staging; deterministic replay semantics are covered in MycelDB/mycel#188.

## Failure signals

The scenario fails on pod readiness/rollout failures, unhealthy or divergent cluster identity, acknowledged graph transaction loss, count divergence, or pod logs containing known replay blockers:

- `stale automation invocation update`
- `handler=graph: graph conflict`
- `restore raft snapshot and replay committed entries`

Artifacts include scenario resources, actor events/read checks, Kubernetes state/logs, and CommonFolio automation event payloads in `events.jsonl`.
