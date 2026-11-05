# Semantic maintenance health on immutable v0.21.4

Tracking: [mycel-lab#46](https://github.com/MycelDB/mycel-lab/issues/46).
See the [acceptance audit](../implementation/semantic-graph-index-issue46-acceptance-audit.md)
and [validation checkpoint](../implementation/semantic-graph-index-reliability-suites-validation.md).

## Decision and evidence boundary

Keep the released daemon, API and SDK unchanged. Correct the Lab consumer rather
than reinterpret the node's last background error as current binding corruption,
clear global errors on one scope's manual success, or ignore `degraded`.

The failed clean `574e690` execution is still failed. Source inspection and
isolated daemon overlays reproduced a genuine no-leader background error that
survives restored leadership, successful manual maintenance and empty queues,
then clears only on a successful subsequent run of that same background loop.
That confirms a mechanism, **not the original exception** or post-restart search
health. The original exception was not retained and cannot be reconstructed.

## Two independent signals

| Public signal | Meaning in v0.21.4 | Lab requirement |
| --- | --- | --- |
| `GetSemanticMaintenanceStatus.degraded`, reason, run counters, worker error/success timestamps | Entry node's module-wide background-loop history, potentially from another scope; manual success does not clear it | Must actually become nondegraded before passing; counters/timestamps are diagnostics only |
| Space maintenance queue depths | Requested space's queues, potentially forwarded to its owner | Nonnegative, no permanent work; all queues empty for normal completion |
| `GetSemanticReliabilityStatus` | Requested space/rule/binding's current public reliability state | Exact scope match, positive stable generation, complete healthy capacity limits, explicit idle/completed rebuild, consistent gap/flag/frontiers; current degradation is rejected |

The maintenance waiter now reads the scoped certificate on every status
observation. Completion additionally requires zero gaps, false rebuild flag and
freshness through the latest observed revision. A generation change or frontier
regression during the wait fails. Queue zeros, healthy cluster polls, timestamps
and successful manual calls are never substitutes for this certificate. Direct
search, revision/provenance and retained-state assertions still run separately.

The API does not expose serving-node/owner/term IDs in these responses. Evidence
labels the background signal's scope explicitly; it does not invent an owner or
term from a timestamp. Entry endpoints and independent cluster ownership
observations remain in the surrounding environment/event/cluster artifacts.

## Narrow finite convergence, not error suppression

Only exact released no-leader or ownership-change messages may wait, with an
optional exact gRPC `Unavailable` wrapper. Matching is anchored, at most 4,096
bytes / 16 joined reasons; every reported joined reason must match. Arbitrary
`Unavailable`, provider, storage, authorization, configuration, missing-group,
unknown or malformed reasons fail immediately. A degraded disabled background
loop cannot converge by itself and is rejected. The API reports only the first
nonempty loop error; recognizing it does not certify other hidden loop errors.

The original operation/parent deadline and 1,024-observation limit remain. A
persistent recognized error still fails. Both node degradation and current scoped
health must be acceptable **in the final observation**. Once the space queues and
binding are certified current, subsequent health-refresh polls invoke only public
status RPCs, not repeated Analyze/Process. New queue/frontier lag re-enables normal
maintenance. Failed or ambiguous mutations are never resubmitted.

Pending-work preservation uses the same classification, retains a bounded node
health history, pins its validated work certificate while waiting, and does not
capture a checkpoint until node health clears. These polls do not analyze or
reconstruct the work. Disabled-background fixtures and work identity, attempts,
generation, graph revision, false-freshness and no-replay fences remain strict.
No backfill, rebuild, cursor reset or repair is added.

## Safe diagnostics and validation

Version-1 evidence separates node-background health from the scoped reliability
certificate. It retains observed/absent state, RPC category, fixed cause class,
run counts, canonical parsed worker timestamps, throttle state and metadata
validity. Invalid/unrecognized text becomes a fixed marker. Raw degradation,
provider/error bodies, foreign IDs and malformed timestamp text are never copied.
Generic maintenance observations preserve both signals before rejection; work
preservation retains its bounded health history. An absent response is not a
healthy zero snapshot.

Authenticated loopback fake-RPC tests cover finite recognized convergence,
read-only polling, persistent/unknown failures, empty queues with stale scope,
unsafe capacity/rebuild states, scope mismatch, generation/frontier regression,
no ambiguous mutation retry, RPC deadlines, private-state redaction and delayed
checkpoint capture. They are **harness evidence only**, not daemon qualification.

Fresh runtime acceptance still requires the complete 16-scenario / 65-phase
aggregate on a newly frozen exact identity, with fresh explicit destructive
approval. Neither consumed approval transfers and no run is started by this fix.
