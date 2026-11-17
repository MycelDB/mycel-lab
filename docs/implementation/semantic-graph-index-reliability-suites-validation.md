# Semantic suite validation checkpoint

Tracking issue: [MycelDB/mycel-lab#46](https://github.com/MycelDB/mycel-lab/issues/46)

Implementation: [Semantic graph-index reliability suites plan](semantic-graph-index-reliability-suites-plan.md)

## Scope

This checkpoint distinguishes historical local candidate execution from current
harness validation and published-image qualification. Passing a dry-run is
planning evidence only. Neither this record nor the suite name authorizes
publication, issue closure, live-provider spend, or another destructive run.

## Current continuation target: v0.21.4

New work targets the released daemon index:

```text
docker.io/myceldb/mycel@sha256:0973cddd9c282abff40d8edeb2e8f7a801e58d40bfbab4c8507ce2d9daae2dd7
```

Tagged daemon source is `034ae41f78721b112a5b8284593b2652bab12c53`, with
API pin `1182464e29c2fb251a550df0a7e667a34195d1b9`. The canonical scenario
manifest/Compose defaults and semantic CI planning now select this exact image.

The new `compose-semantic-pending-restart` case captures versioned public work
identity/generation/revisions with pending queue and unresolved-gap evidence,
restarts retained data, and requires read-only preservation before normal public
maintenance resumes. It deliberately disables background scheduling and does not
claim an in-flight provider interruption or automatic scheduler recovery. A second
restart requires unchanged completed-work attempts and semantic record identity.
No post-boundary repair backfill, forced rebuild, reset, or reconstruction is
allowed. Unknown capabilities, truncated/ambiguous evidence, falsely fresh work,
changed identity/generation, and replayed attempts fail closed.

The completion batch expands the aggregate from the landed four cases /
25 phases to **16 scenarios / 67 phases**: true controlled out-of-order publication,
nine finite provider-failure cases, independent same-space domain isolation, and
configured model/dimension/vector-space migration. It also adds provider-stage planning and
pre-deployment capability rejection for unsupported native composition. See the
[acceptance audit](semantic-graph-index-issue46-acceptance-audit.md) for precise
coverage, evidence levels, and remaining gaps.

Three separately approved v0.21.4 environment-backed aggregates ran against clean
Lab `6615c17`, `574e690` and `cbc4274`. All **failed and stopped**, not qualifying the aggregate.
The previously prepared `3b1c222` four-case bundle remains unexecuted and does not
transfer evidence or approval. Unit/fake-RPC and planning checks validate the
harness only. Native semantic enrichment remains API/adapter-blocked; no hosted
provider run is part of this batch.

### Approved clean 6615c17 execution: failed, no rerun

Retained evidence:
`artifacts/destructive/semantic-graph-index-v0214-clean-6615c17-20261105T025625Z/`.

The one approved invocation passed the contract (11 phases) and recovery (five
phases). Pending-restart passed its baseline but failed pending-work capture
before either restart. Three later phases were aborted; thirteen scenarios were
not run. Totals: **17 phases passed, one failed, 47 not executed**. The aggregate
is not accepted. All attempted final/cluster oracles, cleanup/absence checks,
nine native clean-daemon provenance observations, and artifact safety (three
runs / 112 files) passed; none overrides the failed semantic event.

Exact-source inspection and an isolated synthetic diagnostic identified a Lab
contract mismatch: v0.21.4 sets `rebuild_required` for nonzero unresolved gaps,
including ordinary pending work with idle/completed rebuild state. The old work
and fault readers rejected that boolean while also requiring positive gaps.
The old rejection event copied reliability fields only after the guard, so its
zeros were unpopulated fields, not evidence of missing daemon capabilities. No
new daemon defect was established; the full rejected response was not retained.

### Pending-state contract correction: harness evidence only

The correction shares a strict status validator across work/fault readers. With
an explicit idle/completed rebuild status and capacity `ok`, the public boolean
must equal `(unresolvedGapCount > 0)`. Required/running/failed/unknown rebuilds,
missing evidence, capacity exhaustion, inconsistent flags/frontiers/gap ranges,
false freshness, scope drift, generation/revision changes, and replay still fail.
Completion still requires zero gaps, false flag, and the expected fresh frontier.
Expected provider failures may be degraded, but initial pending capture and final
completion may not. No repair/rebuild is introduced.

Safe version-1 reliability evidence is captured before rejection, including
observed/absent distinction, RPC category, scope-match boolean, generation,
frontiers, gap/flag/rebuild/capacity states and limits, and fixed rejection reason.
Unknown state strings are replaced with a marker; provider/error bodies and
foreign IDs are excluded. Fault discovery, rejection and timeout evidence is
retained. Canonical work checkpoint identity and schema remain unchanged.

Regressions cover the released pending combination through completion, both
readers' unsafe-state negatives, false flags/freshness, bounded private-state
redaction, authenticated fake-RPC failure/retry recovery of the same work, and
rejection diagnostics. These tests are **not a runtime rerun or qualification**.
A changed clean candidate must be reviewed/landed, frozen anew, and explicitly
approved before another destructive execution. The one-run 6615c17 approval is
consumed; its source, manifest and failed evidence remain unchanged.

### Retained merged-head CI failure: connection-drop evidence ordering

PR #94 CI passed. Its merged head `a1caa8e` then failed the unchanged emulator
`TestConnectionDropIsRealTransportFailure` at `gate_test.go:155` with
`transport evidence missing` (run `41962591407`, attempt 1). Planning passed;
quality stopped at tests, so later checks were not claimed as run. That failure
is retained, not relabelled as a pass or hidden by an unchanged-source CI retry.

The drop helper closed its hijacked socket before the handler recorded evidence,
allowing the client to observe EOF first. The follow-up records exactly once
before close and returns immediately from both embedding/generation handlers.
Tests still demand real transport failure and immediate finite evidence without
sleep/retry; a deterministic close witness checks ordering and unsupported
transports remain rejected. No provider-script/scenario assertions are weakened.
The blocked `a1caa8e` preparation did not create an approval manifest or execute a
suite. A later changed clean candidate needs its own passing CI and frozen identity.

### Approved clean 574e690 execution: failed, no rerun

Retained evidence:
`artifacts/destructive/semantic-graph-index-v0214-clean-574e690-20261105T034945Z/`.

The approved one-shot invocation on 2026-11-05 passed contract's 11 phases and
four recovery phases, then failed `semantic-maintenance-await` in
`restart-after-rebuild-and-prove-persistence`: `degraded=true permanent=0`.
Totals: **one scenario passed, one failed, fourteen not run; 15 phases passed,
one failed, 49 unexecuted**. Pending-restart, ordering, protocol faults, domain
isolation and migration were not reached. The reader/transport corrections remain
runtime-unqualified. Final/cluster oracles, cleanup/absence, six native daemon
observations and artifact safety (two runs / 76 files) passed, not acceptance.

Read-only diagnosis against the exact released source reproduced cached node-wide
background errors surviving restored leadership and successful manual empty
maintenance. The entry node's historical health can accompany current owner
queues from another node/scope. The exact original error was not retained, so
this is a confirmed mechanism, not proof that the actual failure was transient
or that post-restart semantic search succeeded. The failed evidence is unchanged.

### Maintenance-health correction: harness evidence only

The [explicit v0.21.4 contract](../design/semantic-maintenance-health.md) keeps the
daemon/API/SDK immutable. Lab records node-background health independently from
current scoped reliability. Only exact bounded ownership/startup reasons can
wait; actual node degradation must clear before passing. Completion additionally
requires empty queues and a complete current scope/generation/capacity/rebuild/
freshness certificate. Unknown, persistent, disabled-loop, permanent-work and
unsafe scoped conditions fail; ambiguous mutations are not retried. Health-only
waits and pending preservation are read-only, with bounded safe metadata.

Authenticated fake-RPC and unit regressions are harness evidence, not runtime
acceptance. The first focused race stress exposed lost standard deadline identity
when expiry occurred inside an RPC; the corrected path preserves parent/operation
context errors. That original failed test log is retained separately from later
changed-source validation. Both runtime approvals are consumed. No new destructive
execution or release publication is performed by this correction.

### Approved clean cbc4274 execution: failed, no rerun

Retained evidence:
`artifacts/destructive/semantic-graph-index-v0214-clean-cbc4274-20261105T141941Z/`.

The approved one-shot invocation on 2026-11-05 failed the contract case at
`semantic-maintenance-await` in `disabled-rule-negative-control` after nine passed
phases. Totals: **zero scenarios passed, one failed, fifteen not run; nine phases
passed, one failed, 55 unexecuted** (one aborted phase plus 54 unstarted scenario
phases). The 30-second deadline retained 160 healthy, empty-queue observations:
latest revision 8, analyzed/fresh 7/7, generation 1, no gaps, completed rebuild.
The rule was explicitly disabled before revision 8 committed. The Lab completion
predicate incorrectly demanded latest freshness while indexing was disabled.

All 166 maintenance observations in the attempted run were nondegraded: the prior
restart/transient-health path was not exercised. Disabled search rejection,
re-enable catch-up and all later scenarios were not reached. Final/cluster
oracles, cleanup/absence, three native daemon observations and artifact safety
(one run / 40 files) passed, not aggregate acceptance. Failed evidence is unchanged.

### Disabled-quiescence correction: harness evidence only

An authenticated fake-RPC regression reproduced the retained 8/7/7 timeout locally
before the fix. The intentional negative-control wait now explicitly selects
`disabled-quiescent` completion, preserving captured pre-disable generation/fresh
frontiers and the exact later acknowledged commit. It is read-only and requires
public disabled-rule reads before/after the scoped status. Healthy capacity,
rebuild state, zero gaps, empty queues and clear background health remain required;
unknown state, drift, re-enable and permission/RPC failures fail closed. Evidence
states `freshnessProved: false`, never false freshness at revision 8.

Default indexing still rejects the 8/7/7 lag. Disabled direct-search rejection and
normal re-enable freshness/provenance assertions remain unchanged. Regressions
cover these boundaries, strict targets, safe rejections and deadlines. These are
local harness tests, not a runtime pass. No daemon/API/SDK change, implicit repair
or new destructive execution is part of this correction. All three approvals are
consumed; #46 remains open and any future run needs a new exact bundle/approval.

### Completion batch: earlier local harness validation only

The uncommitted `issue/46-completion-protocol-composition` implementation passed
all of the following with `GOWORK=off` and read-only module resolution:

- Full Go tests and full race-enabled Go tests, both with `-count=1`.
- Twenty race-enabled repetitions of ordering/failure, maintenance-deadline,
  real HTTP gate/cancellation/connection-drop, composition, and finite-script
  regression tests.
- `go vet ./...` and trimpath/VCS-aware builds of both Lab and the provider emulator.
- Twenty race-enabled repetitions of the new domain isolation, configured model
  mutation/public-RPC, complete bounded model listing, generation-fence, provenance,
  scoped identity, resource clone/cleanup, catalog, and target-validation regressions.
- `python3 scripts/generateSemanticFaultMatrix.py --check` (31 fixture files) and
  `python3 scripts/generateSemanticCompatibilityCases.py --check` (four fixture files).
- Documentation checks, workflow policy, all 36 workflow-script tests, and
  `git diff --check`.

The real CLI also rejected an unsupported generation-plus-embedding composition
with the expected missing-adapter error **before creating any run artifacts**.
Catalog tests additionally prove this rejection precedes live credential lookup,
and provisioning rejects a forged resolved plan that bypasses catalog checks.

Earlier dirty-development planning-only evidence:
`artifacts/dry-runs/issue46-domain-model-final-20261105T015350Z`.
The credential-free aggregate resolves 16 scenarios / 67 phases; manual-live
planning resolves one / four. All 17 terminal results declare `dryRun: true`,
all manifest/Compose daemon selections match immutable v0.21.4, and provider
request counts are zero. Artifact safety passed for 17 runs / 361 files.
The executable is a dirty development harness, not a clean candidate. The older
frozen four-case manifest's SHA-256 is unchanged; its approval remains pending.

Earlier planning-only checkpoint retained separately:
`artifacts/dry-runs/issue46-completion-batch-20261105T004317Z` covered the prior
14-scenario / 55-phase aggregate plus live planning (15 runs / 317 files). It does
not cover the subsequently added domain-isolation and configured-migration cases.

These checks do not prove the new daemon interleavings, failure classifications,
retry/cancellation recovery, or migration/isolation behavior in a real environment.
They do not qualify native composition, open a PR, grant destructive approval,
or transfer the older four-case bundle's identity to this batch.

### Landed pending-restart implementation validation

On branch `issue/46-v0214-pending-restart`, the working-tree implementation passed:

```sh
GOWORK=off go test ./... -count=1
GOWORK=off go test -race ./... -count=1
GOWORK=off go test -race ./internal/reliability/events -run '^TestSemanticWork' -count=20
GOWORK=off go vet ./...
GOWORK=off go build -trimpath -buildvcs=true ./cmd/mycel-lab
make check-docs check-workflows
git diff --check
```

This includes authenticated fake-gRPC dispatch, read-only preservation, both
work and semantic generation fences, strict frontier/capacity evidence,
malformed/unknown checkpoint rejection, bounded RPCs/listing/polling, privacy-safe
RPC categories, and no retry of an ambiguous analysis mutation. Workflow policy
and all 36 workflow-script tests passed. These are harness regressions, not real
daemon restart or provider-protocol acceptance.

### Planning-only checkpoint

Retained local evidence:
`artifacts/dry-runs/issue46-v0214-pending-20261104T231515Z`.

| Suite | Dry-run scenarios | Planned phases |
| --- | --- | --- |
| `semantic-graph-index-release-gate` | 4 | 25 |
| `semantic-graph-index-recovery` | 2 | 10 |
| `semantic-graph-index-live` | 1 | 4 |

All seven terminal results declare `dryRun: true`; both manifest/Compose image
selections match the v0.21.4 index, and provider-request counts are zero. Artifact
safety passed for seven runs / 151 files. The executable records
`vcs.modified=true`: this is development-harness planning evidence, not a clean
committed candidate, environment-backed qualification, live-provider execution,
or approval for any of those operations. Earlier planning roots are retained.

### Superseding clean v0.21.3 checkpoint

A later clean-source run of Lab `9b42793cff87b86681064197745811ede030f334`
passed all three earlier aggregate scenarios / 20 phases against the immutable
v0.21.3 image. Harness SHA-256:
`91f575d44558cb6621fba09781e6ed613a756f1cd4bc282935cc59ef456a8b7a`.
All nine daemon observations were native ARM64 with clean-source provenance;
final oracles, cleanup/absence, and artifact safety passed. Evidence is retained
under `artifacts/destructive/semantic-graph-index-release-gate-clean-9b42793-20261104T181504Z-buildx`,
including `qualification-summary.md` and `qualification-verification.json`.

This completes the earlier clean-harness qualification follow-up **for that exact
old aggregate and daemon only**. It does not cover pending-work restart,
stale/out-of-order completion, the full protocol failure matrix, this new source,
or v0.21.4. Historical evidence below remains unaltered and must not be
transferred to a successor identity.

## Historical local candidate evidence

The retained local artifact root
`artifacts/destructive/semantic-graph-index-release-gate-20261104T155531Z-issue300-v6`
contains three passing non-dry runs:

| Scenario | Run ID | Result |
| --- | --- | --- |
| Contract | `compose-semantic-graph-contract-20261104-155531` | Passed |
| Recovery | `compose-semantic-graph-recovery-20261104-155556` | Passed |
| Protocol emulator | `compose-semantic-openai-emulator-20261104-155627` | Passed |

The preceding independent recovery rerun also passed:
`artifacts/destructive/semantic-graph-index-recovery-20261104T155452Z-issue300-v6-rerun/compose-semantic-graph-recovery-20261104-155452`.

All daemon containers in the aggregate recorded local image ID
`sha256:68ab5e1451c45e1c70bd22b252d4a3005626cda74ae3cbdb5927c27fc75c74c8`.
The image was `local/mycel:dev`, with dirty source and incomplete provenance,
not the subsequently published v0.21.3 image. These passes demonstrate the local
issue-300 recovery fix, not clean-source release qualification.

The artifact safety verifier was rerun successfully against both roots: three
aggregate runs / 110 files and one recovery run / 36 files, respectively. Artifact
roots are local, ignored evidence and are not included in the source commit.

The older `compose-semantic-graph-recovery-20261104-154035` attempt aborted before
any phase ran and lacks a recorded cause. It must not be mistaken for the latest
recovery result or assigned a cause unsupported by its artifacts.

## Historical harness validation (v0.21.3 tranche)

The resumed changes bound the complete semantic search operation, including RPCs,
pagination, and retry sleeps, by the configured search timeout or earlier parent
deadline. Retries remain opt-in and limited to transient transport/readiness
errors. Expected rejection and assertion failures are not retried. Regression
coverage includes restarting pagination without retaining partial results,
warnings, or stale page tokens. Emulator count assertions reject negative,
fractional, and out-of-range values instead of silently weakening expectations.

All validation commands below passed on the resumed working tree, including the
full race-enabled Go suite and all 36 workflow-policy regression tests:

```sh
go test ./... -count=1
go test -race ./...
go vet ./...
make check-docs check-workflows
git diff --check
```

The published v0.21.3 multiarch index was resolved from the registry as:

```text
docker.io/myceldb/mycel@sha256:9ace9d9cacca8ba3fe0e0cf40eaf281ebd32aca97d6269827f75494ba5ce8352
```

Its ARM64 manifest is
`sha256:d16f38862602c093bb0d4de2fd49ead7fbcc7f4fd35b46223483ddd647b76023`.
Both the three-scenario release-gate aggregate and live-provider **planning only**
passed dry-run resolution using the named immutable index. No hosted-provider
credential was read and no provider call was made. The artifact safety verifier
passed for all four dry runs / 85 files under:

```text
artifacts/dry-runs/semantic-graph-index-v0213-20261104T173322Z
```

## Approved published-image execution

After explicit operator approval, the credential-free aggregate ran against the
immutable v0.21.3 image above on native Linux ARM64. All three scenarios passed:

| Scenario | Run ID | Passed phases |
| --- | --- | --- |
| Contract | `compose-semantic-graph-contract-20261104-174028` | 11 |
| Recovery | `compose-semantic-graph-recovery-20261104-174046` | 5 |
| Protocol emulator | `compose-semantic-openai-emulator-20261104-174117` | 4 |

The retained root is:

```text
artifacts/destructive/semantic-graph-index-release-gate-v0213-20261104T174026Z
```

Each final oracle reports successful observations and required assertions. All
nine daemon-container image observations report the approved immutable index,
`linux/arm64`, completed clean-source provenance, and daemon revision
`b9a2dc7ad6e1774ebae64a5a8933c772d62e9f46`.

Every cleanup report confirms deletion and zero remaining clusters, containers,
networks, or volumes. A post-run Docker check also found no remaining semantic
suite containers or networks. Redaction reports are clean, and the independent
artifact safety verifier passed for all three runs / 112 retained files,
including the CLI log and supplemental source checkpoint. No hosted-provider
scenario ran, no hosted-provider credentials were read, and no hosted-provider
calls were made.

The harness executable SHA-256 shared by all three runs is:

```text
2488b33a9f456bb6da34b24df4d83327ab6c9a082842629a4800754592a430f5
```

It was built by `go run` with Go 1.26.5 from the uncommitted
`issue/46-semantic-suites` working tree based on `f39dc45`. Its build metadata
identifies a development module, not clean VCS provenance. The supplemental
`harness-source-checkpoint.json` records hashes of 284 source files immediately
after execution and before documentation-only result updates. This checkpoint
is not a substitute for a clean-source build. No runtime source or scenario
changed after execution.

The tool wrapper did not yet recognize the new suite name, so execution used the
Lab CLI directly with `--confirm-destructive`, the exact approved image, and a
fresh artifact root. No different canonical suite was substituted.

## Follow-up status

The clean v0.21.3 checkpoint above supersedes the original development-harness
qualification gap. The current expanded v0.21.4 aggregate still needs its own
reviewed-source approval and execution; no earlier pass transfers to it.

Pending restart, controlled out-of-order publication, and the nine-case failure
matrix are defined with local harness regressions, but lack fresh environment-backed
acceptance. Native composed semantic execution remains explicitly unsupported by
the pinned API. Same-space domain isolation and configured model/dimension migration
remain distinct coverage gaps identified by the acceptance audit. Nothing in this
record authorizes publication, issue closure, or another destructive/live run.
