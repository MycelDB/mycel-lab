# Issue #46 acceptance-to-evidence audit

Issue: [MycelDB/mycel-lab#46](https://github.com/MycelDB/mycel-lab/issues/46)

Completion batch: PR #93, clean merge `6615c17`. Follow-up corrections: PR #94
(pending reliability), PR #95 (transport evidence), PR #96 (maintenance health),
and disabled-rule quiescence. This audit separates implementation, all eight
failed approved executions and the successful ninth exact-source qualification; it records closure evidence for the declared aggregate scope.

## Evidence levels

- **Historical qualified baseline:** clean Lab `9b42793`, immutable v0.21.3,
  three scenarios / 20 phases. Only that exact scope/image was qualified.
- **Landed pending-restart tranche:** PR #92, Lab `3b1c222`. Its clean four-case /
  25-phase v0.21.4 bundle was prepared and planned, **not destructively executed**.
- **Completion batch:** adds actual out-of-order publication control, nine
  protocol-failure cases, composition contracts/gates, audit hardening, same-space
  domain isolation, and independent configured model/dimension/vector-space migration.
  Canonical aggregate definitions are **16 scenarios / 65 phases**. Local tests
  and dry-runs were followed by the exact successful runtime qualification below.
- **Approved v0.21.4 run (failed):** clean `6615c17` passed contract/recovery,
  then stopped at pending capture before any pending-case restart. Two scenarios
  passed, one failed, thirteen were not run; 17 phases passed, one failed, 47
  unexecuted. Cleanup, provenance and artifact safety passed, not aggregate acceptance.
- **Pending-state correction:** work/fault readers now distinguish v0.21.4's
  unresolved-gap boolean from actual required/running/failed rebuild state, retain
  safe diagnostics before rejection, and preserve all identity/freshness fences.
  Local regressions are not a replacement qualification or approval transfer.
- **Second approved v0.21.4 run (failed):** clean `574e690` passed contract,
  then failed recovery at the first degraded maintenance observation after its
  second restart. One scenario passed, one failed, fourteen not run; 15 phases
  passed, one failed, 49 unexecuted. Pending/fault corrections were not reached.
  Cleanup and artifact safety passed, not aggregate acceptance.
- **Maintenance-health correction (harness only):** distinguishes node-global
  background history from current scope/generation health, retains safe metadata,
  and waits only on exact bounded ownership reasons. The bit must actually clear;
  queue zeros cannot bypass a scoped freshness certificate. No daemon/API change,
  rerun, approval transfer or inference that the original cause was transient.
- **Third approved v0.21.4 run (failed):** clean `cbc4274` stopped at the
  intentionally disabled-rule maintenance wait. Zero scenarios passed, one failed,
  fifteen not run; nine phases passed, one failed, 55 unexecuted. Healthy empty
  queues accompanied latest revision 8 and analyzed/fresh 7/7 while disabled.
  Cleanup and artifact safety passed, not acceptance. The narrow local correction
  explicitly proves disabled quiescence, not indexing; default enabled freshness,
  disabled-search rejection and re-enable catch-up remain strict. No rerun is implied.
- **Fourth approved v0.21.4 run (failed):** clean `81289a7` passed contract
  and recovery; pending-work restart completed but its recovery wait was capped
  by a smaller operation budget and expected 64 partitions against a 16-partition
  fixture. The post-restart work check was not reached. The configuration drift
  was corrected and preflighted separately; this is not a Raft protocol finding.
- **Fifth approved v0.21.4 run (failed):** clean `8b13721` passed contract,
  recovery, pending-work restart, and the OpenAI emulator. The out-of-order case
  proved held-older/newer ordering, explicit supersession, unchanged newer record,
  negative stale search and fresh zero-gap reliability; its final provider
  assertion then failed (`maximum attempts 4 exceeds 2`). Four scenarios passed,
  one failed, eleven unrun; 27 phases passed, one failed, 37 unrun. Five reached
  final/cluster oracles, cleanup and artifact safety passed, not aggregate
  acceptance. The approval is consumed and no retry was made.
- **Per-input accounting correction (source only):** the local emulator counts
  every request for the same input, including normal repeated work/search calls;
  its counter is not a work-retry counter. Corrected the out-of-order and nine
  unexecuted fault cases to require exact total requests AND their exact safe
  per-input distribution, with the per-input cap matching the largest group.
  Public work-attempt identity and ordering checks are unchanged. Reject
  contradictory accounting contracts before provisioning. No corrected case
  has new runtime qualification or transferred approval.
- **Sixth approved v0.21.4 run (failed):** clean `dc97cd4` passed contract
  and recovery. Pending capture, first restart/work preservation and same-work
  completion/search passed; its second rolling restart completed but reusable
  endpoint authentication on ordinal 0 hit its 3s limit **before any recovery
  poll** in the 60s wait. Post-second-restart no-replay assertions were never
  reached. Two scenarios passed, one failed, thirteen unrun; 20 phases passed,
  one failed, 44 unrun. Three reached final/cluster oracles, cleanup and artifact
  safety passed, not acceptance. No Raft protocol defect is proved. The approval
  was consumed exactly once without retry.
- **Recovery session initialization correction (source only):** the shared
  recovery waiter now checks transport without authentication within the
  declared 60s wait. All four affected Compose fixtures declare a separate
  20s concurrent single-login bound and retain 3s read-only RPC bounds, exact
  Raft identity/groups/progress and stable health polls. Login creates a
  session: an ambiguous timeout is **never resubmitted**. Invalid budgets fail
  before provisioning. This code and planning cannot substitute for runtime
  evidence of post-second-restart work or the other thirteen unrun cases.
- **Seventh approved v0.21.4 run (failed):** clean `589df4c` passed contract,
  graph recovery, the complete pending-work restart (including the second
  restart/no-replay assertions), OpenAI emulator, out-of-order ordering and
  the 429/503/real-transport fault cases. The malformed-JSON fault case
  stopped in `semantic-fault-assert` after one explicit public retry: the same
  work ID/revision/semantic generation completed after two attempts, but its
  work generation advanced 1 to 2 and the Lab demanded unchanged generation.
  The final provider-accounting phase was not reached. Eight scenarios passed,
  one failed, seven unrun; 38 phases passed, one failed, 26 uncompleted. Nine
  final/cluster oracles, cleanup, absence and redaction passed. The 362-file
  inventory is preserved; this is not aggregate acceptance. The one-shot
  approval was consumed without retry or repair.
- **Eighth approved v0.21.4 run (failed):** clean candidate `02c1120` with
  bundle `53bc36da74ab4cb0e6816aac960c2ed1270d515793a06acdbb964a9fdb738dda`
  passed 15 of 16 scenarios. All three migration invalidation fences accepted
  v0.21.4's empty zero-result response with exactly one requires-rebuild warning,
  and the first four migration phases passed. The final retained-restart phase
  failed before recovery polling: reusable endpoint-session initialization for
  ordinal 0 hit its 10-second Login deadline after the rolling restart, while
  Raft was still electing leaders. Cleanup, absence and all 16 redaction scans
  passed. The aggregate remains unqualified; the approval was consumed without
  retry.
- **Post-restart session-concurrency correction (source only):** reusable
  recovery sessions now initialize their one-shot authenticated sessions
  concurrently, with a bounded 20-second window and an explicit reserve for
  readiness/Raft RPCs and stable polls. A timed-out Login remains never retried,
  and successful partial sessions are closed on failure.
- **Ninth approved v0.21.4 run (qualified):** clean candidate `6b17781` with
  bundle `94ff95c2c30e9c13a6a464debe6841e203abe2ccbe6fd9fc2dfcf1f210d6c77e`
  passed all **16 scenarios / 65 phases**. The migration fences accepted the
  exact v0.21.4 warning response, retained restart passed, all 16 redaction
  scans passed, and all cleanup/absence checks reported zero residual resources.
  This is the authoritative runtime evidence for the declared deterministic
  aggregate; unsupported native enrichment and hosted live execution remain
  explicit non-goals.
- **Operator-retry generation correction (source only):** automatic retryable
  failures keep work generation; public operator retry must return the same
  scoped ID/revision and attempt, pending with cleared failure/claim/backoff,
  and exactly one version increment. The recovered work must match that exact
  returned generation, keep semantic generation unchanged, and prove two
  attempts, freshness and zero gaps. Reject overflow, missing/ambiguous receipts
  or any unexpected transition without resubmitting mutations. Read-only replay
  of the failed historical observation checks recovered work, **not** the old
  retry response (which was not recorded). This correction does not change the
  frozen failed result or authorize another execution.
- **Unsupported:** pinned v0.21.4 semantic rule APIs have no native generation /
  enrichment projection stage. Unsupported compositions fail resolution with a
  typed capability error, including during dry-run. They never become a passed
  embedding-only scenario.

## Original acceptance criteria

| # | Criterion | Implementation / evidence | Remaining acceptance |
| --- | --- | --- | --- |
| 1 | Committed content is projected, embedded, indexed, and returned from the semantic index | Canonical contract, direct semantic RPC, record/source revision/generation/configuration/model/vector identity assertions. Historical baseline qualified. | Passed in the exact ninth runtime qualification. |
| 2 | Positive assertions cannot pass through lexical/hybrid/scan fallback | Direct semantic search only; strict IDs/provenance, warnings rejected, negative queries. | Passed in the exact ninth runtime qualification. |
| 3 | Backfill/create/update/delete/transactions/freshness/restart/persistence/rebuild | Contract and recovery baseline qualified on v0.21.3. `589df4c` passed both pending-work retained-data restarts, same-work resumption and post-second-restart no-replay checks. | Passed in the exact ninth runtime qualification, including retained restart and rebuild persistence. |
| 4 | Older/out-of-order responses cannot overwrite newer graph state | `589df4c` passed the controlled held-older/newer publication, explicit supersession, unchanged newer record, stale-search rejection, fresh zero-gap frontier and corrected emulator accounting. | Passed in the exact ninth runtime qualification. |
| 5 | Space/domain and authorization isolation | Historical contract has outsider-scope and unauthorized-principal controls. New `compose-semantic-domain-isolation` uses the same actor/principal/space/node UUID, different domains, independent indexes, broad positive/cross-query-negative searches without rule/binding selectors, and exact hydrated payload/provenance. Peer-only update and retained restart preserve the other domain's record. | Passed in the exact ninth runtime qualification. Different spaces alone are not domain-isolation proof. |
| 6 | Dimension/model/projection changes fail closed or require migration/rebuild | Existing projection-change and malformed-vector tests remain separate. New `compose-semantic-model-migration` changes selected configured model, model dimensions, and vector-space identity independently. Each requires generation advancement, explicit rebuild-required state, and a strict pre-rebuild fence (empty results with an explicit requires-rebuild warning or `FailedPrecondition`) before authorized rebuild. Post-rebuild search checks changed configuration/record identity, exact configured route/shape, unchanged acknowledged graph revision, and retained-restart preservation. | Passed in the exact ninth runtime qualification. No daemon/API modification is inferred from harness tests. |
| 7 | Capability-driven profiles; no assumption every provider embeds | Independent generation/embedding/enrichment/structured-output/routing declarations; ordered `providerStages`, exact per-stage profile source/model/endpoint provenance, typed dataflow, finite combined wall-clock ceiling. Tests include generation-only provider plus distinct embeddings. | Native composed execution remains API/adapter-blocked on v0.21.4. Initial forward-compatible contract is implemented; do not invent native execution or require new vendor connectors as an unstated non-goal reversal. |
| 8 | Deterministic tests require no external credentials | Deterministic/emulator profiles reject secret references; new cases use only direct local fixtures or the local emulator. Credential-lookup negative tests. | Passed in the exact ninth runtime qualification without external credentials. |
| 9 | Manual live suites, non-gating initially | Existing canonical manual-live suite and protected workflow. | No hosted execution is claimed or automatically required for closure. |
| 10 | Stable live invariants, not exact hosted scores/vectors | Existing live top-K/lifecycle/provenance/budget invariants. | Keep separate from deterministic exact-vector assertions; live runtime evidence remains unclaimed. |
| 11 | Protected future GitHub execution, finite budgets, safe artifacts | Existing protected environment/workflow controls; 36 workflow-script regressions and policy checks. | Bundle preflight, finite budgets, safe artifacts, redaction and cleanup passed for the ninth qualification; future hosted approvals remain operator responsibilities. |
| 12 | Explicit live runs fail missing configuration/credentials | Existing authenticated capability discovery followed by credential resolution, refusal of unsupported budgets, and configuration failures. New composition capability gate precedes any stage secret lookup. | Preserve fail-closed behavior; a dry-run intentionally does not resolve credentials. |
| 13 | Every subprocess/provider/convergence operation is context-bounded | Existing finite harness/finalization budgets; new finite request sequences, bounded gates/observations/listings, operation/parent deadlines, bounded goroutine cleanup. Audit fixes the legacy maintenance waiter so its timeout bounds RPCs too. | Passed in the exact ninth runtime qualification. |
| 14 | Build/provider/model/index provenance and safe verdict artifacts | Existing sanitized artifacts and safety scanner; new typed ordering/failure/work evidence, no raw provider errors or payloads. Audit rejects the `invalid` build-command sentinel instead of accepting a nonempty unknown identity. | Exact clean-source bundle identities, successful final oracles, mandatory cleanup/absence, and redaction verification all passed in the ninth qualification. |
| 15 | Operator docs for deterministic/emulator/live/future CI modes | Existing operator docs, generated case docs, composition contract/limitations, this acceptance mapping. | Exact ninth-run evidence is attached below and must remain tied to its bundle/candidate identities. |

## Ninth qualification evidence

The authoritative deterministic aggregate run used clean candidate
`6b17781e17b2182983ceb22d35590b2eb246e9b5` and immutable v0.21.4 daemon image
`docker.io/myceldb/mycel@sha256:0973cddd9c282abff40d8edeb2e8f7a801e58d40bfbab4c8507ce2d9daae2dd7`.
The release execution bundle was validated before provisioning:

- Bundle: `issue46-fence-v3-release.yaml`
- Bundle SHA-256: `94ff95c2c30e9c13a6a464debe6841e203abe2ccbe6fd9fc2dfcf1f210d6c77e`
- Validation evidence SHA-256: `9eb9494e70ad9ae2f1edf2748f10a95999ee032ee1ab927b0d78dbb5316ffdfc`
- Destructive artifacts: `tmp/issue46-fence-v3-destructive/`
- Result: 16/16 scenarios and 65/65 phases passed
- Safety: 16/16 redaction scans passed; 16/16 cleanup/absence checks passed with zero residual containers, networks, volumes, or clusters

The migration scenario recorded `rebuildFenceProved: true` for all three
incompatible transitions, with zero results and exactly one requires-rebuild
warning in each response. No external credentials or hosted provider were used.

## New failure matrix

`semantic-provider-failure-matrix` has nine finite local cases:

1. HTTP 429 / `Retry-After`: persisted not-before within declared bounds; an
   immediate normal processing pass must not perform work; normal maintenance
   eventually retries the same generation/work identity.
2. HTTP 503: retryable work remains unresolved, with bounded ordinary recovery.
3. Real connection drop: the HTTP/1 connection is closed without a response;
   envelope status 503 is a synthetic evidence marker, not an actual HTTP status.
4. Malformed JSON.
5. Truncated JSON.
6. Incorrect vector dimensions.
7. Refusal / policy rejection.
8. Caller deadline while provider work is outstanding.
9. Explicit cancellation after an observed in-flight provider gate.

Each case must prove failure **before** recovery: positive public durable work
identity/generations/revisions, one attempt, unresolved gaps, and freshness behind
the affected revision. Permanent/interrupted cases explicitly use the authorized
public retry RPC; retryable cases must honor automatic not-before. Recovery must
complete the same scoped work ID, revision and semantic generation with exactly
two attempts. Automatic retries retain work generation; public operator retries
must advance it **exactly once**, as witnessed by the single retry response and
matched by read-only recovered work. No graph repair,
rebuild, forced backfill, cursor reset, dropped assertion, or silent fallback is
used to obtain a pass. Final direct search and exact finite protocol accounting
remain required. Three requests for the same single-document/query input
(one failed work attempt, one recovered work attempt, one direct search) must
produce exactly one per-input group of three; the public durable work count
remains **two**, not three. A single input-digest counter cannot replace this
public same-work retry proof.

The ordering case deliberately launches a competing **older pinned backfill**,
not two ordinary maintenance workers holding the same rule fence. The newer
maintenance publication can therefore complete while the older provider response
is held. No backfill, analysis, or repair follows release to hide a stale overwrite.

## Closeout decisions

- Item #1 is landed in PR #92; approved clean 6615c17 qualification failed during
  pending capture before restart. The correction is harness code/tests, not a pass.
- Items #2/#3 landed in PR #93. `8b13721` reached and passed the ordering
  event, but the complete scenario failed at provider accounting; the nine
  fault cases were not reached. New source regressions are **not runtime
  qualification**.
- Item #4 has a wired, provider-neutral planning/validation contract and explicit
  unsupported-runtime rejection. Native enrichment execution is not implemented
  or claimed; supporting it requires a real daemon/API/public-adapter prerequisite.
- Item #5's audit is complete as a mapping, **not as a declaration that every
  criterion passed**. The two identified coverage gaps now have independent Lab
  scenarios, strict public-API assertions, local regressions, and planning evidence.
  Their runtime acceptance remains open rather than hidden behind a checklist tick.
- The first eight approved v0.21.4 aggregates failed at bounded harness/oracle or
  recovery-environment gates and do not transfer qualification. The ninth exact
  candidate completed all 16 scenarios and 65 phases; its bundle, cleanup,
  redaction, and absence evidence are authoritative for this declared aggregate.
- #46's semantic graph-index reliability aggregate is qualified for the
  deterministic v0.21.4 scope recorded here. Native composed enrichment and
  hosted live execution remain explicit non-goals and are not silently substituted.
- The prior frozen `3b1c222` four-case bundle is valid only for its old subset. It
  must not be reused as qualification of this aggregate.
- The exact bundle and sanitized runtime evidence are attached to the issue and
  linked from this audit; any future runtime change requires a new candidate,
  bundle, and approval.
