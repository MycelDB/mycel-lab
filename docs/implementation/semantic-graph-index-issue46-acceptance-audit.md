# Issue #46 acceptance-to-evidence audit

Issue: [MycelDB/mycel-lab#46](https://github.com/MycelDB/mycel-lab/issues/46)

Completion batch: PR #93, clean merge `6615c17`. Follow-up corrections: PR #94
(pending reliability), PR #95 (transport evidence), and maintenance-health
convergence. This audit separates implementation, both failed approved executions,
and new harness-only evidence; it is not issue closure.

## Evidence levels

- **Historical qualified baseline:** clean Lab `9b42793`, immutable v0.21.3,
  three scenarios / 20 phases. Only that exact scope/image was qualified.
- **Landed pending-restart tranche:** PR #92, Lab `3b1c222`. Its clean four-case /
  25-phase v0.21.4 bundle was prepared and planned, **not destructively executed**.
- **Completion batch (runtime unqualified):** adds actual out-of-order publication control, nine
  protocol-failure cases, composition contracts/gates, audit hardening, same-space
  domain isolation, and independent configured model/dimension/vector-space migration.
  Canonical aggregate definitions are **16 scenarios / 65 phases**. Local tests
  and dry-runs are harness evidence, never daemon/runtime evidence.
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
- **Unsupported:** pinned v0.21.4 semantic rule APIs have no native generation /
  enrichment projection stage. Unsupported compositions fail resolution with a
  typed capability error, including during dry-run. They never become a passed
  embedding-only scenario.

## Original acceptance criteria

| # | Criterion | Implementation / evidence | Remaining acceptance |
| --- | --- | --- | --- |
| 1 | Committed content is projected, embedded, indexed, and returned from the semantic index | Canonical contract, direct semantic RPC, record/source revision/generation/configuration/model/vector identity assertions. Historical baseline qualified. | Clean 6615c17 contract passed on v0.21.4, but its aggregate failed later; the changed harness still requires fresh full qualification. |
| 2 | Positive assertions cannot pass through lexical/hybrid/scan fallback | Direct semantic search only; strict IDs/provenance, warnings rejected, negative queries. | Preserve these controls in fresh execution; plausible hydration is not proof. |
| 3 | Backfill/create/update/delete/transactions/freshness/restart/persistence/rebuild | Contract and recovery baseline qualified on v0.21.3. Landed pending-work case checks identity/generations/revisions/attempts across two retained-data restarts. | Clean 6615c17 recovery passed; pending capture failed before restart. The corrected pending/fault readers and remaining aggregate cases require fresh qualification. |
| 4 | Older/out-of-order responses cannot overwrite newer graph state | New `semantic-ordering-assert`: hold an older pinned backfill at the provider; commit, maintain, and search a newer revision **before release**; require explicit superseded skip and unchanged newer record after release. Real HTTP barrier and negative oracle tests. | Actual daemon execution of the new case. This is not inferred from concurrent rebuild or sleeps. |
| 5 | Space/domain and authorization isolation | Historical contract has outsider-scope and unauthorized-principal controls. New `compose-semantic-domain-isolation` uses the same actor/principal/space/node UUID, different domains, independent indexes, broad positive/cross-query-negative searches without rule/binding selectors, and exact hydrated payload/provenance. Peer-only update and retained restart preserve the other domain's record. | New case has local regressions and planning evidence only; fresh v0.21.4 runtime qualification remains required. Different spaces alone are not domain-isolation proof. |
| 6 | Dimension/model/projection changes fail closed or require migration/rebuild | Existing projection-change and malformed-vector tests remain separate. New `compose-semantic-model-migration` changes selected configured model, model dimensions, and vector-space identity independently. Each requires generation advancement, explicit rebuild-required state, pre-rebuild `FailedPrecondition`, and authorized rebuild. Post-rebuild search checks changed configuration/record identity, exact configured route/shape, unchanged acknowledged graph revision, and retained-restart preservation. | New case has local regressions and planning evidence only; actual v0.21.4 configuration invalidation, fencing, rebuild, and persistence must still be qualified. No daemon/API modification is inferred from harness tests. |
| 7 | Capability-driven profiles; no assumption every provider embeds | Independent generation/embedding/enrichment/structured-output/routing declarations; ordered `providerStages`, exact per-stage profile source/model/endpoint provenance, typed dataflow, finite combined wall-clock ceiling. Tests include generation-only provider plus distinct embeddings. | Native composed execution remains API/adapter-blocked on v0.21.4. Initial forward-compatible contract is implemented; do not invent native execution or require new vendor connectors as an unstated non-goal reversal. |
| 8 | Deterministic tests require no external credentials | Deterministic/emulator profiles reject secret references; new cases use only direct local fixtures or the local emulator. Credential-lookup negative tests. | Fresh environment execution of expanded cases, not hosted-provider access. |
| 9 | Manual live suites, non-gating initially | Existing canonical manual-live suite and protected workflow. | No hosted execution is claimed or automatically required for closure. |
| 10 | Stable live invariants, not exact hosted scores/vectors | Existing live top-K/lifecycle/provenance/budget invariants. | Keep separate from deterministic exact-vector assertions; live runtime evidence remains unclaimed. |
| 11 | Protected future GitHub execution, finite budgets, safe artifacts | Existing protected environment/workflow controls; 36 workflow-script regressions and policy checks. | Repository environment approval settings and each future run remain operator responsibilities; unit tests cannot grant approval. |
| 12 | Explicit live runs fail missing configuration/credentials | Existing authenticated capability discovery followed by credential resolution, refusal of unsupported budgets, and configuration failures. New composition capability gate precedes any stage secret lookup. | Preserve fail-closed behavior; a dry-run intentionally does not resolve credentials. |
| 13 | Every subprocess/provider/convergence operation is context-bounded | Existing finite harness/finalization budgets; new finite request sequences, bounded gates/observations/listings, operation/parent deadlines, bounded goroutine cleanup. Audit fixes the legacy maintenance waiter so its timeout bounds RPCs too. | Runtime timing evidence still required; local deadline regressions are not provider/cluster qualification. |
| 14 | Build/provider/model/index provenance and safe verdict artifacts | Existing sanitized artifacts and safety scanner; new typed ordering/failure/work evidence, no raw provider errors or payloads. Audit rejects the `invalid` build-command sentinel instead of accepting a nonempty unknown identity. | New clean-source build/runtime identities, successful final oracles, mandatory cleanup/absence, and redaction/artifact verification on the exact candidate. |
| 15 | Operator docs for deterministic/emulator/live/future CI modes | Existing operator docs, generated case docs, composition contract/limitations, this acceptance mapping. | Attach exact new execution results when collected; do not relabel older evidence. |

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
complete the same work and generation with exactly two attempts. No graph repair,
rebuild, forced backfill, cursor reset, dropped assertion, or silent fallback is
used to obtain a pass. Final direct search and exact finite protocol accounting
remain required.

The ordering case deliberately launches a competing **older pinned backfill**,
not two ordinary maintenance workers holding the same rule fence. The newer
maintenance publication can therefore complete while the older provider response
is held. No backfill, analysis, or repair follows release to hide a stale overwrite.

## Closeout decisions

- Item #1 is landed in PR #92; approved clean 6615c17 qualification failed during
  pending capture before restart. The correction is harness code/tests, not a pass.
- Items #2/#3 landed in PR #93 but were not reached by the stopped run. Their
  executable scenarios/oracles and local regressions are **not runtime qualification**.
- Item #4 has a wired, provider-neutral planning/validation contract and explicit
  unsupported-runtime rejection. Native enrichment execution is not implemented
  or claimed; supporting it requires a real daemon/API/public-adapter prerequisite.
- Item #5's audit is complete as a mapping, **not as a declaration that every
  criterion passed**. The two identified coverage gaps now have independent Lab
  scenarios, strict public-API assertions, local regressions, and planning evidence.
  Their runtime acceptance remains open rather than hidden behind a checklist tick.
- Neither approved aggregate completed. The `574e690` recovery failure is not
  overridden by `6615c17`'s earlier recovery pass; pending/ordering/fault/domain/
  migration acceptance remains unexecuted or unqualified. Both approvals are consumed.
- #46 stays open. Final checklist closure requires fresh exact-source runtime
  evidence and reconciliation against each criterion; native composed execution
  remains explicitly capability-blocked rather than silently substituted.
- The prior frozen `3b1c222` four-case bundle is valid only for its old subset. It
  must not be reused as qualification of this new working tree or aggregate.
- No new PR, commit, hosted-provider call, destructive run, remote deployment
  change, or release publication is authorized by this document.
