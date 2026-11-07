# Semantic graph-index reliability suites implementation plan

Status: v0.21.4 batch defines 16 scenarios / 65 phases; runtime acceptance pending, native composition capability-blocked

Tracking issue: [MycelDB/mycel-lab#46](https://github.com/MycelDB/mycel-lab/issues/46)

Design document: [Semantic graph-index reliability suites](../design/semantic-graph-index-reliability-suites.md)

Validation record: [Semantic suite validation checkpoint](semantic-graph-index-reliability-suites-validation.md)

## Implementation checkpoint

The canonical contract, recovery, protocol, live, and release-gate suite names
are implemented. Their current default daemon is the immutable v0.21.4 index
`docker.io/myceldb/mycel@sha256:0973cddd9c282abff40d8edeb2e8f7a801e58d40bfbab4c8507ce2d9daae2dd7`.
Both manifest and Compose image selection are pinned; explicit CLI overrides
remain visible operator choices, not transferable qualification.

Recovery includes converged-index persistence, catch-up, projection rebuild,
and concurrent rebuild delta catch-up. The new dedicated
`compose-semantic-pending-restart` scenario captures durable work ID, scope,
generation, revision range, and attempts before and after retained-data restart.
Background scheduling is deliberately disabled in this fixture; read-only
post-restart checks must prove preservation before normal public maintenance
resumes the same work. It proves controlled durable-work resumption, not automatic
background scheduling or interruption during a provider call. A second restart
checks completed-work and semantic-record identity without replay.

The contract suite contains three scenarios, recovery three, protocol ten, the
standalone failure matrix nine, and the credential-free aggregate
contains **16 scenarios / 65 phases**. The out-of-order case holds an older pinned
backfill response until a newer maintenance publication is searched successfully,
then requires explicit superseded skip and unchanged newer record identity after
release. It does not rely on racing two ordinary rule-fenced workers.

Nine finite emulator cases cover 429/Retry-After, 503, real connection loss,
malformed/truncated JSON, invalid dimensions, refusal, caller deadline, and
observed in-flight cancellation. Failure must be proved before bounded recovery
of the same work/generation; permanent/interrupted cases use an explicit public
retry, not a rebuild or graph repair.

Provider-neutral ordered stage contracts now preserve per-stage profile/model /
endpoint/source identity and reject unsupported runtime adapters before deployment
or secret lookup. Native generation/enrichment semantic execution remains blocked
by the pinned v0.21.4 API; see the [composition contract](../design/semantic-provider-composition.md).

Two dedicated deterministic cases close the audit's implementation gaps. Same-space
multi-domain isolation keeps the actor/principal/node UUID fixed, proves independent
indexes with broad positive and negative searches, and checks hydration, peer-only
updates, and retained restart. Configured migration changes model identity, dimensions,
and vector space independently, requiring a new generation, rebuild-required state,
and a strict rebuild fence before each explicit authorized rebuild. Final search
must retain the acknowledged graph revision and exact configured identity/shape.

These implementations and local harness regressions are not full daemon acceptance.
The approved clean 6615c17 aggregate passed contract/recovery, then failed pending
capture before restart; thirteen cases were not reached. Its work/fault readers
incorrectly rejected v0.21.4's unresolved-gap boolean. The correction preserves
actual rebuild/capacity/freshness fences and retains safe rejection diagnostics.
Fresh approved execution of the changed clean harness is still required. Live profiles remain manual,
separately approved, and non-gating; no hosted execution is part of this batch.

The earlier clean committed Lab `9b42793` aggregate passed three scenarios / 20
phases against immutable v0.21.3, with successful final oracles, cleanup, and
artifact safety. That clean-source run superseded the earlier development-harness
qualification gap, but does not qualify this new harness, expanded scenario set,
or v0.21.4 image. See the validation record for identities and limits.

The [criterion-by-criterion acceptance audit](semantic-graph-index-issue46-acceptance-audit.md)
separates delivered code, historical qualification, new local tests, missing
runtime proof, and actual remaining gaps. Same-space multi-domain isolation and
configured model/dimension/vector-space migration now have independent definitions,
not substitutions using cross-space isolation or a malformed provider vector. Native composed execution
must remain explicitly blocked until real API/adapter support exists. Milestone
checklists are acceptance gates, not implementation-only claims.

## 1. Purpose

This plan describes how to implement provider-neutral semantic graph-index
reliability suites in Mycel Lab. It focuses on reviewable, independently
landable tranches that begin with deterministic credential-free coverage and
later add recovery, protocol-emulator, live-provider, and release-gate support.

The plan is intentionally conservative: Mycel Lab should assert only what public
daemon APIs and artifacts can prove. Assertions that require unavailable daemon
provenance remain explicitly gated until the daemon, API, and SDK expose the
required state safely.

## 2. Delivery strategy

Deliver the work in six milestones:

1. **Foundation and capability gates** — shared model, profile resolution,
   artifact contract, suite registration skeleton.
2. **Deterministic contract suite** — projection/create/update/delete/backfill/
   transaction visibility and direct semantic proof.
3. **Recovery suite** — restart, pending work, persistence, rebuild, stale async
   completion, and retry/failure behavior.
4. **Protocol emulator suites** — local HTTP fixtures for provider-protocol
   compatibility and failure handling.
5. **Live-provider suites** — manual, non-gating profiles with budgets and
   invariant assertions.
6. **Release-gate integration** — selected deterministic suites promoted to
   repeatable release qualification once stable.

Each milestone should include documentation, examples, targeted unit tests, and
at least one destructive dry-run or real-run validation path where applicable.

## 3. Current prerequisites and assumptions

### 3.1 Mycel Lab assumptions

- The reliability harness can already run suites against Compose/k3d-style
  environments.
- Run artifacts, events, operation logs, redaction scans, and final oracle
  reports already exist or can be extended.
- Public Go SDK/gRPC clients are the supported scenario boundary.
- Scenarios should not import internal daemon packages.

### 3.2 Daemon/API assumptions

Level 1 and Level 2 proof can start with the current semantic administration,
maintenance, and direct search APIs if they expose:

- semantic rule and embedding binding identity;
- direct semantic search results with vector-record identity;
- graph entity hydration and authorization behavior;
- maintenance queue state;
- physical search-index readiness and live-record counts.

Level 3 proof is blocked until the daemon/API/SDK expose durable, safe
per-result provenance such as source graph revision, generation/configuration
identity, vector-space/dimensions, and fresh-through frontier.

### 3.3 Deterministic provider assumption

The first implementation needs a deterministic provider profile. If the daemon
already exposes a sufficient fake/token-basis embedding fixture, Mycel Lab can
consume it. If not, the deterministic-suite work should first create a daemon
prerequisite issue/PR and keep Mycel Lab assertions behind a capability gate.

## 4. Milestone 1 — Foundation and capability gates

### 4.1 Goals

- Add semantic-suite model types at the Mycel Lab package boundary.
- Resolve provider profiles without secrets.
- Register draft suites and dry-run capability gates.
- Define the artifact schema before scenario behavior is implemented.

### 4.2 Work items

1. Add typed package for semantic reliability resources, for example:

   ```text
   internal/semanticreliability/
     profile.go
     corpus.go
     assertions.go
     artifacts.go
     capabilities.go
   ```

2. Add or extend resource decoding for `ProviderProfile` with fields for:

   - mode: deterministic, emulator, live;
   - capability slots: embedding, generation, enrichment, structured output,
     routing;
   - endpoint/model/vector-space/dimensions;
   - fixture strategy and optional scripted outcomes;
   - budgets and required enforcement flags;
   - secret references by environment-variable name only;
   - artifact capture policy.

3. Add provider-profile resolution rules:

   - deterministic/emulator profiles reject secrets;
   - live profiles require secret references and fail fast when explicitly run
     without credentials;
   - resolved artifacts include secret reference names, never values;
   - unsupported required capability produces a typed capability error.

4. Add corpus schema and validation for deterministic graph fixtures:

   - nodes, optional edges, labels, properties, payload, metadata;
   - projection include/exclude fields;
   - queries and expected top-K IDs;
   - lifecycle mutation steps;
   - isolation principal/space/domain fixtures.

5. Add semantic artifact skeleton:

   - resolved profile;
   - corpus checksum;
   - semantic operation timeline;
   - maintenance snapshots;
   - direct search observations;
   - final semantic oracle verdict;
   - redaction scan summary.

6. Register placeholder suite definitions:

   - `semantic-graph-index-contract`;
   - `semantic-graph-index-recovery`.

7. Add dry-run output that proves the suite can resolve inputs without deploying
   the environment.

### 4.3 Acceptance criteria

- [x] Provider profiles decode, validate, and resolve in dry-run mode.
- [x] Resolved artifacts exclude secret values; negative tests cover redaction.
- [x] Deterministic, emulator, and live validation paths are distinct.
- [x] Missing live credentials fail configuration after authenticated capability discovery and before provider use.
- [x] Canonical semantic suites are visible to the CLI/listing path.
- [x] Unit tests cover validation failures and artifact redaction rules.

### 4.4 Suggested validation

```sh
go test ./internal/semanticreliability/...
go test ./internal/... # or focused harness/resource packages
mycel-lab run suite semantic-graph-index-contract --dry-run --provider-profile deterministic-token-basis-v1
```

## 5. Milestone 2 — Deterministic contract suite

### 5.1 Goals

Implement `semantic-graph-index-contract` with Level 1 and Level 2 proof for the
minimum lifecycle scenarios.

### 5.2 Scenario phases

A first deterministic contract run should include phases like:

1. **Provision profile resources**
   - apply deterministic inference package;
   - create intelligence profile;
   - create credential/grant/policy only if required by daemon semantics;
   - record catalog IDs.

2. **Create graph fixture**
   - create one or more spaces/domains;
   - create seed nodes with included and excluded projection fields;
   - commit and record graph mutation evidence.

3. **Create semantic rule**
   - configure projection and embedding binding;
   - verify rule binding is enabled and physical index state exists.

4. **Backfill/pre-existing data**
   - create data before enabling or before backfill;
   - run backfill/process maintenance;
   - wait for convergence.

5. **Direct semantic search proof**
   - query deterministic tokens;
   - assert expected node IDs/rank/order where available;
   - assert semantic rule, binding, vector-record identity, and no fallback;
   - hydrate result entity and assert visibility.

6. **Projection proof**
   - assert included content affects retrieval;
   - assert excluded content does not affect retrieval.

7. **Update replacement**
   - update indexed content;
   - process maintenance;
   - assert old query no longer returns stale vector and new query does.

8. **Delete/tombstone**
   - delete an indexed entity;
   - process maintenance;
   - assert it is absent from semantic candidates and hydration cannot leak it.

9. **Transaction visibility**
   - verify staged/rolled-back writes do not produce semantic results;
   - verify committed writes appear only after commit and convergence.

10. **Isolation and filters**
    - create similar data in another domain/space/principal scope;
    - assert results never cross selected scope or authorization boundary.

11. **Final oracle and redaction scan**
    - aggregate verdicts;
    - scan logs/artifacts for secrets and forbidden payload patterns.

### 5.3 Work items

1. Build semantic fixture actor/helper layer using public clients:

   - graph transaction helper;
   - inference/catalog setup helper;
   - semantic rule setup helper;
   - maintenance/backfill helper;
   - direct semantic search helper;
   - hydration helper.

2. Add bounded convergence waiters:

   - wait for maintenance queue drained or expected degraded state;
   - wait for physical index readiness/live record counts;
   - wait for direct semantic expected result with timeout;
   - record every retry/wait observation.

3. Add typed assertions:

   - `AssertSemanticPath`;
   - `AssertExpectedRanking`;
   - `AssertProjectionSelection`;
   - `AssertUpdateReplacesOldRecord`;
   - `AssertDeleteRemovesCandidate`;
   - `AssertTransactionVisibility`;
   - `AssertScopeIsolation`;
   - `AssertArtifactSafety`.

4. Add deterministic corpus fixtures:

   ```text
   tests/semantic/corpus/minimal-token-basis-v1.yaml
   tests/semantic/profiles/deterministic-token-basis-v1.yaml
   tests/semantic/suites/semantic-graph-index-contract.yaml
   ```

5. Add final artifact files:

   ```text
   semantic/resolved-profile.json
   semantic/corpus.json
   semantic/maintenance-timeline.jsonl
   semantic/search-observations.jsonl
   semantic/oracle.json
   semantic/redaction-report.json
   ```

6. Add docs for running the suite manually.

### 5.4 Acceptance criteria

- [ ] The suite runs without external network access or credentials.
- [ ] A committed fixture node becomes searchable through direct semantic search.
- [ ] Search proof includes semantic rule, binding, vector-record identity, and
      expected graph entity identity.
- [ ] Projection include/exclude behavior is asserted.
- [ ] Create, update, delete, backfill, commit, and rollback behavior are
      asserted.
- [ ] Space/domain/authorization isolation is asserted where supported by the
      fixture.
- [ ] All waits are bounded and visible in artifacts.
- [ ] Artifact redaction scan passes.

### 5.5 Suggested validation

```sh
mycel-lab run suite semantic-graph-index-contract \
  --dry-run \
  --provider-profile deterministic-token-basis-v1

mycel-lab run suite semantic-graph-index-contract \
  --confirm-destructive \
  --provider-profile deterministic-token-basis-v1 \
  --image myceldb/mycel:<candidate>
```

## 6. Milestone 3 — Recovery suite

### 6.1 Goals

Implement `semantic-graph-index-recovery` for restart, persistence, rebuild,
stale response, and retry behavior.

### 6.2 Scenario phases

1. **Pending work restart**
   - commit graph data;
   - interrupt daemon before semantic work completes;
   - restart;
   - assert work resumes and converges without duplicate active records.

2. **Persistence across restart**
   - converge fully;
   - restart without rebuild;
   - assert equivalent direct semantic results and index state.

3. **Rebuild from graph truth**
   - invalidate or remove semantic index state using supported admin/test
     operation;
   - rebuild/backfill from graph;
   - compare expected entity IDs/ranking/provenance class with baseline.

4. **Stale/out-of-order completion**
   - use scripted deterministic delay/error outcomes;
   - submit older and newer updates;
   - assert older completion cannot overwrite newer graph state.

5. **Failure and retry**
   - inject timeout, rate limit, malformed dimensions, and temporary error where
     deterministic provider supports it;
   - assert bounded retry, visible dirty/degraded state, final convergence or
     terminal failure per fixture.

### 6.3 Work items

- Add deterministic scripted outcome support in profile/corpus if not already
  present.
- Add environment driver operations for safe restart and optional index
  invalidation/rebuild trigger.
- Add recovery-specific artifacts:
  - restart timeline;
  - provider request script observations;
  - retry/failure timeline;
  - pre/post restart search comparison;
  - rebuild comparison report.
- Add typed assertions for duplicate records, stale overwrite, retry budgets, and
  persistent equivalence.

### 6.4 Acceptance criteria

- [ ] Pending semantic work resumes after restart.
- [x] Fully converged semantic index survives restart (accepted v0.21.3 baseline).
- [x] Rebuild produces equivalent expected semantic results (accepted v0.21.3 baseline).
- [ ] Stale older response cannot replace newer content.
- [ ] Retry/degraded states are observable and bounded.
- [ ] Recovery artifacts explain every interruption and convergence verdict.

The pending-work scenario is implemented with strict work-identity/generation
checks and a controlled scheduling boundary. Its acceptance item stays unchecked
until a freshly approved v0.21.4 environment run is retained and reviewed. Do not
substitute its unit tests for the first or last checklist item.

## 7. Milestone 4 — Provider protocol emulators

### 7.1 Goals

Add local emulator suites for provider protocol compatibility without live
credentials.

### 7.2 Work items

- Extend `cmd/mycel-provider-emulator` or add scenario-local emulator scripts for:
  - OpenAI/OpenRouter-compatible embeddings/generation;
  - Anthropic-style generation/enrichment where connector support exists.
- Add fixture routes for:
  - success;
  - 429 with `Retry-After`;
  - 5xx;
  - timeout/cancellation;
  - malformed JSON;
  - invalid dimensions;
  - refusal/policy-blocked output;
  - routed provider/model provenance.
- Add profile mode `emulator` with base URL and no secrets.
- Add the canonical `semantic-graph-index-protocol` suite for initial
  OpenAI-compatible embedding protocol coverage.
- Assert request mapping without storing sensitive request bodies by default.
- Add no-cross-provider-fallback assertions.

### 7.3 Acceptance criteria

- [x] Emulator profiles run without live credentials for the initial
      OpenAI-compatible embedding route.
- [x] Provider request IDs and safe diagnostics appear in sanitized emulator
      observation artifacts for the initial protocol scenario.
- [ ] Retry, timeout, malformed response, and invalid-dimension cases fail or
      recover according to fixture expectations.
- [x] Artifacts do not contain authorization headers or secret-like values by
      default; emulator profile artifacts disable body/header capture.

## 8. Milestone 5 — Live-provider suites

### 8.1 Goals

Add manually runnable, non-gating live-provider suites with bounded budgets and
invariant-based assertions.

### 8.2 Work items

- Add live profile examples with secret environment-variable references.
- Add the canonical `semantic-graph-index-live` suite for manually gated
  OpenAI-compatible embedding coverage.
- Add CLI/profile validation that fails on missing credentials before deployment.
- Add budget enforcement checks for:
  - max requests;
  - max concurrency;
  - retry count;
  - request timeout;
  - total timeout;
  - optional token/cost counters where provider reports them.
- Add invariant assertions:
  - expected entity appears within top K;
  - unrelated entities remain outside broad acceptance bounds;
  - lifecycle converges;
  - scope/authorization isolation holds;
  - requested/actual provider/model provenance is recorded where available.
- Ensure live artifacts default to no body/header capture.

### 8.3 Acceptance criteria

- [x] Live suites are opt-in and non-gating for the initial
      OpenAI-compatible embedding profile.
- [x] Missing credentials fail as configuration errors for destructive live
      execution after authenticated capability discovery.
- [x] Live runs use invariant assertions, not exact vector or score equality.
- [x] Cost/time/request budgets are enforced or the profile refuses to run for
      required daemon-owned budget capabilities.
- [x] Artifacts remain sanitized by default with body/header capture disabled.

## 9. Milestone 6 — Release-gate integration

### 9.1 Goals

Promote stable deterministic coverage into release qualification while keeping
live-provider suites optional.

### 9.2 Work items

- Add suite manifests to canonical Mycel Lab suite listing.
- Add the `semantic-graph-index-release-gate` aggregate suite for credential-free
  deterministic release qualification.
- Add dry-run prerequisite checks.
- Define artifact retention and redaction requirements for release runs.
- Add optional `workflow_dispatch` scaffolding for deterministic suites.
- Document criteria for making `semantic-graph-index-contract` a required gate.
- Keep live suites behind protected environments and separate reporting.

### 9.3 Acceptance criteria

- [x] Deterministic suites are discoverable as named suite manifests,
      including `semantic-graph-index-release-gate`.
- [x] Dry-run checks validate image/profile/driver planning for the aggregate
      release-gate suite in CI and local dry-run.
- [x] Release qualification docs describe when/how to run the suite.
- [x] Live suites remain excluded from credential-free release gates and require
      explicit selection.

## 10. Cross-repository prerequisites

Some issue #46 requirements cannot be completed solely in Mycel Lab. Track
separate issues/PRs when these are needed:

| Requirement | Repository | Needed for |
| --- | --- | --- |
| deterministic fake/token-basis embeddings and scripted outcomes | `mycel` | exact ranking, retry, stale completion |
| safe per-result semantic provenance in protobuf | `mycel-api` | Level 3 revision/provenance proof |
| regenerated SDK fields | `mycel-go-sdk` | Mycel Lab public client access |
| daemon storage/mapping of graph revision, generation, route/model identity | `mycel` | Level 3 proof, stale cursor/generation checks |
| direct semantic pagination/cursor contract | `mycel-api`, `mycel-go-sdk`, `mycel` | semantic pagination and cursor binding |
| provider protocol connectors/emulator compatibility fields | `mycel`, `mycel-api` where needed | Tier 1/Tier 2 profiles |

Mycel Lab tasks that depend on these fields should implement explicit capability
checks and blocked/skipped verdicts rather than pretending the proof exists.

## 11. Test strategy

### 11.1 Unit tests

- profile validation and secret redaction;
- corpus decoding and deterministic expectation derivation;
- assertion pass/fail behavior;
- artifact schema and redaction scanner;
- capability-gate decisions.

### 11.2 Harness integration tests

- dry-run suite resolution;
- fixture actor operation sequencing;
- synthetic semantic observation oracle tests;
- environment driver restart operation integration where possible.

### 11.3 Destructive reliability runs

- Compose deterministic contract suite;
- Compose deterministic recovery suite;
- local protocol emulator suites;
- optional k3d variants once Compose behavior is stable.

### 11.4 Manual live-provider runs

- canonical dry-run: `mycel-lab run suite semantic-graph-index-live --dry-run`;
- destructive run only with explicit credentials and budgets;
- use bounded corpora;
- retain sanitized artifacts;
- report separately from default gates.

## 12. Documentation deliverables

- Design document and implementation plan.
- Suite authoring guide for semantic corpus/profile resources.
- Operator guide for deterministic suite execution.
- Operator guide for emulator and live-provider execution.
- Artifact schema reference.
- Troubleshooting guide for common failures:
  - no semantic binding;
  - maintenance does not converge;
  - stale vector remains active;
  - dimensions mismatch;
  - missing credentials;
  - redaction finding.

## 13. Proposed issue/PR breakdown

1. **Docs and review plan**
   - land this design and implementation plan.

2. **Provider profile model and dry-run resolution**
   - model, validation, artifacts, tests.

3. **Corpus model and deterministic fixtures**
   - corpus schema, validation, golden expectations.

4. **Semantic fixture client helpers**
   - public API setup/search/maintenance helpers.

5. **Contract suite MVP**
   - create/backfill/search/projection source proof.

6. **Contract lifecycle expansion**
   - update/delete/transaction/isolation/filter/tie coverage.

7. **Recovery suite MVP**
   - restart/persistence/rebuild.

8. **Recovery failure expansion**
   - stale async, retry, degraded states.

9. **Protocol emulator profile support**
   - local emulator and request/response failure cases.

10. **Live-provider manual profiles**
    - docs, secret checks, budget enforcement, invariant oracles.

11. **Release-gate integration**
    - canonical listing, prereq checks, workflow docs.

## 14. Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Public API lacks provenance required by issue #46. | Use proof levels and capability gates; create cross-repo prerequisite issues. |
| Deterministic connector behavior diverges from lab expectations. | Keep independent golden vectors in Mycel Lab; do not share implementation code. |
| Suite runtime becomes too long for default CI. | Keep destructive suites explicit; promote only after runtime is measured. |
| Live-provider costs or secrets leak. | Keep live profiles manual/protected; enforce budgets; default no body/header capture; run redaction scan. |
| Flaky convergence waits. | Use bounded pollers with explicit maintenance/index/search observations in artifacts. |
| Provider-neutral design becomes vendor-shaped. | Require logical capability slots and profile bindings; keep vendor fixtures outside core scenario logic. |
| Recovery scenarios require unsafe internal operations. | Prefer public admin operations; if unavailable, mark scenario as blocked and create daemon/API prerequisites. |

## 15. Review questions

1. Should the first implementation target Compose only, or Compose and k3d?
2. Is `deterministic-token-basis-v1` the right first fixture strategy?
3. Which Level 3 provenance fields should be prioritized in daemon/API work?
4. Which suite should become a release gate first?
5. Should direct semantic pagination be part of issue #46 or split into a later
   API-focused issue?
6. What artifact retention policy is acceptable for deterministic versus live
   runs?
7. What is the minimum live-provider support required before closing issue #46,
   if any?

## 16. Initial definition of done for issue #46

A practical closure target for issue #46 could be:

- `semantic-graph-index-contract` passes in deterministic Compose mode;
- `semantic-graph-index-recovery` passes for restart, persistence, rebuild, and
  stale update behavior in deterministic Compose mode;
- provider profile and artifact contracts are implemented and documented;
- local protocol emulator scaffolding exists for at least the supported
  OpenAI-compatible embedding path, or is explicitly tracked as a follow-up;
- live-provider execution is documented and gated, even if no hosted profile is
  required in CI;
- artifacts prove semantic-index source diagnostics and pass redaction scans;
- unsupported Level 3 claims are tracked in cross-repo prerequisite issues rather
  than being claimed by Mycel Lab.
