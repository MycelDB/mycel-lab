# Provider-Neutral Semantic Graph Index Testing Implementation Plan

Status: Proposed  
Tracking issue: [MycelDB/mycel-lab#46](https://github.com/MycelDB/mycel-lab/issues/46)  
Primary repository: `mycel-lab`  
Related repositories: `mycel-api`, `mycel-go-sdk`, `mycel`

## 1. Purpose

This plan adds provider-neutral, artifact-backed reliability coverage for semantic graph indexing. It turns the scenarios in issue #46 into bounded implementation tranches that can be reviewed and landed independently.

The first deliverable is a deterministic, credential-free end-to-end contract suite. It must prove that:

- graph data is projected by an enabled semantic generation rule;
- embedding work reaches the physical semantic index;
- direct semantic search uses that index rather than lexical, hybrid, graph-scan, or hydration fallback;
- creation, update, deletion, transaction boundaries, backfill, restart, and rebuild preserve the declared contract;
- every pass or failure is supported by sanitized machine-readable artifacts.

Provider protocol emulators and manually runnable hosted-provider profiles follow only after the deterministic contract is stable. Hosted providers never define the core correctness contract.

## 2. Scope

### 2.1 In scope

- A resolved `ProviderProfile` resource with explicit capabilities and secret references.
- A deterministic embedding mode that needs no network or credentials.
- Reusable semantic fixture, lifecycle, maintenance, search, and oracle components.
- Two initial suites:
  - `semantic-graph-index-contract`
  - `semantic-graph-index-recovery`
- Exact deterministic assertions for vectors or vector identities, ordering, scores, revisions, retries, and lifecycle state when the public diagnostics make those claims observable.
- Local protocol emulators for supported provider protocols.
- Manually runnable, non-gating live-provider profiles with invariant-based assertions.
- Bounded request, token, retry, elapsed-time, concurrency, queue, and optional cost budgets.
- Sanitized artifacts that retain enough provenance to reproduce and review a run.
- Future `workflow_dispatch`, scheduled, release-gate, and required-CI integration.

### 2.2 Non-goals

- Treating a hosted model's exact floating-point output or ranking as stable.
- Assuming every provider implements embeddings, generation, structured output, and routing.
- Making live-provider credentials available to pull-request jobs.
- Recording prompts, projected graph data, provider bodies, authorization metadata, or TLS material by default.
- Calling internal daemon packages from Mycel Lab. End-to-end suites use public gRPC contracts; environment drivers are used only for deployment and fault operations.
- Using lexical, hybrid, graph-scan, or hydrated graph matches as substitutes for semantic-index proof.
- Adding unbounded semantic load to `go test ./...`. Unit and contract tests remain fast; environment-backed suites are explicit reliability runs.
- Publishing a new provider connector as part of the first deterministic tranche.

## 3. Current baseline and gaps

### 3.1 Mycel Lab baseline

The reliability harness already provides:

- YAML cluster, actor, scenario, and suite resources;
- deterministic phase events and bounded actor execution;
- public Go SDK/gRPC clients;
- compose and k3d environment drivers;
- resolved scenario snapshots, JSONL event/operation artifacts, Postgres run records, and final oracle reports;
- graph transaction and GQL read actors;
- restart, upgrade, backup/restore, log assertion, and recovery events.

The current schema is intentionally extensible, but behavior and assertion payloads are largely untyped maps. Semantic work must add typed decoding and validation at its package boundary instead of spreading unchecked map lookups through the runner.

### 3.2 Existing daemon surface

The released daemon and generated Go SDK already expose the main operations required by the suites:

- inference package/catalog administration;
- intelligence profile, credential, grant, and policy administration;
- semantic generation rule administration;
- semantic maintenance status, work listing, retry, cancellation, analysis, processing, and backfill;
- client semantic rule listing and direct semantic search;
- graph transactions and hydrated result reads.

The daemon also registers an in-process `fake` inference connector. Its current embedding output is a single constant vector, so it can prove plumbing but cannot support meaningful exact ranking, update, model-mismatch, or cursor tests. The first daemon prerequisite must add an explicitly selected deterministic fixture strategy while preserving the current default behavior for existing tests.

### 3.3 Observability gaps

The current public API can prove an initial path with:

- a direct `SemanticService.SemanticSearch` call;
- non-empty `semantic_rule_id`, `embedding_binding_key`, and `vector_record_id` in results;
- the rule binding's physical search-index state and live record count;
- an empty pending/running/failed maintenance queue;
- exact hydrated node identity and graph-state checks.

It does not currently expose all stored provenance needed for every assertion requested by issue #46:

- `SemanticDirtyWorkItem` internally stores first/last graph revision and work generation, but the admin protobuf omits them;
- `SemanticIndexState` internally stores the graph dirty checkpoint revision and configuration checkpoint, but client summaries omit them;
- stored semantic records retain source hash, endpoint/model/capability IDs, vector-space key, and dimensions, but direct search results expose only a subset;
- there is no public raw-vector diagnostic, and one is not required if an opaque vector-record version or deterministic fixture identity can prove the same contract without exposing data.

No test may claim exact revision, generation, model route, or source-hash coverage until a public, safe diagnostic exposes that value. Additive API work follows the normal dependency order:

1. `mycel-api` source `.proto` change;
2. `mycel-go-sdk` regeneration;
3. daemon adapter implementation;
4. Mycel Lab assertion enablement.

Generated protobuf files must never be hand-edited.

## 4. Design decisions

### 4.1 Separate capabilities

A provider profile declares capabilities independently:

- `embedding`
- `generation`
- `enrichment`
- `structured-output`
- `routing`

`embedding` is required for a semantic graph-index scenario. Other capabilities are optional and cannot silently satisfy it. Anthropic may be used for enrichment or generation while another provider supplies embeddings. OpenRouter may route a model but does not erase the routed provider/model identity from provenance.

### 4.2 Keep the core scenario provider-neutral

Core fixture and assertion logic refers to logical capability slots, not vendor names:

- `embedding.primary`
- optional `enrichment.primary`
- optional `generation.primary`

A `ProviderProfile` binds those slots to deterministic, emulator, or live implementations. The same corpus, graph mutations, and invariant assertions can therefore run against multiple profiles. Provider-specific request shaping and response parsing stay in daemon connectors or emulator adapters, not in scenario logic.

### 4.3 Use three proof levels

Every semantic assertion declares the strongest proof level it requires.

1. **Indexed-path proof**
   - Direct semantic RPC, not a general graph or hybrid query.
   - Physical search index is ready.
   - Each match carries semantic rule, binding, and vector-record identity.
   - Expected graph node identity is returned.
   - No semantic warning or declared fallback is present.

2. **Lifecycle/freshness proof**
   - Indexed-path proof.
   - Maintenance queues are drained with no permanent failures.
   - Index live-record counts and tombstone behavior match the fixture.
   - Update/delete observations replace or remove old matches.

3. **Revision/provenance proof**
   - Lifecycle/freshness proof.
   - Public diagnostics show the expected graph revision/work generation/config checkpoint.
   - Safe model endpoint, model, capability, vector-space, dimensions, and opaque source-version fields match the selected provider profile.

The first end-to-end scenario may land with proof levels 1 and 2 using existing APIs. Proof level 3 is a separate gate and cannot be simulated by timestamps or sleeps.

### 4.4 Direct semantic search is authoritative for semantic-only tests

A semantic-only pass must use `SemanticService.SemanticSearch`. This RPC has no lexical success path, and result vector-record identity proves a physical semantic record participated.

Hybrid behavior is tested separately through query/search diagnostics. A hybrid assertion must require the declared semantic source and may also assert lexical participation. A plausible node order without the required source diagnostics fails.

Hydration validates result visibility and graph consistency, but it is not source proof.

### 4.5 Deterministic embedding strategy

Add an opt-in fixture strategy to the daemon's existing fake connector. The strategy is selected by fixture model metadata or another explicit catalog field; it is never inferred from arbitrary user text and does not change the existing fake connector default.

`token-basis-v1` is the proposed initial strategy:

- fixed, declared dimensions;
- stable normalization and tokenization;
- fixture tokens map to canonical basis vectors;
- multiple fixture tokens produce a deterministic normalized combination;
- unknown text maps by a documented stable hash;
- request IDs and usage are deterministic and sanitized;
- no network, credentials, random source, wall clock, or mutable process-global counters affect output.

The canonical algorithm and vectors live in daemon unit tests. Mycel Lab keeps independent golden vectors for a small corpus so that a daemon and harness cannot accidentally agree through shared implementation code.

The deterministic connector also supports a finite fixture outcome script selected in endpoint/model metadata, never by magic text embedded in graph data. Script entries are keyed by a stable normalized-input digest and capability and may declare bounded delay, retryable/non-retryable error sequences, malformed dimensions, or a blocked response. Per-endpoint/script counters are concurrency-safe, observable through safe request accounting, and have explicit reset/restart semantics. Script validation rejects unbounded delay, attempts, payloads, or unknown outcomes. This supports exact Tier 0 retry and out-of-order-completion tests. Tier 1 emulators repeat those cases through real provider protocols and add HTTP/header/payload mapping coverage.

### 4.6 Provider profiles are resolved resources

Add a new reliability resource:

```yaml
apiVersion: reliability.mycel.io/v1alpha1
kind: ProviderProfile
metadata:
  name: deterministic-token-basis-v1
mode: deterministic
capabilities:
  embedding:
    connectorType: fake
    endpointKey: mlab-deterministic
    modelKey: mlab/token-basis-v1
    providerModelName: mycel-lab-token-basis-v1
    dimensions: 8
    vectorSpaceKey: mlab/token-basis-v1
    fixtureStrategy: token-basis-v1
budgets:
  maxRequests: 500
  maxConcurrentRequests: 4
  maxRetriesPerRequest: 2
  requestTimeout: 5s
  totalTimeout: 3m
artifacts:
  captureBodies: false
  captureHeaders: false
```

Live profiles contain secret references, never secret values:

```yaml
mode: live
capabilities:
  embedding:
    connectorType: openai-compatible
    endpointKey: provider-embedding
    modelKey: provider/model
    secretRef:
      env: MYCEL_LAB_EMBEDDING_API_KEY
budgets:
  maxRequests: 100
  maxInputTokens: 50000
  maxOutputTokens: 0
  maxEstimatedCostUSD: 2.00
  maxConcurrentRequests: 2
  maxRetriesPerRequest: 2
  requestTimeout: 30s
  totalTimeout: 10m
```

Resolution rules:

- the resolved scenario embeds the selected profile and its source SHA-256;
- only the environment variable name is persisted;
- a deterministic or emulator profile rejects secret references;
- a live profile rejects missing required secret values before provisioning;
- `--dry-run` validates references but never resolves secret values;
- unsupported required capabilities fail before deployment;
- a CLI provider-profile override changes only the profile selection and is recorded explicitly.

### 4.7 Keep corpus and expected results reviewable

Each semantic scenario owns a versioned corpus file beside its YAML. A corpus contains:

- stable logical node keys;
- labels and property values;
- explicit projected and excluded fields;
- optional parent/edge structure;
- deterministic fixture tokens;
- mutation steps;
- queries with expected IDs, order, score/fingerprint tolerances, and proof level.

Resolved snapshots embed the normalized corpus hash and fixture classification. Raw corpus values are embedded only for synthetic fixtures explicitly marked safe for artifact capture; otherwise the snapshot contains a redacted structural summary. Node UUIDs allocated at runtime are recorded in a safe key-to-ID map.

The corpus must include deliberately confounding data:

- an excluded field containing a query token;
- a node in another domain with the same projected text;
- a deleted node that was formerly the top match;
- an updated node whose old and new projections rank differently;
- a multi-node transaction used to detect partial visibility;
- at least one exact tie with a documented stable tie-breaker.

### 4.8 Use events for finite lifecycle steps and actors for continuous pressure

Finite deterministic lifecycle work is phase-event driven. Proposed event types are:

- `semantic-provider-provision`
- `semantic-rule-provision`
- `semantic-fixture-apply`
- `semantic-backfill`
- `semantic-maintenance-drain`
- `semantic-search-assert`
- `semantic-index-rebuild`
- `semantic-provider-fault` (emulator profiles only)

Event target maps are decoded immediately into typed semantic event structs and rejected on unknown fields.

Continuous recovery and load coverage uses actors:

- `semantic-search`: repeatedly issues bounded direct semantic searches and records source/freshness observations;
- `semantic-mutator`: applies a deterministic mutation sequence with transaction IDs and acknowledged graph revisions.

This separation makes the contract suite exact and stepwise while letting the recovery suite observe behavior before, during, and after faults.

### 4.9 Provision through public APIs

A semantic provisioner uses authenticated generated admin clients to create:

1. the inference package/catalog resources;
2. an intelligence profile;
3. a credential only when the selected profile requires one;
4. least-privilege grants and policy rules;
5. the semantic generation rule and embedding binding;
6. any requested initial backfill.

Run-scoped keys prevent concurrent suites from colliding. Cleanup runs in reverse dependency order and records every failure. Discarding a disposable environment is not a substitute for reporting cleanup failure.

Pre-existing-data coverage requires a phase that seeds graph nodes before the semantic rule is created. Provider resources may exist earlier, but rule creation and backfill ordering must remain visible in the event artifacts.

### 4.10 Treat semantic convergence as a condition, not a delay

`semantic-maintenance-drain` polls bounded public status APIs until all declared conditions hold:

- pending and running work are zero;
- retryable and permanent failures are zero unless explicitly expected;
- rule and search-index states match the expected state;
- live record count matches the fixture;
- required checkpoint revision/configuration generation is reached when those diagnostics exist.

The poller has an absolute deadline, per-call timeout, bounded exponential backoff with deterministic jitter disabled for deterministic suites, and a final diagnostic capture on failure. Fixed sleeps do not establish correctness.

### 4.11 Make budgets enforceable

Budgets have two enforcement layers because background semantic maintenance, not the harness, performs the actual provider calls.

The harness enforces an orchestration budget shared across all semantic events and actors:

- graph mutations, searches, admin calls, and maintenance actions;
- concurrent operations and harness retries;
- elapsed phase and run time;
- a reserved worst-case provider fan-out before scheduling work.

The daemon intelligence profile/policy enforces provider-facing limits where the public API supports them:

- provider request and retry counts;
- concurrent provider requests;
- input/output token limits;
- elapsed provider time;
- optional estimated cost.

The harness reconciles safe daemon usage diagnostics with its reservation ledger. Protocol-emulator request counts provide an independent exact check. A live profile cannot claim a provider limit that neither the daemon nor a reviewed provider/account control enforces; such a profile fails preflight rather than treating harness RPC counts as provider counts.

The harness stops scheduling new work when a limit is reached and fails the run if a required assertion has not completed. Provider-side retries and daemon-side retries must be visible separately where diagnostics permit, and the combined worst-case retry ceiling must be finite.

### 4.12 Sanitize at creation and before persistence

Secret safety is defense in depth:

- typed resources never expose secret values through JSON tags;
- resolved resources retain secret names only;
- gRPC metadata allowlists exclude authorization and cookie fields;
- provider request/response bodies are not captured by default;
- error strings pass through a redactor for configured secrets, bearer tokens, API-key-shaped values, PEM material, and known credential fields;
- artifacts are scanned after collection and before a run is reported successful;
- fixture graph values are classified, and live suites default to synthetic, non-confidential corpora;
- Kubernetes Secret data and daemon environment values are excluded from collected manifests;
- hashes use a run-local salt when hashing confidential values; the salt is not persisted.

A redaction scanner finding is a gating test failure. Its report contains file path, detector category, and byte range, but not the matched secret.

## 5. Proposed package and artifact layout

```text
internal/reliability/
  provider/
    profile.go              # typed ProviderProfile and validation
    resolve.go              # profile lookup, override, capability checks
    budget.go               # shared bounded request/token/time/cost budget
    redact.go               # structured redaction and artifact scanner
  semantic/
    corpus.go               # corpus schema and normalization
    fixture.go              # graph fixture operations
    provision.go            # inference/profile/policy/rule resources
    maintenance.go          # bounded convergence conditions
    search.go               # direct semantic observations
    assertions.go           # exact and invariant assertion engine
    state.go                # run-scoped IDs, revisions, generations
  actors/
    semantic_search.go
    semantic_mutator.go
  events/
    semantic.go             # typed event registration and dispatch
  oracle/
    semantic.go             # aggregation and final gating report

tests/reliability/
  provider-profiles/
    deterministic-token-basis-v1.yaml
    emulator-openai-compatible.yaml
    live-openrouter.example.yaml
    live-anthropic.example.yaml
  scenarios/
    semantic-graph-index-contract/
      semantic-graph-index-contract.yaml
      corpus.yaml
      README.md
    semantic-graph-index-recovery/
      semantic-graph-index-recovery.yaml
      corpus.yaml
      README.md
  suites/
    semantic-graph-index-contract.yaml
    semantic-graph-index-recovery.yaml
```

Each run writes:

```text
semantic/
  provider-profile.json       # resolved and redacted
  resources.json              # safe IDs and lifecycle state
  fixture.json                # key-to-node mapping and expected graph revisions
  operations.jsonl            # graph/rule/backfill/rebuild operations
  maintenance.jsonl           # bounded status observations
  searches.jsonl              # query IDs/hashes, source diagnostics, IDs, scores; no raw text by default
  budget.json                 # limits, consumed values, stop reason
  assertions.json             # every semantic assertion and evidence reference
  final-report.json
  final-report.md
  redaction-scan.json
```

All schemas include `schemaVersion`. JSONL records include run ID, scenario, phase, event/actor ID, monotonic sequence, start/end timestamps, daemon address identity, provider profile name/hash, and a bounded error category.

## 6. Implementation tranches

Every tranche ends with focused tests, `make test`, `make lint`, documentation updates, an exact-head independent review, and clean regeneration checks where generated code is involved. Amendments after review require renewed validation and review.

### SGT0: Freeze the proof and compatibility contract

Repository: `mycel-lab`  
Risk: low

1. Add this plan and an issue-to-tranche checklist.
2. Inventory exact released API fields used for each proof level.
3. Add fixture corpus and artifact JSON schema drafts under testdata.
4. Define compatibility policy:
   - unsupported daemon APIs fail as `unsupported`, not pass or skip;
   - a suite declares its minimum daemon/API version;
   - deterministic golden changes require a corpus/profile schema version bump;
   - old artifacts remain readable by schema version.
5. Open separate linked issues for any approved API and daemon prerequisites.

Acceptance:

- every issue #46 scenario maps to a tranche and proof level;
- no assertion depends on an unobservable internal value;
- API additions, if any, have named owners and release order.

### SGT1: Provider profile, budget, and redaction foundations

Repository: `mycel-lab`  
Risk: medium

1. Add `ProviderProfile` parsing, catalog/import support, validation, and resolution.
2. Add scenario `providerProfileRef` and CLI override support.
3. Embed the redacted resolved profile and source hash in the resolved scenario.
4. Add shared request/token/time/cost budget accounting.
5. Add structured redaction and post-artifact secret scanning.
6. Reject live runs without required credentials; deterministic dry-runs remain credential-free.

Tests:

- valid deterministic, emulator, and live profiles;
- missing/unknown capability;
- invalid dimensions, budgets, mode/secret combinations, and unknown fields;
- secret values absent from resolved JSON, database records, errors, and artifacts;
- concurrent budget consumption cannot exceed a limit;
- cancellation and deadline enforcement;
- override changes only the resolved provider profile.

Acceptance:

- profile resolution is deterministic;
- no secret value reaches a persisted representation;
- budget exhaustion stops new work and produces a gating failure.

### SGT2: Deterministic daemon embedding prerequisite

Repositories: `mycel`, then `mycel-lab` fixture goldens  
Risk: medium

1. Add the explicit `token-basis-v1` fake-connector strategy.
2. Add finite endpoint/model fixture scripts for bounded delays, error sequences, blocked/refused outcomes, wrong dimensions, and safe request accounting.
3. Define deterministic, concurrency-safe attempt selection plus explicit process-restart/reset semantics.
4. Preserve the existing fake connector default.
5. Validate declared dimensions and return stable, finite, normalized vectors.
6. Return safe deterministic request IDs and usage.
7. Add daemon unit and integration tests for exact vectors, cancellation, delay, retry sequences, out-of-order completion, dimensions, unknown strategy/outcome, restart/reset semantics, and concurrent calls.
8. Publish the canonical algorithm and fixture-script documentation.
9. Add independent Mycel Lab golden vectors and scripts without importing daemon internal code.

Acceptance:

- identical normalized input and profile produce bit-for-bit identical output on supported architectures;
- different fixture axes produce distinct rankable vectors;
- no request performs network I/O or reads credentials;
- scripted work is finite, bounded, and independently countable;
- existing fake connector tests remain compatible.

Release dependency:

- the contract suite declares the first daemon release containing this strategy;
- until that release exists, local image overrides may be used and must be recorded by digest.

### SGT3: Semantic provisioning and finite event engine

Repository: `mycel-lab`  
Risk: high

1. Add run-scoped semantic resource state and safe artifact serialization.
2. Add public-API provisioning for catalog, profile, optional credential, grant, policy, rule, and binding resources.
3. Add reverse-order cleanup.
4. Add typed finite semantic events.
5. Add the bounded maintenance convergence poller.
6. Extend dry-run to render the operation plan without resolving credentials or mutating the daemon.

Tests:

- deterministic resource naming and dependency order;
- partial provisioning failure and cleanup;
- idempotent retry after an ambiguous admin response;
- missing access grant/policy fails closed;
- pre-existing nodes can be seeded before rule creation;
- maintenance timeout captures final status;
- cleanup never logs secret-bearing credential material.

Acceptance:

- a local daemon can be provisioned entirely through public APIs;
- dry-run reports all intended resources and limits;
- interrupted provisioning leaves a complete cleanup ledger.

### SGT4: `semantic-graph-index-contract`

Repository: `mycel-lab`  
Risk: high

Implement the deterministic compose-first contract scenario with these ordered checks:

1. **Pre-rule graph seed**
   - create stable fixture nodes and confounders;
   - record acknowledged transaction IDs and graph revisions.
2. **Rule creation and backfill**
   - provision the deterministic embedding binding;
   - run bounded backfill;
   - wait for maintenance convergence.
3. **Indexed retrieval**
   - assert exact top-K node IDs, order, tie-breaks, and deterministic scores or score tolerances;
   - require vector-record/rule/binding source proof.
4. **Post-rule creation**
   - create a new node and wait for its semantic record before asserting retrieval.
5. **Projection update**
   - update a projected field;
   - prove the old query no longer returns the stale record and the new query returns the replacement.
6. **Excluded-field update**
   - update only an excluded field;
   - prove the projected source identity and live semantic record remain unchanged after convergence;
   - record any conservative dirty-work analysis separately instead of assuming the internal work-item generation cannot advance.
7. **Deletion/tombstone**
   - delete the previous top match;
   - prove it is absent from semantic results and live-record counts converge.
8. **Transaction boundary**
   - prove no semantic work becomes visible for uncommitted graph changes;
   - after a multi-node commit converges, prove all committed changes are indexed from the acknowledged transaction/revision;
   - roll back a second transaction and prove it creates no semantic change;
   - record intermediate asynchronous indexing states, but require atomic semantic visibility only if the public product contract explicitly guarantees it.
9. **Rule lifecycle**
   - disable, re-enable, and modify a projection setting;
   - require explicit stale/rebuild behavior rather than silent compatible-looking reuse.
10. **Isolation and authorization**
    - verify same-content nodes in another space/domain are excluded;
    - verify unauthorized callers cannot list, search, hydrate, or infer through errors.

The suite fails unless every required assertion has an evidence reference. `final-report.json` carries a single gating `assertionsOk` field derived from required semantic assertions, graph-state assertions, budget status, and redaction scan status.

Tests:

- assertion engine unit tests with positive and adversarial evidence;
- missing source diagnostics cannot pass;
- lexical/hybrid-only evidence cannot satisfy direct semantic assertions;
- stale vector-record identity after update fails;
- partial transaction observations fail;
- pagination covers all expected matches without duplicate/skip;
- filter pushdown and post-filter behavior are distinguished in diagnostics.

Acceptance:

- one credential-free compose run is repeatable from a clean checkout;
- exact results and all lifecycle changes are artifact-backed;
- a deliberately corrupted expected ranking produces a failing run.

### SGT5: Public revision and provenance diagnostics

Repositories: `mycel-api` → `mycel-go-sdk` → `mycel` → `mycel-lab`  
Risk: high

Only fields justified by SGT0 are added. Prefer safe IDs and opaque record/configuration versions to raw projected text or vectors. A source fingerprint must be authorization-scoped and non-reversible (for example, a keyed, domain-separated digest); do not expose a plain digest of low-entropy projected text. Candidate additive fields are:

- maintenance work first/last graph revision and generation;
- semantic index graph-dirty checkpoint revision and configuration checkpoint;
- search result or diagnostic opaque source version, dimensions, vector-space key, model endpoint/model/capability IDs, and index generation;
- explicit semantic source/fallback reason for hybrid results when not already present.

Requirements:

- document confidentiality and authorization for each field;
- do not expose credentials, secrets, raw vectors, or projected source text;
- preserve wire compatibility;
- regenerate SDK and daemon bindings from `mycel-api`;
- add adapter tests that prove internal values map correctly;
- make Mycel Lab proof-level-3 assertions conditional on declared API support, never on best-effort inference.

Acceptance:

- exact source revision and generation claims are backed by public fields;
- model/vector provenance is sufficient to detect stale or incompatible reuse;
- older daemons fail the declared minimum-version/capability check clearly.

### SGT6: `semantic-graph-index-recovery`

Repository: `mycel-lab`  
Risk: high; destructive suite

Start with k3d and the deterministic provider profile. Add continuous semantic search and mutation actors around controlled events:

1. establish a converged semantic baseline;
2. restart one follower and prove uninterrupted or bounded-degraded reads;
3. restart the leader and prove retry/recovery behavior;
4. restart all replicas sequentially while retaining PVCs;
5. verify physical semantic records survive daemon restart;
6. invoke a supported public rebuild/recovery operation and rebuild derived semantic state from graph state; if no such operation exists, add it as a cross-repository prerequisite rather than deleting internal files in a running daemon;
7. interrupt backfill/rebuild, restart, resume, and prove no duplicate live records;
8. delay an older embedding response, commit a newer projection, and prove the older completion cannot overwrite the newest graph state;
9. repeat update/delete while one replica is unavailable;
10. finish with exact graph, maintenance, index-count, and semantic retrieval assertions on every reachable ordinal.

The suite records cluster identity and image digest and uses bounded child commands. It never deletes authoritative graph state when testing derived-index rebuild.

Acceptance:

- restart persistence and rebuild correctness are independently demonstrated;
- post-recovery acknowledged writes and successful semantic reads prove progress;
- no leaderless or unhealthy required Raft group is accepted;
- no pending, running, retryable-failed, or permanent-failed semantic work remains;
- final semantic results and graph state converge on all declared read targets.

### SGT7: Local protocol emulators

Repositories: `mycel-lab`, with daemon connector changes only when required  
Risk: medium

Add bounded local emulators for connector protocols that exist in the daemon. The emulator is a small dedicated command/package with no third-party credentials. It supports scripted cases:

- valid embeddings and, where supported, generation and structured output;
- malformed/truncated JSON, invalid structured output, refusal/content-policy responses, and wrong dimensions;
- authentication-header construction plus 401/403 failure without persisted secrets;
- 408/429/5xx retryable failure with bounded `Retry-After`;
- delayed response and cancellation;
- partial/batch response if the connector supports batching;
- changing route/model identity;
- usage fields and safe request IDs.

Deployment must work in compose and k3d without exposing the emulator to the public network. The resolved environment records the emulator image/binary checksum and script hash.

The emulator records only a sanitized request envelope: method, path, content length, capability, model, sequence, and body hash. Body capture is off.

Acceptance:

- protocol error categories and retry behavior are deterministic;
- deadlines cancel provider work and leave bounded queues;
- malformed dimensions fail closed without publishing a bad semantic record;
- retry tests prove the configured combined retry ceiling.

### SGT8: Advanced deterministic matrix

Repository: `mycel-lab`  
Risk: medium

Expand the reusable contract components across the remaining issue #46 matrix:

- projection variants: property, nested property, label, node ID, relationship context, ordered collections, and Unicode normalization;
- multiple rules/bindings/models and purpose selection;
- pagination and compatible/incompatible cursor reuse;
- exact filters and tenant/domain isolation;
- hybrid ranking source/fusion diagnostics;
- schema/rule evolution, model and dimension mismatch, credential rotation, and policy denial;
- idempotent duplicate dirty events and retry/replay;
- bounded queue pressure and backpressure;
- snapshot/backup/restore behavior where semantic derived state is included or deliberately rebuilt.

Each matrix row declares its required daemon capability and proof level. Unsupported cases fail or are explicitly reported `not-run` only when the suite marks them optional; required release-gate cases cannot skip.

### SGT9: Manual live-provider profiles

Repository: `mycel-lab`  
Risk: medium; external cost and confidentiality

Add example profiles and an operator command for manually initiated runs. Initial live runs are non-gating.

Rules:

- explicit live execution fails if credentials, model, route, budget, or network policy are missing;
- live assertions use invariants: expected top-K membership, lifecycle transitions, isolation, source proof, freshness, and provenance;
- exact vectors, exact scores, and stable total ordering are forbidden unless the provider offers an explicit deterministic guarantee and the profile opts in;
- model/route identifiers and provider request IDs are retained only when classified safe;
- prompts/projected data and response bodies are not retained by default;
- synthetic corpora are required unless an operator explicitly selects an approved data classification;
- budget limits are lower than provider account limits and are enforced by the daemon before each provider request, with the harness reserving worst-case fan-out before each triggering operation.

Provider sequencing:

1. an OpenAI-compatible embedding profile exercises the existing connector surface;
2. OpenRouter is added when routing configuration and provenance are supported by the daemon;
3. Anthropic generation/enrichment is added when the matching connector exists;
4. Anthropic is not treated as an embedding provider unless it exposes a supported embedding capability;
5. composed profiles may use one provider for enrichment and another for embeddings only after that pipeline is an explicit daemon contract.

Acceptance:

- a missing credential produces a failing preflight, not a passing skipped run;
- changing only the provider profile can rerun the same provider-neutral scenario;
- invariant failures preserve sanitized evidence and budget consumption.

### SGT10: CI and release integration

Repository: `mycel-lab`  
Risk: medium

Adopt tiers gradually:

- **Tier 0:** unit/golden contracts in normal CI; no network or credentials.
- **Tier 1:** deterministic compose and local protocol-emulator integration jobs.
- **Tier 2:** protected manual `workflow_dispatch` live-provider jobs; non-gating.
- **Tier 3:** opt-in scheduled, release-candidate, or required jobs after flake, cost, and secret-handling reviews.

Before Tier 2:

- use a protected GitHub environment;
- restrict workflow permissions and secret access;
- pin actions and external images by immutable digest;
- set job and command timeouts;
- set concurrency limits and cancellation behavior;
- upload only artifacts that passed the redaction scanner;
- document incident response for credential exposure;
- record daemon, harness, provider-profile, corpus, emulator, and workflow identities.

Promotion to required CI needs a measured reliability window, documented provider quota/cost ceiling, and explicit maintainer approval. A hosted-provider outage must not be silently reclassified as product success.

## 7. Scenario-to-tranche traceability

| # | Issue #46 scenario | Primary tranche(s) |
| ---: | --- | --- |
| 1 | Committed graph node becomes semantically searchable | SGT4, SGT5 |
| 2 | Projection selects only intended graph data | SGT4; entity variants in SGT8 |
| 3 | Update replaces the previous embedding | SGT4 |
| 4 | Delete removes semantic results | SGT4 |
| 5 | Pre-existing graph data is backfilled | SGT4 |
| 6 | Transaction visibility | SGT4 |
| 7 | Freshness and revision tracking | SGT5 |
| 8 | Rapid updates and out-of-order completion | SGT2, SGT6 |
| 9 | Connector failure and retry | SGT2, SGT7 |
| 10 | Restart with pending indexing work | SGT6 |
| 11 | Index persistence across restart | SGT6 |
| 12 | Rebuild from graph truth | SGT6 |
| 13 | Cluster leader failover and replay | SGT6 |
| 14 | Backup and restore | SGT8 |
| 15 | Space and domain isolation | SGT4 |
| 16 | Authorization isolation | SGT4 |
| 17 | Metadata/filter interaction | SGT8 |
| 18 | Deterministic ranking and ties | SGT4 |
| 19 | Pagination and cursor binding | SGT5, SGT8 |
| 20 | Hydration after candidate selection | SGT4, SGT8 |
| 21 | Embedding dimension/model changes | SGT5, SGT8 |
| 22 | Projection/schema changes | SGT4, SGT8 |
| 23 | Empty and unsupported values | SGT8 |
| 24 | Large backfill under concurrent writes | SGT8 |
| 25 | Hybrid lexical plus semantic search | SGT8 |
| 26 | Enrichment plus embedding composition | SGT9, after daemon support |
| 27 | Direct embedding profile | SGT9 |
| 28 | Provider/model migration | SGT5, SGT8, SGT9 |
| 29 | OpenRouter routing provenance | SGT9, after daemon support |
| 30 | Cross-provider invariant comparison | SGT9 |
| 31 | Provider outage | SGT7, SGT9 |
| 32 | Partial enrichment failure | SGT2, SGT7, SGT8 |
| 33 | Model response drift | SGT9 |
| 34 | Malformed, refused, or policy-blocked output | SGT2, SGT7, SGT8 |
| 35 | Provider rate and cost controls | SGT1, SGT7, SGT9 |
| 36 | Secret and artifact hygiene | SGT1 and every later tranche |
| 37 | Live bounded backfill | SGT9 |
| 38 | Live recovery | SGT6 components, composed in SGT9 |

The provider-neutral profile contract is SGT1, deterministic connector support is SGT2, protocol emulation is SGT7, and CI promotion is SGT10; those cross-cutting tranches apply to the numbered rows above.

## 8. Validation strategy

### 8.1 Unit and schema tests

- provider profile and corpus decoding with unknown-field rejection;
- normalized hashing and deterministic resolution;
- budget accounting under concurrency;
- redaction scanner positive and negative fixtures;
- semantic assertion engine with missing, stale, duplicate, and contradictory evidence;
- maintenance convergence state machine;
- event timeout/cancellation behavior;
- artifact schema compatibility.

### 8.2 Integration tests

- generated admin/client gRPC clients against a real local daemon;
- deterministic catalog/profile/policy/rule provisioning and cleanup;
- exact fake-connector vectors and direct semantic search;
- graph update/delete to maintenance queue to physical index;
- emulator protocol failures and retry limits.

### 8.3 Environment-backed reliability tests

- compose contract suite;
- k3d recovery suite;
- negative controls that corrupt an expectation, disable the semantic rule, remove a grant, return a wrong vector dimension, and leave maintenance work failed;
- exact image/profile/corpus A/B runs where only the intended input changes.

### 8.4 Required evidence for review

For each scenario implementation PR, retain:

- exact head and clean-tree identity;
- resolved scenario, provider profile, corpus hash, and image digest;
- command and exit status;
- semantic final report and all assertion evidence references;
- budget and redaction reports;
- unit/integration/reliability test logs;
- generated-file cleanliness where API changes are involved.

## 9. Failure semantics

The following always fail a required suite:

- missing required credential or provider configuration on an explicitly requested live run;
- unsupported required capability;
- semantic query success without semantic source proof;
- unexpected fallback;
- stale or incompatible cursor accepted;
- graph revision/config generation below the declared target when proof level 3 is required;
- pending/running/permanent-failed work at the final barrier;
- wrong dimensions, vector space, model route, opaque source version, live count, or node identity;
- partial transaction visibility;
- budget exhaustion before required assertions complete;
- timeout, cancellation leak, unbounded retry, or queue growth;
- secret/redaction scanner finding;
- artifact write failure for required evidence;
- provider outage in a run that was explicitly requested and started.

Optional exploratory checks may be reported `not-run` only when the suite definition marks them optional and records a structured reason. `not-run` never counts as pass.

## 10. Rollout and merge order

Recommended first milestone:

1. SGT0 plan/proof inventory.
2. SGT1 provider profile, budget, and redaction foundations.
3. SGT2 deterministic daemon connector prerequisite.
4. SGT3 semantic provisioner and finite event engine.
5. SGT4 deterministic contract suite.
6. SGT5 public revision/provenance diagnostics.
7. SGT6 deterministic recovery suite.

The first milestone is complete only when both named suites have artifact-backed positive runs and deliberate negative controls fail for the intended reason.

Subsequent milestone:

1. SGT7 protocol emulators.
2. SGT8 advanced deterministic matrix.
3. SGT9 manual live profiles.
4. SGT10 CI/release promotion.

No tranche is merged without exact-head review and repository-required CI. Cross-repository tranches merge and release strictly in dependency order. Mycel Lab must not reference an unreleased API/SDK tag in a release branch.

## 11. Completion criteria

Issue #46 can close when:

- `semantic-graph-index-contract` and `semantic-graph-index-recovery` are canonical Mycel Lab suites;
- deterministic runs need no network or credentials and assert exact contract results;
- semantic-index use, freshness, revision, generation, and safe provenance are publicly observable and asserted;
- create/update/delete/transaction/backfill/restart/rebuild behavior is covered;
- isolation, authorization, mismatch, retry, and bounded-failure cases fail closed;
- provider profiles cleanly separate embedding, generation/enrichment, structured output, and routing;
- protocol emulators cover the supported connector surfaces;
- Anthropic/OpenRouter-ready live profiles are documented and manually runnable when their required daemon capabilities exist;
- live runs use invariant assertions and fail on missing configuration;
- all work is bounded, sanitized, and artifact-backed;
- future CI workflows can run the same suites without changing their correctness logic.
