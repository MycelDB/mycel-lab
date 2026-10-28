# SGT0 Semantic Graph-Index Proof and Compatibility Contract

Status: Candidate
Parent plan: [Semantic graph-index testing implementation plan](semantic-graph-index-testing-implementation-plan.md)
Tracking issue: [MycelDB/mycel-lab#46](https://github.com/MycelDB/mycel-lab/issues/46)

## 1. Purpose

SGT0 freezes what Mycel Lab can prove with the released API, what it cannot yet prove, how semantic reliability artifacts are versioned, and which cross-repository prerequisites own the missing product behavior.

This contract prevents later suites from turning timestamps, queue observations, plausible hydrated nodes, or generic hybrid results into stronger semantic-index claims than the public product surface supports.

SGT0 does not execute a daemon or define provider profiles. The first environment-backed behavior lands in SGT2–SGT4.

## 2. Reviewed release baseline

The inventory targets the coordinated release line below. The checked-out `develop` trees match the corresponding release trees for the files in this inventory.

| Component | Release | Commit |
| --- | --- | --- |
| `mycel-api` | `v0.20.1` | `c47263158ce009c1fe8825cb6b395d1f4e968ec0` |
| `mycel-go-sdk` | `v0.20.1` | `b772c6f9f154ca0fe0ed9fd2a77095912d597287` |
| Mycel daemon | `v0.20.0` | `52de3ceeeb909f89509bb84cc871fb0230755fde` |

A later suite must record its actual API/SDK/daemon identity and capability probes. These versions are an inventory baseline, not an instruction to accept an older unsupported daemon silently.

## 3. Proof levels

### Level 1: indexed-path proof

A Level 1 assertion requires:

- `SemanticService.SemanticSearch`, not generic graph/query retrieval;
- a result with `semantic_rule_id`, `embedding_binding_key`, `record_id`, `node_id`, and score;
- a ready physical search-index state for the selected binding;
- exact expected node identity;
- no warning or diagnostic indicating stale, missing, degraded, or fallback behavior.

`record_id` is the public physical semantic record identity. Hydration confirms result visibility but is not source proof.

### Level 2: lifecycle/convergence proof

A Level 2 assertion adds:

- bounded maintenance polling;
- zero pending, running, retryable-failed, and permanent-failed required work at the terminal barrier;
- expected physical live-record count;
- update/delete/tombstone behavior with stale `record_id` candidates absent;
- graph reads confirming the expected committed entity state.

On the release baseline, Level 2 establishes terminal queue/index convergence but not exact graph-revision freshness. Harness lifecycle polling uses authorization-scoped Admin Semantic Maintenance status/work RPCs; client rule-summary queue counts are corroborating discovery metadata, not a substitute for maintenance authorization or work-item evidence.

### Level 3: revision/provenance proof

A Level 3 assertion additionally requires public, durable evidence for:

- each result's source graph revision;
- semantic generation/configuration identity;
- configured endpoint/model/capability, vector space, and dimensions;
- requested versus actual route when the provider returns actual-route data;
- distinct analyzer-ingestion and contiguous fresh-through graph revisions;
- bounded unresolved-gap/capacity state;
- source-safe direct semantic pagination where pagination is asserted.

The release baseline cannot satisfy Level 3. Timestamps, the analyzer checkpoint, and empty queues are not substitutes.

## 4. Released public API inventory

### 4.1 Graph commit and transaction evidence

Source: `mycel-api/api/proto/mycel/client/v1/session.proto`.

| RPC/message | Available evidence | Valid claim | Limitation |
| --- | --- | --- | --- |
| `BeginTransaction` / `GraphTransaction` | transaction/session/space/domain IDs, mode, state, `base_revision`, operation ID | The harness opened a transaction at an identified graph base revision. | Does not expose semantic work. |
| `CommitTransaction` / `TransactionCommit` | commit/transaction IDs, `committed_revision`, operation count/time, operation ID, optional idempotency identity | A graph mutation committed durably at an acknowledged revision. | Does not state when semantic indexing reaches that revision. |
| rollback/close/get transaction | terminal transaction state | Rollback/abort state can be correlated with absence of graph and semantic effects. | Absence must be checked through graph plus maintenance/search evidence. |

### 4.2 Direct semantic search

Source: `mycel-api/api/proto/mycel/client/v1/semantic.proto`.

`SemanticSearchRequest` provides:

- space and domain;
- optional semantic rule and binding selection;
- query text;
- limit;
- optional minimum score.

`SemanticSearchResponse` provides results and sanitized warnings. Each result provides:

- `semantic_rule_id`;
- `embedding_binding_key`;
- `record_id`;
- `node_id`;
- score;
- optional hydrated node, matched chunk IDs, and snippet.

This is sufficient for Level 1 because the direct RPC has no lexical mode and exposes physical semantic record identity.

It does **not** provide:

- result source graph revision;
- generation/configuration identity;
- endpoint/model/capability/vector-space/dimension provenance;
- requested or actual route identity;
- page token or next-page token;
- filter input or filter execution diagnostics;
- raw vectors.

Raw vectors are not required for the public contract. Deterministic suites retain safe fixture vector identities/goldens and assert exact scores without exposing production vectors.

### 4.3 Rule and physical-index summaries

Sources:

- `mycel-api/api/proto/mycel/client/v1/semantic.proto`;
- `mycel-api/api/proto/mycel/admin/v1/semantic.proto`.

Available client summary fields include:

- rule ID/key/scope/enabled state;
- rule state;
- binding key/purpose/intelligence-profile ID and key/vector-store ID and key;
- physical search-index state, live-record count, last rebuild time, and last error;
- declared pending/running/retryable-failed/permanent-failed queue counts and last refresh/backfill/error timestamps.

On daemon `v0.20.0`, the client summary mapper populates only `queue_depth_pending`; running, retryable-failed, and permanent-failed remain protobuf-default zero even when corresponding work exists. No client-summary zero is accepted as terminal evidence on this baseline.

The Admin API can validate, create, update, enable/disable, and delete rules and exposes selector, projection source, embedding binding, maintenance, and storage policies.

These client-summary fields prove configured intent and provide only a non-authoritative status snapshot. They do not enumerate maintenance work and are not the SGT4 lifecycle-control surface. Test-side backfill control and the Level 2 terminal barrier require authorization-scoped `AdminSemanticMaintenanceService.GetSemanticMaintenanceStatus` plus targeted `ListSemanticMaintenanceWork` diagnostics from section 4.4.

These fields do not prove per-record revision, model resolution, generation, or contiguous freshness.

### 4.4 Semantic maintenance

Source: `mycel-api/api/proto/mycel/admin/v1/semantic_maintenance.proto`.

Available operations:

- status;
- work listing;
- retry/cancel;
- analyze/process;
- bounded backfill.

Available status evidence includes enabled/degraded state, four queue depths, oldest pending age, timestamps, throttle state, and analyzer/worker run counts.

Available work-item evidence includes rule/binding/target IDs, action/status, attempt count, scheduling/claim timestamps, and sanitized last error.

The public work item omits internal first/last graph revision, source transaction IDs, and work generation. Status timestamps do not establish revision freshness. `BackfillSemanticRule` can regenerate selected records, but there is no public physical-index invalidation/rebuild/recovery operation with pinned revision semantics.

### 4.5 Generic lexical/hybrid search

Source: `mycel-api/api/proto/mycel/client/v1/search.proto`.

Generic Search provides:

- lexical and hybrid modes;
- filters;
- page token and next-page token;
- result source kinds including semantic;
- score components and diagnostics;
- indexed/latest-known graph revisions and freshness state.

This surface can prove hybrid source participation and fusion behavior. It cannot substitute for direct semantic-only proof because:

- no semantic-only `SearchMode` exists;
- pagination belongs to the lexical/hybrid request identity;
- hybrid freshness cannot establish each returned semantic record's source revision/generation;
- a lexical result or hydration success cannot satisfy a semantic-only assertion.

### 4.6 Inference catalog, access, and usage

Sources:

- `mycel-api/api/proto/mycel/admin/v1/inference.proto`;
- `mycel-api/api/proto/mycel/admin/v1/intelligence_access.proto`.

Available catalog evidence includes endpoint connector/network/privacy/auth/operation metadata, model name/kind/dimensions/vector-space metadata, vector-store identity, and endpoint/model capability identity.

Available access evidence includes intelligence profiles, credential/grant/policy IDs, policy decisions, scopes, operations, and privacy requirements. Secret values are not returned.

Usage events provide request/provider-request IDs, configured endpoint/model/capability, status, token counts, latency, error fields, semantic rule, and binding. Usage summaries provide request/success/failure/denial and token/latency totals.

Current policy limits include maximum input tokens, maximum output tokens, and maximum requests per run. The API does not define atomic semantic-run budget scope, retry-attempt accounting, concurrency, aggregate tokens, batching mode/item limits, elapsed deadline, price identity, or cost reservation. Usage does not expose actual routed provider/model or batch item count.

### 4.7 Go SDK exposure

Source: `mycel-go-sdk/client.go` at `v0.20.1`.

The SDK exposes generated clients through:

- `Client.Semantic` and `Client.Search`;
- `AdminClient.Semantic`;
- `AdminClient.SemanticMaintenance`;
- `AdminClient.SemanticMigration`;
- `AdminClient.InferenceCatalog`;
- `AdminClient.InferenceProfiles`;
- `AdminClient.InferenceCredentials`;
- `AdminClient.InferenceGrants`;
- `AdminClient.InferencePolicies`;
- `AdminClient.InferenceUsage`.

These are raw generated service clients; the SDK has no high-level semantic-search convenience method. SGT3 can provision and observe current resources through the generated stubs without adding SDK convenience wrappers. Any new source protobuf fields/RPCs must land in `mycel-api`, then be regenerated and released in `mycel-go-sdk` before Mycel Lab consumes them.

## 5. Released daemon implementation inventory

Sources are from the `v0.20.0` daemon tree.

| Area | Current implementation | Consequence |
| --- | --- | --- |
| Fake embedding connector | `internal/inference/connectors/fake.go` returns a configured vector or constant `[1,0,0]` and keeps process-local hit counts. | Proves plumbing only; cannot provide input-sensitive ranking or restart-safe scripted outcomes. SGT2 prerequisite required. |
| OpenAI-compatible embedding | `internal/inference/connectors/openai_compatible.go` sends one input and returns vector, provider request ID, and usage. | Current provider protocol is usable for future emulator/live tests. It does not decode actual response model/route identity. |
| Semantic records | `internal/semantic/model/semantic.go` stores scope/rule/binding, source hash, configured endpoint/model/capability, access IDs, vector store/space, dimensions, vector, tombstone, and creation time. | No source graph revision or generation/configuration identity exists to expose. SGT5 requires domain/storage work, not adapter-only fields. |
| Dirty work | Internal `SemanticDirtyWorkItem` stores source transaction IDs, first/last graph revision, generation, status, attempts, and error state. | Useful state exists internally, but the public maintenance item omits revision and generation. |
| Index state | Internal `SemanticIndexState` stores `GraphDirtyCheckpointRevision` and semantic config checkpoint. | The graph-dirty checkpoint is the analyzer-consumption cursor, not freshness. |
| Analyzer | `internal/semantic/maintenance/analyzer.go` advances the checkpoint after work is enqueued. | The checkpoint can advance while required work remains unresolved; exposing it as freshness would be incorrect. |
| Fresh-through frontier | No persisted contiguous applied/fresh-through revision or bounded gap/capacity state. | Level 3 freshness is blocked. |
| Physical rebuild | Backfill exists; no supported public pinned rebuild/recovery contract was identified. | Recovery suite must not delete internal files; product/API prerequisite required. |

## 6. Claim matrix

| Claim | Baseline proof | Level | Baseline status | Required follow-up |
| --- | --- | ---: | --- | --- |
| Graph mutation committed | `TransactionCommit.committed_revision` | 1 | Available | None |
| Rule/binding configured | Admin rule plus client summary | 1 | Available | None |
| Direct semantic index participated | direct RPC plus non-empty `record_id`/rule/binding | 1 | Available | None |
| Exact deterministic ranking | direct result IDs/scores | 1 | Blocked by constant fake vector | Deterministic connector issue |
| Physical index ready/count | binding `SearchIndexStatus` | 2 | Available | None |
| Maintenance terminal queue state | admin `GetSemanticMaintenanceStatus` plus targeted `ListSemanticMaintenanceWork`; v0.20 client summary zeros are not evidence | 2 | Available | Admin maintenance authorization in SGT4 preflight |
| Backfill result counts | `BackfillSemanticRuleResponse` | 2 | Available | None |
| Update replaces stale record | before/after `record_id`, result set, live count | 2 | Available after SGT2 | Harness implementation |
| Delete/tombstone removes candidate | direct results plus graph read/live count | 2 | Available after SGT2 | Harness implementation |
| Failed/rolled-back overlay emits no semantic effect | transaction state, graph reads, target work listing, direct search | 2 | Partially observable | Deterministic failed-statement fixture and daemon integration assertion |
| Result source graph revision | none | 3 | Missing | Durable provenance plus API issue |
| Semantic generation/config identity | none | 3 | Missing | Durable provenance plus API issue |
| Contiguous semantic freshness | none; analyzer checkpoint is insufficient | 3 | Missing | Fresh-through/gap product plus API issue |
| Actual routed provider/model | none | 3 | Missing/optional by provider | Neutral inference result/storage/API extension |
| Direct semantic pagination | none | 3 | Missing | Source-safe pagination API issue |
| Semantic-only filters | none | 2/3 | Missing | Direct hard-eligibility filter contract in SGT5 / `mycel-api#36`, exercised in SGT8 |
| Supported physical rebuild | backfill only | 3 | Missing | Product/API rebuild prerequisite |
| Hard provider budgets | partial policy/usage fields | Cross-cutting | Missing | Atomic budget product/API prerequisite |
| Hybrid source/fusion | generic Search source/score diagnostics | 2 | Available | SGT8 harness implementation |
| Authorization isolation | scoped RPC authorization and hydrated visibility | 1/2 | Available | SGT4 harness implementation |

## 7. Compatibility policy

### 7.1 Capability and minimum-version gates

- Each suite declares minimum daemon/API capabilities, not only a version string.
- Required RPC/field/capability absence is `unsupported` and fails a required run.
- `UNIMPLEMENTED`, unknown required enum/state, or missing required diagnostic never becomes pass or skip.
- Optional exploratory cases may report `not-run` only when the suite marks them optional. `not-run` never counts as pass.
- A live run requested explicitly fails preflight when credential, model, route, network policy, or hard-budget enforcement capability is absent.

### 7.2 Schema versioning

- Every corpus, evidence record, and final report carries `schemaVersion`.
- A schema version is immutable once retained run artifacts consume it.
- Any field-shape addition/removal, normalization change, identity change, proof-semantic change, or requiredness change receives a new schema version.
- Readers dispatch by exact schema version. Unknown versions fail clearly; they are not decoded as the latest shape.
- Readers required for migration/review continue to read retained older versions.
- Unknown fields fail validation for the selected version.

### 7.3 Deterministic corpus and connector compatibility

- Corpus normalization and hashing are versioned.
- Provider profile and corpus SHA-256 values are stored in every evidence stream.
- A deterministic algorithm/golden-vector change requires a new fixture strategy and corpus/profile schema version; existing goldens are not rewritten.
- Query rankings may be compared exactly only when the selected provider profile declares deterministic output.
- Hosted-provider results use invariant assertions and never update deterministic goldens.

### 7.4 API and cursor compatibility

- Source `.proto` changes belong to `mycel-api`; generated SDK/daemon bindings are regenerated, never hand-edited.
- Additive wire compatibility does not imply proof compatibility. A suite enables a proof only after the daemon advertises and populates the required semantics.
- Direct semantic continuation is accepted only for the same authorization scope, graph view, query/vector identity, filters, rule/binding, model/vector-space, projection/index generation, ranking/tie-break version, and cursor position.
- Any changed source-safety field returns an explicit stale-cursor failure; generic hybrid cursors are not accepted as direct semantic cursors.

### 7.5 Artifact compatibility and safety

- Required evidence write failure fails the run.
- Required final assertions gate `assertionsOk`; report generation alone is not success.
- Artifacts contain only safe IDs, classified synthetic values, opaque hashes/versions, and sanitized errors by default.
- Secret values, authorization metadata, TLS material, raw provider payloads, unapproved graph values, and raw production vectors are prohibited.
- Redaction scan findings fail the run and identify detector/path/range without repeating the matched secret.

## 8. Draft schema artifacts

SGT0 adds draft contracts under `tests/reliability/testdata/semantic/v1alpha1`:

- `schemas/semantic-corpus.schema.json`;
- `schemas/semantic-evidence.schema.json`;
- `schemas/semantic-final-report.schema.json`;
- synthetic positive examples and contract-negative fixtures.

The corpus draft includes stable logical keys, scope isolation, node and edge fixtures, projection include/exclude paths, deterministic fixture tokens, discriminated node/edge create/update/delete operations, mutation disposition, graph-versus-semantic change expectations, query proof level, minimum score, and exact/invariant result expectations. `artifactCaptureSafe: true` is valid only for classified synthetic data.

The evidence draft defines common provenance, run/phase sequencing, safe semantic IDs/revisions, proof source, evidence references, and sanitized errors. Kind-specific payloads remain explicitly provisional until SGT3.

The final-report draft makes graph, semantic, budget, redaction, and artifact gates explicit. The schema already rejects `assertionsOk: true` unless every gate is true, failed/not-run summary counts are zero, every required assertion passes with evidence, and at least one artifact is referenced. SGT3 must additionally enforce cross-field summary arithmetic, evidence-reference resolution, artifact hashing, and redaction at runtime.

`make check-docs` parses every JSON document and checks basic schema metadata. Go contract tests compile all three schemas with a Draft 2020-12 validator, accept the positive examples, independently reject each true-verdict violation (false gate, non-zero failed/not-run summary, required non-pass assertion, empty required evidence, and empty artifact list), reject malformed-operation, unsafe-classification, and bounded-field fixtures, and require explicit decoder-limit annotations. Runtime readers must enforce `x-mycel-decoder-limits` before or while decoding; schema array/string/object bounds remain defense in depth. SGT1–SGT3 still own runtime decoding and per-evidence-kind payload schemas before these documents become executable inputs or output contracts.

## 9. Cross-repository prerequisites

The following issues are the only SGT0 product/API prerequisites. SDK regeneration is tracked by the corresponding API release work rather than a separate design issue.

| Owner | Prerequisite | Blocks | Tracking |
| --- | --- | --- | --- |
| `mycel` | deterministic input-sensitive fake embedding and finite scripted outcomes | SGT2–SGT4 exact rankings/retries | [MycelDB/mycel#237](https://github.com/MycelDB/mycel/issues/237) |
| `mycel` | durable per-record provenance, bounded analyzed/fresh frontiers, stale-write guard, direct hard-filter execution, migration, and supported rebuild | SGT5–SGT8 | [MycelDB/mycel#238](https://github.com/MycelDB/mycel/issues/238) |
| `mycel-api` | public semantic provenance/freshness/gap diagnostics, direct hard-eligibility filters, source-safe direct pagination, and rebuild control | SGT5–SGT8 | [MycelDB/mycel-api#36](https://github.com/MycelDB/mycel-api/issues/36), blocked by daemon semantics |
| `mycel` | atomic provider attempt/concurrency/batch/token/time/cost budgets | SGT1b and live SGT9 | [MycelDB/mycel#239](https://github.com/MycelDB/mycel/issues/239) |
| `mycel-api` | public budget capability, reservation, denial, batching, and usage accounting | SGT1b and live SGT9 | [MycelDB/mycel-api#37](https://github.com/MycelDB/mycel-api/issues/37), blocked by daemon semantics |

Prerequisite issue bodies must link Mycel Lab issue #46 and this contract, preserve the repository dependency order, and repeat the relevant boundedness/security requirements. They must not promise Anthropic/OpenRouter connectors.

## 10. SGT0 acceptance checklist

- [x] Implementation plan exists with all 38 scenarios mapped to a tranche, proof level, capability, and terminal oracle.
- [x] Released API/SDK/daemon identities are fixed.
- [x] Exact public proof fields and limitations are inventoried.
- [x] Internal analyzer checkpoint is explicitly rejected as a freshness boundary.
- [x] Proof levels distinguish direct semantic source, terminal convergence, and revision/provenance.
- [x] Corpus, evidence, and final-report schemas plus synthetic examples are drafted.
- [x] Compatibility, versioning, unsupported-capability, cursor, and artifact policies are defined.
- [x] Cross-repository prerequisite issues are filed and linked.
- [x] Exact-head validation and independent review pass at `82870b67e4d6f12ea1940ea573a8725c4243b416`.

SGT0 is complete. Beginning SGT1 requires separate operator authorization.
