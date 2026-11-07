# Semantic graph-index reliability suites

Status: draft for review

Tracking issue: [MycelDB/mycel-lab#46](https://github.com/MycelDB/mycel-lab/issues/46)

Related implementation plan: [Semantic graph-index reliability suites implementation plan](../implementation/semantic-graph-index-reliability-suites-plan.md)

## 1. Purpose

Mycel Lab needs provider-neutral reliability suites that prove semantic graph
indexing works end to end. A passing run must prove that committed graph data is
projected, embedded, written to the semantic index, maintained across lifecycle
changes, and returned by a semantic-index search path. Returning a plausible
hydrated graph node is not sufficient.

This design turns issue #46 into an executable harness architecture that can
start with deterministic, credential-free Compose runs and later extend to local
provider protocol emulators and manually gated live provider profiles.

## 2. Goals

- Prove semantic indexing through public daemon APIs and preserved run
  artifacts.
- Keep core scenarios provider-neutral and capability-driven.
- Run the deterministic contract without external network access or secrets.
- Fail closed when freshness, provenance, dimensions, model identity, or source
  diagnostics are missing or inconsistent.
- Retain sanitized artifacts that let a reviewer understand why a run passed.
- Allow future Anthropic/OpenRouter-style generation, enrichment, routing, and
  live-provider profiles without rewriting the semantic lifecycle scenarios.
- Provide a phased path from manual destructive Compose runs to optional release
  gates.

## 3. Non-goals

- Implementing new daemon provider connectors in Mycel Lab.
- Making hosted-provider tests mandatory in pull-request CI.
- Comparing exact hosted-provider vectors or scores.
- Using lexical, hybrid, graph-scan, or hydration-only results as semantic proof.
- Storing secrets, authorization headers, provider request bodies, TLS material,
  or confidential graph content in artifacts by default.
- Calling daemon internal packages directly from Mycel Lab scenarios.

## 4. Design principles

### 4.1 Provider-neutral scenarios

Scenarios describe capabilities, not vendors. A semantic graph-index scenario
requires an `embedding` capability. Optional enrichment, generation,
structured-output, and routing capabilities can participate later, but they
cannot silently satisfy the required embedding role.

Provider-specific behavior belongs in one of three places:

1. daemon connectors and catalog resources;
2. Mycel Lab provider profiles that bind logical capability slots to concrete
   endpoint/model/configuration choices;
3. protocol emulator fixtures for request/response compatibility.

The lifecycle corpus, graph operations, convergence waits, and semantic oracles
should not need to know whether the provider profile is deterministic, emulator,
or live.

### 4.2 Deterministic first

The initial contract suite should use a deterministic fake embedding profile
with a stable token-basis strategy. Deterministic profiles allow exact assertions
for ranking, ties, stale update behavior, retry counts, failure modes, and
artifact provenance without credentials or network access.

Hosted-provider profiles can later assert stable invariants such as top-K
membership, scope isolation, convergence, and provider/model provenance. They
must not assert exact floating-point vectors or scores.

### 4.3 Fail closed

A semantic reliability suite must fail when:

- pending or failed work is reported as fresh;
- older asynchronous connector responses overwrite newer graph state;
- vectors with incompatible dimensions, vector-space keys, provider/model
  identities, projection versions, or index generations are mixed;
- semantic search falls back to lexical, hybrid, graph scanning, or hydration;
- an explicitly requested live profile lacks credentials or required budget
  enforcement;
- artifacts cannot prove which daemon build, harness build, profile, projection,
  graph revision, and semantic index generation were exercised.

### 4.4 Artifact-backed verdicts

Report generation alone is not enough. Each declared assertion contributes to
the final run verdict and emits enough sanitized evidence to explain its result.
A reviewer should be able to inspect artifacts and answer:

- what graph data was committed and at which revisions;
- which semantic rule and embedding binding selected it;
- which provider profile/model/vector-space produced vectors;
- whether maintenance/backfill/rebuild queues converged;
- which semantic records were returned by direct semantic search;
- whether freshness, retry, tombstone, and isolation expectations held;
- whether logs and artifacts remained secret-safe.

## 5. Suite architecture

### 5.1 Suite families

The issue-level catalog should be decomposed into independently reviewable suite
families:

| Suite | Purpose | Default gate |
| --- | --- | --- |
| `semantic-graph-index-contract` | Deterministic lifecycle/source proof, same-space domain isolation, and independent configured model/dimension/vector-space migration. | Manual destructive Compose first; candidate for required CI once stable. |
| `semantic-graph-index-recovery` | Restart, pending work, persisted index, rebuild from graph truth, stale async completion, replay/restore readiness. | Manual destructive Compose first; later release gate. |
| `semantic-provider-protocol-openai-compatible` | Local HTTP emulator for OpenAI/OpenRouter-style embedding/generation protocols. | Non-gating until stable. |
| `semantic-provider-protocol-anthropic` | Local HTTP emulator for Anthropic-style generation/enrichment protocol behavior. | Non-gating until connector support exists. |
| `semantic-graph-index-live` / `semantic-provider-*-live` | Manually runnable hosted-provider invariants under explicit credentials and budgets. | Manual only initially. |
| `semantic-graph-index-release-gate` | Credential-free aggregate of contract, recovery, and OpenAI-compatible protocol-emulator coverage. | Release qualification candidate; live excluded. |
| `semantic-provider-matrix-live` | Cross-provider invariant comparison for selected profiles. | Future scheduled/release environment only. |

The initial implementation should prioritize the first two suites and the
provider-profile/artifact contract needed by all later suites.

### 5.2 Runtime shape

A suite run follows this high-level flow:

1. Resolve scenario, corpus, provider profile, daemon image, environment driver,
   budgets, and artifact policy.
2. Provision the environment using the selected driver.
3. Install deterministic inference package/profile resources.
4. Create spaces/domains/principals and graph fixture data.
5. Create semantic generation rules and embedding bindings.
6. Drive lifecycle operations: create, update, delete, rollback, backfill,
   restart, rebuild, or failover depending on the scenario.
7. Wait for maintenance and search-index convergence through bounded public APIs.
8. Execute direct semantic searches and hydration checks.
9. Run typed oracle assertions.
10. Scan artifacts/logs for secret leakage.
11. Emit final verdict and cleanup.

### 5.3 Provider profiles

A provider profile binds logical capability slots to concrete catalog resources.
Profiles are resolved into non-secret run artifacts.

A deterministic profile should include:

- profile name and source checksum;
- connector type, endpoint key, model key, vector-space key, and dimensions;
- fixture strategy, optional scripted failures, batching mode, and retry policy;
- request, retry, concurrency, timeout, and total-run budgets;
- artifact capture policy.

A live profile should add secret references, never secret values. Resolution must
fail before deployment if a required secret or required budget-enforcement
capability is missing.

### 5.4 Corpus and expected-result fixtures

A semantic scenario owns a versioned corpus fixture. The corpus should describe:

- graph nodes and optional edges to create;
- labels, properties, payload, and metadata used by projections;
- source text tokens expected to influence deterministic vectors;
- excluded fields that must not influence retrieval;
- query fixtures and expected deterministic top-K order;
- lifecycle mutations and expected old/new match behavior;
- isolation principals, spaces, domains, and filters.

The corpus is reviewable input. The oracle derives exact expectations from the
corpus plus the selected deterministic provider profile, not from daemon internals.

## 6. Proof levels

The suite should expose the proof-level language already introduced by SGT0.

### Level 1: indexed-path proof

- Direct semantic search is used.
- Each result carries semantic rule, binding, and vector-record identity.
- Hydrated graph entity identity matches the expected fixture.
- The response exposes no fallback or warning that contradicts semantic-index
  source proof.

### Level 2: lifecycle/convergence proof

- Level 1 proof passes.
- Maintenance queues are drained or in the expected degraded state.
- Live record/tombstone counts match the lifecycle fixture.
- Create/update/delete/backfill/rollback observations match expected visibility.

### Level 3: revision/provenance proof

- Level 2 proof passes.
- Public diagnostics expose per-result source graph revision, semantic work
  generation, projection/configuration identity, provider/model/capability,
  vector-space, dimensions, and semantic fresh-through frontier.
- Cursor, generation, and stale-source invalidation rules can be asserted.

The deterministic contract can start with Levels 1 and 2 where the current
public API is sufficient. Level 3 assertions must remain gated until the daemon,
API, and SDK expose durable provenance safely.

## 7. Assertions and oracle model

The oracle should be typed, not a bag of unstructured map lookups. Suggested
assertion groups:

- `GraphCommitAssertion`: committed/rolled-back/staged data visibility.
- `ProjectionAssertion`: selected fields influence vectors; excluded fields do
  not.
- `MaintenanceAssertion`: pending/running/failed queues, retry counts, and
  convergence boundaries.
- `SemanticSearchAssertion`: direct search source proof, expected IDs, ranking,
  score class or exact deterministic score, and no fallback.
- `LifecycleAssertion`: update replacement, delete/tombstone, backfill exactly
  once, rebuild equivalence.
- `IsolationAssertion`: space/domain/principal authorization boundaries.
- `ProviderProfileAssertion`: selected profile, dimensions, vector-space,
  endpoint/model/capability identity, request counters, and budget conformance.
- `ArtifactSafetyAssertion`: secret redaction and forbidden-payload checks.

Each assertion emits structured events and a final verdict section.

## 8. Scenario coverage map

### Tranche A: contract suite

- committed node becomes semantically searchable;
- projection includes only intended fields;
- update replaces previous embedding;
- delete removes semantic result;
- pre-existing data is backfilled;
- transaction commit/rollback visibility;
- metadata/filter interaction;
- deterministic ranking and tie-breaking where supported;
- direct semantic search source proof;
- independent same-space domains with the same principal/node UUID, broad searches
  without rule/binding selectors, exact hydration, peer-only updates, and retained restart;
- separate selected-model, dimension, and vector-space changes requiring generation
  advancement and a strict rebuild fence before explicit authorized rebuild, followed by exact
  configured identity/shape/provenance and retained-record checks;
- artifact redaction.

The current contract definition contains three cases; the full credential-free
aggregate defines 16 cases / 65 phases. Definitions and local harness tests are not
runtime acceptance: fresh exact-source v0.21.4 qualification remains required.

### Tranche B: recovery suite

- restart with pending semantic work;
- persisted index survives restart;
- rebuild from graph truth;
- stale/out-of-order async response cannot overwrite newer state;
- connector failure/retry leaves visible degraded state and eventually
  converges;
- backup/restore or documented rebuild equivalence;
- cluster/failover coverage once the environment driver has stable primitives.

### Tranche C: protocol emulators

- authentication header construction without artifact leakage;
- endpoint/model/timeout/payload mapping;
- embeddings and optional generation/structured-output decoding;
- request IDs and safe provider diagnostics;
- HTTP 429 + `Retry-After`, 5xx, timeout, cancellation;
- malformed JSON, invalid dimensions, truncation, refusal/policy output;
- bounded retries and no provider fallback.

### Tranche D: live profiles

The initial Tranche D suite is `semantic-graph-index-live`, a canonical wrapper
for the existing manually gated OpenAI-compatible direct embedding scenario.
It covers:

- bounded corpus backfill and search invariants;
- provider/model provenance where available;
- scope and authorization isolation;
- freshness reaches expected committed revision;
- request/token/cost budgets and sanitized artifacts.

### Tranche E: release-gate integration

The initial Tranche E suite is `semantic-graph-index-release-gate`. It is a
credential-free aggregate of `semantic-graph-index-contract`,
`semantic-graph-index-recovery`, and `semantic-graph-index-protocol`. It is
suitable for release qualification dry-runs and explicitly excludes live-provider
credentials.

## 9. Artifact contract

Each run should retain:

- harness build identity and daemon image/build identity;
- resolved non-secret scenario and provider profile;
- corpus checksum;
- graph mutation IDs and committed revisions where public APIs expose them;
- semantic rule/binding IDs, projection version, and index generation where
  available;
- requested and actual provider/model identity where available;
- embedding dimensions and vector-space key;
- maintenance/freshness snapshots before and after each lifecycle operation;
- direct semantic search requests/responses after redaction;
- deterministic expected/actual ranking summaries;
- provider request counters and retry timeline;
- final assertion verdicts;
- redaction scan report.

Artifacts must not contain provider secrets, authorization headers, TLS secrets,
or confidential graph content unless an explicit safe debug mode is enabled for a
local-only environment.

## 10. Compatibility and dependency boundaries

Mycel Lab should not fake provenance that the daemon does not produce. When an
assertion needs state that is not yet public, the suite should record a blocked
capability and skip or fail with an explicit prerequisite, depending on the
selected suite contract.

Cross-repository prerequisites follow this order:

1. define daemon semantics and persistence;
2. update source protobufs in `mycel-api`;
3. regenerate SDK bindings;
4. implement daemon production/mapping;
5. add Mycel Lab assertions.

Generated protobuf files must not be hand-edited.

## 11. CLI and suite registration

The initial CLI should follow existing Mycel Lab conventions. Desired shapes:

```sh
mycel-lab run suite semantic-graph-index-contract \
  --confirm-destructive \
  --image myceldb/mycel:<candidate>

mycel-lab run suite semantic-graph-index-recovery \
  --confirm-destructive \
  --image myceldb/mycel:<candidate>
```

Provider profile selection can be explicit:

```sh
mycel-lab run suite semantic-graph-index-contract \
  --confirm-destructive \
  --provider-profile deterministic-token-basis-v1 \
  --image myceldb/mycel:<candidate>
```

Live profile execution must be opt-in and credential-gated:

```sh
MYCEL_LAB_PROVIDER_PROFILE=openrouter-live \
OPENROUTER_API_KEY=... \
mycel-lab run suite semantic-provider-openrouter-live \
  --confirm-destructive \
  --image myceldb/mycel:<candidate>
```

## 12. Open questions

1. Which daemon release/API version should be the minimum target for Level 1/2
   deterministic suites?
2. Which Level 3 provenance fields are already durable internally but not public?
3. Should direct semantic search gain pagination before or after the first
   deterministic contract suite?
4. Should the deterministic provider package live in Mycel daemon examples,
   Mycel Lab fixtures, or both?
5. What is the first supported environment driver: Compose only, or Compose and
   k3d together?
6. Which suite, if any, should become a release gate first?
7. What artifact retention and redaction defaults are acceptable for live
   provider runs?

## 13. Review checklist

- [ ] The design proves semantic-index use without lexical/hybrid/scan fallback.
- [ ] The first tranche can run without credentials.
- [ ] Provider-specific behavior is isolated to profiles/emulators/connectors.
- [ ] Assertions fail closed when diagnostics are missing.
- [ ] Artifact requirements are sufficient for release review.
- [ ] Level 3 claims are gated on real daemon/API/SDK support.
- [ ] Live providers are optional, bounded, and secret-safe.
