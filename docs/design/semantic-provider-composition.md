# Semantic provider composition contract

Related: [provider profiles](provider-profiles.md),
[issue #46 acceptance audit](../implementation/semantic-graph-index-issue46-acceptance-audit.md).

## Scope and compatibility

A provider is a set of capabilities, not a promise that it supplies embeddings.
Generation/enrichment can produce text for a distinct embedding provider. The
contract does not implement or assume Anthropic/OpenRouter daemon connectors.

A semantic scenario may declare an ordered `semantic.providerStages` pipeline:

```yaml
providerProfileRef: deterministic-token-basis-v1
semantic:
  providerStages:
    - name: enrich
      capability: generation
      profileRef: openai-compatible-procedure-emulator
      inputFrom: graph
    - name: embed
      capability: embedding
      profileRef: deterministic-token-basis-v1
      inputFrom: enrich
```

This fragment illustrates a valid **planning contract**, not a runnable composed
v0.21.4 semantic rule. The full scenario still needs its ordinary semantic rule,
controller, and maintenance configuration.

```mermaid
flowchart LR
    Graph[Committed graph projection] --> G[Generation or enrichment profile]
    G --> E[Separate embedding profile]
    E --> Index[Semantic record with provenance]
```

## Validation and provenance

- One to four explicitly linked stages; unique bounded names; no cycles,
  unknown fields, implicit dependencies, hidden fallback, or silent overrides.
- Exactly one terminal embedding stage; embeddings consume text. Structured
  output produces JSON and requires an explicit text-producing normalization
  stage before embedding. Routing preserves its input type.
- Each stage must advertise its own selected capability. A generation-only
  profile need not declare dimensions or an embedding slot.
- The terminal profile must match the scenario's `providerProfileRef`; a CLI
  override cannot silently rewrite a composed stage.
- Per-stage evidence identifies the exact profile source SHA-256, profile name,
  mode, connector, endpoint/model keys, vector shape where applicable, and input /
  output types. It contains no credential values, provider payloads, or invented
  actual-route provenance.
- Profiles retain their own finite validated budgets. The planned combined
  wall-clock ceiling is finite and at most 24 hours; this ceiling is planning
  metadata, not evidence that an unimplemented adapter enforced a live budget.
- Status is `planned`, never `passed`. Unsupported runtime adapters are listed
  explicitly. A plan does not invoke a provider or resolve a credential.

## Pinned v0.21.4 runtime gate

The current Lab adapters provision direct semantic embeddings through either
`fake/token-basis-v1` or `openai-compatible`. The pinned public semantic rule API
contains source assembly plus embedding bindings, not a native LLM enrichment
stage. Consequently the example above is rejected **before deployment and before
secret lookup**, even in dry-run, with `CompositionCapabilityError` identifying
`semantic-stage/generation/openai-compatible`.

An explicit single embedding stage remains executable through the existing
adapter. Required-adapter capabilities come from implemented harness code, not
an operator CLI claim. Do not make a compound pipeline appear to work by dropping
its generation stage, relabelling embedding as enrichment, running an unrelated
procedure-inference case, or returning an empty successful result.

A future native pipeline needs the corresponding daemon/API/SDK or public-API
adapter, per-stage authorization and secret lifecycle, actual provenance, bounded
execution, and environment-backed acceptance. Its scenario/dataflow contract can
remain unchanged; merely adding a capability name to a map is not implementation.
