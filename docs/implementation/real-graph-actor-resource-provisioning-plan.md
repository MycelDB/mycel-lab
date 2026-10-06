# Real graph actor resource provisioning implementation plan

## Status

Draft implementation plan. This plan extends the reliability harness described in
[`docs/design/reliability-harness.md`](../design/reliability-harness.md) and the
initial harness plan in
[`docs/implementation/reliability-harness-implementation-plan.md`](reliability-harness-implementation-plan.md).

## Problem

Current `mycel-lab` scenarios can deploy a MycelDB cluster, expose Console
endpoints, run Kubernetes disruption events, and emit simulated actor events.
They do not yet execute real graph writes or reads against MycelDB. Actor
profiles such as `graph-committer` and `gql-reader` describe the intended
workload, but no spaces/domains are created and no graph transactions are sent to
live daemon pods.

A real graph actor cannot do useful work without a target space and domain. The
runner should own scenario resource provisioning so actors receive deterministic,
recorded assignments instead of each actor creating ad hoc resources.

## Goals

- Add a runner-managed setup phase that creates scenario-owned MycelDB resources
  before actors start.
- Create principals, spaces, and domains from resolved actor profiles and actor
  group counts.
- Pass resource assignments to actors, including principal credentials, space ID,
  domain ID, and daemon endpoint selection.
- Replace simulated graph committer/readers with real SDK/gRPC-backed actor
  implementations in incremental steps.
- Record all created resources, assignments, and cleanup decisions in run
  artifacts.
- Clean up scenario-owned resources when cleanup is enabled and the environment
  is retained or shared.
- Preserve disposable-cluster behavior: deleting the k3d cluster remains the
  final cleanup boundary.

## Non-goals

- Do not add dynamic raft membership.
- Do not implement production-grade tenant lifecycle management in the lab.
- Do not require cleanup to run after fatal environment loss; cleanup is
  best-effort.
- Do not make actors create their own spaces/domains except where a future actor
  profile explicitly requests actor-managed setup.
- Do not block initial implementation on Postgres-backed catalog execution.

## Current scenario semantics

For the current `raft-3-node-short-outage` scenario:

```yaml
actorGroups:
  - name: journal-users
    profileRef: graph-committer
    count: 4
  - name: readers
    profileRef: gql-reader
    count: 2
    target:
      actorGroups:
        - journal-users
```

The referenced `graph-committer` profile declares:

```yaml
dataScope:
  space:
    mode: per-instance
    prefix: journal
  domain:
    mode: per-instance
    schema: pkm-lite
    prefix: journal-domain
```

The intended resolved runtime shape is therefore:

- 4 writer actors;
- 4 scenario-owned writer principals, unless the profile selects a shared
  identity mode;
- 4 spaces, one per writer actor;
- 4 domains, one per writer actor;
- 2 reader actors that target the writer-owned spaces/domains.

## Proposed architecture

### Runner lifecycle

Extend `runner.RunScenario` with explicit lifecycle stages:

1. Deploy environment.
2. Wait for daemon pods/readiness.
3. Resolve daemon endpoints.
4. Log in as the bootstrap operator.
5. Provision scenario resources.
6. Start actors with concrete assignments.
7. Run phases and disruption events.
8. Stop actors.
9. Run final oracle checks.
10. Capture artifacts.
11. Cleanup scenario-owned resources when applicable.
12. Delete disposable environment unless retained.

### New packages

Suggested package layout:

```text
internal/reliability/mycelclient/   # thin Go client wrapper around Mycel APIs
internal/reliability/provision/     # resource planning, creation, cleanup
internal/reliability/assignments/   # actor assignment model/helpers, optional
internal/reliability/oracle/        # expand existing oracle checks for real graph state
```

`mycelclient` should be intentionally thin. Prefer the public Go SDK once it
exposes the needed admin/client APIs. If the SDK is incomplete, use generated
gRPC clients behind this package so later SDK migration is localized.

### Resource model

Add in-memory and artifact models similar to:

```go
type ScenarioResources struct {
    ScenarioName string               `json:"scenarioName"`
    Operator     OperatorSession      `json:"operator,omitempty"`
    Principals   []PrincipalResource  `json:"principals"`
    Spaces       []SpaceResource      `json:"spaces"`
    Domains      []DomainResource     `json:"domains"`
    Assignments  []ActorAssignment    `json:"assignments"`
    CreatedAt    time.Time            `json:"createdAt"`
}

type ActorAssignment struct {
    ActorID       string `json:"actorId"`
    GroupName     string `json:"groupName"`
    PrincipalID   string `json:"principalId"`
    Username      string `json:"username"`
    PasswordRef   string `json:"passwordRef,omitempty"`
    SpaceID       string `json:"spaceId"`
    DomainID      string `json:"domainId"`
    DaemonAddr    string `json:"daemonAddr"`
    TargetGroup   string `json:"targetGroup,omitempty"`
    TargetActorID string `json:"targetActorId,omitempty"`
}
```

Do not write plaintext per-actor passwords into ordinary logs. For local lab
artifacts, either avoid storing actor passwords or store a redacted `PasswordRef`
that points to an in-memory credential map. The bootstrap lab credential may be
fixed local-only config, but artifacts should still avoid normalizing plaintext
secret sprawl.

### Artifact output

Write the following artifacts:

```text
environment/scenario-resources.json
actors/assignments.json
actors/acknowledged-commits.jsonl
actors/read-checks.jsonl
oracle/final-report.json
oracle/final-report.md
```

`scenario-resources.json` should include stable IDs, names, ownership labels,
and cleanup status. It should not include bearer tokens or refresh tokens.

### Naming and ownership

Every created resource should be clearly scenario-owned:

```text
principal username: mlab-<run-short>-<group>-<ordinal>
space name:         mlab-<run-short>-<group>-<ordinal>
domain key:         mlab-<group>-<ordinal>
```

Attach labels/metadata if the daemon API supports it in the future. Until then,
store ownership in artifacts and use deterministic names.

### Endpoint assignment

Use available Console/lab endpoints or Kubernetes service DNS depending on where
actors run:

- In the current runner, actors run in the lab process on the host, so use local
  port-forward addresses when `--console-endpoints` or actor endpoints are
  enabled.
- If no local port-forwards are requested, create a runner-owned port-forward to
  one or more `myceld-N-client` services for actor traffic.
- Assign actors round-robin across ready node endpoints by default.
- Record the endpoint used by each actor.

## Implementation phases

### Phase 1: Provisioning plan and dry-run artifacts

- Add a provisioner that converts a resolved scenario into a resource plan.
- Interpret initial `dataScope` modes:
  - `space.mode: per-instance`;
  - `domain.mode: per-instance`;
  - `identity.mode: per-instance`.
- Generate actor assignments without calling MycelDB.
- Write `environment/scenario-resources.json` in dry-run and non-dry-run modes.
- Add unit tests for deterministic planning and target resolution.

Validation:

```sh
go test ./internal/reliability/provision ./internal/reliability/runner -count=1
mycel-lab run scenario-file tests/reliability/scenarios/raft-3-node-short-outage/raft-3-node-short-outage.yaml --dry-run
```

### Phase 2: Admin login and resource creation

- Add `mycelclient` operator login using bootstrap credentials.
- Add admin API calls for:
  - create principal;
  - create space;
  - create domain if not included by space creation;
  - list/get resources for idempotency where available.
- Provision resources after environment creation and before actor start.
- Make setup fail the run if required resources cannot be created.
- Add integration smoke against a one-node lab scenario.

Validation:

```sh
mycel-lab run scenario-file tests/reliability/scenarios/one-node-smoke/one-node-smoke.yaml \
  --confirm-destructive \
  --console-endpoints
```

Expected: Console shows created lab-owned spaces/domains during the run.

### Phase 3: Real graph committer actor

- Extend actor factory to receive `ActorAssignment` values.
- Implement `graph-transaction` behavior with real client calls:
  - login actor principal;
  - open session for assigned space/domain;
  - begin read-write transaction;
  - create nodes/edges according to operation mix;
  - commit;
  - record acknowledged transaction details.
- Keep simulated actor as a fallback only for dry-run or unsupported profiles.
- Add bounded retry semantics from actor profile.

Validation:

- One-node graph actor smoke creates visible graph data.
- Acknowledged commit JSONL is populated.

### Phase 4: Real GQL reader actor

- Implement `gql-read` behavior with real read-only queries.
- Resolve reader targets from writer actor assignments.
- Record read results, retry attempts, and observed counts.
- Add read-after-write sampling support for writer actors.

Validation:

- Reader actors observe increasing counts during a no-disruption scenario.
- Read checks are written to `actors/read-checks.jsonl`.

### Phase 5: Final oracle checks

- Compare acknowledged commits against final observed graph state.
- Implement initial `requireConvergedCounts` using per-space/domain node and edge
  counts.
- Implement initial `requireNoAcknowledgedGraphTransactionLoss` by checking that
  every acknowledged transaction marker can be read after recovery.
- Produce `oracle/final-report.json` and Markdown summary.

Validation:

- `raft-3-node-short-outage` passes under normal conditions.
- Intentionally broken scenarios produce actionable final reports.

### Phase 6: Cleanup semantics

- Add cleanup policy to runner options and future scenario spec:
  - `always`;
  - `on-success`;
  - `never`;
  - default `on-success` for retained/shared environments, cluster deletion for
    disposable k3d.
- Delete domains/spaces/principals in reverse dependency order where APIs allow.
- On cleanup failure, mark cleanup status in artifacts but do not hide the run's
  primary test result.

Validation:

- Retained environment runs leave resources when requested.
- Cleanup-enabled runs remove scenario resources and record cleanup status.

## Spec extensions

Initial implementation can infer resources from actor profiles. Later explicit
scenario resource declarations may be useful:

```yaml
resources:
  cleanup: on-success
  principals:
    mode: per-actor
  spaces:
    mode: from-actor-profiles
```

Do not require this spec extension for the first implementation tranche.

## Failure behavior

- If environment creation fails, no MycelDB resource cleanup is attempted.
- If provisioning fails after partial creation, best-effort cleanup should run
  unless `--keep-environment-on-failure` is set.
- If actor execution fails, stop all actors, capture state/logs, run any safe
  oracle checks, and then apply cleanup policy.
- If cleanup fails, preserve artifacts with enough IDs/names for manual cleanup.

## Security and secrets

- Do not log bearer tokens, refresh tokens, or per-actor passwords.
- Do not write plaintext actor passwords to normal artifacts.
- Fixed lab bootstrap credentials are local-only and should be documented as
  unsafe for non-disposable deployments.
- Prefer short-lived sessions for actors.

## Acceptance criteria

The initial complete feature is done when:

- `raft-3-node-short-outage` creates real spaces/domains before warmup;
- graph committer actors perform real committed graph transactions;
- reader actors perform real read-only GQL queries;
- final oracle checks validate acknowledged transaction visibility after the
  pod outage and recovery;
- Console can observe spaces, domains, raft mode, and graph activity while the
  scenario is running;
- artifacts describe resources, assignments, actor events, read checks, and
  final oracle status;
- cleanup behavior is documented and covered by tests.
