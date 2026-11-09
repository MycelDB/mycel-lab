# compose-revision-fence

## Purpose

Exercise revision-fenced graph commits through the real Mycel gRPC endpoint.
The scenario uses deterministic graph fixtures and does not log witness bytes or
payload content.

## Qualification

The event performs two controls:

1. Read the target, mutate an unrelated node, and commit a target update with
the witness. This must succeed.
2. Read the target again, mutate that target, then attempt a stale derived write
with the old witness. This must return `ABORTED` with
`GRAPH_REVISION_FENCE_CONFLICT`, leave no stale mutation visible, and leave the
daemon usable.

The additive protobuf fields are sent and received as unknown wire fields by
this Lab checkout until the matching Go SDK release is consumed. This still
exercises the production wire contract.

## Evidence

Artifacts contain only bounded status/reason booleans and witness-presence /
size indicators. Witness contents and graph payloads are redacted.
