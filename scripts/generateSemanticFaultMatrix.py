#!/usr/bin/env python3
"""Generate finite credential-free #46 fixture definitions, or check drift.

These files are test inputs, not runtime evidence. No daemon is contacted.
"""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IMAGE = 'docker.io/myceldb/mycel@sha256:0973cddd9c282abff40d8edeb2e8f7a801e58d40bfbab4c8507ce2d9daae2dd7'
CASES = [
    ('rate-limit', {'kind': 'http-error', 'status': 429, 'retryAfterMS': 2000}, 'retryable', 'http-error', '429'),
    ('server-error', {'kind': 'http-error', 'status': 503}, 'retryable', 'http-error', '503'),
    ('transport', {'kind': 'connection-drop'}, 'retryable', 'connection-drop', '503'),
    ('malformed', {'kind': 'malformed-json'}, 'permanent', 'malformed-json', '200'),
    ('truncated', {'kind': 'truncated-json'}, 'permanent', 'truncated-json', '200'),
    ('dimensions', {'kind': 'wrong-dimensions', 'dimensions': 3}, 'permanent', 'wrong-dimensions', '200'),
    ('refusal', {'kind': 'refusal'}, 'permanent', 'refusal', '403'),
    ('deadline', {'kind': 'success', 'delayMS': 30000}, 'deadline', 'canceled', '499'),
    ('cancel', {'kind': 'success', 'gate': 'cancel-response', 'gateTimeoutMS': 25000}, 'cancel', 'canceled', '499'),
]


def definition(value, level=0):
    """Emit deterministic block YAML with JSON-quoted scalar strings."""
    indent = '  ' * level
    lines = []
    if isinstance(value, dict):
        items = [((json.dumps(str(k)) if str(k).isdigit() else str(k)) + ':', v) for k, v in value.items()]
    else:
        items = [('-', v) for v in value]
    for key, item in items:
        if isinstance(item, (dict, list)) and item:
            lines.append(indent + key + '\n' + definition(item, level + 1))
        else:
            lines.append(indent + key + ' ' + json.dumps(item) + '\n')
    return ''.join(lines)


def profile(name, outcomes):
    return {'apiVersion': 'reliability.mycel.io/v1alpha1', 'kind': 'ProviderProfile',
            'metadata': {'name': name, 'description': 'Generated finite local provider sequence; no hosted credentials.'},
            'mode': 'emulator', 'capabilities': {'embedding': {
                'connectorType': 'openai-compatible', 'endpointKey': 'local-openai-emulator',
                'baseURL': 'http://mycel-provider-emulator:18080/v1', 'modelKey': 'emulator/embedding-v1',
                'providerModelName': 'embedding-v1', 'dimensions': 8, 'vectorSpaceKey': 'emulator/embedding-v1/8',
                'emulatorScript': {'version': 'v1', 'requireMatch': False, 'maxRequests': len(outcomes), 'entries': [], 'requestSequence': outcomes},
                'batching': {'mode': 'single', 'maxItems': 1}}},
            'budgets': {'enforcement': 'advisory', 'maxOperations': 4096, 'maxRequests': len(outcomes),
                        'maxConcurrentOperations': 16, 'maxConcurrentRequests': 2,
                        'requestTimeout': '40s', 'totalTimeout': '10m'},
            'artifacts': {'captureBodies': False, 'captureHeaders': False}}


def event(kind, **target):
    return {'type': kind, 'target': target}


def base(name, provider, seed):
    return {'apiVersion': 'myceldb.io/reliability/v1', 'kind': 'Scenario',
            'metadata': {'name': name, 'description': 'Generated controlled semantic provider correctness case; requires fresh runtime qualification.'},
            'seed': seed, 'providerProfileRef': provider,
            'environment': {'driver': 'compose', 'namespace': 'mycel-lab', 'options': {
                'composeFiles': ['../mycel/tests/compose/cluster/compose.yml', 'tests/compose/ports.yml', 'tests/compose/semantic-pending-restart.yml'],
                'projectNamePrefix': 'mycel-lab-' + name.removeprefix('compose-'), 'imageOverride': IMAGE,
                'providerEmulatorImage': 'local/mycel-provider-emulator:dev', 'serviceNames': ['myceld-a', 'myceld-b', 'myceld-c'], 'grpcPorts': [19091, 19092, 19093]},
                'capabilities': {'required': ['per-node-endpoints', 'node-exec', 'rolling-restart', 'logs']}},
            'clusterRef': 'raft-3-node', 'clusterOverrides': {'image': IMAGE, 'raft': {'partitionCount': 16}},
            'actorGroups': [{'name': 'semantic-controller', 'profileRef': 'semantic-controller', 'count': 3,
                             'rate': {'mode': 'per-actor', 'commitsPerSecond': 1}}],
            'semantic': {'targetActorGroup': 'semantic-controller', 'rule': {
                'deferProvisioning': True, 'key': name, 'targetLabels': ['Document'], 'bindingKey': 'search',
                'sourceMode': 'self', 'includeProperties': ['text'], 'minimumTextLength': 1,
                'physicalIndex': 'exact', 'dirtyCooldown': '1ms', 'maxBatchSize': 16, 'workerConcurrency': 1},
                'maintenance': {'timeout': '30s', 'pollInterval': '100ms', 'analyzeLimit': 64, 'processLimit': 64}},
            'phases': [], 'assertions': {'final': {'requireHealthyCluster': True, 'healthConvergenceTimeout': '60s',
                                                'healthConvergenceInterval': '2s', 'requireSharedClusterIdentity': True}},
            'artifacts': {'collectScenarioSpec': True, 'collectActorEvents': True, 'collectReadChecks': True}}


def search(node, query, revision, **extra):
    return event('semantic-search-assert', query=query, minimumScore=0.99, expectedResultCount=1,
                 expectedNodeIds=[node], requireProvenance=True, expectSourceRevisionFrom=revision,
                 timeout='30s', pollInterval='100ms', **extra)


def document(name, proof):
    return f'''# {name}

Generated by `scripts/generateSemanticFaultMatrix.py`; edit the declarative matrix
there and regenerate. Definitions are not runtime qualification.

## Purpose

{proof}

## What this test proves

Requires public graph/work/freshness/search evidence, exact local protocol counts,
and no lexical, hybrid, scan, or hydration fallback. All declared assertions affect
the run verdict. Missing capabilities fail closed; no repair rebuild is allowed.

## Topology

Three disposable Compose Raft daemons and one local protocol emulator. Background
semantic scheduling is explicitly disabled so provider injection is controlled;
ordinary public Analyze/Process APIs remain enabled. Ports 19091–19093 are used
sequentially, with mandatory scenario cleanup.

```mermaid
flowchart LR
    Lab --> A[myceld-a]
    Lab --> B[myceld-b]
    Lab --> C[myceld-c]
    A <--> B
    B <--> C
    C <--> A
    A --> E[local provider emulator]
    B --> E
    C --> E
```

## Scenario phases

See the adjacent generated YAML for the exact finite ordering. Provider outcomes
are a global request sequence with no implicit successful fallback after exhaustion.
Expected negative outcomes must be witnessed before any authorized recovery.

## Actors

Three endpoint-local semantic controllers; only the selected isolated controller
scope owns the one-document corpus. No hosted-provider identity or secret is used.

## Tunable parameters

The daemon is pinned to immutable v0.21.4. Public operation timeout is 30 seconds;
profile total budget is ten minutes. Gates, cancellation, listing, RPCs, and waits
are bounded. Supply a clean revision-labelled emulator built from this exact Lab
source, not the development default. Changing a fixture or image requires fresh
evidence; do not weaken assertions to obtain a pass.

## Evidence and artifacts

Retain acknowledged graph revisions, work IDs/generations/revision ranges/attempts,
frontiers and gaps, controlled completion order or failure disposition, direct
semantic record provenance, finite sanitized provider observations, final oracle,
cleanup/absence, and redaction verdicts. The emulator's per-input attempt counter
counts all requests with the same input (including direct search), not just work
retries. Assert the exact request distribution by safe input digest. Fault cases
separately prove the public same-work attempt count; the ordering case proves
supersession and retained newer record identity. Connection-drop status 503 is a synthetic
observation marker: the emulator closes the connection without an HTTP response.

## Common failure modes

Early gate release, stale overwrite, false freshness, dropped/changed work,
wrong failure classification, missing retry delay, excess attempts, invalid source
provenance, request-sequence exhaustion, fallback, and leaked evidence fail the run.
Cancellation/deadline cases require unresolved work and explicit authorized retry;
retryable cases must respect not-before and recover through normal maintenance.

## When to run

Credential-free planning only:

```sh
mycel-lab run scenario {name} --dry-run
```

Real execution modifies disposable Compose resources and requires explicit fresh
operator approval of the exact clean harness, daemon, emulator, and artifact root.
No definition, unit test, or dry-run authorizes it or proves daemon behavior.
'''


def generated_files():
    files = {}
    names = []
    for i, (case, fault, disposition, outcome, status) in enumerate(CASES):
        name = 'compose-semantic-fault-' + case
        provider = 'semantic-fault-' + case
        node = f'00000000-0000-4000-8000-{4640+i:012d}'
        sequence = [fault, {'kind': 'success'}, {'kind': 'success'}]
        files[f'tests/reliability/provider-profiles/{provider}.yaml'] = definition(profile(provider, sequence))
        scenario = base(name, provider, 4640+i)
        target = {'nodeId': node, 'expectRevisionFrom': 'fault-commit', 'disposition': disposition,
                  'timeout': '30s', 'pollInterval': '100ms'}
        if case == 'rate-limit':
            target['minimumBackoff'] = '1500ms'
            target['maximumBackoff'] = '2500ms'
        if case == 'deadline':
            target['processTimeout'] = '200ms'
        if case == 'cancel':
            target['gate'] = 'cancel-response'
        statuses = {status: 1}
        statuses['200'] = statuses.get('200', 0) + 2
        scenario['phases'] = [
            {'name': 'commit-isolated-corpus', 'duration': '1ms', 'events': [
                event('semantic-node-create', nodeId=node, labels=['Document'], text='axis-0', captureRevisionAs='fault-commit'),
                event('semantic-rule-create', semantic='deferred')]},
            {'name': 'prove-failure-before-bounded-recovery', 'duration': '1ms', 'events': [event('semantic-fault-assert', **target)]},
            {'name': 'prove-direct-index-and-protocol-accounting', 'duration': '1ms', 'events': [
                search(node, 'axis-0', 'fault-commit'),
                event('semantic-provider-emulator-assert', minimumRequests=3, maximumRequests=3,
                      maximumAttemptsPerInput=3, expectedRequestsPerInput=[3],
                      requireSingleItem=True, authorization='absent',
                      expectedModels=['embedding-v1'], expectedOutcomes={outcome: 1, 'success': 2}, expectedStatuses=statuses)]},
        ]
        directory = f'tests/reliability/scenarios/{name}'
        files[f'{directory}/{name}.yaml'] = definition(scenario)
        files[f'{directory}/{name}.md'] = document(name, f'Inject `{case}`, prove the expected `{disposition}` state with no false freshness, then recover the same work with exactly two attempts.')
        names.append(name)
    suite = {'apiVersion': 'myceldb.io/reliability/v1', 'kind': 'Suite',
             'metadata': {'name': 'semantic-provider-failure-matrix', 'description': 'Finite local provider failure, backoff, cancellation, and recovery matrix.'},
             'scenarios': [{'path': f'../scenarios/{name}/{name}.yaml'} for name in names],
             'execution': {'mode': 'sequential', 'stopOnFailure': True}}
    files['tests/reliability/suites/semantic-provider-failure-matrix.yaml'] = definition(suite)
    name = 'compose-semantic-out-of-order'
    provider = 'semantic-out-of-order'
    node = '00000000-0000-4000-8000-000000004660'
    sequence = [{'kind': 'success'}, {'kind': 'success'},
                {'kind': 'success', 'gate': 'older-response', 'gateTimeoutMS': 25000}] + [{'kind': 'success'} for _ in range(4)]
    files[f'tests/reliability/provider-profiles/{provider}.yaml'] = definition(profile(provider, sequence))
    scenario = base(name, provider, 4660)
    scenario['phases'] = [
        {'name': 'establish-certified-old-revision', 'duration': '1ms', 'events': [
            event('semantic-node-create', nodeId=node, labels=['Document'], text='axis-0', captureRevisionAs='older-commit'),
            event('semantic-rule-create', semantic='deferred'),
            event('semantic-backfill', force=True, limit=1, expectedSelected=1, minimumGenerated=1),
            event('semantic-maintenance-await', timeout='30s', pollInterval='100ms'),
            search(node, 'axis-0', 'older-commit')]},
        {'name': 'newer-publication-before-older-completion', 'duration': '1ms', 'events': [
            event('semantic-ordering-assert', nodeId=node, gate='older-response', text='axis-2',
                  expectRevisionFrom='older-commit', captureRevisionAs='newer-commit', captureRecordAs='newer-before-release',
                  timeout='30s', pollInterval='100ms')]},
        {'name': 'reject-stale-vector-and-reconcile-real-completions', 'duration': '1ms', 'events': [
            event('semantic-search-assert', query='axis-0', minimumScore=0.99, expectedResultCount=0, forbiddenNodeIds=[node]),
            event('semantic-reliability-assert', requireFreshAtLatest=True, maximumUnresolvedGapCount=0, expectFreshRevisionFrom='newer-commit'),
            event('semantic-provider-emulator-assert', minimumRequests=7, maximumRequests=7, maximumAttemptsPerInput=4,
                  expectedRequestsPerInput=[3, 4], requireSingleItem=True, authorization='absent',
                  expectedModels=['embedding-v1'], expectedOutcomes={'success': 7}, expectedStatuses={'200': 7})]},
    ]
    directory = f'tests/reliability/scenarios/{name}'
    files[f'{directory}/{name}.yaml'] = definition(scenario)
    files[f'{directory}/{name}.md'] = document(name, 'Hold an older pinned backfill response, publish and search a newer graph revision, then release the older response. Require the old publication to be explicitly superseded and the newer record identity to remain unchanged. No repair follows release.')
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    failures = []
    for relative, content in generated_files().items():
        path = ROOT / relative
        if args.check:
            if not path.is_file() or path.read_text() != content:
                failures.append(relative)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
    if failures:
        raise SystemExit('generated semantic matrix drift: ' + ', '.join(failures))
    print(f'{"Checked" if args.check else "Generated"} {len(generated_files())} semantic fixture files; no runtime execution')


if __name__ == '__main__':
    main()
