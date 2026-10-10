# k3d-identity-raft-replay-recovery

Reproduces the identity Raft replay failure tracked by mycel#172.

The scenario starts a 3-node k3d Raft cluster, forces a local system-Raft snapshot on the current system leader, submits a duplicate `admin` principal create that is expected to fail live with `principal already exists`, then restarts that same leader pod.

A broken daemon restores the snapshot and replays the duplicate-principal tail entry during startup, causing the restarted pod to fail readiness with `principal already exists`. A fixed daemon treats the committed duplicate-principal application outcome as replay-safe and returns to healthy cluster state.

## Phases

1. **bootstrap** — allow the cluster and bootstrap identity to settle.
2. **duplicate-principal-tail-and-restart** — snapshot the system-Raft leader, submit the duplicate principal command, and restart that node.
3. **post-restart-health** — final health convergence check.

## Expected result

- Before the daemon fix: the scenario should fail during the restart phase or final health assertion.
- After the daemon fix: the scenario should pass and the cluster should be healthy.
