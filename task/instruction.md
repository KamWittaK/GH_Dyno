Repair the service supervisor in `/app/src/mesh_supervisor.py`. The supplied implementation launches real Linux worker processes from `/app/src/worker.py` but mishandles generation identity, dependency rollouts, checkpointed deadlines, and daemonized descendants. Preserve the existing command-line interface:

`/app/bin/mesh-supervisor --scenario /absolute/scenario.json --events /absolute/events.jsonl --summary /absolute/summary.json`

Run `make -C /app` so the repaired executable exists at `/app/bin/mesh-supervisor`.

A scenario contains `services`, ordered `actions`, and `max_runtime_ms`. Each service has `name`, `depends_on`, `mode`, `ready_delay_ms`, `startup_timeout_ms`, `grace_ms`, and optional `runtime_timeout_ms`, `restart_limit`, or `spawn_helper`. Actions use an absolute offset from supervisor start and are `rollout`, `reexec`, `stale_probe`, or `shutdown`. `stale_probe` starts a replacement and retires the current generation so a delayed old-generation readiness message is delivered while the replacement slot exists. A rollout of service X includes X and every transitive dependent of X.

Maintain these observable invariants for every valid acyclic scenario:

- A generation commits only after it reports readiness and all of its dependencies have committed generations.
- Selector or pipe events belong to the immutable generation that registered them. A delayed event or message from an older generation must never alter a replacement generation.
- A rollout is transactional. No replacement in the affected closure may commit until every replacement in that closure is ready. Before that commit point, any failed readiness, early exit, or startup timeout must terminate only transaction-created generations and leave all previously committed generations running. A successful rollout commits every replacement, then terminates the replaced generations.
- Startup, runtime, and termination-grace deadlines are absolute monotonic deadlines. A `reexec` action must serialize and restore lifecycle state without restarting any elapsed timeout, changing generation numbers, replaying a transition, or changing the supervisor PID reported in the checkpoint event.
- A service generation includes daemonized helpers reported by its worker. Shutdown, rollback, replacement, and escalation must terminate the entire generation, including helpers that left the original process group. Adopted descendants must be collected exactly once. Shutdown is complete only when no managed process or helper remains.
- Each worker process emits at most one `exited` event. Restart budget is charged at most once for a qualifying unexpected exit. No new restart begins after shutdown starts.
- Event records in the requested JSONL file have strictly increasing integer `seq` values. The summary file must remain valid JSON with the existing schema and must accurately report generation counts, restart use, remaining live processes, failures, and shutdown completion.

Create `/app/analysis.json` as UTF-8 JSON with this structure:

```json
{
  "schema_version": 1,
  "hazards": [
    {"id":"stale_event_identity","file":"/app/src/mesh_supervisor.py","function":"name","line":1,"cause":"substantive explanation","invariant":"substantive invariant"},
    {"id":"nontransactional_rollout","file":"/app/src/mesh_supervisor.py","function":"name","line":1,"cause":"substantive explanation","invariant":"substantive invariant"},
    {"id":"deadline_reexec","file":"/app/src/mesh_supervisor.py","function":"name","line":1,"cause":"substantive explanation","invariant":"substantive invariant"},
    {"id":"escaped_descendant","file":"/app/src/mesh_supervisor.py","function":"name","line":1,"cause":"substantive explanation","invariant":"substantive invariant"}
  ],
  "event_identity_model":{"description":"..."},
  "rollout_model":{"commit_point":"...","rollback_behavior":"..."},
  "reexec_model":{"state_transfer":"...","deadline_preservation":"..."}
}
```

Each referenced function and line must exist in the submitted source. Do not modify `/app/src/worker.py` or the scenario contract.

