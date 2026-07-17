# Generation-Safe Rollout Supervisor

This repository contains a Project Dynamo task in `task/`. The agent repairs a Linux service supervisor with coupled defects in generation identity, dependency-aware transactional rollouts, absolute deadline restoration, and daemonized descendant cleanup.

The environment uses real worker processes, process groups, selector-driven readiness, monotonic lifecycle deadlines, and double-fork helpers. The oracle installs a corrected supervisor and a structured root-cause analysis. The verifier rebuilds the submitted implementation and runs independent process-lifecycle scenarios covering startup ordering, rollback, successful rollout, checkpoint timing, escalation, and zero-leak shutdown.

Local validation:

```bash
harbor run -p task --agent oracle
harbor run -p task --agent nop
```
