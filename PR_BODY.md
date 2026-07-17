\## One-sentence problem

The task is done when the supplied Linux service supervisor correctly preserves immutable service-generation identity, performs atomic dependency-aware rollouts, preserves absolute deadlines across re-execution checkpoints, and terminates all managed descendants without stale events, partial commits, duplicate accounting, or process leaks.



\## Success criteria (numbered, mirror instruction.md)

1\. Each service generation commits only after it reports readiness and all dependency generations are committed.

2\. Delayed selector, pipe, or readiness events remain associated with the immutable generation that registered them and cannot alter a replacement generation.

3\. Dependency rollouts are transactional and failed rollouts leave the previous committed graph running.

4\. Successful rollouts commit every replacement before terminating replaced generations.

5\. Deadlines remain absolute across re-execution actions.

6\. Re-execution does not reset generation numbers, replay transitions, or restart elapsed timers.

7\. Shutdown and rollback terminate complete service generations, including daemonized helpers.

8\. Each worker emits at most one terminal event and restart accounting occurs at most once.

9\. No restart begins after shutdown starts.

10\. Event-log sequence numbers are strictly increasing.

11\. The summary JSON accurately reports lifecycle state and shutdown completion.

12\. `/app/analysis.json` identifies the four required lifecycle hazards with valid source references.



\## Calibration results

\- Golden solve.sh: reward 1.0

\- Bad / nop solution: reward < 1.0

\- Local oracle verification: 7/7 tests passed

\- Original buggy implementation: 1/7 tests passed



\## How to run

```bash

harbor run -p task --agent oracle

harbor run -p task --agent nop

