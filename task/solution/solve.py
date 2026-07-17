#!/usr/bin/env python3
import json
from pathlib import Path

SOURCE = Path('/app/src/mesh_supervisor.py')
text = SOURCE.read_text(encoding='utf-8')
lines = text.splitlines()

def line_of(function):
    needle = f'    def {function}'
    for i, line in enumerate(lines, 1):
        if line.startswith(needle):
            return i
    raise RuntimeError(f'missing {function}')

analysis = {
    'schema_version': 1,
    'hazards': [
        {
            'id': 'stale_event_identity',
            'file': '/app/src/mesh_supervisor.py',
            'function': 'safe_gen',
            'line': line_of('safe_gen'),
            'cause': 'Selector readiness belongs to the immutable generation that registered the source; resolving a returned event through a reusable service slot can apply old readiness to a newer generation.',
            'invariant': 'Every event is accepted only when its source object is still active and is the exact source owned by the referenced generation.',
        },
        {
            'id': 'nontransactional_rollout',
            'file': '/app/src/mesh_supervisor.py',
            'function': 'finish_rollout_if_ready',
            'line': line_of('finish_rollout_if_ready'),
            'cause': 'Committing each replacement as soon as it reports ready creates a mixed-generation dependency graph and makes a later readiness failure impossible to roll back safely.',
            'invariant': 'No replacement commits until every generation in the affected dependency closure is ready, after which the closure commits as one transaction.',
        },
        {
            'id': 'deadline_reexec',
            'file': '/app/src/mesh_supervisor.py',
            'function': 'perform_reexec_checkpoint',
            'line': line_of('perform_reexec_checkpoint'),
            'cause': 'Reconstructing relative timeout durations at reload time extends already-running deadlines and changes escalation behavior based on when the checkpoint occurred.',
            'invariant': 'Startup, runtime, and kill deadlines remain absolute monotonic timestamps across state serialization and restoration.',
        },
        {
            'id': 'escaped_descendant',
            'file': '/app/src/mesh_supervisor.py',
            'function': 'terminate',
            'line': line_of('terminate'),
            'cause': 'A double-forked helper leaves the original process group and survives when shutdown signals only the initially spawned process group.',
            'invariant': 'Every registered descendant is terminated with its owning generation and, as an adopted child, is collected exactly once before shutdown completes.',
        },
    ],
    'event_identity_model': {
        'description': 'Selector data carries an immutable generation identifier and source object; stale or deactivated sources cannot resolve to a replacement generation.'
    },
    'rollout_model': {
        'commit_point': 'The commit point occurs only after all replacement generations in the transitive dependent closure have reported generation-matched readiness.',
        'rollback_behavior': 'Before commit, any replacement failure terminates only transaction-created generations and leaves every previously committed generation active.'
    },
    'reexec_model': {
        'state_transfer': 'The checkpoint serializes generation counters, current ownership, transaction phase, and per-generation lifecycle state.',
        'deadline_preservation': 'Absolute monotonic deadline values are serialized and restored without adding a fresh duration.'
    }
}
Path('/app/analysis.json').write_text(json.dumps(analysis, indent=2) + '\n', encoding='utf-8')
