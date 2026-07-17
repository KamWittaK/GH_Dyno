import json
import os
import signal
import subprocess
import tempfile
import time
import hashlib
from pathlib import Path

APP = Path('/app')
BIN = APP / 'bin/mesh-supervisor'
ANALYSIS = APP / 'analysis.json'
WORKER_SHA256 = 'de59e627a44f11a6dbe53f6c11f023275d554b43bc8611b2c867b627c87d701f'


def run_scenario(scenario, timeout=8):
    with tempfile.TemporaryDirectory(prefix='mesh-test-') as td:
        td = Path(td)
        scenario_path = td / 'scenario.json'
        events_path = td / 'events.jsonl'
        summary_path = td / 'summary.json'
        scenario_path.write_text(json.dumps(scenario), encoding='utf-8')
        proc = subprocess.run(
            [str(BIN), '--scenario', str(scenario_path), '--events', str(events_path), '--summary', str(summary_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        summary = json.loads(summary_path.read_text()) if summary_path.exists() else None
        events = [json.loads(line) for line in events_path.read_text().splitlines()] if events_path.exists() else []
        if summary:
            for rec in summary.get('live_processes', []):
                for pid in [rec.get('pid'), *rec.get('helpers', [])]:
                    if pid:
                        try:
                            os.kill(int(pid), signal.SIGKILL)
                        except ProcessLookupError:
                            pass
        return proc, summary, events


def run_scenario_with_replacement_signal(scenario, service, sig, timeout=8):
    """Signal generation 2 immediately after its spawned event is durably logged."""
    with tempfile.TemporaryDirectory(prefix='mesh-hook-test-') as td:
        td = Path(td)
        scenario_path = td / 'scenario.json'
        events_path = td / 'events.jsonl'
        summary_path = td / 'summary.json'
        scenario_path.write_text(json.dumps(scenario), encoding='utf-8')
        proc = subprocess.Popen(
            [str(BIN), '--scenario', str(scenario_path), '--events', str(events_path), '--summary', str(summary_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        signaled_pid = None
        deadline = time.monotonic() + min(timeout, 3)
        try:
            while proc.poll() is None and time.monotonic() < deadline and signaled_pid is None:
                if events_path.exists():
                    records = [
                        json.loads(line)
                        for line in events_path.read_text(encoding='utf-8').splitlines()
                        if line
                    ]
                    spawned = [
                        record for record in records
                        if record.get('event') == 'spawned'
                        and record.get('service') == service
                        and record.get('generation') == 2
                    ]
                    if spawned:
                        signaled_pid = int(spawned[0]['pid'])
                        os.kill(signaled_pid, sig)
                        break
                time.sleep(0.005)
            assert signaled_pid is not None, f'generation 2 of {service} was not observed'
            stdout, stderr = proc.communicate(timeout=max(1, timeout))
        except BaseException:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=2)
            raise

        completed = subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr)
        summary = json.loads(summary_path.read_text(encoding='utf-8')) if summary_path.exists() else None
        events = [
            json.loads(line)
            for line in events_path.read_text(encoding='utf-8').splitlines()
            if line
        ] if events_path.exists() else []
        if summary:
            for rec in summary.get('live_processes', []):
                for pid in [rec.get('pid'), *rec.get('helpers', [])]:
                    if pid:
                        try:
                            os.kill(int(pid), signal.SIGKILL)
                        except ProcessLookupError:
                            pass
        return completed, summary, events, signaled_pid


def event(events, name, **match):
    return [e for e in events if e.get('event') == name and all(e.get(k) == v for k, v in match.items())]




def assert_strictly_increasing_seq(events):
    """Every event has a strictly increasing integer sequence number."""
    seqs = [record.get('seq') for record in events]
    assert seqs, 'scenario emitted no events'
    assert all(isinstance(value, int) and not isinstance(value, bool) for value in seqs)
    assert all(left < right for left, right in zip(seqs, seqs[1:]))
def base_services(helper=False, stubborn=False, runtime_ms=0):
    first = {
        'name': 'db', 'depends_on': [], 'mode': 'stubborn' if stubborn else 'cooperative',
        'ready_delay_ms': 20, 'startup_timeout_ms': 500, 'grace_ms': 100,
    }
    if helper:
        first['spawn_helper'] = True
    if runtime_ms:
        first['runtime_timeout_ms'] = runtime_ms
    return [
        first,
        {'name': 'api', 'depends_on': ['db'], 'mode': 'cooperative', 'ready_delay_ms': 35,
         'startup_timeout_ms': 500, 'grace_ms': 100},
    ]


def test_build_and_analysis_contract():
    """The repaired executable builds and the root-cause analysis names every required hazard."""
    subprocess.run(['make', '-C', '/app'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert BIN.is_file() and os.access(BIN, os.X_OK)
    assert hashlib.sha256((APP / 'src/worker.py').read_bytes()).hexdigest() == WORKER_SHA256
    data = json.loads(ANALYSIS.read_text(encoding='utf-8'))
    assert data.get('schema_version') == 1
    hazards = data.get('hazards')
    assert isinstance(hazards, list)
    required = {
        'stale_event_identity', 'nontransactional_rollout',
        'deadline_reexec', 'escaped_descendant',
    }
    assert {h.get('id') for h in hazards} == required
    identity_model = data.get('event_identity_model')
    rollout_model = data.get('rollout_model')
    reexec_model = data.get('reexec_model')
    assert isinstance(identity_model, dict) and len(identity_model.get('description', '').strip()) >= 30
    assert isinstance(rollout_model, dict)
    assert len(rollout_model.get('commit_point', '').strip()) >= 30
    assert len(rollout_model.get('rollback_behavior', '').strip()) >= 30
    assert isinstance(reexec_model, dict)
    assert len(reexec_model.get('state_transfer', '').strip()) >= 30
    assert len(reexec_model.get('deadline_preservation', '').strip()) >= 30
    source_text = (APP / 'src/mesh_supervisor.py').read_text(encoding='utf-8')
    source_lines = source_text.splitlines()
    for h in hazards:
        assert h.get('file') == '/app/src/mesh_supervisor.py'
        assert isinstance(h.get('function'), str) and f"def {h['function']}" in source_text
        assert isinstance(h.get('line'), int) and 1 <= h['line'] <= len(source_lines)
        assert len(h.get('cause', '').strip()) >= 40
        assert len(h.get('invariant', '').strip()) >= 30
    assert len(data.get('event_identity_model', {}).get('description', '').strip()) >= 30
    rollout = data.get('rollout_model', {})
    assert len(rollout.get('commit_point', '').strip()) >= 30
    assert len(rollout.get('rollback_behavior', '').strip()) >= 30
    reexec = data.get('reexec_model', {})
    assert len(reexec.get('state_transfer', '').strip()) >= 30
    assert len(reexec.get('deadline_preservation', '').strip()) >= 30


def test_dependency_startup_is_generation_ordered():
    """A dependent generation commits only after its dependency generation is ready and committed."""
    scenario = {'services': base_services(), 'actions': [{'at_ms': 320, 'op': 'shutdown'}], 'max_runtime_ms': 1800}
    proc, summary, events = run_scenario(scenario)
    assert_strictly_increasing_seq(events)
    assert proc.returncode == 0, proc.stderr
    assert summary['shutdown_complete'] and not summary['live_processes']
    db_ready = event(events, 'ready', service='db', generation=1)[0]['seq']
    db_commit = event(events, 'committed', service='db', generation=1)[0]['seq']
    api_commit = event(events, 'committed', service='api', generation=1)[0]['seq']
    assert db_ready < db_commit < api_commit
    assert len(event(events, 'exited', service='db', generation=1)) == 1
    assert len(event(events, 'exited', service='api', generation=1)) == 1


def test_rollout_failure_is_atomic():
    """A failed dependent replacement rolls back the entire affected closure without partial commit."""
    scenario = {
        'services': base_services(),
        'actions': [
            {'at_ms': 260, 'op': 'rollout', 'service': 'db', 'fail_service': 'api'},
            {'at_ms': 800, 'op': 'shutdown'},
        ],
        'max_runtime_ms': 2200,
    }
    proc, summary, events = run_scenario(scenario)
    assert_strictly_increasing_seq(events)
    assert proc.returncode == 0, proc.stderr
    assert len(event(events, 'rollout_rolled_back')) == 1
    assert not event(events, 'rollout_committed')
    assert not event(events, 'committed', service='db', generation=2)
    assert not event(events, 'committed', service='api', generation=2)
    db1_shutdown = event(events, 'termination_started', service='db', generation=1)
    api1_shutdown = event(events, 'termination_started', service='api', generation=1)
    assert db1_shutdown and all(e['reason'] == 'shutdown' for e in db1_shutdown)
    assert api1_shutdown and all(e['reason'] == 'shutdown' for e in api1_shutdown)
    assert summary['shutdown_complete']


def test_rollout_startup_timeout_is_atomic():
    """A replacement that cannot become ready before its deadline rolls back without replacing generation 1."""
    scenario = {
        'services': [{
            'name': 'core', 'depends_on': [], 'mode': 'cooperative',
            'ready_delay_ms': 120, 'startup_timeout_ms': 350, 'grace_ms': 80,
        }],
        'actions': [
            {'at_ms': 260, 'op': 'rollout', 'service': 'core'},
            {'at_ms': 900, 'op': 'shutdown'},
        ],
        'max_runtime_ms': 2000,
    }
    proc, summary, events, stopped_pid = run_scenario_with_replacement_signal(
        scenario, 'core', signal.SIGSTOP
    )
    assert_strictly_increasing_seq(events)
    assert proc.returncode == 0, proc.stderr
    rollbacks = event(events, 'rollout_rolled_back')
    assert len(rollbacks) == 1 and rollbacks[0]['reason'] == 'startup_timeout'
    assert not event(events, 'rollout_committed')
    assert not event(events, 'committed', service='core', generation=2)
    generation1_termination = event(events, 'termination_started', service='core', generation=1)
    assert generation1_termination
    assert all(record['reason'] == 'shutdown' for record in generation1_termination)
    generation2_exit = event(events, 'exited', service='core', generation=2)
    assert len(generation2_exit) == 1
    assert summary['shutdown_complete'] and summary['live_processes'] == []


def test_rollout_replacement_early_exit_is_atomic():
    """A replacement that exits before readiness rolls back without committing or terminating generation 1."""
    scenario = {
        'services': [{
            'name': 'core', 'depends_on': [], 'mode': 'cooperative',
            'ready_delay_ms': 180, 'startup_timeout_ms': 600, 'grace_ms': 80,
        }],
        'actions': [
            {'at_ms': 300, 'op': 'rollout', 'service': 'core'},
            {'at_ms': 780, 'op': 'shutdown'},
        ],
        'max_runtime_ms': 1900,
    }
    proc, summary, events, killed_pid = run_scenario_with_replacement_signal(
        scenario, 'core', signal.SIGKILL
    )
    assert_strictly_increasing_seq(events)
    assert proc.returncode == 0, proc.stderr
    rollbacks = event(events, 'rollout_rolled_back')
    assert len(rollbacks) == 1 and rollbacks[0]['reason'] == 'replacement_exit'
    assert not event(events, 'rollout_committed')
    assert not event(events, 'committed', service='core', generation=2)
    generation1_termination = event(events, 'termination_started', service='core', generation=1)
    assert generation1_termination
    assert all(record['reason'] == 'shutdown' for record in generation1_termination)
    assert len(event(events, 'exited', service='core', generation=2)) == 1
    assert summary['shutdown_complete'] and summary['live_processes'] == []


def test_successful_rollout_commits_as_one_transaction():
    """Every replacement is ready before any replacement generation becomes committed."""
    scenario = {
        'services': base_services(),
        'actions': [
            {'at_ms': 260, 'op': 'rollout', 'service': 'db'},
            {'at_ms': 850, 'op': 'shutdown'},
        ],
        'max_runtime_ms': 2400,
    }
    proc, summary, events = run_scenario(scenario)
    assert_strictly_increasing_seq(events)
    assert proc.returncode == 0, proc.stderr
    ready2 = event(events, 'ready', service='db', generation=2) + event(events, 'ready', service='api', generation=2)
    commits2 = event(events, 'committed', service='db', generation=2) + event(events, 'committed', service='api', generation=2)
    assert len(ready2) == len(commits2) == 2
    assert max(e['seq'] for e in ready2) < min(e['seq'] for e in commits2)
    assert len(event(events, 'rollout_committed')) == 1
    assert summary['generation_counts'] == {'db': 2, 'api': 2}
    assert summary['shutdown_complete']


def test_reexec_checkpoint_preserves_absolute_deadline():
    """A hot checkpoint cannot restart a running generation's already-counting runtime timeout."""
    scenario = {
        'services': [{
            'name': 'job', 'depends_on': [], 'mode': 'stubborn', 'ready_delay_ms': 20,
            'startup_timeout_ms': 500, 'runtime_timeout_ms': 300, 'grace_ms': 110,
        }],
        'actions': [
            {'at_ms': 150, 'op': 'reexec'},
            {'at_ms': 850, 'op': 'shutdown'},
        ],
        'max_runtime_ms': 1800,
    }
    proc, summary, events = run_scenario(scenario)
    assert_strictly_increasing_seq(events)
    assert proc.returncode == 0, proc.stderr
    checkpoint = event(events, 'reexec_checkpoint')[0]
    committed = event(events, 'committed', service='job', generation=1)[0]
    timeout = [e for e in event(events, 'termination_started', service='job', generation=1) if e['reason'] == 'runtime_timeout'][0]
    assert checkpoint['pid'] == summary['pid']
    elapsed = timeout['t_ms'] - committed['t_ms']
    assert checkpoint['t_ms'] < timeout['t_ms']
    assert 260 <= elapsed <= 360
    assert timeout['t_ms'] < checkpoint['t_ms'] + 230
    assert len(event(events, 'termination_escalated', service='job', generation=1)) == 1
    assert summary['reexec_epoch'] == 1 and summary['shutdown_complete']


def test_daemonized_descendant_is_terminated_and_reaped():
    """Shutdown includes a double-forked helper and leaves no live process or zombie record."""
    scenario = {
        'services': [{
            'name': 'daemon', 'depends_on': [], 'mode': 'cooperative', 'ready_delay_ms': 20,
            'startup_timeout_ms': 500, 'grace_ms': 100, 'spawn_helper': True,
        }],
        'actions': [{'at_ms': 360, 'op': 'shutdown'}],
        'max_runtime_ms': 1800,
    }
    proc, summary, events = run_scenario(scenario)
    assert_strictly_increasing_seq(events)
    assert proc.returncode == 0, proc.stderr
    registered = event(events, 'helper_registered', service='daemon', generation=1)
    reaped = event(events, 'helper_reaped', service='daemon', generation=1)
    assert len(registered) == len(reaped) == 1
    assert registered[0]['pid'] == reaped[0]['pid']
    assert summary['shutdown_complete'] and summary['live_processes'] == []


def test_stale_generation_event_cannot_ready_replacement():
    """A late readiness message from a retired generation never readies or commits its replacement."""
    scenario = {
        'services': [{
            'name': 'core', 'depends_on': [], 'mode': 'cooperative', 'ready_delay_ms': 500,
            'startup_timeout_ms': 900, 'grace_ms': 100, 'late_ready_on_term': True,
        }],
        'actions': [
            {'at_ms': 700, 'op': 'stale_probe', 'service': 'core'},
            {'at_ms': 1450, 'op': 'shutdown'},
        ],
        'max_runtime_ms': 2600,
    }
    proc, summary, events = run_scenario(scenario)
    assert_strictly_increasing_seq(events)
    assert proc.returncode == 0, proc.stderr
    ready2 = [e for e in event(events, 'ready', service='core', generation=2) if not e.get('late')]
    commits2 = event(events, 'committed', service='core', generation=2)
    assert len(ready2) == len(commits2) == 1
    assert ready2[0]['seq'] < commits2[0]['seq']
    assert commits2[0]['t_ms'] - event(events, 'rollout_started')[0]['t_ms'] >= 400
    assert summary['generation_counts'] == {'core': 2}
    assert summary['shutdown_complete']


def test_event_sequences_are_strictly_increasing_integers():
    """Every emitted JSONL record has a unique, strictly increasing integer sequence number."""
    scenario = {'services': base_services(), 'actions': [{'at_ms': 320, 'op': 'shutdown'}], 'max_runtime_ms': 1800}
    proc, summary, events = run_scenario(scenario)
    assert_strictly_increasing_seq(events)
    assert proc.returncode == 0, proc.stderr
    assert summary['shutdown_complete']
    seqs = [record.get('seq') for record in events]
    assert seqs
    assert all(isinstance(seq, int) and not isinstance(seq, bool) for seq in seqs)
    assert all(left < right for left, right in zip(seqs, seqs[1:]))
    assert seqs == list(range(1, len(seqs) + 1))


def test_restart_budget_is_charged_once_and_stops_at_shutdown():
    """One unexpected exit consumes one restart credit, and shutdown cannot launch another generation."""
    scenario = {
        'services': [{
            'name': 'job', 'depends_on': [], 'mode': 'cooperative', 'ready_delay_ms': 20,
            'startup_timeout_ms': 500, 'runtime_timeout_ms': 150, 'grace_ms': 100,
            'restart_limit': 1,
        }],
        'actions': [{'at_ms': 520, 'op': 'shutdown'}],
        'max_runtime_ms': 1800,
    }
    proc, summary, events = run_scenario(scenario)
    assert_strictly_increasing_seq(events)
    assert proc.returncode == 0, proc.stderr
    charges = event(events, 'restart_charged', service='job')
    spawns = event(events, 'spawned', service='job')
    exits = event(events, 'exited', service='job')
    shutdown = event(events, 'shutdown_started')
    assert len(charges) == 1 and charges[0]['used'] == 1
    assert [record['generation'] for record in spawns] == [1, 2]
    assert len(exits) == 2
    assert len(shutdown) == 1
    assert all(record['seq'] < shutdown[0]['seq'] for record in spawns)
    assert not [record for record in spawns if record['seq'] > shutdown[0]['seq']]
    assert summary['restart_used'] == {'job': 1}
    assert summary['generation_counts'] == {'job': 2}
    assert summary['shutdown_complete'] and summary['live_processes'] == []

def test_restart_budget_and_shutdown_gate():
    """One unexpected exit consumes one restart and no generation starts after shutdown begins."""
    scenario = {
        'services': [{
            'name': 'flaky',
            'depends_on': [],
            'mode': 'stubborn',
            'ready_delay_ms': 20,
            'startup_timeout_ms': 500,
            'runtime_timeout_ms': 160,
            'grace_ms': 70,
            'restart_limit': 1,
        }],
        'actions': [{'at_ms': 620, 'op': 'shutdown'}],
        'max_runtime_ms': 1800,
    }
    proc, summary, events = run_scenario(scenario)
    assert_strictly_increasing_seq(events)
    assert proc.returncode == 0, proc.stderr

    charged = event(events, 'restart_charged', service='flaky')
    assert len(charged) == 1
    assert charged[0]['used'] == 1
    assert summary['restart_used'] == {'flaky': 1}
    assert summary['generation_counts'] == {'flaky': 2}

    shutdown = event(events, 'shutdown_started')
    assert len(shutdown) == 1
    shutdown_seq = shutdown[0]['seq']
    assert all(
        record['seq'] < shutdown_seq
        for record in event(events, 'spawned', service='flaky')
    )
    assert summary['shutdown_complete']
    assert summary['live_processes'] == []
