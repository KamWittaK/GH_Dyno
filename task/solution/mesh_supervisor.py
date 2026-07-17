#!/usr/bin/env python3
import argparse, ctypes, dataclasses, json, os, selectors, signal, subprocess, sys, time
from collections import defaultdict, deque
from pathlib import Path

PR_SET_CHILD_SUBREAPER=36
libc=ctypes.CDLL(None,use_errno=True)

def monotonic(): return time.monotonic()

def set_subreaper():
    if libc.prctl(PR_SET_CHILD_SUBREAPER,1,0,0,0)!=0:
        e=ctypes.get_errno(); raise OSError(e,os.strerror(e))

@dataclasses.dataclass
class Source:
    service:str
    generation_id:int
    active:bool=True

@dataclasses.dataclass
class Gen:
    generation_id:int
    service:str
    number:int
    pid:int
    pgid:int
    proc:subprocess.Popen
    source:Source
    stdout:object
    state:str='starting'
    ready:bool=False
    committed:bool=False
    old:bool=False
    startup_deadline:float|None=None
    runtime_deadline:float|None=None
    kill_deadline:float|None=None
    transaction:int|None=None
    helpers:set[int]=dataclasses.field(default_factory=set)
    exit_recorded:bool=False
    buffer:bytes=b''

class Supervisor:
    def __init__(self,scenario,events_path,summary_path):
        set_subreaper()
        self.scenario=scenario
        self.events_path=Path(events_path); self.summary_path=Path(summary_path)
        self.events_path.parent.mkdir(parents=True,exist_ok=True)
        self.events=self.events_path.open('w',encoding='utf-8',buffering=1)
        self.sel=selectors.DefaultSelector()
        self.services={s['name']:dict(s) for s in scenario['services']}
        self.dependents=defaultdict(set)
        for n,s in self.services.items():
            for d in s.get('depends_on',[]): self.dependents[d].add(n)
        self.gen_seq=defaultdict(int); self.next_gid=1; self.next_txn=1
        self.gens={}; self.current={}; self.rollout=None
        self.start=monotonic(); self.action_index=0; self.seq=0
        self.shutting=False; self.shutdown_started=None
        self.reexec_epoch=0; self.failures=[]
        self.restart_used=defaultdict(int)

    def log(self,event,**kw):
        self.seq+=1
        self.events.write(json.dumps({'seq':self.seq,'t_ms':round((monotonic()-self.start)*1000,3),'event':event,**kw},separators=(',',':'))+'\n')

    def topo(self,subset=None):
        names=set(subset or self.services)
        indeg={n:0 for n in names}
        for n in names:
            indeg[n]=sum(1 for d in self.services[n].get('depends_on',[]) if d in names)
        q=deque(sorted(n for n,v in indeg.items() if v==0)); out=[]
        while q:
            n=q.popleft(); out.append(n)
            for c in sorted(self.dependents[n]&names):
                indeg[c]-=1
                if indeg[c]==0:q.append(c)
        if len(out)!=len(names): raise ValueError('cyclic dependency graph')
        return out

    def spawn(self,name,txn=None,mode_override=None):
        s=self.services[name]; self.gen_seq[name]+=1; number=self.gen_seq[name]
        gid=self.next_gid; self.next_gid+=1
        mode=mode_override or s.get('mode','cooperative')
        cmd=['/usr/bin/python3','/app/src/worker.py','--service',name,'--generation',str(number),'--mode',mode,'--ready-delay-ms',str(s.get('ready_delay_ms',20))]
        if s.get('spawn_helper'): cmd.append('--spawn-helper')
        if s.get('late_ready_on_term'): cmd.append('--late-ready-on-term')
        p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,start_new_session=True,bufsize=0)
        os.set_blocking(p.stdout.fileno(),False)
        src=Source(name,gid)
        g=Gen(gid,name,number,p.pid,p.pid,p,src,p.stdout,startup_deadline=monotonic()+s.get('startup_timeout_ms',1000)/1000,transaction=txn)
        self.gens[gid]=g; self.sel.register(p.stdout,selectors.EVENT_READ,src)
        self.log('spawned',service=name,generation=number,gid=gid,pid=p.pid,transaction=txn)
        return g

    def safe_gen(self,src):
        g=self.gens.get(src.generation_id)
        return g if src.active and g and g.source is src and g.state!='exited' else None

    def read_worker(self,src,fileobj):
        g=self.safe_gen(src)
        if not g:return
        try: chunk=os.read(fileobj.fileno(),65536)
        except BlockingIOError:return
        if not chunk:return
        g.buffer+=chunk
        while b'\n' in g.buffer:
            line,g.buffer=g.buffer.split(b'\n',1)
            if not line:continue
            m=json.loads(line)
            typ=m.get('type')
            if typ in ('ready','failed','fast_exit') and (m.get('service')!=g.service or m.get('generation')!=g.number):
                self.log('stale_message_ignored',gid=g.generation_id); continue
            if typ=='ready':
                g.ready=True; self.log('ready',service=g.service,generation=g.number,gid=g.generation_id,late=bool(m.get('late',False)))
                if g.transaction is None:self.commit_initials()
                else:self.finish_rollout_if_ready()
            elif typ=='failed':
                self.log('readiness_failed',service=g.service,generation=g.number,gid=g.generation_id)
                if self.rollout and g.transaction==self.rollout['id']:self.abort_rollout('readiness_failure')
                else:self.terminate(g,'readiness_failure')
            elif typ=='helper':
                hp=int(m['pid']); g.helpers.add(hp); self.log('helper_registered',service=g.service,generation=g.number,pid=hp)
            elif typ=='signal':
                self.log('signal_observed',service=g.service,generation=g.number,signal=m.get('signal'))

    def commit_initials(self):
        changed=True
        while changed:
            changed=False
            for g in list(self.gens.values()):
                if g.transaction is not None or not g.ready or g.committed or g.state=='exited':continue
                if all(d in self.current and self.gens[self.current[d]].committed for d in self.services[g.service].get('depends_on',[])):
                    self.commit(g); changed=True

    def commit(self,g):
        oldid=self.current.get(g.service)
        if oldid and oldid!=g.generation_id:
            old=self.gens.get(oldid)
            if old and old.state!='exited': old.old=True; self.terminate(old,'replaced')
        self.current[g.service]=g.generation_id; g.committed=True; g.state='running'
        r=self.services[g.service].get('runtime_timeout_ms',0)
        if r:g.runtime_deadline=monotonic()+r/1000
        self.log('committed',service=g.service,generation=g.number,gid=g.generation_id)

    def affected(self,root):
        seen={root}; q=deque([root])
        while q:
            n=q.popleft()
            for c in self.dependents[n]:
                if c not in seen:seen.add(c);q.append(c)
        return seen

    def begin_rollout(self,root,fail_service=None):
        if self.rollout:return
        a=self.affected(root); tid=self.next_txn; self.next_txn+=1
        self.rollout={'id':tid,'affected':sorted(a),'replacements':{},'phase':'preparing'}
        self.log('rollout_started',transaction=tid,root=root,affected=sorted(a))
        for n in self.topo(a):
            g=self.spawn(n,tid,'fail_ready' if n==fail_service else None)
            self.rollout['replacements'][n]=g.generation_id

    def finish_rollout_if_ready(self):
        if not self.rollout or self.rollout['phase']!='preparing':return
        gs=[self.gens[x] for x in self.rollout['replacements'].values()]
        if not all(g.ready and g.state!='exited' for g in gs):return
        self.rollout['phase']='committing'; tid=self.rollout['id']
        for n in self.topo(self.rollout['affected']):self.commit(self.gens[self.rollout['replacements'][n]])
        self.log('rollout_committed',transaction=tid); self.rollout=None

    def abort_rollout(self,reason):
        if not self.rollout:return
        r=self.rollout; r['phase']='aborting'
        for gid in list(r['replacements'].values()):
            g=self.gens.get(gid)
            if g and g.state!='exited':self.terminate(g,'rollback')
        self.log('rollout_rolled_back',transaction=r['id'],reason=reason); self.rollout=None

    def terminate(self,g,reason):
        if g.state in ('exited','terminating'):return
        g.state='terminating'; self.log('termination_started',service=g.service,generation=g.number,gid=g.generation_id,reason=reason)
        try:os.killpg(g.pgid,signal.SIGTERM)
        except ProcessLookupError:pass
        for hp in list(g.helpers):
            try:os.kill(hp,signal.SIGTERM)
            except ProcessLookupError:pass
        g.kill_deadline=monotonic()+self.services[g.service].get('grace_ms',120)/1000

    def escalate(self,g):
        if g.state=='exited':return
        try:os.killpg(g.pgid,signal.SIGKILL)
        except ProcessLookupError:pass
        for hp in list(g.helpers):
            try:os.kill(hp,signal.SIGKILL)
            except ProcessLookupError:pass
        g.kill_deadline=None; self.log('termination_escalated',service=g.service,generation=g.number,gid=g.generation_id)

    def reap(self):
        while True:
            try:pid,status=os.waitpid(-1,os.WNOHANG)
            except ChildProcessError:break
            if pid==0:break
            g=next((x for x in self.gens.values() if x.pid==pid),None)
            if g:self.record_exit(g,status)
            else:
                for owner in self.gens.values():
                    if pid in owner.helpers:
                        owner.helpers.discard(pid); self.log('helper_reaped',service=owner.service,generation=owner.number,pid=pid); break

    def record_exit(self,g,status):
        if g.exit_recorded:return
        g.exit_recorded=True;g.state='exited';g.source.active=False
        try:self.sel.unregister(g.stdout)
        except Exception:pass
        try:g.stdout.close()
        except Exception:pass
        self.log('exited',service=g.service,generation=g.number,gid=g.generation_id,status=status)
        if self.current.get(g.service)==g.generation_id:self.current.pop(g.service,None)
        if self.rollout and g.transaction==self.rollout['id'] and self.rollout['phase']=='preparing':self.abort_rollout('replacement_exit')
        if not self.shutting and not g.old and g.transaction is None:
            lim=self.services[g.service].get('restart_limit',0)
            if self.restart_used[g.service]<lim:
                self.restart_used[g.service]+=1; self.log('restart_charged',service=g.service,used=self.restart_used[g.service]); self.spawn(g.service,None)

    def perform_reexec_checkpoint(self):
        snapshot={
            'epoch':self.reexec_epoch+1,
            'next_gid':self.next_gid,
            'next_txn':self.next_txn,
            'current':dict(self.current),
            'gen_seq':dict(self.gen_seq),
            'rollout':json.loads(json.dumps(self.rollout)) if self.rollout else None,
            'deadlines':{str(g.generation_id):{'startup':g.startup_deadline,'runtime':g.runtime_deadline,'kill':g.kill_deadline} for g in self.gens.values() if g.state!='exited'},
        }
        encoded=json.dumps(snapshot,separators=(',',':'))
        restored=json.loads(encoded)
        self.reexec_epoch=restored['epoch']; self.next_gid=restored['next_gid']; self.next_txn=restored['next_txn']
        self.current={k:int(v) for k,v in restored['current'].items()}; self.gen_seq=defaultdict(int,{k:int(v) for k,v in restored['gen_seq'].items()})
        self.rollout=restored['rollout']
        for gid,d in restored['deadlines'].items():
            g=self.gens.get(int(gid))
            if g:g.startup_deadline=d['startup'];g.runtime_deadline=d['runtime'];g.kill_deadline=d['kill']
        self.log('reexec_checkpoint',epoch=self.reexec_epoch,pid=os.getpid())

    def process_actions(self):
        elapsed=(monotonic()-self.start)*1000
        acts=self.scenario.get('actions',[])
        while self.action_index<len(acts) and acts[self.action_index].get('at_ms',0)<=elapsed:
            a=acts[self.action_index]; self.action_index+=1; op=a['op']
            if op=='rollout':self.begin_rollout(a['service'],a.get('fail_service'))
            elif op=='reexec':self.perform_reexec_checkpoint()
            elif op=='stale_probe':
                self.begin_rollout(a['service'])
                oldid=self.current.get(a['service'])
                if oldid:self.terminate(self.gens[oldid],'stale_probe')
            elif op=='shutdown':self.begin_shutdown()

    def process_deadlines(self):
        t=monotonic()
        for g in list(self.gens.values()):
            if g.state=='exited':continue
            if g.startup_deadline and not g.ready and t>=g.startup_deadline:
                g.startup_deadline=None
                if self.rollout and g.transaction==self.rollout['id']:self.abort_rollout('startup_timeout')
                else:self.terminate(g,'startup_timeout')
            if g.runtime_deadline and g.committed and t>=g.runtime_deadline:
                g.runtime_deadline=None;self.terminate(g,'runtime_timeout')
            if g.kill_deadline and t>=g.kill_deadline:self.escalate(g)

    def begin_shutdown(self):
        if self.shutting:return
        self.shutting=True;self.shutdown_started=monotonic()
        if self.rollout:self.abort_rollout('shutdown')
        self.log('shutdown_started')
        for g in list(self.gens.values()):
            if g.state!='exited':self.terminate(g,'shutdown')

    def done(self):
        if self.shutting and not any(g.state!='exited' or g.helpers for g in self.gens.values()):return True
        max_ms=self.scenario.get('max_runtime_ms',5000)
        if (monotonic()-self.start)*1000>max_ms:
            self.failures.append('max_runtime_exceeded');self.begin_shutdown()
            if self.shutdown_started and monotonic()-self.shutdown_started>2:return True
        return False

    def write_summary(self):
        live=[]
        for g in self.gens.values():
            if g.state!='exited' or g.helpers:live.append({'service':g.service,'generation':g.number,'pid':g.pid,'state':g.state,'helpers':sorted(g.helpers)})
        summary={
            'schema_version':1,'pid':os.getpid(),'reexec_epoch':self.reexec_epoch,
            'current_generations':{n:self.gens[gid].number for n,gid in self.current.items()},
            'generation_counts':dict(self.gen_seq),'restart_used':dict(self.restart_used),
            'live_processes':live,'failures':self.failures,'shutdown_complete':not live,
        }
        self.summary_path.write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')

    def run(self):
        for n in self.topo():self.spawn(n)
        while not self.done():
            self.process_actions();self.process_deadlines();self.reap()
            for key,_ in self.sel.select(0.02):self.read_worker(key.data,key.fileobj)
            self.process_deadlines();self.reap()
        self.reap();self.log('supervisor_finished');self.write_summary();return 0 if not self.failures else 1

def main():
    p=argparse.ArgumentParser();p.add_argument('--scenario',required=True);p.add_argument('--events',required=True);p.add_argument('--summary',required=True)
    a=p.parse_args();scenario=json.loads(Path(a.scenario).read_text());return Supervisor(scenario,a.events,a.summary).run()

if __name__=='__main__':raise SystemExit(main())
