#!/usr/bin/env python3
import argparse, json, os, signal, sys, time

running = True
stubborn = False
service_name = ''
generation_number = 0
late_ready_on_term = False

def emit(obj):
    print(json.dumps(obj, separators=(",", ":")), flush=True)

def on_term(sig, frame):
    global running
    if late_ready_on_term:
        emit({"type":"ready","service":service_name,"generation":generation_number,"late":True})
    emit({"type":"signal","pid":os.getpid(),"signal":sig})
    if not stubborn:
        running = False

def spawn_helper():
    first = os.fork()
    if first == 0:
        os.setsid()
        second = os.fork()
        if second > 0:
            os._exit(0)
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
        signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
        emit({"type":"helper","pid":os.getpid()})
        while True:
            time.sleep(0.05)
    os.waitpid(first, 0)

def main():
    global stubborn, service_name, generation_number, late_ready_on_term
    p=argparse.ArgumentParser()
    p.add_argument('--service',required=True)
    p.add_argument('--generation',type=int,required=True)
    p.add_argument('--mode',choices=['cooperative','stubborn','fail_ready','fast_exit'],default='cooperative')
    p.add_argument('--ready-delay-ms',type=int,default=20)
    p.add_argument('--spawn-helper',action='store_true')
    p.add_argument('--late-ready-on-term',action='store_true')
    a=p.parse_args()
    stubborn = a.mode == 'stubborn'
    service_name = a.service
    generation_number = a.generation
    late_ready_on_term = a.late_ready_on_term
    signal.signal(signal.SIGTERM,on_term)
    signal.signal(signal.SIGINT,on_term)
    if a.spawn_helper:
        spawn_helper()
    if a.mode == 'fast_exit':
        emit({"type":"fast_exit","service":a.service,"generation":a.generation})
        return 23
    time.sleep(a.ready_delay_ms/1000)
    if a.mode == 'fail_ready':
        emit({"type":"failed","service":a.service,"generation":a.generation})
    else:
        emit({"type":"ready","service":a.service,"generation":a.generation,"pid":os.getpid()})
    while running:
        time.sleep(0.05)
    return 0

if __name__=='__main__':
    raise SystemExit(main())
