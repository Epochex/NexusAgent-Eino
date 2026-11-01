"""Reproduce original proxy connection exhaustion, then validate tuned relay."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess

DIRECTORY=Path(__file__).resolve().parent


def check_baseline(measurement,connection_errors):
    if measurement['concurrency_limit']!=200 or measurement['logical_requests']!=400:
        raise RuntimeError('baseline workload differs')
    if measurement['successes']>=measurement['logical_requests'] or connection_errors<=0:
        raise RuntimeError('baseline connection exhaustion not reproduced; comparison inconclusive')


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--duration',type=int,default=180)
    parser.add_argument('--rps',type=int,default=100)
    parser.add_argument('--output',type=Path,default=Path('.superpowers/gateway-capacity.json'))
    args=parser.parse_args()
    if not 1<=args.duration<=600 or not 1<=args.rps<=200: raise ValueError('workload bounds exceeded')
    output=args.output.resolve()
    output.parent.mkdir(parents=True,exist_ok=True)
    env={**os.environ,'GATEWAY_SESSION_SECRET':secrets.token_urlsafe(48)}
    base=['docker','compose','-f',str(DIRECTORY/'compose.yaml')]
    baseline=base+['-f',str(DIRECTORY/'compose.baseline.yaml')]
    def run(command,**kw): return subprocess.run(command,env=env,check=True,**kw)
    if run(base+['ps','-aq'],capture_output=True,text=True).stdout.strip():
        raise RuntimeError('existing lab; inspect and remove it explicitly')
    try:
        run(baseline+['up','-d'])
        run(base+['exec','-T','-w','/suite','fixture','python','workload.py','baseline','--output','/tmp/baseline.json'])
        logs=run(base+['logs','--no-color','proxy'],capture_output=True,text=True).stdout
        connection_errors=logs.count('worker_connections are not enough')
        baseline_file=output.with_name(output.stem+'-baseline.json')
        run(base+['cp','fixture:/tmp/baseline.json',str(baseline_file)])
        before=json.loads(baseline_file.read_text('utf-8'))
        check_baseline(before['measurements'][0],connection_errors)
        run(base+['up','-d','--no-deps','proxy'])
        optimized_file=output.with_name(output.stem+'-optimized.json')
        try:
            run(base+['exec','-T','-w','/suite','fixture','python','workload.py','optimized',
                     '--duration',str(args.duration),'--rps',str(args.rps),'--output','/tmp/optimized.json'])
        finally:
            # Preserve sanitized checkpoints even if telemetry validation fails.
            # A missing early checkpoint must not replace the workload exception.
            subprocess.run(base+['cp','fixture:/tmp/baseline.json',str(baseline_file)],env=env,check=False)
            subprocess.run(base+['cp','fixture:/tmp/optimized.json',str(optimized_file)],env=env,check=False)
        after=json.loads(optimized_file.read_text('utf-8'))
        burst,fault_before,fault_after,steady=after['measurements']
        if burst['successes']!=burst['logical_requests']: raise RuntimeError('tuned burst did not complete')
        if fault_before['successes']!=800 or fault_after['successes']!=1000: raise RuntimeError('fault comparison changed')
        if fault_after['upstream']['dispatches']!=1200: raise RuntimeError('unexplained retry amplification')
        if steady['successes']!=steady['logical_requests'] or steady['upstream']['dispatches']!=steady['logical_requests']:
            raise RuntimeError('sustained relay failed or amplified calls')
        report={'scope':after['scope'],'baseline_proxy_connection_errors':connection_errors,'baseline':before,'optimized':after}
        output.write_text(json.dumps(report,ensure_ascii=False,indent=2),'utf-8')
        run(base+['cp','fixture:/tmp/optimized.prom',str(output.with_suffix('.prom'))])
        print(json.dumps({'proxy_connection_errors':connection_errors,'baseline_completed':before['measurements'][0]['successes'],
              'tuned_completed':burst['successes'],'sustained_requests':steady['logical_requests'],'sustained_successes':steady['successes']}))
    finally: run(base+['down','--remove-orphans'])


if __name__=='__main__': main()
