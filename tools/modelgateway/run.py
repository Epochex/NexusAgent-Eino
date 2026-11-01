"""Own the disposable Compose lifecycle; run with Python 3.11+ and Docker."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess

DIRECTORY=Path(__file__).resolve().parent


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,default=Path('.superpowers/modelgateway-report.json'))
    args=parser.parse_args()
    output=args.output.resolve()
    env={**os.environ,'GATEWAY_SESSION_SECRET':secrets.token_urlsafe(48)}
    compose=['docker','compose','-f',str(DIRECTORY/'compose.yaml')]
    def run(*parts,**kw): return subprocess.run([*compose,*parts],env=env,check=True,**kw)
    existing=run('ps','-aq',capture_output=True,text=True).stdout.strip()
    if existing: raise RuntimeError('lab already exists; refusing to replace it; inspect and remove this lab explicitly')
    try:
        run('up','-d')
        subprocess.run(['python','-m','unittest','discover','-s',str(DIRECTORY),'-v'],check=True)
        result=subprocess.run([*compose,'exec','-T','-w','/suite','fixture','python','acceptance.py','--output','/tmp/gateway-report.json'],env=env)
        output.parent.mkdir(parents=True,exist_ok=True)
        run('cp','fixture:/tmp/gateway-report.json',str(output))
        report=json.loads(output.read_text('utf-8'))
        assert report['total']==13 and report['passed']==13 and result.returncode==0, 'gateway acceptance failed; inspect report'
        print('Gateway acceptance: %d/%d cases passed; report %s'%(report['passed'],report['total'],output))
    finally:
        run('down','--remove-orphans')


if __name__=='__main__': main()
