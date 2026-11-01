"""Controlled local relay capacity and failure-policy experiments, not an SLA."""
import argparse
import concurrent.futures as futures
import json
import math
import os
from pathlib import Path
import time

from acceptance import Lab,FIXTURE
import operations
import probe

AUTH=Path('/tmp/gateway-capacity-auth.json')


def stats():
    with probe.request(FIXTURE,'/stats') as response: return probe.read_json(response)


def await_idle():
    for _ in range(100):
        current=stats()
        if current['active']==0: return current
        time.sleep(.1)
    raise RuntimeError('upstream requests did not drain')


def measure(lab,model,count=400,concurrency=200,duration=None,rps=None):
    await_idle()
    lab.reset()
    before=lab.api('/api/token/'+str(lab.token_id))['used_quota']
    results=[]
    request_ids=[]
    def call(index):
        try:
            body={'model':model,'stream':True,'max_tokens':64,'stream_options':{'include_usage':True},
                  'messages':[{'role':'user','content':'TEXT'}]}
            result=probe.chat('http://proxy:8080',lab.key,body,timeout=8)
            return {'success':result['complete'] and result.get('text')=='verified 上海','status':result['status'],
                    'ttft_ms':result['ttft_ms'],'elapsed_ms':result['elapsed_ms'],'request_id':result['request_id']}
        except Exception as exc:
            return {'success':False,'status':0,'error_type':type(exc).__name__,'ttft_ms':None,'elapsed_ms':None,'request_id':None}
    start=time.perf_counter()
    with futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        if duration is None:
            results=list(pool.map(call,range(count)))
        else:
            pending=set()
            scheduled=0
            while time.perf_counter()-start<duration:
                next_at=start+scheduled/rps
                if next_at>time.perf_counter(): time.sleep(next_at-time.perf_counter())
                done,pending=futures.wait(pending,timeout=0,return_when=futures.FIRST_COMPLETED)
                results.extend(f.result() for f in done)
                if len(pending)>=2*concurrency:
                    results.append({'success':False,'status':0,'error_type':'generator_backpressure','ttft_ms':None,'elapsed_ms':None,'request_id':None})
                else: pending.add(pool.submit(call,scheduled))
                scheduled+=1
            results.extend(f.result() for f in futures.as_completed(pending))
    elapsed=time.perf_counter()-start
    state=await_idle()
    after=lab.api('/api/token/'+str(lab.token_id))['used_quota']
    times=sorted(r['ttft_ms'] for r in results if r['ttft_ms'] is not None)
    statuses={str(code):sum(r['status']==code for r in results) for code in sorted({r['status'] for r in results})}
    request_ids=[r['request_id'] for r in results if r['request_id']]
    return {'model':model,'logical_requests':len(results),'successes':sum(r['success'] for r in results),
        'elapsed_seconds':round(elapsed,3),'concurrency_limit':concurrency,'target_rps':rps,
        'observed_rps':round(len(results)/elapsed,3),'ttft_samples':len(times),
        'content_ttft_p95_ms':round(times[math.ceil(.95*len(times))-1],3) if times else None,
        'statuses':statuses,'upstream':state,'gateway_quota_delta':after-before,
        '_request_ids':request_ids}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('phase',choices=['baseline','optimized'])
    parser.add_argument('--duration',type=int,default=180)
    parser.add_argument('--rps',type=int,default=100)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if not 1<=args.duration<=600 or not 1<=args.rps<=200: raise ValueError('workload bounds exceeded')
    lab=Lab()
    def checkpoint(measurements):
        data={'phase':args.phase,'scope':'local fixed-response providers, 25ms first-content delay and 500ms hold; no live-model performance claim',
              'measurements':[{k:v for k,v in measurement.items() if k!='_request_ids'} for measurement in measurements],
              'operations':None,'validation_complete':False}
        args.output.write_text(json.dumps(data,ensure_ascii=False,indent=2),'utf-8')
    if args.phase=='baseline':
        lab.setup()
        lab.channel('probe-hold',1,'hold','gpt-4o-mini',100)
        lab.channel('probe-flaky',1,'flaky','gpt-4o-mini',100)
        for setting in ['ModelRatio','CompletionRatio']:
            lab.option(setting,json.dumps({m:1 for m in ['probe-openai','probe-claude','probe-gemini','probe-failover','probe-half','probe-timeout','probe-hold','probe-flaky']}))
        # Synthetic token quota covers the explicit workload, never unlimited.
        lab.key,lab.token_id=lab.token('capacity',quota=5000000)
        AUTH.write_text(json.dumps({'headers':lab.headers,'key':lab.key,'token_id':lab.token_id,'start':int(time.time())-1}),'utf-8')
        os.chmod(AUTH,0o600)
        measurements=[measure(lab,'probe-hold')]
        checkpoint(measurements)
    else:
        auth=json.loads(AUTH.read_text('utf-8'))
        lab.headers=auth['headers'];lab.key=auth['key'];lab.token_id=auth['token_id']
        measurements=[measure(lab,'probe-hold')]
        checkpoint(measurements)
        lab.option('RetryTimes','0')
        measurements.append(measure(lab,'probe-flaky',count=1000,concurrency=64))
        checkpoint(measurements)
        # Validate error-log coverage BEFORE spending time on the long workload.
        time.sleep(1)
        operations.snapshot(lab.api,auth['start'],int(time.time()),expected_request_ids={r for m in measurements for r in m['_request_ids']})
        channels=lab.api('/api/channel/?p=1&page_size=100')['items']
        if not any(c['name']=='oai-probe-flaky' for c in channels): lab.channel('probe-flaky',1,'oai','gpt-4o-mini',0)
        lab.option('RetryTimes','1')
        lab.option('AutomaticRetryStatusCodes','503')
        measurements.append(measure(lab,'probe-flaky',count=1000,concurrency=64))
        checkpoint(measurements)
        measurements.append(measure(lab,'probe-hold',concurrency=200,duration=args.duration,rps=args.rps))
        checkpoint(measurements)
    # Let async consume/error log writes settle before freezing the time window.
    time.sleep(2)
    start=json.loads(AUTH.read_text('utf-8'))['start']
    known=set()
    for result in measurements: known.update(result.pop('_request_ids'))
    data={'phase':args.phase,'scope':'local fixed-response providers, 25ms first-content delay and 500ms hold; no live-model performance claim',
          'measurements':measurements,'operations':None}
    # Persist the measured workload even if the following telemetry audit fails.
    args.output.write_text(json.dumps(data,ensure_ascii=False,indent=2),'utf-8')
    telemetry=operations.snapshot(lab.api,start,int(time.time()),expected_request_ids=known)
    data['operations']=telemetry
    data['validation_complete']=True
    args.output.write_text(json.dumps(data,ensure_ascii=False,indent=2),'utf-8')
    args.output.with_suffix('.prom').write_text(operations.prometheus(telemetry),'utf-8')
    print(json.dumps({'phase':args.phase,'measurements':measurements,'channel_series':len(telemetry['channels'])},ensure_ascii=True),flush=True)


if __name__=='__main__': main()
