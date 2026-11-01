"""End-to-end acceptance of the pinned local New-API deployment.

Run after compose up on a NEW ephemeral instance. Refuses initialized instances;
this cannot configure a production gateway. Generated secrets stay in memory.
"""
import argparse
import concurrent.futures
import http.cookiejar
import json
import math
from pathlib import Path
import secrets
import threading
import time
import urllib.request

import probe

if not __debug__: raise RuntimeError('acceptance assertions require Python without -O')

BASE='http://proxy:8080'
FIXTURE='http://fixture:8080'
MODELS={'probe-openai':(1,'oai','gpt-4o-mini'),'probe-claude':(14,'claude','claude-sonnet-4-20250514'),
        'probe-gemini':(24,'gemini','gemini-2.0-flash'),'probe-failover':(1,'fail','gpt-4o-mini'),
        'probe-half':(1,'half','gpt-4o-mini'),'probe-timeout':(1,'slow','gpt-4o-mini')}
TOOL={'type':'function','function':{'name':'inspect','description':'Inspect a host','parameters':{'type':'object','properties':{'host':{'type':'string'}},'required':['host']}}}
# A valid 1px PNG; no network image fetching.
PNG='iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/l9sAAAAASUVORK5CYII='


class Lab:
    def __init__(self):
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),probe.NoRedirect(),urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.headers={}
        self.rows=[]
        self.observations=[]
        self.observation_lock=threading.Lock()

    def api(self,path,body=None,method=None):
        req=urllib.request.Request(BASE+path,data=None if body is None else json.dumps(body).encode(),
            headers={'Content-Type':'application/json',**self.headers},method=method)
        with self.opener.open(req,timeout=10) as response: obj=probe.read_json(response)
        if not obj.get('success'): raise RuntimeError('management operation failed: '+path)
        return obj.get('data')

    def setup(self):
        for _ in range(60):
            try: setup=self.api('/api/setup'); break
            except (OSError,ValueError): time.sleep(.5)
        else: raise RuntimeError('gateway did not become ready')
        if setup['status'] or setup.get('root_init'): raise RuntimeError('refusing initialized instance; recreate ephemeral lab')
        password=secrets.token_urlsafe(24)+'aA1!'
        self.api('/api/setup',{'username':'probe_admin','password':password,'confirmPassword':password,'SelfUseModeEnabled':False,'DemoSiteEnabled':False})
        login=self.api('/api/user/login',{'username':'probe_admin','password':password})
        # v1 rc47 issues a short-lived dashboard access JWT instead of the old
        # opaque user token. Support this pinned API, not arbitrary versions.
        if login.get('access_token'): self.headers['Authorization']='Bearer '+login['access_token']
        user=login.get('user',login)
        self.headers['New-Api-User']=str(user['id'])
        self.option('ModelRatio',json.dumps({m:1 for m in MODELS}))
        self.option('CompletionRatio',json.dumps({m:1 for m in MODELS}))
        self.option('RetryTimes','1')
        self.option('LogConsumeEnabled','true')
        for model,(kind,peer,upstream) in MODELS.items(): self.channel(model,kind,peer,upstream,100)
        self.channel('probe-failover',1,'oai','gpt-4o-mini',0)
        self.key,self.token_id=self.token('normal',quota=1000000)
        self.denied,_=self.token('model-restricted',quota=100000,model_limits='probe-openai')
        self.empty,_=self.token('empty',quota=0)

    def option(self,key,value): self.api('/api/option/',{'key':key,'value':value},'PUT')

    def channel(self,model,kind,peer,upstream,priority):
        self.api('/api/channel/',{'mode':'single','channel':{'name':peer+'-'+model,'type':kind,'key':'fixture-only-no-provider-key',
            'base_url':'http://fixture:8080/'+peer,'models':model,'group':'default','priority':priority,'weight':1,'status':1,'auto_ban':0,
            'model_mapping':json.dumps({model:upstream})}})

    def token(self,name,quota,model_limits=''):
        self.api('/api/token/',{'name':name,'expired_time':-1,'remain_quota':quota,'unlimited_quota':False,
            'group':'default','model_limits_enabled':bool(model_limits),'model_limits':model_limits})
        data=self.api('/api/token/?p=1&page_size=100')
        items=data['items'] if isinstance(data,dict) else data
        token=next(t for t in items if t['name']==name)
        key=self.api('/api/token/'+str(token['id'])+'/key',{})['key']
        return key,token['id']

    def records(self):
        with probe.request(FIXTURE,'/records') as response: return probe.read_json(response)

    def reset(self):
        with probe.request(FIXTURE,'/reset',{}) as response: probe.read_json(response)

    def inference(self,model,stream=True,tool=False,messages=None,key=None):
        body={'model':model,'stream':stream,'max_tokens':64,'messages':messages or [{'role':'system','content':'Synthetic protocol probe.'},{'role':'user','content':'TOOL' if tool else 'TEXT'}]}
        if stream: body['stream_options']={'include_usage':True}
        if tool: body['tools']=[TOOL]
        started=time.perf_counter()
        try: result=probe.chat(BASE,key or self.key,body)
        except Exception as exc:
            with self.observation_lock:
                self.observations.append({'model':model,'stream':stream,'success':False,'ttft_ms':None,'error':True,
                    'interrupted':stream and bool(getattr(exc,'stream_started',False)),
                    'unknown_stream_stage':stream and not bool(getattr(exc,'stream_started',False))})
            raise
        observation={'model':model,'stream':stream,'success':result['complete'],'ttft_ms':result['ttft_ms'],
                     'elapsed_ms':(time.perf_counter()-started)*1000,'error':result['status']!=200,
                     'interrupted':stream and result['status']==200 and not result['complete']}
        with self.observation_lock: self.observations.append(observation)
        return result

    def case(self,name,model,fn):
        started=time.perf_counter()
        try:
            result=fn()
            row={'case':name,'model':model,'passed':True,**(result or {})}
        except Exception as exc:
            row={'case':name,'model':model,'passed':False,'error_type':type(exc).__name__}
        row['elapsed_ms']=round((time.perf_counter()-started)*1000,3)
        self.rows.append(row)
        print(json.dumps(row,ensure_ascii=True),flush=True)

    def protocol_case(self,model,stream):
        self.reset()
        first=self.inference(model,stream,tool=True)
        assert first['status']==200 and first['complete'] and first['tools']
        tool=first['tools'][0]
        assert tool['name']=='inspect' and tool['arguments']=={'host':'上海'}
        messages=[{'role':'system','content':'Synthetic protocol probe.'},{'role':'user','content':'TOOL'},
            {'role':'assistant','content':None,'tool_calls':[{'id':tool['id'],'type':'function','function':{'name':tool['name'],'arguments':json.dumps(tool['arguments'])}}]},
            {'role':'tool','tool_call_id':tool['id'],'content':'host is healthy'},
            {'role':'user','content':[{'type':'text','text':'TEXT'}, {'type':'image_url','image_url':{'url':'data:image/png;base64,'+PNG}}]}]
        second=self.inference(model,stream,messages=messages)
        assert second['complete'] and second['text']=='verified 上海'
        records=self.records()
        assert len(records)==2
        assert all(r['model']==MODELS[model][2] for r in records)
        assert records[0]['system_matches'] and records[0]['schema_matches']
        assert records[1]['has_tool_result'] and records[1]['has_image']
        assert records[1]['system_matches'] and records[1]['image_matches'] and records[1]['result_matches']
        assert records[1]['tool_result_references']==(['inspect'] if model=='probe-gemini' else [tool['id']])
        return {'channel':MODELS[model][1],'upstream_attempts':len(records),'ttft_ms':second['ttft_ms'],'usage_known':second['usage'] is not None}

    def rejection(self,key,model):
        self.reset()
        result=self.inference(model,key=key)
        assert result['status'] in (401,403,429) and not result['complete']
        assert not self.records()
        return {'status':result['status'],'upstream_attempts':0}

    def failover(self):
        self.reset()
        before=self.api('/api/token/'+str(self.token_id))['used_quota']
        result=self.inference('probe-failover')
        records=self.records()
        assert result['complete'] and result['text']=='verified 上海'
        assert len(records)==2 and records[0]['path'].startswith('/fail/') and records[1]['path'].startswith('/oai/')
        after=self.api('/api/token/'+str(self.token_id))['used_quota']
        assert after-before==16
        return {'upstream_attempts':2,'channel':'fail->oai','ttft_ms':result['ttft_ms'],'gateway_quota_delta':after-before}

    def half(self):
        self.reset()
        result=self.inference('probe-half')
        assert result['status']==200 and result['text']=='verified 上海' and result['ttft_ms'] is not None
        assert not result['complete'] and result['finish'] is None and len(self.records())==1
        return {'upstream_attempts':1,'channel':'half','stream_interrupted':True,'ttft_ms':result['ttft_ms'],
                'usage_source':'gateway-estimate' if result['usage'] else 'unknown','provider_usage_known':False}

    def timeout(self):
        self.reset()
        before=self.api('/api/token/'+str(self.token_id))['used_quota']
        baseline=self.inference('probe-timeout',False)
        baseline_attempts=len(self.records())
        assert not baseline['complete'] and baseline_attempts==2
        # The pinned gateway wraps transport timeouts as HTTP 500. Permit only
        # the explicit rejection status from our controlled upstream contract;
        # do not blindly retry an ambiguous transport outcome.
        self.option('AutomaticRetryStatusCodes','503')
        self.reset()
        started=time.perf_counter()
        result=self.inference('probe-timeout',False)
        elapsed=time.perf_counter()-started
        attempts=len(self.records())
        assert not result['complete'] and result['status']>=400 and elapsed<7 and attempts==1
        after=self.api('/api/token/'+str(self.token_id))['used_quota']
        assert after==before
        return {'baseline_upstream_attempts':baseline_attempts,'upstream_attempts':attempts,'channel':'slow','status':result['status'],'gateway_quota_delta':after-before}

    def metrics(self):
        groups=[]
        for model in MODELS:
            rows=[o for o in self.observations if o['model']==model]
            if not rows: continue
            times=sorted(o['ttft_ms'] for o in rows if o['ttft_ms'] is not None)
            streams=[o for o in rows if o['stream']]
            groups.append({'model':model,'configured_channel':MODELS[model][1],
                'channel_scope':'priority fallback oai after explicit 503' if model=='probe-failover' else 'single fixture channel',
                'logical_requests':len(rows),'successes':sum(o['success'] for o in rows),
                'stream_requests':len(streams),'stream_interruptions':sum(o.get('interrupted',False) for o in streams),
                'stream_failures_unknown_stage':sum(o.get('unknown_stream_stage',False) for o in streams),
                'ttft_samples':len(times),'ttft_p95_ms':round(times[math.ceil(.95*len(times))-1],3) if times else None})
        # Explicitly includes failure injection / denied requests, not an uptime
        # estimate. Never average these groups into a production availability.
        return groups

    def load(self):
        self.reset()
        total=120
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            results=list(pool.map(lambda _:self.inference('probe-openai'),range(total)))
        successes=sum(r['complete'] and r['text']=='verified 上海' for r in results)
        attempts=len(self.records())
        assert successes==total and attempts==total
        times=sorted(r['ttft_ms'] for r in results if r['ttft_ms'] is not None)
        return {'channel':'oai','logical_requests':total,'successes':successes,'upstream_attempts':attempts,
                'concurrency':12,'ttft_p95_ms':round(times[math.ceil(.95*len(times))-1],3)}

    def rate_limit(self):
        self.option('ModelRequestRateLimitCount','1')
        self.option('ModelRequestRateLimitSuccessCount','0')
        self.option('ModelRequestRateLimitEnabled','true')
        try:
            self.reset()
            first=self.inference('probe-openai')
            second=self.inference('probe-openai')
            assert first['complete'] and second['status']==429 and len(self.records())==1
            return {'status':429,'logical_requests':2,'upstream_attempts':1,'channel':'oai'}
        finally: self.option('ModelRequestRateLimitEnabled','false')

    def run(self):
        self.setup()
        for model in ['probe-openai','probe-claude','probe-gemini']:
            for stream in [False,True]:
                self.case(('stream' if stream else 'json')+'-tool-image-roundtrip',model,lambda m=model,s=stream:self.protocol_case(m,s))
        self.case('model-acl-before-dispatch','probe-claude',lambda:self.rejection(self.denied,'probe-claude'))
        self.case('empty-quota-before-dispatch','probe-openai',lambda:self.rejection(self.empty,'probe-openai'))
        self.case('503-priority-failover','probe-failover',self.failover)
        self.case('partial-stream-no-replay','probe-half',self.half)
        self.case('bounded-relay-timeout','probe-timeout',self.timeout)
        self.case('bounded-concurrency','probe-openai',self.load)
        self.case('rate-limit-before-dispatch','probe-openai',self.rate_limit)
        return {'scope':'real pinned New-API; deterministic local providers; not model-quality or production SLA',
            'gateway_image':'calciumion/new-api@sha256:938fa2713d4946723c9e9ce3076351c33f7770699fec92d23f3a26fd95791026',
            'cases':self.rows,'metrics':self.metrics(),'passed':sum(r['passed'] for r in self.rows),'total':len(self.rows)}


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    report=Lab().run()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2),'utf-8')
    raise SystemExit(0 if report['passed']==report['total'] else 1)
