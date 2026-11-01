"""Read-only New-API model/channel operations snapshots; no inference calls."""
import argparse
import json
import math
import os
from pathlib import Path
import time
import tempfile
from urllib.parse import urlencode,urlparse

import probe


class IncompleteSnapshot(ValueError): pass


def collect(page,max_pages=500):
    if not 1<=max_pages<=500: raise ValueError('max_pages must be 1..500')
    unique={}
    expected=None
    for number in range(1,max_pages+1):
        data=page(number)
        total=int(data['total'])
        if expected is None: expected=total
        if total!=expected: raise IncompleteSnapshot('log window changed during pagination')
        for row in data['items']:
            row_id=row.get('id')
            if not isinstance(row_id,int) or row_id<=0: raise IncompleteSnapshot('missing stable log identity')
            if row_id in unique and unique[row_id]!=row: raise IncompleteSnapshot('log identity changed')
            unique[row_id]=row
        if len(unique)==expected: return list(unique.values())
        if len(unique)>expected or not data['items']: raise IncompleteSnapshot('incomplete log window')
    raise IncompleteSnapshot('pagination cap; window not complete')


def summarize(rows):
    unique={}
    for row in rows:
        row_id=row.get('id')
        if not isinstance(row_id,int) or row_id<=0: raise IncompleteSnapshot('missing stable log identity')
        if row_id in unique and unique[row_id]!=row: raise IncompleteSnapshot('conflicting duplicate log')
        unique[row_id]=row
    groups={}
    requests=set()
    uncorrelated=0
    included=0
    for row in unique.values():
        kind=row.get('type')
        if kind not in (2,5) or not row.get('model_name'): continue
        included+=1
        request_id=row.get('request_id')
        if request_id: requests.add(request_id)
        else: uncorrelated+=1
        key=(str(row['model_name']),int(row.get('channel') or 0))
        if len(key[0])>256: raise ValueError('model label too long')
        if key not in groups:
            if len(groups)>=1000: raise ValueError('model/channel series limit')
            groups[key]={'model':key[0],'channel_id':key[1],'log_events':0,'error_events':0,'consume_events':0,
                'completed_streams':0,'gateway_ok_streams':0,'stream_errors':0,'unknown_stream_outcomes':0,
                'gateway_quota_units':0,'_frt':[]}
        group=groups[key]
        group['log_events']+=1
        if kind==5:
            group['error_events']+=1
            continue
        group['consume_events']+=1
        group['gateway_quota_units']+=int(row.get('quota') or 0)
        try: other=json.loads(row.get('other') or '{}')
        except (TypeError,ValueError): other={}
        if not isinstance(other,dict): other={}
        frt=other.get('frt')
        if isinstance(frt,(int,float)) and not isinstance(frt,bool) and math.isfinite(frt) and 0<=frt<=3600000:
            group['_frt'].append(float(frt))
        if row.get('is_stream'):
            state=other.get('stream_status') or {}
            if not isinstance(state,dict): state={}
            if state.get('status')=='error' or state.get('response_status') in ('failed','cancelled','incomplete'):
                group['stream_errors']+=1
            else:
                if state.get('status')=='ok': group['gateway_ok_streams']+=1
                if state.get('status')=='ok' and state.get('response_status')=='completed': group['completed_streams']+=1
                else: group['unknown_stream_outcomes']+=1
    for group in groups.values():
        values=sorted(group.pop('_frt'))
        group['gateway_frt_samples']=len(values)
        group['gateway_frt_p95_ms']=values[math.ceil(.95*len(values))-1] if values else None
    return {'logical_requests':len(requests),'uncorrelated_events':uncorrelated,'log_records':included,
            'channels':sorted(groups.values(),key=lambda g:(g['model'],g['channel_id']))}


def prometheus(data):
    def label(value): return str(value).replace('\\','\\\\').replace('\n','\\n').replace('"','\\"')
    mapping={'log_events':'log_events','error_events':'error_events','consume_events':'consume_events',
        'gateway_ok_streams':'gateway_ok_streams','stream_errors':'stream_errors',
        'unknown_stream_outcomes':'unknown_stream_outcomes','gateway_quota_units':'gateway_quota_units',
        'gateway_frt_samples':'gateway_frt_samples','gateway_frt_p95_ms':'gateway_frt_p95_ms'}
    lines=[]
    metadata={'gateway_collection_success':1 if data.get('complete') else 0,
        'gateway_collection_attempt_timestamp_seconds':data.get('collected_at'),
        'gateway_collection_last_success_timestamp_seconds':data.get('last_success'),
        'gateway_window_start_timestamp_seconds':data.get('window_start'),
        'gateway_window_end_timestamp_seconds':data.get('window_end')}
    for name,value in metadata.items():
        if value is not None: lines.extend(['# TYPE '+name+' gauge',name+' '+str(value)])
    for metric,field in mapping.items():
        name='gateway_window_'+metric
        lines.append('# TYPE '+name+' gauge')
        for group in data['channels']:
            value=group[field]
            if value is not None:
                lines.append(f'{name}{{model="{label(group["model"])}",channel_id="{group["channel_id"]}"}} {value}')
    return '\n'.join(lines)+'\n'


def snapshot(api,start,end,max_pages=500,expected_request_ids=None):
    if start<0 or end<start: raise ValueError('invalid fixed log window')
    def page(number):
        return api('/api/log/?'+urlencode({'p':number,'page_size':100,'start_timestamp':start,'end_timestamp':end}))
    rows=collect(page,max_pages)
    if expected_request_ids is not None:
        found={r.get('request_id') for r in rows}
        if not set(expected_request_ids)<=found:
            raise IncompleteSnapshot('correlated request missing from log snapshot')
    now=int(time.time())
    return {'window_start':start,'window_end':end,'complete':True,'collected_at':now,'last_success':now,**summarize(rows)}


def atomic_write(path,content):
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary=None
    try:
        with tempfile.NamedTemporaryFile(mode='w',encoding='utf-8',dir=path.parent,prefix='.'+path.name,delete=False) as stream:
            temporary=Path(stream.name)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary,path)
    finally:
        if temporary is not None: temporary.unlink(missing_ok=True)


def publish_failure(output,prom_path,error_type,now=None):
    last_success=None
    try:
        previous=json.loads(output.read_text('utf-8'))
        if isinstance(previous,dict): last_success=previous.get('last_success')
    except (OSError,ValueError): pass
    data={'complete':False,'collected_at':int(time.time()) if now is None else now,
          'last_success':last_success,'error_type':error_type,'channels':[]}
    atomic_write(output,json.dumps(data,ensure_ascii=False,indent=2))
    if prom_path: atomic_write(prom_path,prometheus(data))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--base-url',default='http://127.0.0.1:3000')
    parser.add_argument('--user-id',required=True)
    parser.add_argument('--token-env',default='NEW_API_ADMIN_TOKEN')
    parser.add_argument('--window-seconds',type=int,default=300)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--prometheus',type=Path)
    args=parser.parse_args()
    url=urlparse(args.base_url)
    if url.scheme!='https' and not (url.scheme=='http' and url.hostname in ('127.0.0.1','localhost','[::1]','::1')):
        raise ValueError('use HTTPS or loopback HTTP for management credentials')
    if url.username or url.password or url.query or url.fragment: raise ValueError('credentials/query not allowed in base URL')
    if not 1<=args.window_seconds<=86400: raise ValueError('window must be 1..86400 seconds')
    token=os.environ[args.token_env]
    def api(path):
        with probe.request(args.base_url.rstrip('/'),path,headers={'Authorization':'Bearer '+token,'New-Api-User':args.user_id}) as response:
            if response.status!=200: raise RuntimeError('management log request failed')
            obj=probe.read_json(response)
            if not obj.get('success'): raise RuntimeError('management log request rejected')
            return obj['data']
    end=int(time.time())-2
    try: data=snapshot(api,end-args.window_seconds,end)
    except Exception as exc:
        publish_failure(args.output,args.prometheus,type(exc).__name__)
        raise SystemExit('log collection failed; inspect collection health') from None
    atomic_write(args.output,json.dumps(data,ensure_ascii=False,indent=2))
    if args.prometheus: atomic_write(args.prometheus,prometheus(data))
    print(json.dumps({'complete':True,'log_records':data['log_records'],'model_channel_series':len(data['channels'])}))


if __name__=='__main__': main()
