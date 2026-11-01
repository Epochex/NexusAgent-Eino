"""Bounded protocol probes. Inference requests are NEVER automatically retried."""
import json
import time
import urllib.error
import urllib.request

MAX_BODY=4*1024*1024


class ProtocolError(ValueError):
    def __init__(self,message,stream_started=False):
        super().__init__(message)
        self.stream_started=stream_started


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        return None


def sse_events(stream,limit=262144):
    """Parse UTF-8 SSE, bounding line, event AND total body, including comments."""
    parts=[]
    size=total=0
    deadline=time.monotonic()+30
    while True:
        raw=stream.readline(limit+1)
        if time.monotonic()>deadline: raise ProtocolError('stream duration limit')
        total+=len(raw)
        if len(raw)>limit or total>MAX_BODY:
            raise ProtocolError('stream byte limit')
        if not raw:
            if parts: raise ProtocolError('unterminated SSE event')
            return
        try: line=raw.decode('utf-8').rstrip('\r\n')
        except UnicodeDecodeError as exc: raise ProtocolError('invalid UTF-8') from exc
        if not line:
            if parts: yield '\n'.join(parts)
            parts=[]
            size=0
        elif line.startswith('data:'):
            part=line[5:]
            if part.startswith(' '): part=part[1:]
            size+=len(raw)
            if size>limit: raise ProtocolError('SSE event limit')
            parts.append(part)


def read_openai_stream(stream,start=None,clock=None):
    clock=clock or time.perf_counter
    start=clock() if start is None else start
    text=[]
    calls={}
    usage=None
    ttft=None
    finish=None
    done=False
    error=False
    for data in sse_events(stream):
        if data=='[DONE]':
            done=True
            break
        try: obj=json.loads(data)
        except json.JSONDecodeError as exc: raise ProtocolError('invalid SSE JSON') from exc
        if not isinstance(obj,dict): raise ProtocolError('SSE object required')
        if obj.get('error'): error=True
        if obj.get('usage') is not None: usage=obj['usage']
        for choice in obj.get('choices',[]):
            if choice.get('index',0)!=0: raise ProtocolError('only n=1 supported')
            delta=choice.get('delta') or {}
            content=delta.get('content') or ''
            tool_delta=delta.get('tool_calls') or []
            if ttft is None and (content or any(t.get('function',{}).get('name') or t.get('function',{}).get('arguments') for t in tool_delta)):
                ttft=(clock()-start)*1000
            text.append(content)
            for call in tool_delta:
                index=call.get('index',0)
                target=calls.setdefault(index,{'id':'','name':'','arguments':''})
                if call.get('id'): target['id']=call['id']
                fn=call.get('function',{})
                target['name']+=fn.get('name','')
                target['arguments']+=fn.get('arguments','')
            if choice.get('finish_reason') is not None: finish=choice['finish_reason']
    tools=[]
    for index in sorted(calls):
        call=calls[index].copy()
        try: call['arguments']=json.loads(call['arguments'])
        except json.JSONDecodeError as exc: raise ProtocolError('incomplete tool JSON') from exc
        if not call['name'] or not call['id']: raise ProtocolError('missing tool identity')
        tools.append(call)
    complete=done and finish in ('stop','tool_calls') and not error
    if finish=='tool_calls' and not tools: complete=False
    return {'complete':complete,'text':''.join(text),'tools':tools,'usage':usage,'ttft_ms':ttft,'finish':finish}


def request(base,path,body=None,headers=None,timeout=10,opener=None):
    req=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),
                               headers={'Content-Type':'application/json',**(headers or {})})
    open_request=opener.open if opener else urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect()).open
    try: return open_request(req,timeout=timeout)
    except urllib.error.HTTPError as response: return response


def read_json(response):
    data=response.read(MAX_BODY+1)
    if len(data)>MAX_BODY: raise ProtocolError('response byte limit')
    return json.loads(data)


def chat(base,key,body,timeout=10):
    started=time.perf_counter()
    with request(base,'/v1/chat/completions',body,{'Authorization':'Bearer '+key},timeout) as response:
        request_id=response.headers.get('X-Oneapi-Request-Id')
        if response.status!=200:
            # Do not export provider errors: they can contain prompts/keys.
            return {'status':response.status,'request_id':request_id,'complete':False,'ttft_ms':None,'usage':None,'elapsed_ms':(time.perf_counter()-started)*1000}
        if body.get('stream'):
            try:
                if 'text/event-stream' not in response.headers.get('Content-Type',''): raise ProtocolError('expected SSE')
                result=read_openai_stream(response,started)
            except Exception as exc:
                raise ProtocolError('invalid or interrupted SSE',stream_started=True) from exc
        else:
            obj=read_json(response)
            choice=obj['choices'][0]
            message=choice['message']
            tools=[{'id':t['id'],'name':t['function']['name'],'arguments':json.loads(t['function']['arguments'])} for t in message.get('tool_calls',[])]
            result={'complete':choice.get('finish_reason') in ('stop','tool_calls'),'text':message.get('content') or '',
                    'tools':tools,'usage':obj.get('usage'),'ttft_ms':None,'finish':choice.get('finish_reason')}
        return {'status':200,'request_id':request_id,**result,'elapsed_ms':(time.perf_counter()-started)*1000}
