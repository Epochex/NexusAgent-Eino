"""Deterministic upstreams for protocol/dispatch testing, never model quality."""
import json
import base64
import threading
import time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer

LOCK=threading.Lock()
RECORDS=[]
COUNTS={}
ACTIVE=0
PEAK_ACTIVE=0
TOOL={'name':'inspect','arguments':{'host':'上海'}}


def audit(obj):
    systems=[]
    schemas=[]
    images=[]
    results=[]
    def text(value):
        return value if isinstance(value,str) else ''.join(p.get('text','') for p in value or [])
    def parts(values):
        for part in values:
            if part.get('type')=='image_url': images.append(part['image_url']['url'].partition(',')[2])
            if part.get('type')=='image': images.append(part['source'].get('data',''))
            if 'inlineData' in part: images.append(part['inlineData'].get('data',''))
            if part.get('type')=='tool_result': results.append(text(part.get('content')))
            if 'functionResponse' in part:
                response=part['functionResponse'].get('response',{})
                # New-API wraps OpenAI's string tool result in content.
                results.append(response.get('content',response.get('result','')))
    for message in obj.get('messages',[]):
        if message.get('role')=='system': systems.append(text(message.get('content')))
        if message.get('role')=='tool': results.append(text(message.get('content')))
        if isinstance(message.get('content'),list): parts(message['content'])
    if obj.get('system') is not None: systems.append(text(obj['system']))
    if obj.get('systemInstruction'): systems.append(text(obj['systemInstruction'].get('parts')))
    for content in obj.get('contents',[]): parts(content.get('parts',[]))
    for tool in obj.get('tools',[]):
        if 'function' in tool: schemas.append((tool['function'].get('name'),tool['function'].get('parameters',{})))
        if 'input_schema' in tool: schemas.append((tool.get('name'),tool['input_schema']))
        for fn in tool.get('functionDeclarations',[]): schemas.append((fn.get('name'),fn.get('parameters',{})))
    expected_image=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/l9sAAAAASUVORK5CYII=')
    try: image_matches=len(images)==1 and base64.b64decode(images[0],validate=True)==expected_image
    except ValueError: image_matches=False
    schema_matches=len(schemas)==1 and schemas[0][0]=='inspect'
    if schema_matches:
        schema=schemas[0][1]
        schema_matches=(str(schema.get('type')).lower()=='object' and schema.get('required')==['host'] and
            set(schema.get('properties',{}))=={'host'} and str(schema['properties']['host'].get('type')).lower()=='string')
    return {'system_matches':systems==['Synthetic protocol probe.'],'schema_matches':schema_matches,
            'image_matches':image_matches,'result_matches':results==['host is healthy']}


class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args): pass

    def send_json(self,obj,status=200):
        raw=json.dumps(obj,ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path=='/stats':
            with LOCK: stats={'dispatches':sum(COUNTS.values()),'by_path':dict(COUNTS),'active':ACTIVE,'peak_active':PEAK_ACTIVE}
            self.send_json(stats)
        elif self.path=='/records':
            with LOCK: records=list(RECORDS)
            self.send_json(records)
        else: self.send_json({'ok':True})

    def do_POST(self):
        global ACTIVE,PEAK_ACTIVE
        size=int(self.headers.get('Content-Length','0'))
        if size>1048576: return self.send_json({'error':'body too big'},413)
        obj=json.loads(self.rfile.read(size))
        if self.path=='/reset':
            with LOCK:
                RECORDS.clear()
                COUNTS.clear()
                PEAK_ACTIVE=ACTIVE
            return self.send_json({'ok':True})
        # Export only synthetic assertions; never record credentials.
        raw=json.dumps(obj,ensure_ascii=False)
        model=obj.get('model') or self.path.split('/models/')[-1].split(':')[0]
        refs=[]
        for message in obj.get('messages',[]):
            if message.get('role')=='tool': refs.append(message.get('tool_call_id'))
            if isinstance(message.get('content'),list):
                refs.extend(p.get('tool_use_id') for p in message['content'] if p.get('type')=='tool_result')
        for content in obj.get('contents',[]):
            refs.extend(p['functionResponse'].get('name') for p in content.get('parts',[]) if 'functionResponse' in p)
        with LOCK:
            COUNTS[self.path]=COUNTS.get(self.path,0)+1
            ordinal=COUNTS[self.path]
            ACTIVE+=1
            PEAK_ACTIVE=max(PEAK_ACTIVE,ACTIVE)
            RECORDS.append({'path':self.path,'model':model,'has_tool_result':('tool_result' in raw or 'tool_call_id' in raw or 'functionResponse' in raw),
                            'has_image':('image_url' in raw or '"image"' in raw or 'inlineData' in raw or 'inline_data' in raw),
                            'has_system':'system' in raw,'has_tool_schema':'inspect' in raw,
                            'tool_result_references':refs,**audit(obj)})
            if len(RECORDS)>1000: del RECORDS[0]
        try:
            if self.path.startswith('/fail/') or (self.path.startswith('/flaky/') and ordinal%5==0):
                return self.send_json({'error':{'message':'fixture overload','type':'server_error','code':'overloaded'}},503)
            if self.path.startswith('/slow/'): time.sleep(4)
            wants_tool='TOOL' in raw and not ('tool_result' in raw or 'tool_call_id' in raw or 'functionResponse' in raw)
            stream=obj.get('stream',False) or ':streamGenerateContent' in self.path
            half=self.path.startswith('/half/')
            if '/claude/' in self.path: return self.claude(model,wants_tool,stream)
            if '/gemini/' in self.path: return self.gemini(model,wants_tool,stream)
            return self.openai(model,wants_tool,stream,half)
        except (BrokenPipeError,ConnectionResetError): pass
        finally:
            with LOCK: ACTIVE-=1

    def start_stream(self):
        self.send_response(200)
        self.send_header('Content-Type','text/event-stream')
        self.send_header('Connection','close')
        self.end_headers()

    def event(self,obj,event=None):
        if event: self.wfile.write(('event: '+event+'\n').encode())
        data=obj if isinstance(obj,str) else json.dumps(obj,ensure_ascii=False)
        self.wfile.write(('data: '+data+'\n\n').encode())
        self.wfile.flush()

    def openai(self,model,tool,stream,half):
        usage={'prompt_tokens':12,'completion_tokens':4,'total_tokens':16}
        call={'id':'call_probe','type':'function','function':{'name':'inspect','arguments':json.dumps(TOOL['arguments'],ensure_ascii=False)}}
        finish='tool_calls' if tool else 'stop'
        if not stream:
            return self.send_json({'id':'chatcmpl-probe','object':'chat.completion','created':1,'model':model,
                                   'choices':[{'index':0,'message':{'role':'assistant','content':None if tool else 'verified 上海',**({'tool_calls':[call]} if tool else {})},'finish_reason':finish}],'usage':usage})
        self.start_stream()
        def chunk(delta,reason=None):
            return {'id':'chatcmpl-probe','object':'chat.completion.chunk','created':1,'model':model,
                    'choices':[{'index':0,'delta':delta,'finish_reason':reason}]}
        self.event(chunk({'role':'assistant'}))
        time.sleep(.025)
        if tool:
            self.event(chunk({'tool_calls':[{'index':0,'id':'call_probe','type':'function','function':{'name':'inspect','arguments':'{"host":'}}]}))
            self.event(chunk({'tool_calls':[{'index':0,'function':{'arguments':'"上海"}'}}]}))
        else: self.event(chunk({'content':'verified 上海'}))
        if self.path.startswith('/hold/'): time.sleep(.5)
        if half: return
        self.event(chunk({},finish))
        self.event({'id':'chatcmpl-probe','object':'chat.completion.chunk','created':1,'model':model,'choices':[],'usage':usage})
        self.event('[DONE]')

    def claude(self,model,tool,stream):
        content=[{'type':'tool_use','id':'call_probe','name':'inspect','input':TOOL['arguments']}] if tool else [{'type':'text','text':'verified 上海'}]
        reason='tool_use' if tool else 'end_turn'
        message={'id':'msg_probe','type':'message','role':'assistant','model':model,'content':content,'stop_reason':reason,'stop_sequence':None,'usage':{'input_tokens':12,'output_tokens':4}}
        if not stream: return self.send_json(message)
        self.start_stream()
        self.event({'type':'message_start','message':{**message,'content':[],'stop_reason':None,'usage':{'input_tokens':12,'output_tokens':0}}},'message_start')
        block={'type':'tool_use','id':'call_probe','name':'inspect','input':{}} if tool else {'type':'text','text':''}
        self.event({'type':'content_block_start','index':0,'content_block':block},'content_block_start')
        time.sleep(.025)
        deltas=[{'type':'input_json_delta','partial_json':'{"host":'},{'type':'input_json_delta','partial_json':'"上海"}'}] if tool else [{'type':'text_delta','text':'verified 上海'}]
        for delta in deltas: self.event({'type':'content_block_delta','index':0,'delta':delta},'content_block_delta')
        self.event({'type':'content_block_stop','index':0},'content_block_stop')
        self.event({'type':'message_delta','delta':{'stop_reason':reason,'stop_sequence':None},'usage':{'output_tokens':4}},'message_delta')
        self.event({'type':'message_stop'},'message_stop')

    def gemini(self,model,tool,stream):
        part={'functionCall':{'name':'inspect','args':TOOL['arguments']}} if tool else {'text':'verified 上海'}
        obj={'candidates':[{'content':{'role':'model','parts':[part]},'finishReason':'STOP','index':0}],
             'usageMetadata':{'promptTokenCount':12,'candidatesTokenCount':4,'totalTokenCount':16},'modelVersion':model}
        if not stream: return self.send_json(obj)
        self.start_stream()
        time.sleep(.025)
        self.event(obj)


class FixtureServer(ThreadingHTTPServer):
    # Avoid measuring the stdlib fixture's default accept backlog of five as
    # gateway capacity. This fixture setting is identical in both profiles.
    request_queue_size=512


if __name__=='__main__': FixtureServer(('0.0.0.0',8080),Handler).serve_forever()
