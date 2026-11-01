import io
import unittest

import probe
import fixture


def wire(*events):
    import json
    return io.BytesIO(b''.join(('data: '+(e if isinstance(e,str) else json.dumps(e,ensure_ascii=False))+'\r\n\r\n').encode() for e in events))


class StreamContract(unittest.TestCase):
    def test_tool_fragments_and_usage_preserve_identity(self):
        result=probe.read_openai_stream(wire(
            {'choices':[{'delta':{'role':'assistant'}}]},
            {'choices':[{'delta':{'tool_calls':[{'index':0,'id':'call_a','function':{'name':'inspect','arguments':'{"host":'}}]}}]},
            {'choices':[{'delta':{'tool_calls':[{'index':0,'function':{'arguments':'"上海"}'}}]},'finish_reason':'tool_calls'}]},
            {'choices':[],'usage':{'prompt_tokens':8,'completion_tokens':4}}, '[DONE]'))
        self.assertTrue(result['complete'])
        self.assertEqual(result['tools'],[{'id':'call_a','name':'inspect','arguments':{'host':'上海'}}])
        self.assertEqual(result['usage'],{'prompt_tokens':8,'completion_tokens':4})

    def test_done_alone_and_eof_are_not_success(self):
        for stream in [wire('[DONE]'),wire({'choices':[{'delta':{'content':'partial'}}]}),
                       wire({'choices':[{'delta':{'content':'partial'}}]},'[DONE]')]:
            with self.subTest(stream=stream):
                self.assertFalse(probe.read_openai_stream(stream)['complete'])

    def test_malformed_and_oversized_events_fail(self):
        with self.assertRaises(probe.ProtocolError):
            probe.read_openai_stream(wire('{broken'))
        with self.assertRaises(probe.ProtocolError):
            list(probe.sse_events(io.BytesIO(b'data: '+b'x'*100+b'\n\n'),limit=32))

    def test_content_time_excludes_role_and_usage(self):
        times=iter([10.,20.,30.])
        result=probe.read_openai_stream(wire(
            {'choices':[{'delta':{'role':'assistant'}}]},
            {'choices':[],'usage':{'prompt_tokens':8,'completion_tokens':0}},
            {'choices':[{'delta':{'content':'ok'},'finish_reason':'stop'}]},'[DONE]'),start=1.,clock=lambda:next(times))
        self.assertEqual(result['ttft_ms'],9000.)
        self.assertTrue(result['complete'])

    def test_sse_comments_and_multiline_data(self):
        raw=io.BytesIO(b': keepalive\r\nevent: message\r\ndata: {"a":\r\ndata: 1}\r\n\r\n')
        self.assertEqual(list(probe.sse_events(raw)),['{"a":\n1}'])

    def test_unknown_usage_is_not_zero(self):
        result=probe.read_openai_stream(wire({'choices':[{'delta':{'content':'ok'},'finish_reason':'stop'}]},'[DONE]'))
        self.assertIsNone(result['usage'])
        self.assertTrue(result['complete'])


class FixtureOracle(unittest.TestCase):
    def test_rejects_corrupt_system_schema_image_and_tool_result(self):
        import acceptance
        import copy
        body={'messages':[{'role':'system','content':'Synthetic protocol probe.'},
            {'role':'tool','tool_call_id':'call_probe','content':'host is healthy'},
            {'role':'user','content':[{'type':'image_url','image_url':{'url':'data:image/png;base64,'+acceptance.PNG}}]}],
            'tools':[acceptance.TOOL]}
        valid=fixture.audit(body)
        self.assertTrue(all(valid[k] for k in ['system_matches','schema_matches','image_matches','result_matches']))
        for field,mutate in [
            ('system_matches',lambda b:b['messages'][0].update(content='wrong system')),
            ('schema_matches',lambda b:b['tools'][0]['function']['parameters']['properties']['host'].update(type='integer')),
            ('image_matches',lambda b:b['messages'][2]['content'][0]['image_url'].update(url='data:image/png;base64,eA==')),
            ('result_matches',lambda b:b['messages'][1].update(content='wrong tool result'))]:
            altered=copy.deepcopy(body)
            mutate(altered)
            with self.subTest(field=field): self.assertFalse(fixture.audit(altered)[field])


class FailureAccounting(unittest.TestCase):
    def test_half_case_rejects_http_error_without_partial_output(self):
        import acceptance
        lab=acceptance.Lab()
        lab.reset=lambda:None
        lab.records=lambda:[{}]
        lab.inference=lambda *a,**kw:{'status':502,'complete':False,'text':'','ttft_ms':None,'finish':None}
        with self.assertRaises(AssertionError): lab.half()

    def test_opened_stream_exception_remains_in_interruption_denominator(self):
        import acceptance
        from unittest.mock import patch
        lab=acceptance.Lab()
        lab.key='fixture-key'
        error=probe.ProtocolError('truncated')
        error.stream_started=True
        with patch.object(probe,'chat',side_effect=error):
            with self.assertRaises(probe.ProtocolError): lab.inference('probe-openai')
        metrics=lab.metrics()[0]
        self.assertEqual(metrics['logical_requests'],1)
        self.assertEqual(metrics['successes'],0)
        self.assertEqual(metrics['stream_interruptions'],1)

    def test_real_http_parser_failure_records_opened_stream(self):
        import threading
        from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
        class Truncated(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_POST(self):
                self.send_response(200)
                self.send_header('Content-Type','text/event-stream')
                self.end_headers()
                self.wfile.write(b'data: {"choices":[] }')
        server=ThreadingHTTPServer(('127.0.0.1',0),Truncated)
        thread=threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        try:
            with self.assertRaises(probe.ProtocolError) as caught:
                probe.chat('http://127.0.0.1:'+str(server.server_port),'fixture',{'stream':True})
            self.assertTrue(caught.exception.stream_started)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__=='__main__': unittest.main()
