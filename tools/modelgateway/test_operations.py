import json
import unittest
import tempfile
from pathlib import Path

import operations


def record(row_id,request_id,channel,kind=2,stream=True,status='ok',response='completed',frt=120):
    return {'id':row_id,'request_id':request_id,'channel':channel,'type':kind,'model_name':'gateway-chat',
            'is_stream':stream,'quota':16 if kind==2 else 0,
            'other':json.dumps({'frt':frt,'stream_status':{'status':status,'response_status':response}})}


class ChannelOperations(unittest.TestCase):
    def test_failure_then_fallback_deduplicates_log_rows_not_channel_events(self):
        rows=[record(1,'a',7,5),record(2,'a',8),record(2,'a',8)]
        result=operations.summarize(rows)
        self.assertEqual(result['logical_requests'],1)
        self.assertEqual(result['log_records'],2)
        self.assertEqual(result['channels'][0]['error_events'],1)
        self.assertEqual(result['channels'][1]['completed_streams'],1)
        self.assertEqual(result['channels'][1]['gateway_quota_units'],16)

    def test_billed_interruption_and_missing_timing_are_not_success_or_zero_latency(self):
        result=operations.summarize([record(3,'b',7,status='error',response='unknown',frt=None)])
        channel=result['channels'][0]
        self.assertEqual(channel['completed_streams'],0)
        self.assertEqual(channel['stream_errors'],1)
        self.assertEqual(channel['gateway_quota_units'],16)
        self.assertIsNone(channel['gateway_frt_p95_ms'])

    def test_ambiguous_stream_completion_stays_unknown(self):
        result=operations.summarize([record(4,'c',7,response='unknown')])
        self.assertEqual(result['channels'][0]['unknown_stream_outcomes'],1)

    def test_pagination_cap_is_not_silent_partial_success(self):
        def page(number): return {'total':3,'items':[record(number,str(number),7)]}
        with self.assertRaises(operations.IncompleteSnapshot): operations.collect(page,max_pages=2)

    def test_duplicate_rows_cannot_hide_incomplete_snapshot(self):
        def page(number): return {'total':2,'items':[record(1,'same',7)]}
        with self.assertRaises(operations.IncompleteSnapshot): operations.collect(page,max_pages=2)

    def test_prometheus_snapshot_is_gauge_and_escapes_labels(self):
        data=operations.summarize([record(1,'a',7)])
        data['channels'][0]['model']='quoted"model\\x'
        text=operations.prometheus(data)
        self.assertIn('# TYPE gateway_window_log_events gauge',text)
        self.assertIn('model="quoted\\"model\\\\x"',text)
        self.assertNotIn('request_id=',text)

    def test_unrelated_ids_cannot_hide_a_missing_correlated_request(self):
        def api(path): return {'total':2,'items':[record(1,'other1',7),record(2,'other2',7)]}
        with self.assertRaises(operations.IncompleteSnapshot):
            operations.snapshot(api,1,2,expected_request_ids={'missing'})

    def test_failed_collection_replaces_stale_metrics_with_health_and_freshness(self):
        with tempfile.TemporaryDirectory() as directory:
            report=Path(directory)/'snapshot.json'
            metrics=Path(directory)/'snapshot.prom'
            report.write_text(json.dumps({'last_success':100,'complete':True}),'utf-8')
            metrics.write_text('old_gateway_success 9000\n','utf-8')
            operations.publish_failure(report,metrics,'IncompleteSnapshot',now=200)
            value=metrics.read_text('utf-8')
            self.assertNotIn('old_gateway_success',value)
            self.assertIn('gateway_collection_success 0',value)
            self.assertIn('gateway_collection_last_success_timestamp_seconds 100',value)
            self.assertIn('gateway_collection_attempt_timestamp_seconds 200',value)

    def test_snapshot_exposes_window_timestamps(self):
        data={'complete':True,'last_success':200,'collected_at':200,'window_start':100,'window_end':198,'channels':[]}
        value=operations.prometheus(data)
        self.assertIn('gateway_window_end_timestamp_seconds 198',value)
        self.assertIn('gateway_collection_success 1',value)


if __name__=='__main__': unittest.main()
