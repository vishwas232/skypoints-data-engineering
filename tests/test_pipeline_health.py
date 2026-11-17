import argparse
from contextlib import redirect_stdout
from datetime import datetime,timedelta,timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.check_pipeline import assess,check_pipeline,main,policy,source_spec

NOW=datetime(2026,11,18,6,tzinfo=timezone.utc)


def inputs():
    raw=[dict(BATCH_ID='b',STATUS='COMPLETED',FINISHED_AT=NOW-timedelta(hours=2))]
    transformed=[dict(STATUS='COMPLETED',COMPLETED_AT=NOW-timedelta(hours=1),
                      ITEM_ROWS=100,REJECTED_ITEMS=5)]
    return raw,transformed


def failed(raw,transformed):
    return {r['name'] for r in assess(raw,transformed,NOW,26,0.05,2) if not r['passed']}


class HealthPolicyTests(unittest.TestCase):
    def test_boundaries_pass(self):
        raw,transformed=inputs()
        self.assertEqual(set(),failed(raw,transformed))
        raw[0]['FINISHED_AT']=NOW-timedelta(hours=26)
        transformed[0]['COMPLETED_AT']=NOW-timedelta(hours=24)
        self.assertEqual(set(),failed(raw,transformed))

    def test_absent_or_incomplete_sources_fail(self):
        raw,transformed=inputs()
        self.assertEqual({'raw_batch_present'},failed([],[]))
        self.assertIn('transform_present',failed(raw,[]))
        raw[0]['STATUS']='FAILED'
        self.assertIn('raw_completed',failed(raw,transformed))

    def test_stale_future_missing_and_timezone_dates(self):
        raw,transformed=inputs()
        for ended in (NOW-timedelta(hours=27),NOW+timedelta(seconds=1),None):
            raw[0]['FINISHED_AT']=ended
            self.assertIn('raw_fresh',failed(raw,transformed))
        raw,transformed=inputs()
        raw[0]['FINISHED_AT']=raw[0]['FINISHED_AT'].replace(tzinfo=None)
        transformed[0]['COMPLETED_AT']=transformed[0]['COMPLETED_AT'].astimezone(timezone(timedelta(hours=5,minutes=30)))
        self.assertEqual(set(),failed(raw,transformed))

    def test_transform_lag_and_future_completion_fail(self):
        raw,transformed=inputs()
        for ended in (NOW+timedelta(seconds=1),NOW-timedelta(hours=3),None):
            transformed[0]['COMPLETED_AT']=ended
            self.assertIn('transform_within_sla',failed(raw,transformed))

    def test_rejection_rates_and_invalid_counts(self):
        raw,transformed=inputs()
        for total,rejected in ((100,6),(0,1),(1,-1),(-1,0)):
            transformed[0].update(ITEM_ROWS=total,REJECTED_ITEMS=rejected)
            self.assertIn('items_reject_rate',failed(raw,transformed))
        transformed[0].update(ITEM_ROWS=0,REJECTED_ITEMS=0)
        self.assertEqual(set(),failed(raw,transformed))

    def test_document_and_item_denominators_stay_separate(self):
        raw,transformed=inputs()
        transformed[0].update(DOCUMENT_ROWS=1,REJECTED_DOCUMENTS=1,ITEM_ROWS=100,REJECTED_ITEMS=0)
        self.assertIn('documents_reject_rate',failed(raw,transformed))
        self.assertNotIn('items_reject_rate',failed(raw,transformed))

    def test_source_and_numeric_policy_validation(self):
        self.assertEqual(('MEMBER','SKYPOINTS_IND'),source_spec('MEMBER:SKYPOINTS_IND'))
        for bad in ('','OTHER:SOURCE','MEMBER:x','MEMBER:X;SELECT','REDEMPTION:'+('X'*101)):
            with self.assertRaises(argparse.ArgumentTypeError):
                source_spec(bad)
        for limits in ((0,0.05,2),(26,-1,2),(26,1.1,2),(26,0.1,-1),(float('nan'),0,2),(26,float('inf'),2)):
            with self.assertRaises(ValueError):
                policy(*limits)


class HealthCursor:
    def __init__(self,violation=False):
        self.violation=violation
        self.statements=[]
        self.closed=False
        self.description=[]
        self.rows=[]

    def execute(self,sql,params=None):
        self.statements.append((sql,params))
        rows=[]
        if 'SELECT BATCH_ID,STATUS,FINISHED_AT FROM' in sql:
            rows=inputs()[0]
        elif 'INPUT_ROWS AS ITEM_ROWS' in sql:
            rows=inputs()[1]
        elif self.violation:
            rows=[{'MEMBER_NAME':'PRIVATE_CANARY'}]
        self.description=[(k,) for k in rows[0]] if rows else []
        self.rows=[tuple(r.values()) for r in rows]

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def close(self):
        self.closed=True


class HealthConnection:
    def __init__(self,**kwargs):
        self.cur=HealthCursor(**kwargs)
    def cursor(self):
        return self.cur


class HealthCommandTests(unittest.TestCase):
    def test_shipped_checks_are_selects_and_parameters_are_bound(self):
        conn=HealthConnection()
        report=check_pipeline(conn,[('MEMBER','SKYPOINTS_IND')],now=NOW)
        self.assertEqual('PASS',report['status'])
        self.assertGreater(len(report['checks']),30)
        self.assertTrue(conn.cur.closed)
        self.assertTrue(any(params==('MEMBER','SKYPOINTS_IND') for sql,params in conn.cur.statements))
        self.assertFalse(any(sql.lstrip().startswith(('INSERT ','UPDATE ','DELETE ','CREATE ','ALTER ')) for sql,p in conn.cur.statements))

    def test_violations_never_appear_in_aggregate_output(self):
        conn=HealthConnection(violation=True)
        report=check_pipeline(conn,[('MEMBER','SKYPOINTS_IND')],now=NOW)
        self.assertEqual('FAIL',report['status'])
        self.assertNotIn('PRIVATE_CANARY',json.dumps(report))

    def test_no_sources_or_duplicate_sources_cannot_pass(self):
        for sources in ([],[('MEMBER','X'),('MEMBER','X')]):
            with self.assertRaises(ValueError):
                check_pipeline(HealthConnection(),sources,now=NOW)

    def test_mutating_sql_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory,'08_verify_raw_integrity.sql').write_text('DELETE FROM SOME_TABLE;',encoding='utf-8')
            with self.assertRaisesRegex(ValueError,'read-only'):
                check_pipeline(HealthConnection(),[('MEMBER','X')],now=NOW,sql_dir=directory)

    def test_connection_errors_are_sanitized_and_exit_two(self):
        output=io.StringIO()
        with patch('sys.argv',['check_pipeline','--source','MEMBER:X']), \
             patch('src.check_pipeline.connect_snowflake',side_effect=RuntimeError('SECRET_CANARY')), \
             patch('dotenv.load_dotenv'),redirect_stdout(output):
            with self.assertRaises(SystemExit) as caught:
                main()
        self.assertEqual(2,caught.exception.code)
        self.assertNotIn('SECRET_CANARY',output.getvalue())
        self.assertEqual('ERROR',json.loads(output.getvalue())['status'])


if __name__=='__main__':
    unittest.main()
