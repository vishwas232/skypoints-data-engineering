from pathlib import Path
import unittest
from unittest.mock import patch

from src.ingest_raw import load_file

ROOT=Path(__file__).resolve().parents[1]


class Cursor:
    def __init__(self,previous=(),fail_prefix=None):
        self.previous=previous
        self.fail_prefix=fail_prefix
        self.statements=[]
        self.closed=False

    def execute(self,sql,params=None):
        self.statements.append((sql,params))
        if self.fail_prefix and sql.startswith(self.fail_prefix):
            raise RuntimeError('injected ingestion failure')

    def fetchall(self):
        return self.previous

    def close(self):
        self.closed=True


class Connection:
    def __init__(self,**kwargs):
        self.cur=Cursor(**kwargs)
        self.commits=self.rollbacks=0
    def cursor(self):
        return self.cur
    def commit(self):
        self.commits+=1
    def rollback(self):
        self.rollbacks+=1


class IngestTransactionTests(unittest.TestCase):
    def test_ingest_commits_raw_and_completion_once(self):
        conn=Connection()
        self.assertEqual(('LOADED',3),load_file(conn,ROOT/'data/samples/IND.xlsx','MEMBER'))
        self.assertEqual((1,0),(conn.commits,conn.rollbacks))
        self.assertTrue(conn.cur.closed)
        inserts=[sql for sql,p in conn.cur.statements if sql.startswith('INSERT INTO SKYPOINTS.RAW_MEMBER_PROFILE')]
        self.assertEqual(3,len(inserts))

    def test_completed_replay_performs_no_writes(self):
        conn=Connection(previous=[('COMPLETED',)])
        self.assertEqual(('SKIPPED_ALREADY_LOADED',0),load_file(conn,ROOT/'data/samples/IND.xlsx','MEMBER'))
        self.assertEqual(1,len(conn.cur.statements))
        self.assertEqual(0,conn.commits)

    def test_duplicate_batch_ids_cannot_silently_skip(self):
        conn=Connection(previous=[('COMPLETED',),('COMPLETED',)])
        with self.assertRaisesRegex(RuntimeError,'Duplicate'):
            load_file(conn,ROOT/'data/samples/IND.xlsx','MEMBER')
        self.assertEqual((0,1),(conn.commits,conn.rollbacks))

    def test_unfinished_batch_requires_investigation(self):
        conn=Connection(previous=[('STARTED',)])
        with self.assertRaisesRegex(RuntimeError,'investigate'):
            load_file(conn,ROOT/'data/samples/IND.xlsx','MEMBER')
        self.assertEqual(1,len(conn.cur.statements))

    def test_raw_or_completion_failure_rolls_back_and_retry_works(self):
        for prefix in ('INSERT INTO SKYPOINTS.RAW_MEMBER_PROFILE','UPDATE SKYPOINTS.INGEST_BATCH'):
            conn=Connection(fail_prefix=prefix)
            with self.assertRaises(RuntimeError):
                load_file(conn,ROOT/'data/samples/IND.xlsx','MEMBER')
            self.assertEqual((0,1),(conn.commits,conn.rollbacks))
            conn.cur.fail_prefix=None
            self.assertEqual(('LOADED',3),load_file(conn,ROOT/'data/samples/IND.xlsx','MEMBER'))

    def test_invalid_json_serialization_rolls_back(self):
        conn=Connection()
        with patch('src.ingest_raw.records',return_value=iter([{'payload':{'bad':float('nan')}}])):
            with self.assertRaises(ValueError):
                load_file(conn,ROOT/'data/samples/IND.xlsx','MEMBER')
        self.assertEqual((0,1),(conn.commits,conn.rollbacks))


if __name__=='__main__':
    unittest.main()
