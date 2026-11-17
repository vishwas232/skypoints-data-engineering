from datetime import date
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from src.source_contracts import snapshot_file, flat_file_rows, json_documents, write_parts
from src.transform_members import transform, parse_date, run
from src.ingest_raw import load_file
from test_transform_members import raw, MAP, FakeConnection
from test_ingest_transactions import Connection

ROOT = Path(__file__).resolve().parents[1]


class SourceContractsTests(unittest.TestCase):
    def test_all_pdf_rows_and_fields(self):
        rows=list(flat_file_rows(ROOT/'data/samples/members_pdf.txt'))
        self.assertEqual(5,len(rows))
        for index,payload in rows:
            item=raw(); item['PAYLOAD']=payload; item['DOB_FORMAT']='DDMMYYYY'
            mapping={('SKYPOINTS_IND',payload['Member_Id']):['PDF:'+payload['Member_Id']]}
            result=transform(item,mapping,date(2026,11,18))
            self.assertEqual('VALID',result['VALIDATION_STATUS'],result['VALIDATION_ERRORS'])
            self.assertEqual(date(1985,5,3),result['DOB'])
            self.assertEqual(41,result['AGE'])
            self.assertTrue(result['STALE_MEMBER'])
            self.assertEqual('Sam',result['AGENT_NAME'])
        self.assertEqual('PHIL',rows[2][1]['Country'])

    def test_explicit_dob_contract(self):
        self.assertEqual(date(1985,5,3),parse_date('03051985','PDF','DOB','DDMMYYYY'))
        self.assertEqual(date(1985,3,5),parse_date('03051985','PDF','DOB','MMDDYYYY'))
        with self.assertRaises(ValueError): parse_date('03051985','PDF','DOB')

    def test_snapshot_hash_and_contents_remain_consistent(self):
        with tempfile.TemporaryDirectory() as d:
            source=Path(d)/'file.txt'; source.write_bytes(b'original')
            with snapshot_file(source) as snap:
                source.write_bytes(b'changed')
                self.assertEqual(b'original',snap.path.read_bytes())
                self.assertEqual(hashlib.sha256(b'original').hexdigest(),snap.sha256)
            self.assertFalse(snap.path.exists())

    def test_legacy_mixed_fingerprint_rolls_back(self):
        conn=Connection()
        with patch('src.ingest_raw.records',return_value=iter([{'batch_id':'wrong','payload':{}}])):
            with self.assertRaisesRegex(RuntimeError,'fingerprint'):
                load_file(conn,ROOT/'data/samples/IND.xlsx','MEMBER')
        self.assertEqual((0,1),(conn.commits,conn.rollbacks))

    def test_completed_replay_before_global_date_guard(self):
        conn=FakeConnection(done=True)
        original=conn.cur.execute
        def execute(sql,params=None):
            if sql.startswith('SELECT MAX(AS_OF_DATE)'):
                raise AssertionError('Completed replay must not consult date guard')
            original(sql,params)
        conn.cur.execute=execute
        self.assertEqual('SKIPPED',run(conn,'batch',date(2024,3,1))['status'])

    def test_postcode_contract(self):
        for bad in ('ABCDE','123456','1.5'):
            self.assertEqual('REJECTED',transform(raw(PostCode=bad),MAP,date(2026,11,18))['VALIDATION_STATUS'])
        self.assertEqual('00123',transform(raw(PostCode='00123'),MAP,date(2026,11,18))['POST_CODE'])

    def test_flat_structural_errors_are_retained(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'x.txt'
            p.write_text('|H|Member_Name|Member_Id|Enrollment_Date\n|D|A|1\n|X|B|2|20200101\n')
            rows=list(flat_file_rows(p))
            self.assertIn('DETAIL_FIELD_COUNT_MISMATCH',rows[0][1]['_parse_errors'])
            self.assertIn('INVALID_DETAIL_MARKER',rows[1][1]['_parse_errors'])
            for header in ('|D|A|1|20200101','|H|Member_Name|Member_Name|Enrollment_Date','|H|Name|ID|Unexpected','|H|Member_Name|Name|Member_Id|Enrollment_Date'):
                p.write_text(header)
                with self.assertRaises(ValueError): list(flat_file_rows(p))

    def test_json_stream_formats_and_bounds(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'x.json'
            for content,expected in [('[{"a":1},{"b":2}]',2),('[]',0),('{"a":1}',1),('[12,3,null,false]',4)]:
                p.write_text(content)
                self.assertEqual(expected,len(list(json_documents(p))))
            for content in ('[1,]','[1 2]','[','{}x','[{}]x'):
                p.write_text(content)
                with self.assertRaises(ValueError): list(json_documents(p))
            p.write_text('['+' '*65534+'1e2]')
            self.assertEqual(100.0,list(json_documents(p))[0][1])
            for content in ('{"member_id":1,"member_id":2}','[{"a":1,"a":2}]','{"a":NaN}'):
                p.write_text(content)
                with self.assertRaises(ValueError): list(json_documents(p))
            p.write_text('{"a":"'+'x'*100+'"}')
            with self.assertRaises(ValueError): list(json_documents(p,50))
            p=Path(d)/'x.jsonl'; p.write_text('{"a":1}\n\n{"b":2}\n')
            self.assertEqual([1,3],[n for n,r in json_documents(p)])

    def test_gzip_parts_are_valid_and_bounded(self):
        with tempfile.TemporaryDirectory() as d:
            rows=[{'i':i,'text':'x'*200} for i in range(20)]
            parts,count=write_parts(iter(rows),Path(d),1024)
            actual=[]
            for p in parts:
                with gzip.open(p,'rt',encoding='utf-8') as f: actual.extend(json.loads(line) for line in f)
            self.assertEqual(rows,actual); self.assertEqual(20,count); self.assertGreater(len(parts),1)


if __name__=='__main__': unittest.main()
