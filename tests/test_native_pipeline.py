from datetime import datetime,timezone
from pathlib import Path
import unittest
from src.pipeline import Namespace,Contract,Pipeline,split_sql

ROOT=Path(__file__).resolve().parents[1]


class NativeSafetyTests(unittest.TestCase):
    def test_write_namespaces_are_explicitly_development_or_test(self):
        for schema in ('SKYPOINTS','PUBLIC','PRODUCTION','SKYPOINTS_PROD','SKYPOINTS_TEST_BAD','SKYPOINTS_DEV;DROP DATABASE X','"SKYPOINTS_DEV"'):
            with self.assertRaises(ValueError): Namespace('DEMO',schema)
        for schema in ('SKYPOINTS_DEV','SKYPOINTS_DEV_ASSESSMENT','SKYPOINTS_TEST_AABBCCDDEEFF'):
            self.assertIn('"'+schema+'"',Namespace('DEMO',schema).sql)

    def test_declared_global_identity_requires_explicit_authority(self):
        with self.assertRaises(ValueError): Contract('PDF','MEMBER',datetime(2024,1,1),identity_scope='GLOBAL')
        contract=Contract('PDF','MEMBER',datetime(2024,1,1),identity_scope='GLOBAL',identity_authority='PDF_PROGRAM')
        self.assertEqual(64,len(contract.batch_id('a'*64)))
        other=Contract('PDF','MEMBER',datetime(2024,1,1),identity_scope='GLOBAL',identity_authority='OTHER')
        self.assertNotEqual(contract.batch_id('a'*64),other.batch_id('a'*64))

    def test_contract_types_formats_and_versions(self):
        for kwargs in ({'sequence':-1},{'sequence':True},{'sequence':10**38},{'dob_format':'AUTO'},{'identity_scope':'GUESS'},{'default_country':'bad;SQL'},{'enforce_name_key':1},{'allow_transaction_updates':'yes'}):
            with self.assertRaises(ValueError): Contract('PDF','MEMBER',datetime(2024,1,1),**kwargs)
        with self.assertRaises(ValueError): Contract('JSON','REDEMPTION',datetime(2024,1,1))
        utc=Contract('PDF','MEMBER',datetime(2024,1,1,tzinfo=timezone.utc))
        naive=Contract('PDF','MEMBER',datetime(2024,1,1))
        self.assertEqual(utc.batch_id('digest'),naive.batch_id('digest'))

    def test_cleanup_refuses_existing_or_non_test_namespaces(self):
        for schema in ('SKYPOINTS_DEV','SKYPOINTS_TEST_AABBCCDDEEFF'):
            with self.assertRaises(RuntimeError): Pipeline(None,Namespace('DEMO',schema)).drop_owned_test_schema()

    def test_failed_query_retains_its_own_id_and_closes_cursor(self):
        class FailedCursor:
            sfqid='FAILED_QUERY_ID'
            closed=False
            def execute(self,sql): raise RuntimeError('Synthetic SQL failure')
            def close(self): self.closed=True
        cursor=FailedCursor()
        class Connection:
            def cursor(self): return cursor
        engine=Pipeline(Connection(),Namespace('DEMO','SKYPOINTS_DEV'))
        engine.query_ids.append('PREVIOUS_SUCCESS')
        with self.assertRaisesRegex(RuntimeError,'Synthetic SQL failure'):
            engine.execute('SYNTHETIC INVALID SQL')
        self.assertEqual(['PREVIOUS_SUCCESS','FAILED_QUERY_ID'],engine.query_ids)
        self.assertTrue(cursor.closed)

    def test_sql_splitter_preserves_literals_dollar_bodies_and_comments(self):
        sql="-- comment; ignored\nSELECT 'a;''b'; CREATE FUNCTION F() RETURNS STRING AS $$ SELECT ';'; $$; -- trailing"
        parts=split_sql(sql)
        self.assertEqual(2,len(parts)); self.assertIn("'a;''b'",parts[0]); self.assertIn("SELECT ';';",parts[1])
        for bad in ("SELECT 'unterminated",'CREATE FUNCTION X AS $$ body'):
            with self.assertRaises(ValueError): split_sql(bad)

    def test_all_native_sql_uses_explicit_namespace_tokens(self):
        for file in sorted((ROOT/'sql').glob('1[0-5]_*.sql')):
            text=file.read_text(encoding='utf-8')
            self.assertNotIn('SKYPOINTS.',text)
            self.assertGreater(len(split_sql(text)),0)


if __name__=='__main__': unittest.main()
