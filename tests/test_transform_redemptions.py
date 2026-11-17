import copy
from datetime import date
import json
from pathlib import Path
import unittest

from src.transform_redemptions import (flatten, plan, run, feed_date,
                                       index_inputs, TXN_FIELDS, STAGE_FIELDS)

AS_OF = date(2026, 11, 17)
SOURCE = 'SKYPOINTS_REDEMPTIONS'
MAPS = [dict(SOURCE_SYSTEM=SOURCE,SOURCE_MEMBER_ID='223457',GLOBAL_MEMBER_ID='verified-global')]
MEMBERS = [dict(GLOBAL_MEMBER_ID='verified-global')]
ROOT = Path(__file__).resolve().parents[1]


def raw(rid='raw1', bid='batch1', payload=None):
    return dict(RAW_RECORD_ID=rid,BATCH_ID=bid,SOURCE_SYSTEM=SOURCE,
                PAYLOAD=payload if payload is not None else json.loads(
                    (ROOT/'data/samples/redemptions/example.json').read_text()))


def flattened(record=None, maps=None, members=None):
    identities, current = index_inputs(MAPS if maps is None else maps, MEMBERS if members is None else members)
    return flatten(record or raw(),identities,current,AS_OF)


class ValidationTests(unittest.TestCase):
    def test_sample_flattens_and_joins_only_verified_identity(self):
        rows = flattened()
        self.assertEqual(['DOCUMENT','ITEM','ITEM'],[r['RECORD_KIND'] for r in rows])
        self.assertTrue(all(r['VALIDATION_STATUS']=='VALID' for r in rows))
        self.assertEqual(3,len({r['RECORD_ID'] for r in rows}))
        self.assertEqual(date(2024,1,10),rows[1]['TXN_DATE'])
        self.assertEqual(12000,rows[1]['MILES_REDEEMED'])
        self.assertEqual('verified-global',rows[1]['GLOBAL_MEMBER_ID'])

    def test_no_identity_guessing_or_cross_namespace_match(self):
        wrong_namespace = [dict(MAPS[0],SOURCE_SYSTEM='SKYPOINTS_IND')]
        for mappings in ([],wrong_namespace,MAPS+MAPS):
            rows = flattened(maps=mappings)
            self.assertTrue(all(r['VALIDATION_STATUS']=='REJECTED' for r in rows))
            self.assertIn('UNRESOLVED_OR_AMBIGUOUS_IDENTITY',rows[1]['VALIDATION_ERRORS'])
        for members in ([],MEMBERS+MEMBERS):
            self.assertIn('MEMBER_NOT_CURRENT_OR_AMBIGUOUS',flattened(members=members)[0]['VALIDATION_ERRORS'])

    def test_invalid_document_shapes_and_empty_arrays(self):
        for payload in ('null','not json',[],123,False):
            rows = flattened(raw(payload=payload))
            self.assertEqual(1,len(rows))
            self.assertEqual('REJECTED',rows[0]['VALIDATION_STATUS'])
        payload = raw()['PAYLOAD']
        for bad in (None,{},'wrong'):
            rows = flattened(raw(payload=dict(payload,redemptions=bad)))
            self.assertEqual(['INVALID_REDEMPTIONS_ARRAY'],rows[0]['VALIDATION_ERRORS'])
        rows = flattened(raw(payload=dict(payload,redemptions=[])))
        self.assertEqual(1,len(rows))
        self.assertEqual('VALID',rows[0]['VALIDATION_STATUS'])

    def test_invalid_elements_preserve_indexes_and_valid_siblings(self):
        payload = raw()['PAYLOAD']
        payload['redemptions'].insert(1,None)
        rows = flattened(raw(payload=payload))
        self.assertEqual([-1,0,1,2],[r['ITEM_INDEX'] for r in rows])
        self.assertEqual('REJECTED',rows[2]['VALIDATION_STATUS'])
        self.assertEqual(2,len(plan(rows,{})))

    def test_date_contract(self):
        self.assertEqual(date(2024,2,29),feed_date('20240229'))
        self.assertEqual(date(2024,2,29),feed_date('2024-02-29'))
        for value in ('20230229','20241301','2024111','01/02/2024',True,{},'2024-01-01x'):
            with self.assertRaises(ValueError):
                feed_date(value)
        payload = raw()['PAYLOAD']
        payload['feed_date']='20270101'
        self.assertIn('FUTURE_FEED_DATE',flattened(raw(payload=payload))[1]['VALIDATION_ERRORS'])
        payload['feed_date']='20240101'
        self.assertIn('TRANSACTION_AFTER_FEED_OR_PROCESSING_DATE',flattened(raw(payload=payload))[1]['VALIDATION_ERRORS'])

    def test_miles_exact_integer_range(self):
        for miles in (-1,0,1.5,'12000',True,None,10**18,[],{}):
            payload = raw()['PAYLOAD']
            payload['redemptions'][0]['miles_redeemed']=miles
            rows=flattened(raw(payload=payload))
            self.assertIn('INVALID_MILES_REDEEMED',rows[1]['VALIDATION_ERRORS'])
            self.assertIsNone(rows[1]['MILES_REDEEMED'])
        payload=raw()['PAYLOAD']
        payload['redemptions'][0]['miles_redeemed']=10**18-1
        self.assertEqual('VALID',flattened(raw(payload=payload))[1]['VALIDATION_STATUS'])

    def test_invalid_text_is_quarantined_without_column_overflow(self):
        for key,value in [('txn_id','x'*101),('partner','x'*256),('status','x'*31),
                          ('txn_id',{}),('partner',123),('status','UNKNOWN')]:
            payload=raw()['PAYLOAD']
            payload['redemptions'][0][key]=value
            self.assertEqual('REJECTED',flattened(raw(payload=payload))[1]['VALIDATION_STATUS'])
        payload=raw()['PAYLOAD']
        payload['member_id']={'fake':'223457'}
        self.assertIn('INVALID_MEMBER_ID',flattened(raw(payload=payload))[0]['VALIDATION_ERRORS'])

    def test_identical_duplicates_have_one_target_and_stable_winner(self):
        rows = flattened()+flattened(raw('raw2'))
        first = plan(rows,{})
        second = plan(list(reversed(rows)),{})
        self.assertEqual(2,len(first))
        self.assertEqual({r['SOURCE_RECORD_ID'] for r in first},{r['SOURCE_RECORD_ID'] for r in second})
        self.assertTrue(all(r['VALIDATION_STATUS']=='VALID' for r in rows))
        self.assertEqual([],plan(flattened(),{r['TRANSACTION_KEY']:r for r in first}))

    def test_conflicting_amount_or_status_never_overwrites(self):
        for field,value in [('miles_redeemed',999),('status','COMPLETED')]:
            payload=raw()['PAYLOAD']
            index = 1 if field=='status' else 0
            payload['redemptions'][index][field]=value
            original=flattened()
            changed=flattened(raw('raw2',payload=payload))
            inserts=plan(original+changed,{})
            self.assertEqual(1,len(inserts))
            self.assertEqual('REJECTED',original[index+1]['VALIDATION_STATUS'])
            existing=plan(flattened(),{})
            later=flattened(raw('raw2',payload=payload))
            self.assertEqual([],plan(later,{r['TRANSACTION_KEY']:r for r in existing}))
            self.assertIn('CONFLICTING_TRANSACTION_FACTS',later[index+1]['VALIDATION_ERRORS'])

    def test_transaction_keys_are_namespaced_and_ownership_conflicts(self):
        other=raw('other')
        other['SOURCE_SYSTEM']='OTHER_FEED'
        mappings=MAPS+[dict(MAPS[0],SOURCE_SYSTEM='OTHER_FEED')]
        rows=flattened()+flattened(other,maps=mappings)
        self.assertEqual(4,len(plan(rows,{})))
        original=flattened()
        changed=flattened(raw('raw2'))
        changed[1]['GLOBAL_MEMBER_ID']='different-owner'
        self.assertEqual(1,len(plan(original+changed,{})))
        self.assertEqual('REJECTED',changed[1]['VALIDATION_STATUS'])


class MemoryConnection:
    """Transaction/control-flow double, NOT a Snowflake SQL engine.

    Stores DML parameters so replay and rollback tests exercise multiple runs.
    Invariant SQL is tested manually on Snowflake as described in the runbook.
    """
    def __init__(self, mappings=None):
        self.data={'raw':[raw()], 'maps':copy.deepcopy(MAPS if mappings is None else mappings),
                   'members':copy.deepcopy(MEMBERS),'stage':[], 'transactions':[], 'runs':[], 'rejects':[]}
        self.working=None
        self.commits=self.rollbacks=0
        self.fail=None
        self.bad_invariant=False
        self.statements=[]

    def cursor(self):
        return MemoryCursor(self)

    def commit(self):
        self.data=self.working
        self.working=None
        self.commits+=1

    def rollback(self):
        self.working=None
        self.rollbacks+=1


class MemoryCursor:
    def __init__(self,conn):
        self.conn=conn
        self.description=[]
        self.rows=[]

    def execute(self,sql,params=None):
        c=self.conn
        c.statements.append((sql,params))
        if c.fail and sql.startswith(c.fail):
            raise RuntimeError('injected failure')
        rows=[]
        if sql=='BEGIN TRANSACTION':
            c.working=copy.deepcopy(c.data)
        d=c.working
        if sql.startswith('SELECT * FROM SKYPOINTS.INGEST_BATCH'):
            rows=[dict(STATUS='COMPLETED',FEED_TYPE='REDEMPTION',INPUT_ROWS=sum(r['BATCH_ID']==params[0] for r in d['raw']))]
        elif sql.startswith('SELECT * FROM SKYPOINTS.RAW_REDEMPTION_FEED'):
            rows=[r for r in d['raw'] if r['BATCH_ID']==params[0]]
        elif sql.startswith('SELECT SOURCE_SYSTEM,SOURCE_MEMBER_ID,GLOBAL_MEMBER_ID'):
            rows=d['maps']
        elif sql.startswith('SELECT GLOBAL_MEMBER_ID FROM SKYPOINTS.MEMBER_CURRENT'):
            rows=d['members']
        elif sql.startswith('SELECT * FROM SKYPOINTS.REDEMPTION_TRANSACTION'):
            rows=d['transactions']
        elif sql.startswith('SELECT * FROM SKYPOINTS.STG_REDEMPTION_RECORD'):
            rows=[r for r in d['stage'] if r['BATCH_ID']==params[0]]
        elif sql.startswith('SELECT STATUS FROM SKYPOINTS.REDEMPTION_TRANSFORM_RUN'):
            rows=[{'STATUS':r['STATUS']} for r in d['runs'] if r['RUN_ID']==params[0]]
        elif sql.startswith('DELETE FROM SKYPOINTS.STG_REDEMPTION_RECORD'):
            d['stage']=[r for r in d['stage'] if r['BATCH_ID']!=params[0]]
        elif sql.startswith('DELETE FROM SKYPOINTS.REJECTED_RECORD'):
            d['rejects']=[r for r in d['rejects'] if r['BATCH_ID']!=params[0]]
        elif sql.startswith('INSERT INTO SKYPOINTS.STG_REDEMPTION_RECORD'):
            d['stage'].append(dict(zip(STAGE_FIELDS+['VALIDATION_ERRORS'],params)))
        elif sql.startswith('INSERT INTO SKYPOINTS.REDEMPTION_TRANSACTION'):
            d['transactions'].append(dict(zip(TXN_FIELDS,params)))
        elif sql.startswith('INSERT INTO SKYPOINTS.REJECTED_RECORD'):
            d['rejects'].append(dict(zip(['REJECTION_ID','BATCH_ID','RAW_RECORD_ID','ERROR_DETAIL'],params)))
        elif sql.startswith('INSERT INTO SKYPOINTS.REDEMPTION_TRANSFORM_RUN'):
            keys=['RUN_ID','BATCH_ID','AS_OF_DATE','DOCUMENT_ROWS','ACCEPTED_DOCUMENTS','REJECTED_DOCUMENTS',
                  'ITEM_ROWS','ACCEPTED_ITEMS','REJECTED_ITEMS','NEW_TRANSACTIONS','REPLAY_ITEMS']
            d['runs'].append(dict(zip(keys,params),STATUS='COMPLETED'))
        elif sql.startswith('-- Every statement') and c.bad_invariant:
            rows=[{'TRANSACTION_KEY':'broken'}]
        self.description=[(k,) for k in rows[0]] if rows else []
        self.rows=[tuple(r.values()) for r in rows]

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class OrchestrationTests(unittest.TestCase):
    def test_run_commit_replay_and_another_observation_batch(self):
        conn=MemoryConnection()
        result=run(conn,'batch1',AS_OF)
        self.assertEqual((2,2),(result['accepted_items'],result['new_transactions']))
        before=copy.deepcopy(conn.data)
        self.assertEqual('SKIPPED',run(conn,'batch1',AS_OF)['status'])
        self.assertEqual(before,conn.data)
        conn.data['raw'].append(raw('raw2','batch2'))
        self.assertEqual(0,run(conn,'batch2',AS_OF)['new_transactions'])
        self.assertEqual(2,len(conn.data['transactions']))
        self.assertEqual(2,conn.data['runs'][-1]['REPLAY_ITEMS'])
        self.assertFalse(any(s.startswith(('CREATE ','ALTER ','TRUNCATE ')) for s,p in conn.statements))

    def test_unmapped_sample_quarantines_then_verified_mapping_reprocesses(self):
        conn=MemoryConnection(mappings=[])
        result=run(conn,'batch1',AS_OF)
        self.assertEqual((2,0),(result['rejected_items'],result['new_transactions']))
        self.assertEqual(3,len(conn.data['rejects']))
        conn.data['maps']=copy.deepcopy(MAPS)
        result=run(conn,'batch1',AS_OF)
        self.assertEqual(2,result['new_transactions'])
        self.assertEqual([],conn.data['rejects'])
        self.assertEqual(2,len(conn.data['runs']))

    def test_failure_rolls_back_all_writes_then_retry_succeeds(self):
        conn=MemoryConnection()
        conn.fail='INSERT INTO SKYPOINTS.REDEMPTION_TRANSFORM_RUN'
        before=copy.deepcopy(conn.data)
        with self.assertRaises(RuntimeError):
            run(conn,'batch1',AS_OF)
        self.assertEqual(before,conn.data)
        self.assertEqual((0,1),(conn.commits,conn.rollbacks))
        conn.fail=None
        self.assertEqual(2,run(conn,'batch1',AS_OF)['new_transactions'])

    def test_reconciliation_failure_prevents_commit(self):
        conn=MemoryConnection()
        conn.bad_invariant=True
        with self.assertRaisesRegex(RuntimeError,'reconciliation'):
            run(conn,'batch1',AS_OF)
        self.assertEqual([],conn.data['transactions'])
        self.assertEqual(0,conn.commits)

    def test_existing_rekey_or_accepted_raw_mutation_fails_closed(self):
        for mutate in ('map','raw'):
            conn=MemoryConnection()
            run(conn,'batch1',AS_OF)
            if mutate=='map':
                conn.data['maps'][0]['GLOBAL_MEMBER_ID']='different-owner'
            else:
                conn.data['raw'][0]['PAYLOAD']['redemptions'][0]['miles_redeemed']=999
            before=copy.deepcopy(conn.data)
            with self.assertRaises(RuntimeError):
                run(conn,'batch1',AS_OF)
            self.assertEqual(before,conn.data)

    def test_conflicting_later_batch_is_quarantined_without_changing_target(self):
        conn=MemoryConnection()
        run(conn,'batch1',AS_OF)
        target=copy.deepcopy(conn.data['transactions'])
        later=raw('raw2','batch2')
        later['PAYLOAD']['redemptions'][0]['miles_redeemed']=1
        conn.data['raw'].append(later)
        result=run(conn,'batch2',AS_OF)
        self.assertEqual((1,0),(result['rejected_items'],result['new_transactions']))
        self.assertEqual(target,conn.data['transactions'])

    def test_empty_array_is_an_audited_document_with_no_transactions(self):
        conn=MemoryConnection()
        conn.data['raw'][0]['PAYLOAD']['redemptions']=[]
        result=run(conn,'batch1',AS_OF)
        self.assertEqual((1,0,0),(result['documents'],result['items'],result['new_transactions']))
        self.assertEqual(1,len(conn.data['stage']))
        self.assertEqual(1,conn.data['runs'][0]['ACCEPTED_DOCUMENTS'])

    def test_duplicate_raw_ids_abort_without_writes(self):
        conn=MemoryConnection()
        conn.data['raw'].append(copy.deepcopy(conn.data['raw'][0]))
        with self.assertRaisesRegex(RuntimeError,'Duplicate raw'):
            run(conn,'batch1',AS_OF)
        self.assertEqual(0,conn.commits)
        self.assertEqual([],conn.data['stage'])

    def test_lost_commit_response_is_safe_to_retry(self):
        class LostResponse(MemoryConnection):
            def commit(self):
                super().commit()
                raise RuntimeError('commit succeeded but response lost')
        conn=LostResponse()
        with self.assertRaisesRegex(RuntimeError,'response lost'):
            run(conn,'batch1',AS_OF)
        self.assertEqual(2,len(conn.data['transactions']))
        self.assertEqual('SKIPPED',run(conn,'batch1',AS_OF)['status'])
        self.assertEqual(1,len(conn.data['runs']))


if __name__=='__main__':
    unittest.main()
