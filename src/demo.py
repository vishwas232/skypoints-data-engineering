"""Reproducible file-to-Snowflake acceptance demo in an owned isolated schema.

python -m src.demo --keep-schema --report docs/evidence/native_acceptance.json
No default development or legacy schema is reset, truncated, or dropped.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date,datetime,timedelta
import json
from pathlib import Path
import tempfile
import time
import uuid
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor

from src.pipeline import Pipeline,Contract,InjectedFailure,connection_and_namespace,ROOT,COUNTRIES

HEADER=['Member_Name','Member_Id','Enrollment_Date','Last_Flight_Date','Tier_Code',
        'Agent_Name','State','Country','DOB','Is_Active','Post_Code']


def member(member_id='223457',name='Elena',country='USA',**changes):
    row=dict(zip(HEADER,[name,member_id,'20101012','20121013','GLD','Sam','CA',country,'03051985','A','']))
    row.update(changes)
    return row


def write_members(path,rows,headers=HEADER):
    import csv
    with Path(path).open('w',encoding='utf-8',newline='') as stream:
        writer=csv.writer(stream,delimiter='|',lineterminator='\n')
        writer.writerow(['','H',*headers])
        for row in rows: writer.writerow(['','D',*(row.get(k,'') for k in headers)])
    return Path(path)


def run_demo(engine: Pipeline,as_of: date,progress=print):
    if not engine.ns.is_test or not engine.created_test_schema:
        raise ValueError('Demo requires a fresh, owned, isolated test schema')
    start=time.perf_counter()
    results=[]
    contract=Contract('PDF_MEMBERS','MEMBER',datetime(2024,1,15),'PDF','DDMMYYYY','GLOBAL','SKYPOINTS_PDF')
    redemption=replace(contract,source='PDF_REDEMPTIONS',feed='REDEMPTION',source_format='JSON')
    def check(condition,message):
        if not condition: raise AssertionError(message)
    def done(name,**details):
        result={'scenario':name,'status':'PASS',**details}; results.append(result)
        progress(json.dumps(result,default=str))
    def count(table,where='',params=None):
        return int(engine.scalar(f'SELECT COUNT(*) FROM {engine.ns.sql}.{table} '+where,params))
    def load(path,c):
        raw=engine.ingest(path,c,as_of)
        processed=engine.process(raw['batch_id'],as_of)
        return raw,processed
    def errors(bid,code):
        return int(engine.scalar(f'''SELECT COUNT(*) FROM {engine.ns.sql}.REJECTED_RECORD
         WHERE BATCH_ID=%s AND ARRAY_CONTAINS(TO_VARIANT(%s),ERROR_CODES)''',(bid,code)))
    with tempfile.TemporaryDirectory(prefix='skypoints-demo-') as directory:
        directory=Path(directory)
        baseline,processed=load(ROOT/'data/samples/members_pdf.txt',contract)
        check(processed['accepted_rows']==5,'PDF members were not all accepted')
        check(count('MEMBER_CURRENT')==5,'Expected five original PDF members')
        for table in ('TABLE_INDIA','TABLE_USA','TABLE_AUSTRALIA','TABLE_CANADA','TABLE_PHILIPPINES'):
            check(count(table)==1,'Country distribution mismatch: '+table)
        expected_age=as_of.year-1985-((as_of.month,as_of.day)<(5,3))
        check(engine.scalar(f'SELECT COUNT(*) FROM {engine.ns.sql}.MEMBER_CURRENT WHERE AGE=%s AND STALE_MEMBER AND IS_ACTIVE=\'A\'',(expected_age,))==5,'Derived PDF values mismatch')
        done('pdf_file_to_raw_staging_and_five_countries',accepted=5,age=expected_age)
        check(engine.ingest(ROOT/'data/samples/members_pdf.txt',contract,as_of)['status']=='SKIPPED_RAW','File replay did not skip')
        check(engine.process(baseline['batch_id'],as_of)['status']=='SKIPPED','Transformation replay did not skip')
        done('duplicate_file_and_completed_batch_replay')
        red_raw,red_result=load(ROOT/'data/samples/redemptions/example.json',redemption)
        joined=engine.execute(f'SELECT MEMBER_NAME,TXN_ID,MILES_REDEEMED FROM {engine.ns.sql}.REDEMPTION_MEMBER_DETAIL ORDER BY TXN_ID')
        check(joined==[{'MEMBER_NAME':'Elena','TXN_ID':'RX10091','MILES_REDEEMED':12000},
                       {'MEMBER_NAME':'Elena','TXN_ID':'RX10092','MILES_REDEEMED':5000}],'PDF redemption join mismatch')
        done('pdf_json_bulk_load_flatten_and_elena_join',transactions=2)
        health=engine.health(['MEMBER:PDF_MEMBERS','REDEMPTION:PDF_REDEMPTIONS'])
        check(health['status']=='PASS','Fresh valid baseline must pass unchanged health thresholds')
        done('fresh_valid_pipeline_health',failed_checks=health['failed_checks'])
        movement=write_members(directory/'movement.txt',[member(country='IND')])
        load(movement,replace(contract,snapshot_ts=datetime(2024,2,1)))
        check(count('TABLE_USA')==0 and count('TABLE_INDIA')==2 and count('COUNTRY_MOVEMENT_AUDIT')==1,'Movement did not remove old country')
        done('country_movement_and_old_target_removal')
        late=write_members(directory/'late.txt',[member(country='USA')])
        load(late,replace(contract,snapshot_ts=datetime(2023,1,1)))
        check(count('TABLE_USA')==0 and count('COUNTRY_MOVEMENT_AUDIT')==1,'Late older record reversed current state')
        done('late_arrival_cannot_reverse_newer_country')
        conflict=write_members(directory/'version_conflict.txt',[member(country='AUS')])
        raw,_=load(conflict,replace(contract,snapshot_ts=datetime(2024,2,1)))
        check(errors(raw['batch_id'],'CONFLICTING_MEMBER_VERSION')==1,'Equal-version conflict was not quarantined')
        check(count('TABLE_AUSTRALIA')==1,'Conflicting member changed current country')
        done('conflicting_member_versions_are_quarantined')

        negative=replace(contract,source='SYNTHETIC_MEMBERS',identity_authority='SYNTHETIC_FIXTURE',snapshot_ts=datetime.combine(as_of,datetime.min.time()))
        rows=[member('QA_NULL','Synthetic Unknown',country='GB',DOB='',Last_Flight_Date='',Post_Code='00123'),
              member('QA_GEO_NULL','Synthetic Missing Country',country=''),
              member('QA_GEO_OTHER','Synthetic Other Country',country='DEU'),
              member('QA_GEO_BAD','Synthetic Invalid Country',country='123'),
              member('QA_DATE','Synthetic Invalid Date',Enrollment_Date='20211313'),
              member('QA_MISSING','',country='USA'),
              member('QA_POST','Synthetic Invalid Post',Post_Code='ABCDE'),
              member('QA_FLAG','Synthetic Invalid Flag',Is_Active='Z'),
              member('QA_N1','Synthetic Duplicate Name'),member('QA_N2','Synthetic Duplicate Name'),
              member('QA_DUP','Synthetic Exact Duplicate'),member('QA_DUP','Synthetic Exact Duplicate'),
              member('QA_CONFLICT','Synthetic Conflict A'),member('QA_CONFLICT','Synthetic Conflict B'),
              member('QA_DOB','Synthetic Invalid DOB',DOB='31021985'),
              member('QA_LONG','X'*256),member('QA_TIER','Synthetic Long Tier',Tier_Code='TOOLONG'),
              member('QA_AGENT','Synthetic Long Agent',Agent_Name='X'*256),
              member('QA_STATE','Synthetic Long State',State='TOOLONG'),
              member('QA_FUTURE','Synthetic Future',Enrollment_Date='20270101'),
              member('QA_VERSION','Synthetic Invalid Version',Source_Update_Ts='not-a-timestamp'),
              member('QA_SEQUENCE','Synthetic Invalid Sequence',Source_Sequence='1.5'),
              member('QA_90','Synthetic Ninety',Last_Flight_Date=(as_of-timedelta(days=90)).strftime('%Y%m%d')),
              member('QA_91','Synthetic Ninety One',Last_Flight_Date=(as_of-timedelta(days=91)).strftime('%Y%m%d'))]
        raw,result=load(write_members(directory/'validation.txt',rows,HEADER+['Source_Update_Ts','Source_Sequence']),negative)
        for code in ('INVALID_ENROLLMENT_DATE','MISSING_MEMBER_NAME','INVALID_POST_CODE','INVALID_ACTIVE_FLAG',
                     'DUPLICATE_MEMBER_NAME','DUPLICATE_SOURCE_RECORD','CONFLICTING_MEMBER_VERSION','INVALID_DOB',
                     'MEMBER_NAME_TOO_LONG','TIER_TOO_LONG','AGENT_TOO_LONG','STATE_TOO_LONG','FUTURE_DATE','INVALID_SOURCE_VERSION','INVALID_COUNTRY'):
            check(errors(raw['batch_id'],code)>0,'Missing quarantine category: '+code)
        unknown=engine.execute(f"SELECT AGE,STALE_MEMBER,POST_CODE FROM {engine.ns.sql}.MEMBER_CURRENT WHERE GLOBAL_MEMBER_ID='SYNTHETIC_FIXTURE:QA_NULL'")[0]
        check(unknown=={'AGE':None,'STALE_MEMBER':None,'POST_CODE':'00123'},'Null/postcode semantics failed')
        for mid,stale in [('QA_90',False),('QA_91',True)]:
            check(engine.scalar(f'SELECT STALE_MEMBER FROM {engine.ns.sql}.MEMBER_CURRENT WHERE SOURCE_MEMBER_ID=%s',(mid,)) is stale,'90-day boundary failed')
        check(engine.scalar(f"SELECT {engine.ns.sql}.AGE_ON('2000-02-29'::DATE,'2023-02-28'::DATE)")==22,'Leap-year February boundary failed')
        check(engine.scalar(f"SELECT {engine.ns.sql}.AGE_ON('2000-02-29'::DATE,'2023-03-01'::DATE)")==23,'Leap-year March boundary failed')
        done('mandatory_types_dates_names_ids_postcodes_nulls_and_derived_boundaries',accepted=result['accepted_rows'],rejected=result['rejected_rows'])
        for mid in ('QA_GEO_NULL','QA_GEO_OTHER'):
            check(count('TABLE_OTHER_COUNTRIES','WHERE SOURCE_MEMBER_ID=%s',(mid,))==1,'Optional/unlisted country was not routed to OTHER')
        geo=directory/'unknown_country_move.txt'
        _,moved=load(write_members(geo,[member('QA_GEO_NULL','Synthetic Missing Country',country='USA')]),replace(negative,snapshot_ts=datetime.combine(as_of,datetime.min.time()),sequence=1))
        check(count('TABLE_OTHER_COUNTRIES',"WHERE SOURCE_MEMBER_ID='QA_GEO_NULL'")==0 and count('TABLE_USA',"WHERE SOURCE_MEMBER_ID='QA_GEO_NULL'")==1,'Unknown-to-known movement left old country row')
        load(write_members(geo,[member('QA_GEO_NULL','Synthetic Missing Country',country='')]),replace(negative,snapshot_ts=datetime.combine(as_of,datetime.min.time()),sequence=2))
        check(count('TABLE_OTHER_COUNTRIES',"WHERE SOURCE_MEMBER_ID='QA_GEO_NULL'")==1 and count('TABLE_USA',"WHERE SOURCE_MEMBER_ID='QA_GEO_NULL'")==0,'Known-to-unknown movement left old country row')
        check(count('COUNTRY_MOVEMENT_AUDIT',"WHERE GLOBAL_MEMBER_ID='SYNTHETIC_FIXTURE:QA_GEO_NULL'")==2,'Null country changes were missing movement audit')
        done('optional_unlisted_countries_and_null_safe_movement_audit')
        malformed_flat=directory/'malformed_details.txt'
        malformed_flat.write_text('|H|'+'|'.join(HEADER)+'\n|D|Synthetic Malformed|QA_PARSE|20200101\n'+
          '|X|'+'|'.join(str(member('QA_MARKER','Synthetic Bad Marker').get(k,'')) for k in HEADER)+'\n')
        raw,result=load(malformed_flat,replace(negative,source='SYNTHETIC_PARSE'))
        check(result['rejected_rows']==2,'Malformed flat details were not quarantined')
        check(errors(raw['batch_id'],'DETAIL_FIELD_COUNT_MISMATCH')==1 and errors(raw['batch_id'],'INVALID_DETAIL_MARKER')==1,'Flat source reason codes missing')
        done('flat_file_structural_rejections_retain_original_fields')

        for code,fmt in [('IND','EXCEL_ISO'),('USA','EXCEL_US'),('AUS','EXCEL_ISO')]:
            excel=Contract('EXCEL_'+code,'MEMBER',datetime(2024,1,15),fmt,'YYYYMMDD','LOCAL',default_country=code,enforce_name_key=False)
            raw,result=load(ROOT/f'data/samples/{code}.xlsx',excel)
            check(result['input_rows']==3 and result['rejected_rows']==3,'Unverified Excel identities must stay quarantined')
        done('all_original_excel_samples_without_guessed_identity_links',raw_rows=9)
        from openpyxl import Workbook
        book=Workbook(); sheet=book.active
        sheet.append(['ID','Name','DOB','EnrollmentDate','Country','IsActive'])
        sheet.append(['QA_TYPE',123,datetime(1985,5,3),datetime(2020,1,1),'IND',True])
        typed_path=directory/'typed.xlsx'; book.save(typed_path); book.close()
        raw,result=load(typed_path,replace(negative,source='SYNTHETIC_TYPES',source_format='EXCEL_ISO'))
        check(errors(raw['batch_id'],'INVALID_NAME_TYPE')==1 and errors(raw['batch_id'],'INVALID_ACTIVE_FLAG_TYPE')==1,'Excel value types were silently coerced')
        done('excel_field_type_validation')
        for source,name,country,gid in [('LOCAL_A','Synthetic Local A','IND','CONTROLLED_A'),('LOCAL_B','Synthetic Local B','USA','CONTROLLED_B')]:
            c=Contract(source,'MEMBER',datetime(2024,1,1),'PDF','DDMMYYYY','LOCAL')
            path=write_members(directory/(source+'.txt'),[member('1',name,country)])
            raw=engine.ingest(path,c,as_of)
            check(engine.process(raw['batch_id'],as_of)['rejected_rows']==1,'Unmapped local identity did not reject')
            engine.add_mappings([{'source_system':source,'source_member_id':'1','global_member_id':gid}])
            check(engine.process(raw['batch_id'],as_of)['accepted_rows']==1,'Verified mapping did not permit reprocessing')
        check(count('MEMBER_CURRENT',"WHERE GLOBAL_MEMBER_ID IN ('CONTROLLED_A','CONTROLLED_B')")==2,'Local-ID collision merged identities')
        try: engine.add_mappings([{'source_system':'LOCAL_A','source_member_id':'1','global_member_id':'CONTROLLED_B'}])
        except RuntimeError: pass
        else: raise AssertionError('Identity rekey was not refused')
        done('local_id_collisions_verified_mapping_reprocess_and_rekey_refusal')

        payload=json.loads((ROOT/'data/samples/redemptions/example.json').read_text())
        red_path=directory/'redemptions.json'
        red_path.write_text(json.dumps(payload))
        load(red_path,replace(redemption,snapshot_ts=datetime(2024,1,16)))
        check(count('REDEMPTION_TRANSACTION')==2,'Cross-batch transaction replay duplicated facts')
        done('cross_batch_redemption_deduplication')
        duplicate_payload={'member_id':'223457','feed_date':'20240115',
                           'redemptions':[dict(payload['redemptions'][0]),dict(payload['redemptions'][0])]}
        red_path.write_text(json.dumps(duplicate_payload))
        _,duplicate_result=load(red_path,replace(redemption,snapshot_ts=datetime(2024,1,16)))
        check(duplicate_result['accepted_rows']==2 and count('REDEMPTION_TRANSACTION')==2,'Identical repeated transaction IDs were not deduplicated')
        duplicate_payload['redemptions'][1]['miles_redeemed']=13000
        red_path.write_text(json.dumps(duplicate_payload))
        raw,duplicate_result=load(red_path,replace(redemption,snapshot_ts=datetime(2024,1,16)))
        check(duplicate_result['rejected_rows']==2 and errors(raw['batch_id'],'CONFLICTING_TRANSACTION_VERSION')==2,'Conflicting duplicate transaction IDs were not quarantined')
        done('identical_and_conflicting_duplicate_transaction_ids_within_document')
        payload['redemptions'][0]['miles_redeemed']=999
        red_path.write_text(json.dumps(payload))
        raw,result=load(red_path,replace(redemption,snapshot_ts=datetime(2024,1,17)))
        check(result['rejected_rows']==1,'Unversioned/unauthorized correction was not quarantined')
        check(engine.scalar(f"SELECT MILES_REDEEMED FROM {engine.ns.sql}.REDEMPTION_TRANSACTION WHERE TXN_ID='RX10091'")==12000,'Conflicting miles overwrote target')
        payload=json.loads((ROOT/'data/samples/redemptions/example.json').read_text())
        payload['redemptions'][1]['status']='COMPLETED'
        red_path.write_text(json.dumps(payload))
        _,result=load(red_path,replace(redemption,snapshot_ts=datetime(2024,4,1),allow_transaction_updates=True))
        check(result['accepted_rows']==2,'Approved transaction version update failed')
        check(engine.scalar(f"SELECT STATUS FROM {engine.ns.sql}.REDEMPTION_TRANSACTION WHERE TXN_ID='RX10092'")=='COMPLETED','Pending-to-completed update missing')
        check(count('REDEMPTION_CHANGE_AUDIT')==1,'Approved fact correction audit missing')
        done('transaction_conflict_hold_and_explicitly_versioned_upsert')

        malformed={'member_id':'NO_SUCH_MEMBER','feed_date':'20240115','redemptions':[None,{'txn_id':'BAD','txn_date':'20240110','partner':'Synthetic','miles_redeemed':1.5,'status':'PENDING'}]}
        red_path.write_text(json.dumps(malformed))
        raw,result=load(red_path,replace(redemption,source='NEGATIVE_REDEMPTIONS',snapshot_ts=datetime(2024,1,15)))
        check(result['rejected_rows']==2 and result['rejected_documents']==1,'Invalid JSON document/item audit failed')
        red_path.write_text(json.dumps({'member_id':'223457','feed_date':'20240115','redemptions':[]}))
        _,result=load(red_path,replace(redemption,source='EMPTY_REDEMPTIONS'))
        check(result['input_rows']==0 and result['documents']==1 and result['rejected_documents']==0,'Empty array envelope disappeared')
        negative_health=engine.health(['REDEMPTION:NEGATIVE_REDEMPTIONS'])
        check(negative_health['status']=='FAIL','Negative quality fixture must not report healthy')
        done('invalid_and_empty_json_reconciliation_and_negative_health')

        retry_path=write_members(directory/'retry.txt',[member(country='CAN')])
        retry=engine.ingest(retry_path,replace(contract,snapshot_ts=datetime(2024,6,1)),as_of)
        tables=('STG_MEMBER_PROFILE','REJECTED_RECORD','MEMBER_CURRENT',*list(COUNTRIES.values()),'COUNTRY_MOVEMENT_AUDIT','PIPELINE_RUN')
        def state():
            return {t:engine.execute(f'SELECT COUNT(*) AS N,HASH_AGG(*) AS H FROM {engine.ns.sql}.{t}')[0] for t in tables}
        before=state()
        try: engine.process(retry['batch_id'],as_of,fail_before_commit=True)
        except InjectedFailure: pass
        else: raise AssertionError('Failure injection did not occur')
        check(before==state(),'Server rollback left partial business writes')
        check(engine.process(retry['batch_id'],as_of)['status']=='COMPLETED','Retry after rollback failed')
        check(engine.scalar(f"SELECT COUNTRY_CODE FROM {engine.ns.sql}.MEMBER_CURRENT WHERE GLOBAL_MEMBER_ID='SKYPOINTS_PDF:223457'")=='CAN','Retry did not move member')
        done('server_rollback_attempt_audit_and_restart')
        ack_path=write_members(directory/'acknowledgement.txt',[member(country='CAN')])
        ack_raw=engine.ingest(ack_path,replace(contract,snapshot_ts=datetime(2024,7,1)),as_of)
        actual_commit=engine.connection.commit
        lost=[False]
        class LostAcknowledgement(RuntimeError): pass
        def commit_then_lose_response():
            actual_commit()
            if not lost[0]:
                lost[0]=True
                raise LostAcknowledgement('Simulated lost client acknowledgement after actual server commit')
        with patch.object(engine.connection,'commit',side_effect=commit_then_lose_response):
            try: engine.process(ack_raw['batch_id'],as_of)
            except LostAcknowledgement: pass
            else: raise AssertionError('Expected lost-acknowledgement fault')
        check(engine.process(ack_raw['batch_id'],as_of)['status']=='SKIPPED','Uncertain committed batch did not skip safely')
        done('actual_commit_with_simulated_lost_client_acknowledgement')
        # Prove that the actual Snowflake DML mutex blocks a second writer.
        second=None
        try:
            second,second_ns=connection_and_namespace(engine.ns.schema)
            competitor=Pipeline(second,second_ns)
            competitor.execute('ALTER SESSION SET LOCK_TIMEOUT=2')
            engine.execute('BEGIN TRANSACTION')
            engine.execute(f'UPDATE {engine.ns.sql}.PIPELINE_LOCK SET OWNER_SESSION=CURRENT_SESSION() WHERE LOCK_ID=1')
            def blocked_writer():
                try:
                    with competitor.transaction(): pass
                except Exception as exc:
                    return 'lock' in str(getattr(exc,'msg',exc)).lower()
                return False
            with ThreadPoolExecutor(max_workers=1) as pool:
                future=pool.submit(blocked_writer)
                try: blocked=future.result(timeout=15)
                finally: engine.connection.rollback()
            check(blocked,'Second native writer did not hit the held Snowflake lock')
            with competitor.transaction(): pass
        finally:
            engine.connection.rollback()
            if second: second.close()
        done('actual_native_writer_mutex_and_lock_timeout_recovery')
        engine.process(baseline['batch_id'],as_of+timedelta(days=1))
        check(engine.process(baseline['batch_id'],as_of)['status']=='SKIPPED','Completed replay after a later processing date failed')
        done('completed_replay_after_later_processing_date')
        integrity=engine.verify()
        final_health=engine.health(['MEMBER:PDF_MEMBERS','REDEMPTION:EMPTY_REDEMPTIONS'])
        check(final_health['status']=='PASS','Final valid-source health failed')
        done('final_full_reconciliation',**integrity)
        return {'status':'PASS','schema':engine.ns.schema,'as_of':str(as_of),'scenarios':results,
                'elapsed_seconds':time.perf_counter()-start,'sql_query_count':len(engine.query_ids),
                'query_ids':engine.query_ids,'positive_health':health,'negative_health':negative_health,
                'final_positive_health':final_health,'counts':{t:count(t) for t in ('RAW_MEMBER_PROFILE','RAW_REDEMPTION_FEED','MEMBER_CURRENT','REDEMPTION_TRANSACTION','REJECTED_RECORD','PIPELINE_RUN')},
                'pdf_join':joined}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--keep-schema',action='store_true')
    parser.add_argument('--as-of',type=date.fromisoformat,default=date(2026,11,18))
    parser.add_argument('--report',type=Path)
    args=parser.parse_args()
    connection=None; engine=None
    try:
        connection,ns=connection_and_namespace('SKYPOINTS_TEST_'+uuid.uuid4().hex[:12].upper())
        engine=Pipeline(connection,ns); engine.deploy()
        report=run_demo(engine,args.as_of)
        report['schema_retained']=args.keep_schema
        if args.report:
            args.report.parent.mkdir(parents=True,exist_ok=True)
            args.report.write_text(json.dumps(report,indent=2,default=str),encoding='utf-8')
        print(json.dumps({'status':'PASS','schema':ns.schema,'schema_retained':args.keep_schema,
                          'scenarios':len(report['scenarios']),'elapsed_seconds':report['elapsed_seconds']}))
    except Exception as exc:
        print(json.dumps({'status':'FAIL','error_category':type(exc).__name__,'errno':getattr(exc,'errno',None),'sqlstate':getattr(exc,'sqlstate',None)}))
        raise SystemExit(1) from None
    finally:
        if engine and engine.created_test_schema and not args.keep_schema: engine.drop_owned_test_schema()
        if connection: connection.close()


if __name__=='__main__': main()
