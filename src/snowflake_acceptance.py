"""Opt-in real Snowflake acceptance tests using uniquely named TEMP tables.

python -m src.snowflake_acceptance --run
No imports open a connection. Persistent schema must already exist.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import re
import uuid

from src.ingest_raw import connect_snowflake
from src.transform_members import digest,run as members
from src.transform_redemptions import run as redemptions

TABLES=('INGEST_BATCH','RAW_MEMBER_PROFILE','RAW_REDEMPTION_FEED',
        'STG_MEMBER_PROFILE','MEMBER_IDENTITY_MAP','REJECTED_RECORD','MEMBER_CURRENT',
        'TABLE_INDIA','TABLE_USA','TABLE_AUSTRALIA','TABLE_CANADA','TABLE_PHILIPPINES',
        'TABLE_OTHER_COUNTRIES','REDEMPTION_TRANSACTION','COUNTRY_MOVEMENT_AUDIT',
        'MEMBER_BATCH_VERSION','MEMBER_TRANSFORM_RUN','STG_REDEMPTION_RECORD','REDEMPTION_TRANSFORM_RUN')


def rewrite(sql,prefix):
    if not re.fullmatch(r'ACCEPT_[A-F0-9]{12}_',prefix):
        raise ValueError('Invalid isolation prefix')
    def replace(match):
        table=match.group(1)
        if table not in TABLES:
            raise ValueError('Unrecognized table in acceptance SQL')
        return 'SKYPOINTS.'+prefix+table
    return re.sub(r'\bSKYPOINTS\.([A-Z_]+)\b',replace,sql)


class IsolatedCursor:
    def __init__(self,cursor,prefix,fail_run_audit=False):
        self.cursor,self.prefix,self.fail_run_audit=cursor,prefix,fail_run_audit

    def execute(self,sql,params=None):
        if self.fail_run_audit and sql.startswith('INSERT INTO SKYPOINTS.MEMBER_TRANSFORM_RUN'):
            raise InjectedFailure('Synthetic pre-commit failure')
        sql=rewrite(sql,self.prefix)
        return self.cursor.execute(sql,params) if params is not None else self.cursor.execute(sql)

    @property
    def description(self):
        return self.cursor.description

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def close(self):
        self.cursor.close()


class IsolatedConnection:
    def __init__(self,connection,prefix):
        self.connection,self.prefix=connection,prefix
        self.fail_run_audit=False
    def cursor(self):
        return IsolatedCursor(self.connection.cursor(),self.prefix,self.fail_run_audit)
    def commit(self):
        self.connection.commit()
    def rollback(self):
        self.connection.rollback()


class InjectedFailure(RuntimeError):
    pass


def exercise(connection,as_of,progress=print):
    prefix='ACCEPT_'+uuid.uuid4().hex[:12].upper()+'_'
    cur=connection.cursor()
    try:
        # Only TEMP DDL, with unique target names. No drops, permanent creates,
        # or namespace shadowing. All tables exist before any fixture DML.
        for table in TABLES:
            cur.execute('CREATE TEMPORARY TABLE SKYPOINTS.'+prefix+table+' LIKE SKYPOINTS.'+table)
    finally:
        cur.close()
    scoped=IsolatedConnection(connection,prefix)
    passed=[]
    def done(name):
        passed.append(name)
        progress(json.dumps({'scenario':name,'status':'PASS'}))
    def sql(statement,params=None):
        c=scoped.cursor()
        try:
            c.execute(statement,params)
            return c.fetchall() if c.description else []
        finally:
            c.close()
    def count(table):
        return int(sql('SELECT COUNT(*) FROM SKYPOINTS.'+table)[0][0])
    def check(condition):
        if not condition:
            raise AssertionError('Synthetic acceptance assertion failed')
    def seed(batch,feed,payload,updated=None):
        source='ACCEPT_MEMBER' if feed=='MEMBER' else 'ACCEPT_REDEMPTION'
        sql("INSERT INTO SKYPOINTS.INGEST_BATCH (BATCH_ID,SOURCE_SYSTEM,FEED_TYPE,STATUS,INPUT_ROWS,ACCEPTED_ROWS,FINISHED_AT) VALUES (%s,%s,%s,'COMPLETED',1,1,CURRENT_TIMESTAMP())",(batch,source,feed))
        rid=digest(['SYNTHETIC',batch])
        if feed=='MEMBER':
            sql('INSERT INTO SKYPOINTS.RAW_MEMBER_PROFILE (RAW_RECORD_ID,BATCH_ID,SOURCE_SYSTEM,SOURCE_FILE,SOURCE_COUNTRY,SOURCE_UPDATE_TS,SOURCE_SEQUENCE,PAYLOAD) SELECT %s,%s,%s,%s,%s,%s,0,PARSE_JSON(%s)',
                (rid,batch,source,'SYNTHETIC_ONLY.json','IND',updated,json.dumps(payload)))
        else:
            sql('INSERT INTO SKYPOINTS.RAW_REDEMPTION_FEED (RAW_RECORD_ID,BATCH_ID,SOURCE_SYSTEM,SOURCE_FILE,PAYLOAD) SELECT %s,%s,%s,%s,PARSE_JSON(%s)',
                (rid,batch,source,'SYNTHETIC_ONLY.json',json.dumps(payload)))
        scoped.commit()
    for source,local in [('ACCEPT_MEMBER','SYNTHETIC_M'),('ACCEPT_REDEMPTION','SYNTHETIC_R')]:
        sql("INSERT INTO SKYPOINTS.MEMBER_IDENTITY_MAP (SOURCE_SYSTEM,SOURCE_MEMBER_ID,GLOBAL_MEMBER_ID,MAPPING_METHOD) VALUES (%s,%s,'SYNTHETIC_GLOBAL','SYNTHETIC_TEST_ONLY')",(source,local))
    scoped.commit()
    base={'ID':'SYNTHETIC_M','Name':'SYNTHETIC TEST MEMBER','DOB':'1990-01-01','EnrollmentDate':'2020-01-01','CountryCode':'IND'}
    seed('M1','MEMBER',base,datetime(2024,1,1))
    check(members(scoped,'M1',as_of)['accepted']==1)
    check(count('MEMBER_CURRENT')==1 and count('TABLE_INDIA')==1)
    check(members(scoped,'M1',as_of)['status']=='SKIPPED')
    done('member_load_and_replay')
    seed('M2','MEMBER',dict(base,CountryCode='AU'),datetime(2024,2,1))
    members(scoped,'M2',as_of)
    check(count('TABLE_INDIA')==0 and count('TABLE_AUSTRALIA')==1 and count('COUNTRY_MOVEMENT_AUDIT')==1)
    done('country_movement_and_old_country_removal')
    seed('M0','MEMBER',base,datetime(2023,1,1))
    members(scoped,'M0',as_of)
    check(count('TABLE_AUSTRALIA')==1 and count('COUNTRY_MOVEMENT_AUDIT')==1)
    done('late_older_record_cannot_reverse_country')
    seed('MBAD','MEMBER',dict(base,Name=''),datetime(2025,1,1))
    check(members(scoped,'MBAD',as_of)['rejected']==1)
    check(count('TABLE_AUSTRALIA')==1)
    done('invalid_newer_member_does_not_replace_current')
    payload={'member_id':'SYNTHETIC_R','feed_date':'20240115','redemptions':[
        {'txn_id':'SYNTHETIC_TXN','txn_date':'20240110','partner':'SYNTHETIC PARTNER','miles_redeemed':10,'status':'COMPLETED'}]}
    seed('R1','REDEMPTION',payload)
    check(redemptions(scoped,'R1',as_of)['new_transactions']==1)
    check(redemptions(scoped,'R1',as_of)['status']=='SKIPPED')
    seed('R2','REDEMPTION',payload)
    check(redemptions(scoped,'R2',as_of)['new_transactions']==0 and count('REDEMPTION_TRANSACTION')==1)
    done('verified_redemption_link_and_cross_batch_replay')
    payload['redemptions'][0]['miles_redeemed']=20
    seed('R3','REDEMPTION',payload)
    check(redemptions(scoped,'R3',as_of)['rejected_items']==1)
    check(sql('SELECT MILES_REDEEMED FROM SKYPOINTS.REDEMPTION_TRANSACTION')==[(10,)])
    done('conflicting_transaction_quarantined_without_overwrite')
    seed('M3','MEMBER',dict(base,CountryCode='USA'),datetime(2025,2,1))
    before={table:sql('SELECT * FROM SKYPOINTS.'+table+' ORDER BY 1') for table in TABLES}
    scoped.fail_run_audit=True
    try:
        members(scoped,'M3',as_of)
    except InjectedFailure:
        pass
    else:
        raise AssertionError('Expected synthetic pre-commit failure')
    scoped.fail_run_audit=False
    after={table:sql('SELECT * FROM SKYPOINTS.'+table+' ORDER BY 1') for table in TABLES}
    check(before==after)
    members(scoped,'M3',as_of)
    check(count('TABLE_AUSTRALIA')==0 and count('TABLE_USA')==1 and count('COUNTRY_MOVEMENT_AUDIT')==2)
    done('server_rollback_and_successful_retry')
    return {'status':'PASS','scenarios':passed,'isolation':'unique session temporary tables'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true',required=True,help='Explicit opt-in to warehouse-backed temporary fixture tests')
    parser.add_argument('--as-of',type=date.fromisoformat,default=date(2026,11,18))
    args=parser.parse_args()
    from dotenv import load_dotenv
    load_dotenv()
    conn=None
    try:
        conn=connect_snowflake()
        print(json.dumps(exercise(conn,args.as_of)))
    except Exception as exc:
        print(json.dumps({'status':'FAIL','error_type':type(exc).__name__,
                          'errno':getattr(exc,'errno',None),'sqlstate':getattr(exc,'sqlstate',None)}))
        raise SystemExit(1) from None
    finally:
        # Closing the session removes temporary fixtures, including on failure.
        if conn is not None:
            conn.close()


if __name__=='__main__':
    main()
