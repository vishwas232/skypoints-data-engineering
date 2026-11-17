"""Read-only pipeline checks. Prints aggregate JSON, never result rows or secrets.

Run after the serialized pipeline, with explicit expected source/SLA policy.
Exit 0: checks passed, 1: health failure, 2: configuration/query error.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re

from src.ingest_raw import connect_snowflake
from src.transform_members import query

SQL_FILES = ('08_verify_raw_integrity.sql', '05_verify_member_transform.sql',
             '07_verify_redemption_transform.sql')


def source_spec(value):
    if not re.fullmatch(r'(MEMBER|REDEMPTION):[A-Z0-9_]{1,100}',value):
        raise argparse.ArgumentTypeError('Expected MEMBER:SOURCE_SYSTEM or REDEMPTION:SOURCE_SYSTEM')
    return tuple(value.split(':',1))


def policy(max_age_hours, max_reject_rate, max_transform_lag_hours):
    if (not all(math.isfinite(v) for v in (max_age_hours,max_reject_rate,max_transform_lag_hours))
            or max_age_hours <= 0 or max_transform_lag_hours < 0 or not 0 <= max_reject_rate <= 1):
        raise ValueError('Invalid SLA policy')


def utc(value):
    # Audit TIMESTAMP_NTZ values must have been written in UTC (see runbook).
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def assess(raw, transformed, now, max_age_hours, max_reject_rate, max_transform_lag_hours):
    """Return aggregate check outcomes for the latest raw batch of one source."""
    policy(max_age_hours,max_reject_rate,max_transform_lag_hours)
    checks=[]
    def add(name,passed):
        checks.append({'name':name,'passed':bool(passed)})
    add('raw_batch_present',len(raw)==1)
    if len(raw)!=1:
        return checks
    batch=raw[0]
    add('raw_completed',batch['STATUS']=='COMPLETED')
    finished=batch['FINISHED_AT']
    age=(now-utc(finished)).total_seconds()/3600 if finished else None
    add('raw_fresh',age is not None and 0<=age<=max_age_hours)
    add('transform_present',len(transformed)==1)
    if len(transformed)!=1:
        return checks
    result=transformed[0]
    add('transform_completed',result['STATUS']=='COMPLETED')
    ended=result['COMPLETED_AT']
    lag=(utc(ended)-utc(finished)).total_seconds()/3600 if finished and ended else None
    add('transform_within_sla',lag is not None and 0<=lag<=max_transform_lag_hours and utc(ended)<=now)
    # Document and item quality have separate denominators, never double-count.
    for label,total_key,reject_key in (('documents','DOCUMENT_ROWS','REJECTED_DOCUMENTS'),
                                       ('items','ITEM_ROWS','REJECTED_ITEMS')):
        if total_key not in result:
            continue
        total,rejected=result[total_key],result[reject_key]
        add(label+'_counts_valid',total>=0 and 0<=rejected<=total)
        add(label+'_reject_rate',total>=0 and 0<=rejected<=total and (rejected/total if total else 0)<=max_reject_rate)
    return checks


def check_pipeline(connection, sources, now=None, max_age_hours=26,
                   max_reject_rate=0.05, max_transform_lag_hours=2, sql_dir=None):
    policy(max_age_hours,max_reject_rate,max_transform_lag_hours)
    if not sources or len(set(sources))!=len(sources):
        raise ValueError('At least one unique expected source is required')
    for feed,source in sources:
        source_spec(feed+':'+source)
    now=now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('Check time must include timezone')
    cur=connection.cursor()
    checks=[]
    try:
        directory=Path(sql_dir) if sql_dir else Path(__file__).resolve().parents[1]/'sql'
        for filename in SQL_FILES:
            for index,statement in enumerate((directory/filename).read_text(encoding='utf-8').split(';'),1):
                if not statement.strip():
                    continue
                # Only the shipped SELECT/CTE invariant scripts are loaded.
                code=re.sub(r'--[^\n]*','',statement).strip()
                if not re.match(r'^(SELECT|WITH)\b',code,re.I):
                    raise ValueError('Health scripts must be read-only SELECT statements')
                cur.execute(statement)
                checks.append({'name':filename+':'+str(index),'passed':cur.fetchone() is None})
        for feed,source in sources:
            raw=query(cur,"""SELECT BATCH_ID,STATUS,FINISHED_AT FROM SKYPOINTS.INGEST_BATCH
                WHERE FEED_TYPE=%s AND SOURCE_SYSTEM=%s
                ORDER BY STARTED_AT DESC,BATCH_ID DESC LIMIT 1""",(feed,source))
            transformed=[]
            if raw:
                # Static allowlist chooses identifiers, external values stay bound parameters.
                if feed=='MEMBER':
                    sql="""SELECT STATUS,COMPLETED_AT,INPUT_ROWS AS ITEM_ROWS,REJECTED_ROWS AS REJECTED_ITEMS
                        FROM SKYPOINTS.MEMBER_TRANSFORM_RUN WHERE BATCH_ID=%s
                        ORDER BY COMPLETED_AT DESC,RUN_ID DESC LIMIT 1"""
                else:
                    sql="""SELECT STATUS,COMPLETED_AT,DOCUMENT_ROWS,REJECTED_DOCUMENTS,ITEM_ROWS,REJECTED_ITEMS
                        FROM SKYPOINTS.REDEMPTION_TRANSFORM_RUN WHERE BATCH_ID=%s
                        ORDER BY COMPLETED_AT DESC,RUN_ID DESC LIMIT 1"""
                transformed=query(cur,sql,(raw[0]['BATCH_ID'],))
            outcomes=assess(raw,transformed,now,max_age_hours,max_reject_rate,max_transform_lag_hours)
            checks.extend(dict(c,name=feed+':'+source+':'+c['name']) for c in outcomes)
        return {'schema_version':1,'checked_at':now.astimezone(timezone.utc).isoformat(),
                'status':'PASS' if all(c['passed'] for c in checks) else 'FAIL',
                'failed_checks':sum(not c['passed'] for c in checks),'checks':checks}
    finally:
        cur.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=source_spec,action='append',required=True,
                        help='Repeat for every expected source, e.g. MEMBER:SKYPOINTS_IND')
    parser.add_argument('--max-age-hours',type=float,default=26)
    parser.add_argument('--max-reject-rate',type=float,default=0.05)
    parser.add_argument('--max-transform-lag-hours',type=float,default=2)
    args=parser.parse_args()
    try:
        policy(args.max_age_hours,args.max_reject_rate,args.max_transform_lag_hours)
        if len(set(args.source))!=len(args.source):
            raise ValueError('Duplicate expected source')
    except ValueError as exc:
        parser.error(str(exc))
    from dotenv import load_dotenv
    load_dotenv()
    conn=None
    try:
        conn=connect_snowflake()
        report=check_pipeline(conn,args.source,max_age_hours=args.max_age_hours,
                              max_reject_rate=args.max_reject_rate,
                              max_transform_lag_hours=args.max_transform_lag_hours)
        print(json.dumps(report))
        code=0 if report['status']=='PASS' else 1
    except Exception:
        # Connector exceptions can contain credentials, SQL parameters, or row data.
        print(json.dumps({'schema_version':1,'status':'ERROR','message':'Health checks could not complete. Inspect configuration and secured operational logs.'}))
        code=2
    finally:
        if conn is not None:
            conn.close()
    raise SystemExit(code)


if __name__=='__main__':
    main()
