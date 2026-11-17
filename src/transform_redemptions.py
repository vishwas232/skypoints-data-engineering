"""Phase 5: validate nested redemption feeds and link verified current members.

Bounded assessment adapter. Schedule one writer for the whole SKYPOINTS schema.
Conflicting versions require review, never arrival-time overwrites.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime
import json
from pathlib import Path
import re

from src.ingest_raw import connect_snowflake, records
from src.transform_members import digest, query, unique

TXN_FIELDS = ['TRANSACTION_KEY', 'SOURCE_SYSTEM', 'TXN_ID', 'GLOBAL_MEMBER_ID',
              'SOURCE_MEMBER_ID', 'FEED_DATE', 'TXN_DATE', 'PARTNER',
              'MILES_REDEEMED', 'STATUS', 'RAW_RECORD_ID', 'LAST_BATCH_ID',
              'SOURCE_RECORD_ID']
FACT_FIELDS = ['SOURCE_SYSTEM', 'TXN_ID', 'GLOBAL_MEMBER_ID', 'SOURCE_MEMBER_ID',
               'TXN_DATE', 'PARTNER', 'MILES_REDEEMED', 'STATUS']
STAGE_FIELDS = ['RECORD_ID', 'RAW_RECORD_ID', 'BATCH_ID', 'RECORD_KIND',
                'ITEM_INDEX', 'RAW_SHA256', 'TRANSACTION_KEY', 'SOURCE_SYSTEM',
                'TXN_ID', 'GLOBAL_MEMBER_ID', 'SOURCE_MEMBER_ID', 'FEED_DATE',
                'TXN_DATE', 'PARTNER', 'MILES_REDEEMED', 'STATUS', 'VALIDATION_STATUS']
STATUSES = {'PENDING', 'COMPLETED', 'CANCELLED', 'FAILED'}


def scalar(value, limit):
    """IDs may be strings or integers. Never stringify containers or booleans."""
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError('Invalid scalar')
    value = str(value).strip()
    if not value or value.upper() == 'NULL' or len(value) > limit:
        raise ValueError('Invalid scalar')
    return value


def feed_date(value):
    value = scalar(value, 10)
    if re.fullmatch(r'[0-9]{8}', value):
        return datetime.strptime(value, '%Y%m%d').date()
    if re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
        return date.fromisoformat(value)
    raise ValueError('Invalid date')


def invalid(row, code):
    row['VALIDATION_ERRORS'] = sorted(set(row['VALIDATION_ERRORS'] + [code]))
    row['VALIDATION_STATUS'] = 'REJECTED'


def signature(row):
    # Feed date is observation provenance, not a transaction business version.
    return tuple(row[k] for k in FACT_FIELDS)


def flatten(raw, identities, members, as_of):
    """One DOCUMENT row always, plus one ITEM row for every array element.

    DOC validity covers the envelope and member link. ITEM validity also covers
    transaction fields. RAW retains originals when narrow fields cannot fit.
    """
    doc = dict.fromkeys(STAGE_FIELDS)
    doc.update(RECORD_ID=digest([raw['RAW_RECORD_ID'], -1]),
               RAW_RECORD_ID=raw['RAW_RECORD_ID'], BATCH_ID=raw['BATCH_ID'],
               RECORD_KIND='DOCUMENT', ITEM_INDEX=-1, RAW_SHA256=digest(raw),
               SOURCE_SYSTEM=raw['SOURCE_SYSTEM'], VALIDATION_STATUS='VALID',
               VALIDATION_ERRORS=[])
    payload = raw['PAYLOAD']
    try:
        payload = json.loads(payload) if isinstance(payload, str) else payload
    except (ValueError, TypeError):
        payload = None
    if not isinstance(payload, dict):
        invalid(doc, 'INVALID_DOCUMENT_OBJECT')
        return [doc]
    try:
        doc['SOURCE_MEMBER_ID'] = scalar(payload.get('member_id'), 18)
    except ValueError:
        invalid(doc, 'INVALID_MEMBER_ID')
    try:
        doc['FEED_DATE'] = feed_date(payload.get('feed_date'))
        if doc['FEED_DATE'] > as_of:
            invalid(doc, 'FUTURE_FEED_DATE')
    except ValueError:
        invalid(doc, 'INVALID_FEED_DATE')
    mapping = identities.get((doc['SOURCE_SYSTEM'], doc['SOURCE_MEMBER_ID']), [])
    if len(mapping) != 1 or not isinstance(mapping[0], str) or not mapping[0].strip() or len(mapping[0]) > 100:
        invalid(doc, 'UNRESOLVED_OR_AMBIGUOUS_IDENTITY')
    elif len(members.get(mapping[0], [])) != 1:
        invalid(doc, 'MEMBER_NOT_CURRENT_OR_AMBIGUOUS')
    else:
        doc['GLOBAL_MEMBER_ID'] = mapping[0]
    items = payload.get('redemptions')
    if not isinstance(items, list):
        invalid(doc, 'INVALID_REDEMPTIONS_ARRAY')
        return [doc]
    output = [doc]
    for index, item in enumerate(items):
        row = dict(doc, RECORD_ID=digest([raw['RAW_RECORD_ID'], index]),
                   RECORD_KIND='ITEM', ITEM_INDEX=index,
                   VALIDATION_ERRORS=list(doc['VALIDATION_ERRORS']))
        output.append(row)
        if not isinstance(item, dict):
            invalid(row, 'INVALID_TRANSACTION_OBJECT')
            continue
        for key, field, limit in [('txn_id', 'TXN_ID', 100), ('partner', 'PARTNER', 255), ('status', 'STATUS', 30)]:
            try:
                # These business text fields must be strings, unlike numeric member IDs.
                if not isinstance(item.get(key), str):
                    raise ValueError('Not text')
                row[field] = scalar(item[key], limit)
            except ValueError:
                invalid(row, 'INVALID_' + field)
        if row['TXN_ID'] is not None:
            row['TRANSACTION_KEY'] = digest([row['SOURCE_SYSTEM'], row['TXN_ID']])
        if row['STATUS'] is not None:
            row['STATUS'] = row['STATUS'].upper()
            if row['STATUS'] not in STATUSES:
                invalid(row, 'UNSUPPORTED_STATUS')
        try:
            row['TXN_DATE'] = feed_date(item.get('txn_date'))
            if row['TXN_DATE'] > as_of or (row['FEED_DATE'] and row['TXN_DATE'] > row['FEED_DATE']):
                invalid(row, 'TRANSACTION_AFTER_FEED_OR_PROCESSING_DATE')
        except ValueError:
            invalid(row, 'INVALID_TXN_DATE')
        miles = item.get('miles_redeemed')
        if isinstance(miles, bool) or not isinstance(miles, int) or not 0 < miles <= 999999999999999999:
            invalid(row, 'INVALID_MILES_REDEEMED')
        else:
            row['MILES_REDEEMED'] = miles
    return output


def plan(rows, current):
    """Reject conflicting valid candidates together, including different owners.

    Invalid siblings do not suppress an otherwise valid candidate. Existing
    immutable facts win over conflicting later observations without alteration.
    """
    groups = defaultdict(list)
    for row in rows:
        if row['RECORD_KIND'] == 'ITEM' and row['VALIDATION_STATUS'] == 'VALID':
            groups[row['TRANSACTION_KEY']].append(row)
    inserts = []
    for key, candidates in groups.items():
        facts = {signature(r) for r in candidates}
        existing = current.get(key)
        if len(facts) != 1 or (existing is not None and signature(existing) not in facts):
            for row in candidates:
                invalid(row, 'CONFLICTING_TRANSACTION_FACTS')
        elif existing is None:
            first = min(candidates, key=lambda r: r['RECORD_ID'])
            inserts.append(dict(first, LAST_BATCH_ID=first['BATCH_ID'], SOURCE_RECORD_ID=first['RECORD_ID']))
    return inserts


def index_inputs(maps, current_members):
    identities, members = defaultdict(list), defaultdict(list)
    for row in maps:
        identities[(row['SOURCE_SYSTEM'], row['SOURCE_MEMBER_ID'])].append(row['GLOBAL_MEMBER_ID'])
    for row in current_members:
        members[row['GLOBAL_MEMBER_ID']].append(row)
    return identities, members


def run(connection, batch_id, as_of):
    cur = connection.cursor()
    try:
        cur.execute('BEGIN TRANSACTION')
        batches = query(cur, 'SELECT * FROM SKYPOINTS.INGEST_BATCH WHERE BATCH_ID=%s', (batch_id,))
        if len(batches) != 1 or batches[0]['FEED_TYPE'] != 'REDEMPTION' or batches[0]['STATUS'] != 'COMPLETED':
            raise RuntimeError('Exactly one completed REDEMPTION batch required')
        raw = query(cur, 'SELECT * FROM SKYPOINTS.RAW_REDEMPTION_FEED WHERE BATCH_ID=%s', (batch_id,))
        unique(raw, ['RAW_RECORD_ID'], 'raw redemption')
        if len(raw) != batches[0]['INPUT_ROWS']:
            raise RuntimeError('Raw document count mismatch')
        maps = query(cur, 'SELECT SOURCE_SYSTEM,SOURCE_MEMBER_ID,GLOBAL_MEMBER_ID FROM SKYPOINTS.MEMBER_IDENTITY_MAP')
        current_members = query(cur, 'SELECT GLOBAL_MEMBER_ID FROM SKYPOINTS.MEMBER_CURRENT')
        identities, members = index_inputs(maps, current_members)
        transactions = query(cur, 'SELECT * FROM SKYPOINTS.REDEMPTION_TRANSACTION')
        unique(transactions, ['TRANSACTION_KEY'], 'transaction key')
        for row in transactions:
            if (identities.get((row['SOURCE_SYSTEM'],row['SOURCE_MEMBER_ID'])) != [row['GLOBAL_MEMBER_ID']]
                    or len(members.get(row['GLOBAL_MEMBER_ID'], [])) != 1 or not row['SOURCE_RECORD_ID']):
                raise RuntimeError('Existing transaction linkage changed or lacks Phase 5 provenance; reviewed rebuild required')
        history = query(cur, 'SELECT * FROM SKYPOINTS.STG_REDEMPTION_RECORD WHERE BATCH_ID=%s', (batch_id,))
        unique(history, ['RECORD_ID'], 'staging redemption')
        raw_by_id = {r['RAW_RECORD_ID']: r for r in raw}
        for row in history:
            if row['RAW_RECORD_ID'] not in raw_by_id or row['RAW_SHA256'] != digest(raw_by_id[row['RAW_RECORD_ID']]):
                raise RuntimeError('Processed RAW modified or removed; reviewed rebuild required')
        run_id = digest(['REDEMPTION_V1',batch_id,as_of,sorted(raw,key=lambda r:r['RAW_RECORD_ID']),
                         sorted(maps,key=lambda r:json.dumps(r,sort_keys=True)),
                         sorted(current_members,key=lambda r:r['GLOBAL_MEMBER_ID'])])
        done = query(cur, 'SELECT STATUS FROM SKYPOINTS.REDEMPTION_TRANSFORM_RUN WHERE RUN_ID=%s', (run_id,))
        if done:
            if len(done) != 1 or done[0]['STATUS'] != 'COMPLETED':
                raise RuntimeError('Invalid redemption run state')
            connection.rollback()
            return {'status':'SKIPPED','run_id':run_id}
        staged = [row for record in raw for row in flatten(record,identities,members,as_of)]
        inserts = plan(staged, {r['TRANSACTION_KEY']:r for r in transactions})
        by_id = {r['RECORD_ID']:r for r in staged}
        for row in history:
            if row['RECORD_KIND'] == 'ITEM' and row['VALIDATION_STATUS'] == 'VALID':
                replacement = by_id.get(row['RECORD_ID'])
                if replacement is None or replacement['VALIDATION_STATUS'] != 'VALID' or signature(row) != signature(replacement):
                    raise RuntimeError('Previously accepted item invalidated; reviewed rebuild required')
        cur.execute('DELETE FROM SKYPOINTS.STG_REDEMPTION_RECORD WHERE BATCH_ID=%s', (batch_id,))
        cur.execute("DELETE FROM SKYPOINTS.REJECTED_RECORD WHERE BATCH_ID=%s AND FEED_TYPE='REDEMPTION'", (batch_id,))
        for row in staged:
            cur.execute('INSERT INTO SKYPOINTS.STG_REDEMPTION_RECORD ('+','.join(STAGE_FIELDS)+',VALIDATION_ERRORS) SELECT '+','.join(['%s']*len(STAGE_FIELDS))+',PARSE_JSON(%s)',
                        tuple(row[k] for k in STAGE_FIELDS)+(json.dumps(row['VALIDATION_ERRORS']),))
            if row['VALIDATION_STATUS'] == 'REJECTED':
                cur.execute("INSERT INTO SKYPOINTS.REJECTED_RECORD (REJECTION_ID,BATCH_ID,RAW_RECORD_ID,FEED_TYPE,ERROR_CODE,ERROR_DETAIL) VALUES (%s,%s,%s,'REDEMPTION','REDEMPTION_VALIDATION_FAILED',%s)",
                            (row['RECORD_ID'],batch_id,row['RAW_RECORD_ID'],json.dumps({'kind':row['RECORD_KIND'],'item_index':row['ITEM_INDEX'],'errors':row['VALIDATION_ERRORS']})))
        for row in inserts:
            cur.execute('INSERT INTO SKYPOINTS.REDEMPTION_TRANSACTION ('+','.join(TXN_FIELDS)+') VALUES ('+','.join(['%s']*len(TXN_FIELDS))+')', tuple(row[k] for k in TXN_FIELDS))
        docs = [r for r in staged if r['RECORD_KIND']=='DOCUMENT']
        items = [r for r in staged if r['RECORD_KIND']=='ITEM']
        accepted_docs = sum(r['VALIDATION_STATUS']=='VALID' for r in docs)
        accepted_items = sum(r['VALIDATION_STATUS']=='VALID' for r in items)
        counts = (len(docs),accepted_docs,len(docs)-accepted_docs,len(items),accepted_items,len(items)-accepted_items,len(inserts),accepted_items-len(inserts))
        cur.execute("INSERT INTO SKYPOINTS.REDEMPTION_TRANSFORM_RUN (RUN_ID,BATCH_ID,AS_OF_DATE,STATUS,DOCUMENT_ROWS,ACCEPTED_DOCUMENTS,REJECTED_DOCUMENTS,ITEM_ROWS,ACCEPTED_ITEMS,REJECTED_ITEMS,NEW_TRANSACTIONS,REPLAY_ITEMS) VALUES (%s,%s,%s,'COMPLETED',%s,%s,%s,%s,%s,%s,%s,%s)",
                    (run_id,batch_id,as_of)+counts)
        sql = (Path(__file__).resolve().parents[1]/'sql/07_verify_redemption_transform.sql').read_text(encoding='utf-8')
        for statement in sql.split(';'):
            if statement.strip() and query(cur,statement):
                raise RuntimeError('Redemption reconciliation failed; rolled back')
        connection.commit()
        return {'status':'COMPLETED','run_id':run_id,'documents':len(docs),'items':len(items),
                'accepted_items':accepted_items,'rejected_items':len(items)-accepted_items,'new_transactions':len(inserts)}
    except Exception:
        connection.rollback()
        raise
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch-id')
    parser.add_argument('--as-of',required=True,type=date.fromisoformat)
    parser.add_argument('--dry-run',action='store_true')
    parser.add_argument('--json-file',type=Path)
    args = parser.parse_args()
    if args.dry_run:
        if not args.json_file:
            parser.error('--json-file is required for --dry-run')
        # No credentials, map, member data, or Snowflake connection in local mode.
        staged = []
        for r in records(args.json_file,'REDEMPTION'):
            staged.extend(flatten({k.upper():v for k,v in r.items()}, {}, {}, args.as_of))
        items = [r for r in staged if r['RECORD_KIND']=='ITEM']
        print(json.dumps({'mode':'DRY_RUN_UNLINKED','documents':sum(r['RECORD_KIND']=='DOCUMENT' for r in staged),
                          'items':len(items),'rejected_items':sum(r['VALIDATION_STATUS']=='REJECTED' for r in items),
                          'reason_codes':sorted({e for r in staged for e in r['VALIDATION_ERRORS']})}))
        return
    if not args.batch_id or args.json_file:
        parser.error('Use --batch-id for Snowflake mode, or --dry-run --json-file for local mode')
    from dotenv import load_dotenv
    load_dotenv()
    conn = None
    try:
        conn = connect_snowflake()
        print(json.dumps(run(conn,args.batch_id,args.as_of)))
    except Exception:
        raise SystemExit('Redemption transformation failed. An unacknowledged commit may have succeeded; retry identical arguments. Inspect securely without shared payload logs.') from None
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    main()
