"""Phase 4 small-batch adapter. No connection is opened during import or tests.

Run one writer only; see docs/phase4.md for the input contracts and recovery.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import json
import re

from src.ingest_raw import connect_snowflake

COUNTRIES = {'IN': 'IND', 'IND': 'IND', 'US': 'USA', 'USA': 'USA',
             'AU': 'AUS', 'AUS': 'AUS', 'CA': 'CAN', 'CAN': 'CAN',
             'PH': 'PHL', 'PHL': 'PHL', 'GB': 'GBR', 'GBR': 'GBR',
             'NZ': 'NZL', 'NZL': 'NZL'}
TARGETS = {'IND': 'TABLE_INDIA', 'USA': 'TABLE_USA', 'AUS': 'TABLE_AUSTRALIA',
           'CAN': 'TABLE_CANADA', 'PHL': 'TABLE_PHILIPPINES'}
FIELDS = ['SOURCE_SYSTEM', 'SOURCE_MEMBER_ID', 'GLOBAL_MEMBER_ID', 'MEMBER_NAME',
          'ENROLLMENT_DATE', 'LAST_FLIGHT_DATE', 'TIER_CODE', 'AGENT_NAME',
          'STATE_CODE', 'COUNTRY_CODE', 'POST_CODE', 'DOB', 'AGE', 'STALE_MEMBER',
          'IS_ACTIVE', 'MEMBER_TYPE', 'SOURCE_UPDATE_TS', 'SOURCE_SEQUENCE']


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str,
                                     separators=(',', ':')).encode()).hexdigest()


def text(value):
    if value is None:
        return None
    value = str(value).strip()
    return None if value.upper() in ('', 'NULL', 'NONE') else value


def parse_date(value, source):
    value = text(value)
    if value is None:
        return None
    # USA's documented sample format is MMDDYYYY, with lost leading zeroes.
    if source == 'SKYPOINTS_USA' and re.fullmatch(r'\d{7,8}', value):
        return datetime.strptime(value.zfill(8), '%m%d%Y').date()
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        return date.fromisoformat(value)
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}T00:00:00(?:\.0+)?', value):
        return datetime.fromisoformat(value).date()
    raise ValueError('Unsupported date format')


def timestamp(value):
    if not value:
        return None
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    # Contract: NTZ values are UTC; aware inputs are normalized to UTC.
    return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value


def transform(raw, identities, as_of, version=None):
    p = raw['PAYLOAD']
    p = json.loads(p) if isinstance(p, str) else p
    errors = []
    if not isinstance(p, dict):
        p = {}
        errors.append('INVALID_PAYLOAD_OBJECT')
    out = dict.fromkeys(FIELDS)
    out.update(RAW_RECORD_ID=raw['RAW_RECORD_ID'], BATCH_ID=raw['BATCH_ID'],
               INGESTED_AT=raw.get('INGESTED_AT'), SOURCE_SYSTEM=raw['SOURCE_SYSTEM'], AS_OF_DATE=as_of)
    def pick(*keys):
        return next((text(p[k]) for k in keys if text(p.get(k)) is not None), None)
    out['SOURCE_MEMBER_ID'] = pick('ID', 'Unique ID')
    out['MEMBER_NAME'] = pick('Name', 'Member Name')
    out['COUNTRY_CODE'] = COUNTRIES.get((pick('CountryCode', 'Country') or text(raw.get('SOURCE_COUNTRY')) or '').upper())
    out['TIER_CODE'] = pick('TierCode', 'Tier Type')
    out['MEMBER_TYPE'] = pick('Individual or Corporate')
    out['IS_ACTIVE'] = pick('IsActive')
    out['AGENT_NAME'], out['STATE_CODE'], out['POST_CODE'] = pick('AgentName'), pick('StateCode'), pick('PostCode')
    for key, limit in [('SOURCE_MEMBER_ID', 18), ('MEMBER_NAME', 255), ('TIER_CODE', 5),
                       ('AGENT_NAME', 255), ('STATE_CODE', 5), ('POST_CODE', 20)]:
        if out[key] and len(out[key]) > limit:
            errors.append('TOO_LONG_' + key)
            out[key] = None  # Full invalid value remains in RAW, not a narrow staging column.
    for key in ('SOURCE_MEMBER_ID', 'MEMBER_NAME', 'COUNTRY_CODE'):
        if not out[key]:
            errors.append('MISSING_OR_INVALID_' + key)
    for key, aliases in [('DOB', ('DOB', 'Date of Birth')),
                         ('ENROLLMENT_DATE', ('EnrollmentDate', 'Date of Enrollment')),
                         ('LAST_FLIGHT_DATE', ('FlightDate', 'Flight Date', 'Date of Flight'))]:
        try:
            out[key] = parse_date(pick(*aliases), out['SOURCE_SYSTEM'])
            if out[key] and out[key] > as_of:
                errors.append('FUTURE_' + key)
        except ValueError:
            errors.append('INVALID_' + key)
    if not out['ENROLLMENT_DATE']:
        errors.append('MISSING_ENROLLMENT_DATE')
    if out['DOB'] and out['ENROLLMENT_DATE'] and out['DOB'] > out['ENROLLMENT_DATE']:
        errors.append('DOB_AFTER_ENROLLMENT')
    if out['LAST_FLIGHT_DATE'] and out['ENROLLMENT_DATE'] and out['LAST_FLIGHT_DATE'] < out['ENROLLMENT_DATE']:
        errors.append('FLIGHT_BEFORE_ENROLLMENT')
    if out['MEMBER_TYPE'] not in (None, 'I', 'C'):
        errors.append('INVALID_MEMBER_TYPE')
        out['MEMBER_TYPE'] = None
    if out['IS_ACTIVE'] not in (None, 'Y', 'N'):
        errors.append('INVALID_IS_ACTIVE')
        out['IS_ACTIVE'] = None
    mapping = identities.get((out['SOURCE_SYSTEM'], out['SOURCE_MEMBER_ID']), [])
    if len(mapping) != 1 or not text(mapping[0]) or len(str(mapping[0])) > 100:
        errors.append('UNRESOLVED_OR_AMBIGUOUS_IDENTITY')
    else:
        out['GLOBAL_MEMBER_ID'] = mapping[0]
    try:
        out['SOURCE_UPDATE_TS'] = timestamp(raw.get('SOURCE_UPDATE_TS') or (version or {}).get('SOURCE_UPDATE_TS'))
        seq = raw.get('SOURCE_SEQUENCE')
        seq = seq if seq is not None else (version or {}).get('SOURCE_SEQUENCE')
        if seq is not None and not re.fullmatch(r'\d{1,38}', str(seq)):
            raise ValueError('Invalid sequence')
        out['SOURCE_SEQUENCE'] = int(seq) if seq is not None else 0
        if out['SOURCE_UPDATE_TS'] is None:
            errors.append('MISSING_SOURCE_UPDATE_TS')
        elif out['SOURCE_UPDATE_TS'].date() > as_of:
            errors.append('FUTURE_SOURCE_UPDATE_TS')
    except (ValueError, TypeError, OverflowError):
        errors.append('INVALID_SOURCE_VERSION')
    dob, flight = out['DOB'], out['LAST_FLIGHT_DATE']
    out['AGE'] = as_of.year - dob.year - ((as_of.month, as_of.day) < (dob.month, dob.day)) if dob else None
    if out['AGE'] is not None and not 0 <= out['AGE'] <= 130:
        errors.append('AGE_OUT_OF_RANGE')
        out['AGE'] = None
    out['STALE_MEMBER'] = (as_of - flight).days > 90 if flight else None
    out['VALIDATION_ERRORS'] = sorted(set(errors))
    out['VALIDATION_STATUS'] = 'REJECTED' if errors else 'VALID'
    return out


def order(row):
    return (row['SOURCE_UPDATE_TS'], row['SOURCE_SEQUENCE'], row['RAW_RECORD_ID'])


def winners(rows):
    result = {}
    for row in rows:
        if row['VALIDATION_STATUS'] == 'VALID':
            key = row['GLOBAL_MEMBER_ID']
            if key not in result or order(row) > order(result[key]):
                result[key] = row
    return result


def query(cursor, sql, params=None):
    cursor.execute(sql, params) if params is not None else cursor.execute(sql)
    return [dict(zip([c[0] for c in cursor.description], r)) for r in cursor.fetchall()]


def unique(rows, key, label):
    counts = Counter(tuple(r[k] for k in key) for r in rows)
    if any(n > 1 for n in counts.values()):
        raise RuntimeError('Duplicate ' + label + '; repair upstream before retry')


def run(connection, batch_id, as_of):
    """All writes, reconciliation and run completion are one DML transaction.

    Reads all staging history: deliberately bounded assessment implementation.
    No DDL here: Snowflake DDL implicitly commits active transactions.
    """
    cur = connection.cursor()
    try:
        cur.execute('BEGIN TRANSACTION')
        batches = query(cur, "SELECT * FROM SKYPOINTS.INGEST_BATCH WHERE BATCH_ID=%s", (batch_id,))
        if len(batches) != 1 or batches[0]['STATUS'] != 'COMPLETED' or batches[0]['FEED_TYPE'] != 'MEMBER':
            raise RuntimeError('Exactly one completed MEMBER raw batch required')
        raw = query(cur, 'SELECT * FROM SKYPOINTS.RAW_MEMBER_PROFILE WHERE BATCH_ID=%s', (batch_id,))
        unique(raw, ['RAW_RECORD_ID'], 'raw record')
        if len(raw) != batches[0]['INPUT_ROWS']:
            raise RuntimeError('Raw batch count mismatch')
        maps = query(cur, 'SELECT * FROM SKYPOINTS.MEMBER_IDENTITY_MAP')
        identities = {}
        for row in maps:
            identities.setdefault((row['SOURCE_SYSTEM'], row['SOURCE_MEMBER_ID']), []).append(row['GLOBAL_MEMBER_ID'])
        versions = query(cur, 'SELECT * FROM SKYPOINTS.MEMBER_BATCH_VERSION WHERE BATCH_ID=%s', (batch_id,))
        if len(versions) > 1:
            raise RuntimeError('Ambiguous batch source version')
        history = query(cur, 'SELECT * FROM SKYPOINTS.STG_MEMBER_PROFILE')
        unique(history, ['RAW_RECORD_ID'], 'staging record')
        current = query(cur, 'SELECT * FROM SKYPOINTS.MEMBER_CURRENT')
        unique(current, ['GLOBAL_MEMBER_ID'], 'current member')
        # Mapping changes require a controlled rebuild, never silently rekey current data.
        for row in history:
            if row['VALIDATION_STATUS'] == 'VALID' and identities.get((row['SOURCE_SYSTEM'], row['SOURCE_MEMBER_ID'])) != [row['GLOBAL_MEMBER_ID']]:
                raise RuntimeError('Previously accepted identity changed; controlled rebuild required')
        previous_runs = query(cur, "SELECT MAX(AS_OF_DATE) AS D FROM SKYPOINTS.MEMBER_TRANSFORM_RUN WHERE STATUS='COMPLETED'")
        if previous_runs[0]['D'] and as_of < previous_runs[0]['D']:
            raise RuntimeError('Processing date cannot go backwards')
        run_id = digest([batch_id, as_of, sorted(raw, key=lambda r:r['RAW_RECORD_ID']),
                         sorted(maps, key=lambda r:json.dumps(r, sort_keys=True, default=str)), versions])
        done = query(cur, 'SELECT STATUS FROM SKYPOINTS.MEMBER_TRANSFORM_RUN WHERE RUN_ID=%s', (run_id,))
        if done:
            if len(done) != 1 or done[0]['STATUS'] != 'COMPLETED':
                raise RuntimeError('Invalid transform run state')
            connection.rollback()
            return {'status': 'SKIPPED', 'run_id': run_id}
        staged = [transform(r, identities, as_of, versions[0] if versions else None) for r in raw]
        new_by_id = {r['RAW_RECORD_ID']: r for r in staged}
        for previous in history:
            if previous['BATCH_ID'] == batch_id and previous['VALIDATION_STATUS'] == 'VALID':
                replacement = new_by_id.get(previous['RAW_RECORD_ID'])
                if replacement is None or any(previous[k] != replacement[k] for k in FIELDS if k not in ('AGE', 'STALE_MEMBER')):
                    raise RuntimeError('Accepted source data or metadata changed; controlled rebuild required')
        # Accepted immutable raw rows must not disappear after metadata edits.
        new_valid = {r['RAW_RECORD_ID'] for r in staged if r['VALIDATION_STATUS'] == 'VALID'}
        if any(r['BATCH_ID'] == batch_id and r['VALIDATION_STATUS'] == 'VALID' and r['RAW_RECORD_ID'] not in new_valid for r in history):
            raise RuntimeError('Accepted record invalidated; controlled rebuild required')
        combined = [r for r in history if r['BATCH_ID'] != batch_id] + staged
        chosen = winners(combined)
        old = {r['GLOBAL_MEMBER_ID']: r for r in current}
        # Current must have provenance in history before we replace affected identities.
        if set(old) - set(chosen):
            raise RuntimeError('Current members missing staging provenance')
        cur.execute('DELETE FROM SKYPOINTS.STG_MEMBER_PROFILE WHERE BATCH_ID=%s', (batch_id,))
        cur.execute("DELETE FROM SKYPOINTS.REJECTED_RECORD WHERE BATCH_ID=%s AND FEED_TYPE='MEMBER'", (batch_id,))
        cols = ['RAW_RECORD_ID', 'BATCH_ID'] + FIELDS + ['INGESTED_AT', 'VALIDATION_STATUS', 'AS_OF_DATE']
        for row in staged:
            cur.execute('INSERT INTO SKYPOINTS.STG_MEMBER_PROFILE (' + ','.join(cols) + ',VALIDATION_ERRORS,VALIDATED_AT) SELECT ' + ','.join(['%s'] * len(cols)) + ',PARSE_JSON(%s),CURRENT_TIMESTAMP()',
                        tuple(row[k] for k in cols) + (json.dumps(row['VALIDATION_ERRORS']),))
            if row['VALIDATION_ERRORS']:
                cur.execute("INSERT INTO SKYPOINTS.REJECTED_RECORD (REJECTION_ID,BATCH_ID,RAW_RECORD_ID,FEED_TYPE,ERROR_CODE,ERROR_DETAIL) VALUES (%s,%s,%s,'MEMBER','VALIDATION_FAILED',%s)",
                            (digest([batch_id,row['RAW_RECORD_ID']]),batch_id,row['RAW_RECORD_ID'],json.dumps(row['VALIDATION_ERRORS'])))
        for gid, row in chosen.items():
            before = old.get(gid)
            if before and before['COUNTRY_CODE'] != row['COUNTRY_CODE']:
                cur.execute('INSERT INTO SKYPOINTS.COUNTRY_MOVEMENT_AUDIT (MOVEMENT_ID,GLOBAL_MEMBER_ID,FROM_COUNTRY,TO_COUNTRY,SOURCE_UPDATE_TS,BATCH_ID) VALUES (%s,%s,%s,%s,%s,%s)',
                            (digest([run_id,gid,before['COUNTRY_CODE'],row['COUNTRY_CODE']]),gid,before['COUNTRY_CODE'],row['COUNTRY_CODE'],row['SOURCE_UPDATE_TS'],row['BATCH_ID']))
            # Refresh date-dependent fields for every current member at this as-of date.
            dob, flight = row['DOB'], row['LAST_FLIGHT_DATE']
            row['AGE'] = as_of.year-dob.year-((as_of.month,as_of.day)<(dob.month,dob.day)) if dob else None
            row['STALE_MEMBER'] = (as_of-flight).days > 90 if flight else None
            cur.execute('DELETE FROM SKYPOINTS.MEMBER_CURRENT WHERE GLOBAL_MEMBER_ID=%s', (gid,))
            cur.execute('INSERT INTO SKYPOINTS.MEMBER_CURRENT ('+','.join(FIELDS)+',LAST_BATCH_ID,WINNER_RAW_RECORD_ID,AS_OF_DATE) VALUES ('+','.join(['%s']*(len(FIELDS)+3))+')',
                        tuple(row[k] for k in FIELDS)+(row['BATCH_ID'],row['RAW_RECORD_ID'],as_of))
        # Atomic mirror refresh removes the old country even for inactive members.
        for country, table in {**TARGETS, '*': 'TABLE_OTHER_COUNTRIES'}.items():
            cur.execute('DELETE FROM SKYPOINTS.'+table)
            predicate = 'COUNTRY_CODE NOT IN (\'IND\',\'USA\',\'AUS\',\'CAN\',\'PHL\')' if country == '*' else 'COUNTRY_CODE=%s'
            cur.execute('INSERT INTO SKYPOINTS.'+table+' SELECT * FROM SKYPOINTS.MEMBER_CURRENT WHERE '+predicate,
                        (country,) if country != '*' else ())
        accepted = sum(r['VALIDATION_STATUS'] == 'VALID' for r in staged)
        applied = sum(r['BATCH_ID'] == batch_id for r in chosen.values())
        cur.execute("INSERT INTO SKYPOINTS.MEMBER_TRANSFORM_RUN (RUN_ID,BATCH_ID,AS_OF_DATE,STATUS,INPUT_ROWS,ACCEPTED_ROWS,REJECTED_ROWS,WINNING_ROWS,SUPERSEDED_ROWS) VALUES (%s,%s,%s,'COMPLETED',%s,%s,%s,%s,%s)",
                    (run_id,batch_id,as_of,len(staged),accepted,len(staged)-accepted,applied,accepted-applied))
        # SQL invariant queries are also runnable manually; any violation aborts all writes.
        from pathlib import Path
        sql = (Path(__file__).resolve().parents[1]/'sql/05_verify_member_transform.sql').read_text(encoding='utf-8')
        for statement in sql.split(';'):
            if statement.strip() and query(cur, statement):
                raise RuntimeError('Phase 4 reconciliation failed; rolled back')
        connection.commit()
        return {'status':'COMPLETED','input':len(staged),'accepted':accepted,'rejected':len(staged)-accepted,'run_id':run_id}
    except Exception:
        connection.rollback()
        raise
    finally:
        cur.close()


def main():
    from dotenv import load_dotenv
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch-id', required=True)
    parser.add_argument('--as-of', required=True, type=date.fromisoformat)
    args = parser.parse_args()
    conn = connect_snowflake()
    try:
        print(json.dumps(run(conn,args.batch_id,args.as_of)))
    except Exception:
        # Connector errors can contain SQL parameters and member data.
        raise SystemExit('Transformation failed and rolled back. Inspect securely; no payload or credentials logged.') from None
    finally:
        conn.close()


if __name__ == '__main__':
    main()
