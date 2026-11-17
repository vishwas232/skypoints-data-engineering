"""Primary Snowflake-native pipeline. All writes require a dev/test namespace.

Use ``python -m src.pipeline --help``. Legacy small-batch scripts remain available
for compatibility, but are not the scalable path or the deployment entry point.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re
import tempfile
import time
import uuid

from src.ingest_raw import connect_snowflake, raw_id, workbook_rows
from src.source_contracts import snapshot_file, flat_file_rows, json_documents, write_parts
from src.transform_members import digest

ROOT = Path(__file__).resolve().parents[1]
COUNTRIES = {'IND':'TABLE_INDIA','USA':'TABLE_USA','AUS':'TABLE_AUSTRALIA',
             'CAN':'TABLE_CANADA','PHL':'TABLE_PHILIPPINES','OTHER':'TABLE_OTHER_COUNTRIES'}
MEMBER_COLUMNS = ('GLOBAL_MEMBER_ID,SOURCE_SYSTEM,SOURCE_MEMBER_ID,MEMBER_NAME,ENROLLMENT_DATE,'
 'LAST_FLIGHT_DATE,TIER_CODE,AGENT_NAME,STATE_CODE,COUNTRY_CODE,POST_CODE,DOB,AGE,STALE_MEMBER,'
 'IS_ACTIVE,SOURCE_UPDATE_TS,SOURCE_SEQUENCE,FACT_HASH,WINNER_RAW_RECORD_ID,LAST_BATCH_ID,AS_OF_DATE,UPDATED_AT').split(',')


@dataclass(frozen=True)
class Namespace:
    database: str
    schema: str

    def __post_init__(self):
        if not re.fullmatch(r'[A-Z_][A-Z0-9_]{0,254}', self.database):
            raise ValueError('Use an uppercase unquoted Snowflake database identifier')
        if not re.fullmatch(r'SKYPOINTS_DEV(?:_[A-Z0-9]+)*|SKYPOINTS_TEST_[A-F0-9]{12}', self.schema):
            raise ValueError('Writes require SKYPOINTS_DEV[_SUFFIX] or a unique SKYPOINTS_TEST_<12hex> schema')

    @property
    def sql(self):
        return f'"{self.database}"."{self.schema}"'

    @property
    def is_test(self):
        return self.schema.startswith('SKYPOINTS_TEST_')


@dataclass(frozen=True)
class Contract:
    source: str
    feed: str
    snapshot_ts: datetime
    source_format: str = 'PDF'
    dob_format: str = 'DDMMYYYY'
    identity_scope: str = 'LOCAL'
    identity_authority: str = ''
    sequence: int = 0
    enforce_name_key: bool = True
    allow_transaction_updates: bool = False
    default_country: str | None = None

    def __post_init__(self):
        if not re.fullmatch(r'[A-Z0-9_]{1,100}', self.source):
            raise ValueError('Invalid source namespace')
        if self.feed not in ('MEMBER','REDEMPTION') or self.source_format not in ('PDF','EXCEL_ISO','EXCEL_US','JSON'):
            raise ValueError('Invalid feed/format')
        if (self.feed=='REDEMPTION') != (self.source_format=='JSON'):
            raise ValueError('Redemptions require JSON and members require PDF/Excel')
        if self.dob_format not in ('DDMMYYYY','MMDDYYYY','YYYYMMDD'):
            raise ValueError('Explicit supported DOB format required')
        if self.identity_scope not in ('LOCAL','GLOBAL'):
            raise ValueError('Invalid identity scope')
        if not isinstance(self.enforce_name_key,bool) or not isinstance(self.allow_transaction_updates,bool):
            raise ValueError('Contract policy switches must be booleans')
        if self.identity_scope=='GLOBAL' and not re.fullmatch(r'[A-Z0-9_]{1,32}',self.identity_authority):
            raise ValueError('Declared global identities require a named shared authority')
        if isinstance(self.sequence,bool) or not isinstance(self.sequence,int) or not 0<=self.sequence<10**38:
            raise ValueError('Sequence must be a nonnegative 38-digit integer')
        if not isinstance(self.snapshot_ts,datetime):
            raise ValueError('Trusted business snapshot timestamp required')
        value=self.snapshot_ts
        if value.tzinfo:
            value=value.astimezone(timezone.utc).replace(tzinfo=None)
        object.__setattr__(self,'snapshot_ts',value)
        if self.default_country is not None and not re.fullmatch(r'[A-Z]{2,5}',self.default_country):
            raise ValueError('Invalid fallback country')

    def batch_id(self,file_digest):
        return digest(['NATIVE_V1',asdict(self),file_digest])


def split_sql(text):
    """Split shipped SQL, preserving quoted strings and SQL UDF dollar bodies."""
    result, buffer, quote, dollar, comment, i = [], [], None, False, False, 0
    while i<len(text):
        char=text[i]
        if comment:
            if char=='\n': comment=False; buffer.append('\n')
            i+=1; continue
        if not quote and not dollar and text[i:i+2]=='--':
            comment=True; i+=2; continue
        if not quote and text[i:i+2]=='$$':
            dollar=not dollar; buffer.append('$$'); i+=2; continue
        if not dollar and char in ("'",'"'):
            if quote==char:
                if i+1<len(text) and text[i+1]==char:
                    buffer.extend([char,char]); i+=2; continue
                quote=None
            elif quote is None: quote=char
        if char==';' and quote is None and not dollar:
            if ''.join(buffer).strip(): result.append(''.join(buffer).strip())
            buffer=[]
        else: buffer.append(char)
        i+=1
    if quote or dollar: raise ValueError('Unterminated SQL quote/body')
    if ''.join(buffer).strip(): result.append(''.join(buffer).strip())
    return result


class InjectedFailure(RuntimeError):
    pass


class Pipeline:
    def __init__(self,connection,namespace: Namespace):
        self.connection,self.ns=connection,namespace
        self.query_ids=[]
        self.created_test_schema=False

    def execute(self,sql,params=None):
        cur=self.connection.cursor()
        try:
            cur.execute(sql,params) if params is not None else cur.execute(sql)
            if getattr(cur,'sfqid',None): self.query_ids.append(cur.sfqid)
            columns=[c[0] for c in cur.description] if cur.description else []
            return [dict(zip(columns,row)) for row in cur.fetchall()] if columns else []
        finally: cur.close()

    def scalar(self,sql,params=None):
        rows=self.execute(sql,params)
        return next(iter(rows[0].values())) if rows else None

    def script(self,name):
        sql=(ROOT/'sql'/name).read_text(encoding='utf-8').replace('{{ns}}',self.ns.sql)
        if '{{' in sql: raise ValueError('Unresolved SQL template token')
        for statement in split_sql(sql): self.execute(statement)

    def deploy(self):
        exists=self.scalar(f'SELECT COUNT(*) FROM "{self.ns.database}".INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=%s',(self.ns.schema,))
        if self.ns.is_test and exists:
            raise RuntimeError('Test namespace already exists; refuse to adopt or reset it')
        if exists:
            marker=self.scalar(f'SELECT COUNT(*) FROM "{self.ns.database}".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=\'PIPELINE_MANIFEST\'',(self.ns.schema,))
            objects=self.scalar(f'SELECT COUNT(*) FROM "{self.ns.database}".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s',(self.ns.schema,))
            if objects and not marker: raise RuntimeError('Existing development schema has no project ownership manifest')
            if marker and self.execute(f'SELECT APPLICATION,SCHEMA_VERSION FROM {self.ns.sql}.PIPELINE_MANIFEST')!=[{'APPLICATION':'SKYPOINTS_NATIVE','SCHEMA_VERSION':1}]:
                raise RuntimeError('Development schema manifest is incompatible')
        self.execute(f'CREATE SCHEMA IF NOT EXISTS {self.ns.sql}')
        self.created_test_schema=self.ns.is_test and not exists
        self.script('10_native_schema.sql')
        self.connection.commit()
        return {'status':'DEPLOYED','schema':self.ns.schema}

    def drop_owned_test_schema(self):
        if not self.ns.is_test or not self.created_test_schema:
            raise RuntimeError('Cleanup is allowed only for the unique test schema created by this instance')
        self.execute(f'DROP SCHEMA {self.ns.sql} CASCADE')
        self.created_test_schema=False

    @contextmanager
    def transaction(self):
        self.execute('BEGIN TRANSACTION')
        try:
            # A DML lock is held until commit/rollback, serializing ALL native
            # ingestion, mapping, and transform writers in this schema.
            self.execute(f'UPDATE {self.ns.sql}.PIPELINE_LOCK SET OWNER_SESSION=CURRENT_SESSION(),TOUCHED_AT=CURRENT_TIMESTAMP() WHERE LOCK_ID=1')
            if self.scalar(f'SELECT COUNT(*) FROM {self.ns.sql}.PIPELINE_LOCK')!=1:
                raise RuntimeError('Invalid pipeline lock configuration')
            yield
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def record_attempt(self,batch,phase,status,error=None,source=None,feed=None):
        self.execute(f'INSERT INTO {self.ns.sql}.PIPELINE_ATTEMPT (ATTEMPT_ID,BATCH_ID,PHASE,STATUS,ERROR_CATEGORY,QUERY_ID,SOURCE_SYSTEM,FEED_TYPE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                     (uuid.uuid4().hex,batch,phase,status,error,self.query_ids[-1] if self.query_ids else None,source,feed))

    def attempt(self,batch,phase,status,error=None,source=None,feed=None):
        self.record_attempt(batch,phase,status,error,source,feed)
        self.connection.commit()

    def ingest(self,path: Path,contract: Contract,as_of: date,part_bytes=64*1024*1024):
        started=time.perf_counter()
        if contract.snapshot_ts.date()>as_of:
            raise ValueError('Source business snapshot is after processing date')
        with snapshot_file(path) as snapshot:
            bid=contract.batch_id(snapshot.sha256)
            existing=self.execute(f'SELECT STATUS FROM {self.ns.sql}.INGEST_BATCH WHERE BATCH_ID=%s',(bid,))
            if len(existing)>1: raise RuntimeError('Duplicate batch ledger keys')
            if existing:
                if existing[0]['STATUS'] not in ('LOADED','COMPLETED'): raise RuntimeError('Unexpected persisted batch state')
                self.attempt(bid,'INGEST','REPLAY_CONFIRMED',source=contract.source,feed=contract.feed)
                return {'status':'SKIPPED_RAW','batch_id':bid,'seconds':time.perf_counter()-started}
            iterator=(json_documents(snapshot.path) if contract.feed=='REDEMPTION' else
                      flat_file_rows(snapshot.path) if contract.source_format=='PDF' else workbook_rows(snapshot.path))
            def prepared():
                for number,payload in iterator:
                    yield {'raw_record_id':raw_id(bid,number),'batch_id':bid,'source_system':contract.source,
                           'source_file':snapshot.original_name,'source_row_number':number,
                           'source_country':contract.default_country,'payload':payload}
            try:
                with tempfile.TemporaryDirectory(prefix='skypoints-parts-') as directory:
                    parts,count=write_parts(prepared(),Path(directory),part_bytes)
                    # Physical uploads are unique per attempt. A retry with a
                    # different chunk size cannot accidentally COPY stale parts.
                    upload_prefix=f'{bid}/{uuid.uuid4().hex}/'
                    stage=f'@{self.ns.sql}.INGEST_STAGE/{upload_prefix}'
                    for part in parts:
                        uri=part.resolve().as_uri().replace("'","''")
                        result=self.execute(f"PUT '{uri}' {stage} AUTO_COMPRESS=FALSE OVERWRITE=FALSE")
                        if any(str(r.get('status',r.get('STATUS',''))).upper() not in ('UPLOADED','SKIPPED') for r in result):
                            raise RuntimeError('Stage upload did not complete')
                    with self.transaction():
                        if self.scalar(f'SELECT COUNT(*) FROM {self.ns.sql}.INGEST_BATCH WHERE BATCH_ID=%s',(bid,)):
                            return {'status':'SKIPPED_RAW','batch_id':bid}
                        self.execute(f'''INSERT INTO {self.ns.sql}.INGEST_BATCH
                        (BATCH_ID,SOURCE_SYSTEM,FEED_TYPE,SOURCE_FILE,FILE_SHA256,SOURCE_UPDATE_TS,SOURCE_SEQUENCE,
                         SOURCE_FORMAT,DOB_FORMAT,IDENTITY_SCOPE,IDENTITY_AUTHORITY,ENFORCE_NAME_KEY,ALLOW_TRANSACTION_UPDATES,
                         AS_OF_DATE,STATUS,INPUT_ROWS,STAGE_PREFIX,FINISHED_AT)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'LOADED',%s,%s,NULL)''',
                        (bid,contract.source,contract.feed,snapshot.original_name,snapshot.sha256,contract.snapshot_ts,contract.sequence,
                         contract.source_format,contract.dob_format,contract.identity_scope,contract.identity_authority,
                         contract.enforce_name_key,contract.allow_transaction_updates,as_of,count,upload_prefix))
                        table='RAW_MEMBER_PROFILE' if contract.feed=='MEMBER' else 'RAW_REDEMPTION_FEED'
                        if parts:
                            self.execute(f'''COPY INTO {self.ns.sql}.{table}
                            (RAW_RECORD_ID,BATCH_ID,SOURCE_SYSTEM,SOURCE_FILE,SOURCE_ROW_NUMBER,SOURCE_COUNTRY,PAYLOAD)
                            FROM (SELECT $1:raw_record_id::VARCHAR,$1:batch_id::VARCHAR,$1:source_system::VARCHAR,
                             $1:source_file::VARCHAR,$1:source_row_number::INTEGER,$1:source_country::VARCHAR,$1:payload
                             FROM {stage}) FILE_FORMAT=(TYPE=JSON COMPRESSION=AUTO) ON_ERROR='ABORT_STATEMENT' FORCE=TRUE''')
                        actual=self.scalar(f'SELECT COUNT(*) FROM {self.ns.sql}.{table} WHERE BATCH_ID=%s',(bid,))
                        if actual!=count: raise RuntimeError('COPY count reconciliation failed')
                        if contract.identity_scope=='GLOBAL': self.register_declared_global(bid,contract,table)
                        self.execute(f'UPDATE {self.ns.sql}.INGEST_BATCH SET FINISHED_AT=CURRENT_TIMESTAMP() WHERE BATCH_ID=%s',(bid,))
                self.attempt(bid,'INGEST','COMPLETED',source=contract.source,feed=contract.feed)
            except Exception as e:
                self.connection.rollback()
                self.attempt(bid,'INGEST','FAILED',type(e).__name__,contract.source,contract.feed)
                raise
        return {'status':'LOADED','batch_id':bid,'input_rows':count,'parts':len(parts),
                'seconds':time.perf_counter()-started,'source_bytes':snapshot.size}

    def register_declared_global(self,bid,contract,table):
        expr=('COALESCE(PAYLOAD:Member_Id,PAYLOAD:Member_ID,PAYLOAD:"Member ID",PAYLOAD:ID,PAYLOAD:"Unique ID")'
              if contract.feed=='MEMBER' else 'PAYLOAD:member_id')
        self.execute(f'''MERGE INTO {self.ns.sql}.MEMBER_IDENTITY_MAP m USING (
         SELECT DISTINCT SOURCE_SYSTEM,{self.ns.sql}.CLEAN_TEXT({expr}) AS SOURCE_MEMBER_ID,
          %s||':'||{self.ns.sql}.CLEAN_TEXT({expr}) AS GLOBAL_MEMBER_ID
         FROM {self.ns.sql}.{table} WHERE BATCH_ID=%s
          AND LENGTH({self.ns.sql}.CLEAN_TEXT({expr})) BETWEEN 1 AND 18
          AND TYPEOF({expr}) IN ('VARCHAR','INTEGER')
        ) s ON m.SOURCE_SYSTEM=s.SOURCE_SYSTEM AND m.SOURCE_MEMBER_ID=s.SOURCE_MEMBER_ID
        WHEN NOT MATCHED THEN INSERT (SOURCE_SYSTEM,SOURCE_MEMBER_ID,GLOBAL_MEMBER_ID,MAPPING_METHOD)
         VALUES (s.SOURCE_SYSTEM,s.SOURCE_MEMBER_ID,s.GLOBAL_MEMBER_ID,'DECLARED_GLOBAL_CONTRACT')''',
         (contract.identity_authority,bid))
        conflicts=self.scalar(f'''SELECT COUNT(*) FROM {self.ns.sql}.{table} r JOIN {self.ns.sql}.MEMBER_IDENTITY_MAP m
         ON r.SOURCE_SYSTEM=m.SOURCE_SYSTEM AND {self.ns.sql}.CLEAN_TEXT({expr})=m.SOURCE_MEMBER_ID
         WHERE r.BATCH_ID=%s AND m.GLOBAL_MEMBER_ID IS DISTINCT FROM (%s||':'||m.SOURCE_MEMBER_ID)''',(bid,contract.identity_authority))
        if conflicts: raise RuntimeError('Declared global contract conflicts with an existing mapping')

    def add_mappings(self,rows):
        with self.transaction():
            for row in rows:
                source,local,global_id=(row[k] for k in ('source_system','source_member_id','global_member_id'))
                if not re.fullmatch(r'[A-Z0-9_]{1,100}',source) or not isinstance(local,str) or not 1<=len(local)<=18 or not local.strip() or local!=local.strip() or not isinstance(global_id,str) or not 1<=len(global_id)<=100 or not global_id.strip() or global_id!=global_id.strip() or global_id.upper() in ('NULL','NONE'):
                    raise ValueError('Invalid reviewed identity mapping')
                old=self.execute(f'SELECT GLOBAL_MEMBER_ID FROM {self.ns.sql}.MEMBER_IDENTITY_MAP WHERE SOURCE_SYSTEM=%s AND SOURCE_MEMBER_ID=%s',(source,local))
                if old and (len(old)!=1 or old[0]['GLOBAL_MEMBER_ID']!=global_id):
                    raise RuntimeError('Identity rekey requires a reviewed migration, never overwrite')
                if not old:
                    self.execute(f"INSERT INTO {self.ns.sql}.MEMBER_IDENTITY_MAP (SOURCE_SYSTEM,SOURCE_MEMBER_ID,GLOBAL_MEMBER_ID,MAPPING_METHOD) VALUES (%s,%s,%s,'OWNER_VERIFIED_IMPORT')",(source,local,global_id))

    def metadata_fingerprint(self,batch):
        table='RAW_MEMBER_PROFILE' if batch['FEED_TYPE']=='MEMBER' else 'RAW_REDEMPTION_FEED'
        expr=('COALESCE(r.PAYLOAD:Member_Id,r.PAYLOAD:Member_ID,r.PAYLOAD:"Member ID",r.PAYLOAD:ID,r.PAYLOAD:"Unique ID")'
              if batch['FEED_TYPE']=='MEMBER' else 'r.PAYLOAD:member_id')
        sql=f'''SELECT HASH_AGG(m.SOURCE_SYSTEM,m.SOURCE_MEMBER_ID,m.GLOBAL_MEMBER_ID,m.MAPPING_METHOD
        {',c.GLOBAL_MEMBER_ID' if batch['FEED_TYPE']=='REDEMPTION' else ''}) AS H
        FROM (SELECT DISTINCT r.SOURCE_SYSTEM,{self.ns.sql}.CLEAN_TEXT({expr}) AS MID
          FROM {self.ns.sql}.{table} r WHERE r.BATCH_ID=%s) k
        LEFT JOIN {self.ns.sql}.MEMBER_IDENTITY_MAP m ON k.SOURCE_SYSTEM=m.SOURCE_SYSTEM AND k.MID=m.SOURCE_MEMBER_ID
        {f'LEFT JOIN {self.ns.sql}.MEMBER_CURRENT c ON m.GLOBAL_MEMBER_ID=c.GLOBAL_MEMBER_ID' if batch['FEED_TYPE']=='REDEMPTION' else ''}'''
        return str(self.scalar(sql,(batch['BATCH_ID'],)))

    def process(self,bid,as_of: date,fail_before_commit=False,retry_quarantine=False):
        if not re.fullmatch(r'[a-f0-9]{64}',bid): raise ValueError('Invalid batch ID')
        if fail_before_commit and not self.ns.is_test: raise ValueError('Failure injection requires isolated test schema')
        started=time.perf_counter()
        batch={}
        try:
            with self.transaction():
                batches=self.execute(f'SELECT * FROM {self.ns.sql}.INGEST_BATCH WHERE BATCH_ID=%s',(bid,))
                if len(batches)!=1: raise RuntimeError('Exactly one immutable ingested batch required')
                batch=batches[0]
                fingerprint=self.metadata_fingerprint(batch)
                nonce=int(self.scalar(f'SELECT COUNT(*) FROM {self.ns.sql}.PIPELINE_RUN WHERE BATCH_ID=%s',(bid,))) if retry_quarantine else 0
                run_id=digest(['NATIVE_V1',bid,as_of,fingerprint,nonce])
                previous=self.execute(f'SELECT STATUS FROM {self.ns.sql}.PIPELINE_RUN WHERE RUN_ID=%s',(run_id,))
                if previous:
                    if len(previous)!=1 or previous[0]['STATUS']!='COMPLETED': raise RuntimeError('Corrupt completed-run checkpoint')
                    self.record_attempt(bid,'TRANSFORM','REPLAY_CONFIRMED',source=batch['SOURCE_SYSTEM'],feed=batch['FEED_TYPE'])
                    return {'status':'SKIPPED','batch_id':bid,'run_id':run_id,'seconds':time.perf_counter()-started}
                latest=self.scalar(f'SELECT MAX(AS_OF_DATE) FROM {self.ns.sql}.PIPELINE_RUN')
                if latest and as_of<latest: raise ValueError('A new processing date cannot precede completed processing')
                self.execute('SET BATCH_ID=%s',(bid,)); self.execute('SET AS_OF=%s',(as_of,)); self.execute('SET RUN_ID=%s',(run_id,))
                if batch['FEED_TYPE']=='MEMBER':
                    self.script('11_native_member_stage.sql')
                    self.script('12_native_member_apply.sql')
                    self.sync_countries(bid)
                else:
                    self.script('13_native_redemption_stage.sql')
                    self.script('14_native_redemption_apply.sql')
                self.quarantine(batch)
                if fail_before_commit: raise InjectedFailure('Deliberate pre-commit failure')
                counts=self.audit(batch,run_id,as_of)
                self.verify(batch_id=bid)
                self.execute(f"UPDATE {self.ns.sql}.INGEST_BATCH SET STATUS='COMPLETED' WHERE BATCH_ID=%s",(bid,))
            self.attempt(bid,'TRANSFORM','COMPLETED',source=batch['SOURCE_SYSTEM'],feed=batch['FEED_TYPE'])
        except Exception as e:
            self.connection.rollback()
            self.attempt(bid,'TRANSFORM','FAILED',type(e).__name__,batch.get('SOURCE_SYSTEM'),batch.get('FEED_TYPE'))
            raise
        return {'status':'COMPLETED','batch_id':bid,'run_id':run_id,**counts,'seconds':time.perf_counter()-started}

    def sync_countries(self,bid):
        affected=f'(SELECT DISTINCT GLOBAL_MEMBER_ID FROM {self.ns.sql}.STG_MEMBER_PROFILE WHERE BATCH_ID=%s AND VALIDATION_STATUS=\'VALID\')'
        columns=','.join('c.'+k for k in MEMBER_COLUMNS)
        for country,table in COUNTRIES.items():
            match=("c.COUNTRY_CODE NOT IN ('IND','USA','AUS','CAN','PHL')" if country=='OTHER' else f"c.COUNTRY_CODE='{country}'")
            self.execute(f'''DELETE FROM {self.ns.sql}.{table} t USING {self.ns.sql}.MEMBER_CURRENT c,{affected} a
             WHERE t.GLOBAL_MEMBER_ID=c.GLOBAL_MEMBER_ID AND c.GLOBAL_MEMBER_ID=a.GLOBAL_MEMBER_ID AND NOT ({match})''',(bid,))
            self.execute(f'''MERGE INTO {self.ns.sql}.{table} t USING (
             SELECT {columns} FROM {self.ns.sql}.MEMBER_CURRENT c JOIN {affected} a ON c.GLOBAL_MEMBER_ID=a.GLOBAL_MEMBER_ID WHERE {match}
            ) s ON t.GLOBAL_MEMBER_ID=s.GLOBAL_MEMBER_ID
            WHEN MATCHED AND (t.FACT_HASH<>s.FACT_HASH OR t.AS_OF_DATE<>s.AS_OF_DATE OR t.WINNER_RAW_RECORD_ID<>s.WINNER_RAW_RECORD_ID) THEN UPDATE ALL BY NAME
            WHEN NOT MATCHED THEN INSERT ALL BY NAME''',(bid,))

    def quarantine(self,batch):
        bid,feed=batch['BATCH_ID'],batch['FEED_TYPE']
        self.execute(f'DELETE FROM {self.ns.sql}.REJECTED_RECORD WHERE BATCH_ID=%s',(bid,))
        source=(f"SELECT RAW_RECORD_ID AS RID,RAW_RECORD_ID,'MEMBER' AS KIND,-1 AS IDX,VALIDATION_ERRORS FROM {self.ns.sql}.STG_MEMBER_PROFILE WHERE BATCH_ID=%s AND VALIDATION_STATUS='REJECTED'"
                if feed=='MEMBER' else f"SELECT RECORD_ID AS RID,RAW_RECORD_ID,RECORD_KIND AS KIND,ITEM_INDEX AS IDX,VALIDATION_ERRORS FROM {self.ns.sql}.STG_REDEMPTION_RECORD WHERE BATCH_ID=%s AND VALIDATION_STATUS='REJECTED'")
        self.execute(f'''INSERT INTO {self.ns.sql}.REJECTED_RECORD
         (REJECTION_ID,BATCH_ID,RAW_RECORD_ID,RECORD_KIND,ITEM_INDEX,FEED_TYPE,ERROR_CODES)
         SELECT RID,%s,RAW_RECORD_ID,KIND,IDX,%s,VALIDATION_ERRORS FROM ({source})''',(bid,feed,bid))

    def audit(self,batch,run_id,as_of):
        bid,feed=batch['BATCH_ID'],batch['FEED_TYPE']
        if feed=='MEMBER':
            row=self.execute(f"SELECT COUNT(*) AS N,COUNT_IF(VALIDATION_STATUS='VALID') AS V FROM {self.ns.sql}.STG_MEMBER_PROFILE WHERE BATCH_ID=%s",(bid,))[0]
            winners=self.scalar(f'''SELECT COUNT(*) FROM {self.ns.sql}.MEMBER_CURRENT c JOIN {self.ns.sql}.STG_MEMBER_PROFILE s
              ON c.WINNER_RAW_RECORD_ID=s.RAW_RECORD_ID WHERE s.BATCH_ID=%s AND s.VALIDATION_STATUS='VALID' ''',(bid,))
            docs,reject_docs=0,0
        else:
            row=self.execute(f"SELECT COUNT(*) AS N,COUNT_IF(VALIDATION_STATUS='VALID') AS V FROM {self.ns.sql}.STG_REDEMPTION_RECORD WHERE BATCH_ID=%s AND RECORD_KIND='ITEM'",(bid,))[0]
            winners=self.scalar(f'''SELECT COUNT(*) FROM {self.ns.sql}.REDEMPTION_TRANSACTION t JOIN {self.ns.sql}.STG_REDEMPTION_RECORD s
             ON t.SOURCE_RECORD_ID=s.RECORD_ID WHERE s.BATCH_ID=%s AND s.RECORD_KIND='ITEM' AND s.VALIDATION_STATUS='VALID' ''',(bid,))
            doc=self.execute(f"SELECT COUNT(*) AS N,COUNT_IF(VALIDATION_STATUS='REJECTED') AS R FROM {self.ns.sql}.STG_REDEMPTION_RECORD WHERE BATCH_ID=%s AND RECORD_KIND='DOCUMENT'",(bid,))[0]
            docs,reject_docs=int(doc['N']),int(doc['R'] or 0)
        total,valid=int(row['N']),int(row['V'] or 0)
        self.execute(f'''INSERT INTO {self.ns.sql}.PIPELINE_RUN
        (RUN_ID,BATCH_ID,FEED_TYPE,AS_OF_DATE,STATUS,INPUT_ROWS,ACCEPTED_ROWS,REJECTED_ROWS,WINNING_ROWS,REPLAY_ROWS,DOCUMENT_ROWS,REJECTED_DOCUMENTS)
        VALUES (%s,%s,%s,%s,'COMPLETED',%s,%s,%s,%s,%s,%s,%s)''',
        (run_id,bid,feed,as_of,total,valid,total-valid,winners,valid-int(winners),docs,reject_docs))
        return {'input_rows':total,'accepted_rows':valid,'rejected_rows':total-valid,
                'winning_rows':int(winners),'replay_rows':valid-int(winners),'documents':docs,'rejected_documents':reject_docs}

    def verify(self,batch_id=None):
        self.execute('SET VERIFY_BATCH=%s',(batch_id or '',))
        sql=(ROOT/'sql/15_native_verify.sql').read_text().replace('{{ns}}',self.ns.sql)
        failures=[]
        for i,statement in enumerate(split_sql(sql),1):
            if self.scalar('SELECT COUNT(*) FROM ('+statement+')'): failures.append(i)
        if failures: raise RuntimeError('Native reconciliation violations: '+','.join(map(str,failures)))
        return {'status':'PASS','invariant_checks':len(split_sql(sql))}

    def health(self,sources,max_age_hours=26,max_reject_rate=0.05,max_transform_lag_hours=2):
        from src.check_pipeline import assess,policy,source_spec
        policy(max_age_hours,max_reject_rate,max_transform_lag_hours)
        if not sources or len(sources)!=len(set(sources)): raise ValueError('Unique expected sources required')
        checks=[]
        try:
            self.verify()
            checks.append({'name':'integrity','passed':True})
        except RuntimeError:
            checks.append({'name':'integrity','passed':False})
        now=datetime.now(timezone.utc)
        for spec in sources:
            feed,source=source_spec(spec)
            for phase in ('INGEST','TRANSFORM'):
                attempt=self.execute(f'''SELECT STATUS FROM {self.ns.sql}.PIPELINE_ATTEMPT
                 WHERE SOURCE_SYSTEM=%s AND FEED_TYPE=%s AND PHASE=%s
                 ORDER BY RECORDED_AT DESC,ATTEMPT_ID DESC LIMIT 1''',(source,feed,phase))
                checks.append({'name':spec+':latest_'+phase.lower()+'_attempt',
                               'passed':bool(attempt and attempt[0]['STATUS'] in ('COMPLETED','REPLAY_CONFIRMED'))})
            batches=self.execute(f'''SELECT BATCH_ID,STATUS,FINISHED_AT FROM {self.ns.sql}.INGEST_BATCH
             WHERE SOURCE_SYSTEM=%s AND FEED_TYPE=%s ORDER BY STARTED_AT DESC,BATCH_ID DESC LIMIT 1''',(source,feed))
            results=[]
            if batches:
                raw=batches[0]
                raw['STATUS']='COMPLETED' if raw['STATUS'] in ('LOADED','COMPLETED') else raw['STATUS']
                results=self.execute(f'''SELECT STATUS,COMPLETED_AT,INPUT_ROWS AS ITEM_ROWS,REJECTED_ROWS AS REJECTED_ITEMS,
                 DOCUMENT_ROWS,REJECTED_DOCUMENTS FROM {self.ns.sql}.PIPELINE_RUN
                 WHERE BATCH_ID=%s ORDER BY COMPLETED_AT DESC,RUN_ID DESC LIMIT 1''',(raw['BATCH_ID'],))
                if feed=='MEMBER':
                    for row in results:
                        row.pop('DOCUMENT_ROWS'); row.pop('REJECTED_DOCUMENTS')
            checks.extend(dict(x,name=spec+':'+x['name']) for x in assess(batches,results,now,max_age_hours,max_reject_rate,max_transform_lag_hours))
        return {'status':'PASS' if all(c['passed'] for c in checks) else 'FAIL','checked_at':now.isoformat(),
                'failed_checks':sum(not c['passed'] for c in checks),'checks':checks}


def connection_and_namespace(schema):
    from dotenv import load_dotenv
    load_dotenv()
    connection=connect_snowflake()
    cur=connection.cursor()
    try:
        cur.execute('SELECT CURRENT_DATABASE()'); database=cur.fetchone()[0]
        cur.execute("ALTER SESSION SET QUERY_TAG='SKYPOINTS_ASSESSMENT_NATIVE', LOCK_TIMEOUT=30, ERROR_ON_NONDETERMINISTIC_MERGE=TRUE")
    finally: cur.close()
    try: return connection,Namespace(database,schema)
    except Exception:
        connection.close(); raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--schema',default='SKYPOINTS_DEV')
    sub=parser.add_subparsers(dest='command',required=True)
    sub.add_parser('deploy'); sub.add_parser('verify')
    health=sub.add_parser('health'); health.add_argument('--source',action='append',required=True)
    health.add_argument('--max-age-hours',type=float,default=26)
    health.add_argument('--max-reject-rate',type=float,default=0.05)
    health.add_argument('--max-transform-lag-hours',type=float,default=2)
    ingest=sub.add_parser('ingest')
    ingest.add_argument('--file',type=Path,required=True)
    ingest.add_argument('--feed',choices=('MEMBER','REDEMPTION'),required=True)
    ingest.add_argument('--source',required=True)
    ingest.add_argument('--format',choices=('PDF','EXCEL_ISO','EXCEL_US','JSON'),required=True)
    ingest.add_argument('--snapshot-ts',type=datetime.fromisoformat,required=True)
    ingest.add_argument('--as-of',type=date.fromisoformat,required=True)
    ingest.add_argument('--identity-scope',choices=('LOCAL','GLOBAL'),required=True)
    ingest.add_argument('--identity-authority',default='')
    ingest.add_argument('--dob-format',choices=('DDMMYYYY','MMDDYYYY','YYYYMMDD'),help='Required explicit field-level contract for member feeds')
    ingest.add_argument('--sequence',type=int,default=0)
    ingest.add_argument('--country')
    ingest.add_argument('--allow-transaction-updates',action='store_true')
    ingest.add_argument('--allow-duplicate-names',action='store_true',help='Explicit contract deviation, never an identity merge')
    ingest.add_argument('--part-bytes',type=int,default=64*1024*1024)
    proc=sub.add_parser('process'); proc.add_argument('--batch-id',required=True)
    proc.add_argument('--as-of',required=True,type=date.fromisoformat)
    proc.add_argument('--retry-quarantine',action='store_true')
    mapping=sub.add_parser('map'); mapping.add_argument('--file',type=Path,required=True)
    args=parser.parse_args()
    if args.command=='ingest' and args.feed=='MEMBER' and args.dob_format is None:
        parser.error('Member ingestion requires explicit --dob-format; do not guess compact DOB values')
    # Validate schema before opening a connection or touching any environment.
    Namespace('VALIDATE',args.schema)
    connection=None
    try:
        connection,ns=connection_and_namespace(args.schema)
        engine=Pipeline(connection,ns)
        if args.command=='deploy': result=engine.deploy()
        elif args.command=='verify': result=engine.verify()
        elif args.command=='health': result=engine.health(args.source,args.max_age_hours,args.max_reject_rate,args.max_transform_lag_hours)
        elif args.command=='map':
            engine.add_mappings(json.loads(args.file.read_text(encoding='utf-8')))
            result={'status':'MAPPINGS_IMPORTED'}
        elif args.command=='process': result=engine.process(args.batch_id,args.as_of,retry_quarantine=args.retry_quarantine)
        else:
            contract=Contract(args.source,args.feed,args.snapshot_ts,args.format,args.dob_format or 'DDMMYYYY',
                              args.identity_scope,args.identity_authority,args.sequence,
                              not args.allow_duplicate_names,args.allow_transaction_updates,args.country)
            result=engine.ingest(args.file,contract,args.as_of,args.part_bytes)
        print(json.dumps(result,default=str))
        if result.get('status')=='FAIL': raise SystemExit(1)
    except Exception as exc:
        print(json.dumps({'status':'ERROR','error_category':type(exc).__name__,
                          'errno':getattr(exc,'errno',None),'sqlstate':getattr(exc,'sqlstate',None),
                          'message':str(exc) if type(exc) in (ValueError,RuntimeError,AssertionError) else 'Inspect the query ID in secured warehouse logs',
                          'query_id':getattr(exc,'sfqid',None)}))
        raise SystemExit(1) from None
    finally:
        if connection: connection.close()


if __name__=='__main__': main()
