"""Measure executed synthetic workloads, never extrapolate them as proven capacity."""
import argparse
from datetime import date,datetime,timedelta
import hashlib
import json
from pathlib import Path
import tempfile
import time
import tracemalloc
import uuid
from dataclasses import replace
from src.pipeline import Pipeline,Contract,connection_and_namespace,ROOT
from src.demo import HEADER,member,write_members


def benchmark(engine,rows,as_of):
    if not engine.ns.is_test or not engine.created_test_schema:
        raise ValueError('Benchmarks require a fresh owned isolated schema')
    contract=Contract('BENCHMARK_MEMBERS','MEMBER',datetime.combine(as_of-timedelta(days=1),datetime.min.time()),
                      'PDF','YYYYMMDD','GLOBAL','BENCHMARK_SYNTHETIC')
    def code_hashes():
        return {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
                for directory in ('src','sql') for p in sorted((ROOT/directory).iterdir()) if p.suffix in ('.py','.sql')}
    initial_hashes=code_hashes()
    before=time.perf_counter()
    tracemalloc.start()
    with tempfile.TemporaryDirectory(prefix='skypoints-benchmark-') as directory:
        path=Path(directory)/'benchmark.txt'
        countries=('IND','USA','AU','CAN','PHIL')
        def records():
            for i in range(rows):
                yield member(f'B{i:012d}',f'Benchmark Member {i}',countries[i%5],
                             Enrollment_Date='20200101',Last_Flight_Date=(as_of-timedelta(days=30)).strftime('%Y%m%d'),DOB='20000101')
        write_members(path,records())
        generated_seconds=time.perf_counter()-before
        loaded=engine.ingest(path,contract,as_of)
        processed=engine.process(loaded['batch_id'],as_of)
        count=int(engine.scalar(f'SELECT COUNT(*) FROM {engine.ns.sql}.MEMBER_CURRENT'))
        if count!=rows or processed['accepted_rows']!=rows: raise AssertionError('Benchmark did not reconcile')
        update=Path(directory)/'incremental.txt'
        write_members(update,[member('B000000000000','Benchmark Member 0','CAN',Enrollment_Date='20200101',
                                   Last_Flight_Date=(as_of-timedelta(days=30)).strftime('%Y%m%d'),DOB='20000101')])
        incremental_raw=engine.ingest(update,replace(contract,snapshot_ts=datetime.combine(as_of,datetime.min.time())),as_of)
        incremental=engine.process(incremental_raw['batch_id'],as_of)
        if int(engine.scalar(f'SELECT COUNT(*) FROM {engine.ns.sql}.MEMBER_CURRENT'))!=rows:
            raise AssertionError('Incremental update changed member cardinality')
        engine.verify()
    _,peak=tracemalloc.get_traced_memory(); tracemalloc.stop()
    warehouse=engine.scalar('SELECT CURRENT_WAREHOUSE()')
    warehouse_size=next((r.get('size') for r in engine.execute('SHOW WAREHOUSES') if r.get('name')==warehouse),None)
    hashes=code_hashes()
    if hashes!=initial_hashes: raise RuntimeError('Code changed during benchmark; rerun a frozen revision')
    elapsed=loaded['seconds']+processed['seconds']
    return {'status':'PASS','rows':rows,'as_of':str(as_of),'schema':engine.ns.schema,
            'warehouse_size':warehouse_size,'generator_seconds':generated_seconds,'bulk_ingestion':loaded,
            'set_based_transform':processed,'measured_load_transform_seconds':elapsed,
            'observed_rows_per_second':rows/elapsed,'incremental_one_row_ingestion':incremental_raw,
            'incremental_one_row_transform':incremental,'python_tracemalloc_peak_bytes':peak,
            'memory_scope':'Python allocator during workload; not Snowflake memory or process RSS',
            'query_count':len(engine.query_ids),'query_ids':engine.query_ids,'code_sha256':hashes,
            'limitations':'Single small synthetic workload, one warehouse, no sustained or billion-row throughput claim'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows',type=int,default=1000)
    parser.add_argument('--as-of',type=date.fromisoformat,default=date(2026,11,18))
    parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--keep-schema',action='store_true')
    args=parser.parse_args()
    if not 1<=args.rows<=1_000_000: parser.error('Use 1..1,000,000 rows for this bounded benchmark driver')
    connection=None; engine=None
    try:
        connection,ns=connection_and_namespace('SKYPOINTS_TEST_'+uuid.uuid4().hex[:12].upper())
        engine=Pipeline(connection,ns); engine.deploy()
        result=benchmark(engine,args.rows,args.as_of)
        result['schema_retained']=args.keep_schema
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(json.dumps(result,indent=2,default=str),encoding='utf-8')
        print(json.dumps({k:result[k] for k in ('status','rows','warehouse_size','measured_load_transform_seconds','observed_rows_per_second','python_tracemalloc_peak_bytes')}))
    except Exception as exc:
        print(json.dumps({'status':'FAIL','error_category':type(exc).__name__,'errno':getattr(exc,'errno',None)}))
        raise SystemExit(1) from None
    finally:
        if engine and engine.created_test_schema and not args.keep_schema: engine.drop_owned_test_schema()
        if connection: connection.close()


if __name__=='__main__': main()
