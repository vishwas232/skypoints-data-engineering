"""Compatibility entry point for isolated, file-based warehouse acceptance.

There is no SQL rewriting or temporary-table shadowing. Every run creates a
fresh owned SKYPOINTS_TEST_<uuid> schema through the native pipeline.
"""
import argparse
from datetime import date
import json
from pathlib import Path
import uuid
from src.pipeline import Pipeline, connection_and_namespace
from src.demo import run_demo


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true',required=True)
    parser.add_argument('--as-of',type=date.fromisoformat,default=date(2026,11,18))
    parser.add_argument('--keep-schema',action='store_true')
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
        print(json.dumps({'status':'PASS','schema':ns.schema,'scenarios':len(report['scenarios'])}))
    except Exception as exc:
        print(json.dumps({'status':'FAIL','schema':engine.ns.schema if engine else None,
                          'error_category':type(exc).__name__,'errno':getattr(exc,'errno',None),
                          'assertion':str(exc) if isinstance(exc,AssertionError) else None}))
        raise SystemExit(1) from None
    finally:
        if engine and engine.created_test_schema and not args.keep_schema:
            engine.drop_owned_test_schema()
        if connection: connection.close()


if __name__=='__main__': main()
