"""Profile SkyPoints country workbooks while preserving source values.

Usage: python src/profile_inputs.py --data-dir data --output profiling_report.json
Requires: pip install openpyxl
"""
import argparse
import json
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from openpyxl import load_workbook

FILES = ('IND.xlsx', 'USA.xlsx', 'AUS.xlsx')

def profile_file(path: Path):
    if not path.is_file():
        raise FileNotFoundError(f'Missing input: {path}')
    book = load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = book.active
        rows = list(sheet.iter_rows(values_only=True))
        if not rows:
            raise ValueError(f'Workbook has no rows: {path}')
        headers = [str(v).strip() if v is not None else f'UNNAMED_{i}' for i,v in enumerate(rows[0], start=1)]
        records = [dict(zip(headers, row)) for row in rows[1:] if any(v is not None for v in row)]
        id_col = 'Unique ID' if 'Unique ID' in headers else 'ID'
        if id_col not in headers:
            raise ValueError(f'No identifier column in {path.name}: {headers}')
        ids = [str(rec[id_col]) for rec in records if rec.get(id_col) is not None]
        issues=[]
        for line, rec in enumerate(records, start=2):
            dob_key = next((k for k in ('DOB','Date of Birth') if k in headers), None)
            if dob_key is None:
                issues.append({'row':line,'type':'MISSING_COLUMN','field':'DOB'})
            elif rec.get(dob_key) in (None,'','NULL'):
                issues.append({'row':line,'type':'MISSING_VALUE','field':dob_key})
            enrollment = rec.get('Date of Enrollment',rec.get('EnrollmentDate'))
            if isinstance(enrollment,str):
                try:
                    datetime.strptime(enrollment,'%Y-%m-%d')
                except ValueError:
                    issues.append({'row':line,'type':'INVALID_DATE','field':'enrollment_date','value':enrollment})
        duplicates = [member for member,count in Counter(ids).items() if count>1]
        return {'file':path.name,'sheet':sheet.title,'columns':headers,'record_count':len(records),
                'ids':ids,'duplicate_ids_within_file':duplicates,'issues':issues,'records':records}
    finally:
        book.close()

def run(data_dir: Path):
    profiles=[profile_file(data_dir / name) for name in FILES]
    owner={}
    collisions=[]
    for profile in profiles:
        for member_id in profile['ids']:
            if member_id in owner:
                collisions.append({'id':member_id,'files':[owner[member_id],profile['file']]})
            else:
                owner[member_id]=profile['file']
    return {'profiles':profiles,'cross_file_id_collisions':collisions,
            'warning':'IDs may be country-local; do not merge on ID alone without verified global identity.'}

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--data-dir',type=Path,default=Path('data'))
    parser.add_argument('--output',type=Path,default=Path('profiling_report.json'))
    args=parser.parse_args()
    report=run(args.data_dir)
    args.output.write_text(json.dumps(report,indent=2,default=lambda x:x.isoformat() if isinstance(x,(datetime,date)) else str(x)),encoding='utf-8')
    print(f"Profiled {sum(p['record_count'] for p in report['profiles'])} records; report: {args.output}")
    for p in report['profiles']:
        print(f"  {p['file']}: {p['record_count']} records, {len(p['issues'])} issues")
    print(f"Cross-file ID collisions: {len(report['cross_file_id_collisions'])}")
