"""Phase 3: ingest source Excel workbooks and optional redemption JSON to Snowflake RAW.

Usage: python -m src.ingest_raw --data-dir data/samples --feed members
       python -m src.ingest_raw --feed redemptions --json-file data/samples/redemptions/example.json
       python -m src.ingest_raw --feed members --dry-run

Small-sample adapter: performs transactional row inserts. At production volume,
replace this insert loop with staged Parquet/JSON and Snowflake COPY INTO.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from openpyxl import load_workbook

COUNTRY_FILES = {"IND.xlsx": "IND", "USA.xlsx": "USA", "AUS.xlsx": "AUS"}


def _jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def batch_id(source_system: str, feed_type: str, digest: str) -> str:
    return hashlib.sha256(f"{source_system}|{feed_type}|{digest}".encode()).hexdigest()


def raw_id(batch: str, row: int) -> str:
    return hashlib.sha256(f"{batch}|{row}".encode()).hexdigest()


def workbook_rows(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """Stream sheet rows. Preserve source headers and data, including invalid dates."""
    book = load_workbook(path, read_only=True, data_only=True)
    try:
        rows = book.active.iter_rows(values_only=True)
        headers_row = next(rows, None)
        if headers_row is None:
            raise ValueError(f"Empty workbook: {path}")
        headers = [str(value).strip() if value is not None else f"UNNAMED_{i}"
                   for i, value in enumerate(headers_row, 1)]
        if len(set(headers)) != len(headers):
            raise ValueError(f"Repeated column headers: {path}")
        for idx, row in enumerate(rows, start=2):
            if any(value is not None for value in row):
                yield idx, {headers[i]: _jsonable(value) for i, value in enumerate(row)}
    finally:
        book.close()


def redemption_rows(path: Path) -> Iterator[tuple[int, Any]]:
    """Accept one JSON document, a JSON array, or JSON-lines files."""
    if path.suffix.lower() == ".jsonl":
        with path.open(encoding="utf-8") as f:
            for line_number, line in enumerate(f, 1):
                if line.strip():
                    yield line_number, json.loads(line)
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    for line_number, doc in enumerate(payload if isinstance(payload, list) else [payload], 1):
        yield line_number, doc


def records(path: Path, feed_type: str):
    source_system = f"SKYPOINTS_{COUNTRY_FILES[path.name]}" if feed_type == "MEMBER" else "SKYPOINTS_REDEMPTIONS"
    country = COUNTRY_FILES[path.name] if feed_type == "MEMBER" else None
    digest = file_hash(path)
    bid = batch_id(source_system, feed_type, digest)
    iterator = workbook_rows(path) if feed_type == "MEMBER" else redemption_rows(path)
    for row_number, payload in iterator:
        yield {"raw_record_id": raw_id(bid, row_number), "batch_id": bid,
               "source_system": source_system, "source_file": path.name,
               "source_row_number": row_number, "source_country": country,
               "payload": payload}


def connect_snowflake():
    try:
        import snowflake.connector
    except ImportError as error:
        raise RuntimeError("Install dependencies: python -m pip install -r requirements.txt") from error
    required = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Missing Snowflake environment variables: {', '.join(missing)}")
    return snowflake.connector.connect(account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"], password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"], database=os.environ["SNOWFLAKE_DATABASE"],
        schema="SKYPOINTS", autocommit=False)


def load_file(connection, path: Path, feed_type: str) -> tuple[str, int]:
    source_system = f"SKYPOINTS_{COUNTRY_FILES[path.name]}" if feed_type == "MEMBER" else "SKYPOINTS_REDEMPTIONS"
    digest = file_hash(path)
    bid = batch_id(source_system, feed_type, digest)
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT STATUS FROM SKYPOINTS.INGEST_BATCH WHERE BATCH_ID = %s", (bid,))
        previous = cursor.fetchone()
        if previous and previous[0] == "COMPLETED":
            return "SKIPPED_ALREADY_LOADED", 0
        if previous:
            # No attempt to overwrite an interrupted batch without explicit recovery.
            raise RuntimeError(f"Batch {bid} already exists with status {previous[0]}; investigate before retry")
        cursor.execute("""INSERT INTO SKYPOINTS.INGEST_BATCH
            (BATCH_ID, SOURCE_SYSTEM, SOURCE_FILE, FILE_SHA256, FEED_TYPE, STATUS, INPUT_ROWS)
            VALUES (%s, %s, %s, %s, %s, 'STARTED', 0)""",
            (bid, source_system, path.name, digest, feed_type))
        count = 0
        for item in records(path, feed_type):
            encoded = json.dumps(item["payload"], ensure_ascii=False, allow_nan=False)
            if feed_type == "MEMBER":
                cursor.execute("""INSERT INTO SKYPOINTS.RAW_MEMBER_PROFILE
                    (RAW_RECORD_ID, BATCH_ID, SOURCE_SYSTEM, SOURCE_FILE,
                     SOURCE_ROW_NUMBER, SOURCE_COUNTRY, PAYLOAD)
                    SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)""",
                    (item["raw_record_id"], bid, source_system, path.name,
                     item["source_row_number"], item["source_country"], encoded))
            else:
                cursor.execute("""INSERT INTO SKYPOINTS.RAW_REDEMPTION_FEED
                    (RAW_RECORD_ID, BATCH_ID, SOURCE_SYSTEM, SOURCE_FILE,
                     SOURCE_ROW_NUMBER, PAYLOAD)
                    SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s)""",
                    (item["raw_record_id"], bid, source_system, path.name,
                     item["source_row_number"], encoded))
            count += 1
        cursor.execute("""UPDATE SKYPOINTS.INGEST_BATCH SET STATUS='COMPLETED',
            INPUT_ROWS=%s, ACCEPTED_ROWS=%s, FINISHED_AT=CURRENT_TIMESTAMP()
            WHERE BATCH_ID=%s""", (count, count, bid))
        connection.commit()
        return "LOADED", count
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def main():
    from dotenv import load_dotenv
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feed", choices=("members", "redemptions"), default="members")
    parser.add_argument("--data-dir", type=Path, default=Path("data/samples"))
    parser.add_argument("--json-file", type=Path)
    parser.add_argument("--dry-run", action="store_true", help="Read and summarize without Snowflake access")
    args = parser.parse_args()
    if args.feed == "members":
        paths = [args.data_dir / name for name in COUNTRY_FILES]
        feed_type = "MEMBER"
    else:
        if not args.json_file:
            parser.error("--json-file is required for redemption ingestion")
        paths, feed_type = [args.json_file], "REDEMPTION"
    for path in paths:
        if not path.is_file():
            parser.error(f"Missing source file: {path}")
    if args.dry_run:
        for path in paths:
            rows = list(records(path, feed_type))
            print(f"DRY RUN {path.name}: {len(rows)} records; batch={rows[0]['batch_id'] if rows else batch_id('SKYPOINTS_REDEMPTIONS', feed_type, file_hash(path))}")
        return
    connection = connect_snowflake()
    try:
        for path in paths:
            status, count = load_file(connection, path, feed_type)
            print(f"{path.name}: {status}; inserted={count}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
