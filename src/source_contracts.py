"""Bounded source adapters and immutable disk snapshots; no Snowflake access."""
from __future__ import annotations

from contextlib import contextmanager
import csv
from dataclasses import dataclass
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
from typing import Iterator

ALIASES = {
    'ID': ('Member_Id', 'Member_ID', 'Member ID', 'ID', 'Unique ID'),
    'Name': ('Member_Name', 'Member Name', 'Name'),
    'EnrollmentDate': ('Enrollment_Date', 'EnrollmentDate', 'Date of Enrollment'),
    'FlightDate': ('Last_Flight_Date', 'FlightDate', 'Flight Date', 'Date of Flight'),
    'TierCode': ('Tier_Code', 'TierCode', 'Tier Type'),
    'AgentName': ('Agent_Name', 'AgentName'),
    'StateCode': ('State', 'StateCode'),
    'Country': ('Country', 'CountryCode'),
    'PostCode': ('Post_Code', 'Postcode', 'Post Code', 'PostCode'),
    'DOB': ('DOB', 'Date_of_Birth', 'Date of Birth'),
    'IsActive': ('Is_Active', 'Active Member', 'IsActive'),
}
KNOWN_HEADERS = {name for values in ALIASES.values() for name in values}
KNOWN_HEADERS.update(('Source_Update_Ts', 'Source_Sequence', 'Individual or Corporate'))


def normalize_member_payload(payload):
    result = dict(payload)
    for target, aliases in ALIASES.items():
        result[target] = next((payload[k] for k in aliases if payload.get(k) not in (None, '')), None)
    return result


@dataclass(frozen=True)
class Snapshot:
    path: Path
    sha256: str
    size: int
    original_name: str


@contextmanager
def snapshot_file(path: Path):
    """Hash exactly the immutable bytes subsequently parsed, with bounded RAM."""
    path = Path(path)
    with tempfile.TemporaryDirectory(prefix='skypoints-snapshot-') as directory:
        target = Path(directory) / path.name
        hasher, size = hashlib.sha256(), 0
        with path.open('rb') as source, target.open('xb') as output:
            for block in iter(lambda: source.read(1024 * 1024), b''):
                hasher.update(block)
                output.write(block)
                size += len(block)
        yield Snapshot(target, hasher.hexdigest(), size, path.name)


def flat_file_rows(path: Path) -> Iterator[tuple[int, dict]]:
    """Read the PDF H/D layout. Header, not the inconsistent position table, wins.

    Bad detail counts/markers become quarantinable RAW objects. An ambiguous
    header fails ingestion, since assigning its fields would invent semantics.
    """
    with Path(path).open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.reader(stream, delimiter='|', strict=True)
        header = None
        for number, cells in enumerate(reader, 1):
            if not any(v.strip() for v in cells):
                continue
            if cells and cells[0] == '':
                cells = cells[1:]
            marker, values = cells[0].strip(), cells[1:]
            if header is None:
                if marker != 'H':
                    raise ValueError('Member feed must begin with an H header')
                header = [v.strip() for v in values]
                if len(header) != len(set(header)) or any(not v for v in header):
                    raise ValueError('Duplicate or empty member header')
                if any(sum(k in header for k in aliases)>1 for aliases in ALIASES.values()):
                    raise ValueError('Ambiguous duplicate aliases in member header')
                normalized = normalize_member_payload(dict.fromkeys(header, 'present'))
                if any(normalized[k] is None for k in ('ID', 'Name', 'EnrollmentDate')):
                    raise ValueError('Missing required member header')
                if set(header) - KNOWN_HEADERS:
                    raise ValueError('Unknown member header; explicit schema contract required')
                continue
            errors = []
            if marker != 'D':
                errors.append('INVALID_DETAIL_MARKER')
            if len(values) != len(header):
                errors.append('DETAIL_FIELD_COUNT_MISMATCH')
            payload = {k: values[i] if i < len(values) else None for i, k in enumerate(header)}
            if errors:
                payload['_parse_errors'] = errors
                payload['_source_values'] = values
            yield number, payload
        if header is None:
            raise ValueError('Empty member feed')


def json_documents(path: Path, max_document_chars=8 * 1024 * 1024):
    """Stream JSONL or a top-level JSON array; bound each document's memory.

    JSON syntax failures abort the file. Semantic invalidity is handled in SQL.
    A single JSON document is bounded; use JSONL for high-volume transport.
    """
    path = Path(path)
    def unique_object(pairs):
        result={}
        for key,value in pairs:
            if key in result: raise ValueError('Duplicate JSON object key')
            result[key]=value
        return result
    def invalid_constant(value):
        raise ValueError('Nonfinite JSON number is not allowed')
    def decode(value):
        return json.loads(value,object_pairs_hook=unique_object,parse_constant=invalid_constant)
    def finish(buffer,stream):
        if buffer.strip() or any(block.strip() for block in iter(lambda:stream.read(65536),'')):
            raise ValueError('Data after JSON array')
    if path.suffix.lower() == '.jsonl':
        with path.open(encoding='utf-8-sig') as stream:
            number = 0
            while True:
                line = stream.readline(max_document_chars + 1)
                if not line:
                    break
                number += 1
                if len(line) > max_document_chars:
                    raise ValueError('JSON document exceeds configured bound')
                if line.strip():
                    yield number, decode(line)
        return
    with path.open(encoding='utf-8-sig') as stream:
        first = ''
        while not first:
            char = stream.read(1)
            if not char:
                raise ValueError('Empty JSON feed')
            first = char.strip()
        if first != '[':
            value = first + stream.read(max_document_chars + 1)
            if len(value) > max_document_chars:
                raise ValueError('JSON document exceeds configured bound')
            yield 1, decode(value)
            return
        decoder, buffer, eof, number = json.JSONDecoder(object_pairs_hook=unique_object,parse_constant=invalid_constant), '', False, 0
        expect_value, allow_end = True, True
        while True:
            buffer = buffer.lstrip()
            if not buffer and not eof:
                block = stream.read(65536)
                buffer += block
                eof = not block
                continue
            if expect_value:
                if buffer.startswith(']') and allow_end:
                    finish(buffer[1:],stream)
                    return
                try:
                    value, end = decoder.raw_decode(buffer)
                except ValueError:
                    if eof or len(buffer) > max_document_chars:
                        raise ValueError('Invalid or oversized JSON array document') from None
                    block = stream.read(65536)
                    buffer += block
                    eof = not block
                    continue
                # A scalar token may end at a chunk boundary. Read ahead before
                # deciding where its delimiter is; never split 123 into 12,3.
                if end == len(buffer) and not eof:
                    block = stream.read(65536)
                    buffer += block
                    eof = not block
                    continue
                tail=buffer[end:].lstrip()
                if tail and tail[0] not in ',]' and not eof:
                    block=stream.read(65536)
                    buffer+=block
                    eof=not block
                    continue
                if end > max_document_chars:
                    raise ValueError('JSON document exceeds configured bound')
                number += 1
                yield number, value
                buffer, expect_value = buffer[end:], False
            elif buffer.startswith(','):
                buffer, expect_value, allow_end = buffer[1:], True, False
            elif buffer.startswith(']'):
                finish(buffer[1:],stream)
                return
            elif not buffer and not eof:
                continue
            else:
                raise ValueError('Expected JSON array separator')


def write_parts(rows, directory: Path, part_bytes=64 * 1024 * 1024):
    """Write deterministic gzipped NDJSON parts. Limit is uncompressed bytes."""
    if part_bytes < 1024:
        raise ValueError('part_bytes must be at least 1024')
    paths, count, used, output = [], 0, 0, None
    try:
        for row in rows:
            line = (json.dumps(row, ensure_ascii=False, allow_nan=False, separators=(',', ':')) + '\n').encode('utf-8')
            if output is None or (used and used + len(line) > part_bytes):
                if output:
                    output.close()
                path = Path(directory) / f'part-{len(paths):05d}.json.gz'
                paths.append(path)
                output = gzip.GzipFile(filename=str(path), mode='wb', mtime=0)
                used = 0
            output.write(line)
            used += len(line)
            count += 1
    finally:
        if output:
            output.close()
    return paths, count
