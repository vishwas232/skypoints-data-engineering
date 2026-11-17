import json
import tempfile
import unittest
from pathlib import Path
from src.ingest_raw import (batch_id, file_hash, raw_id, records, redemption_rows, workbook_rows)

ROOT = Path(__file__).resolve().parents[1]


class IngestRawTests(unittest.TestCase):
    def test_member_files_preserve_rows_and_source(self):
        for filename, country in (("IND.xlsx", "IND"), ("USA.xlsx", "USA"), ("AUS.xlsx", "AUS")):
            items = list(records(ROOT / 'data/samples' / filename, 'MEMBER'))
            self.assertEqual(3, len(items))
            self.assertEqual({country}, {item['source_country'] for item in items})
            self.assertEqual(3, len({item['raw_record_id'] for item in items}))

    def test_invalid_aus_date_is_preserved_in_raw(self):
        items = list(records(ROOT / 'data/samples/AUS.xlsx', 'MEMBER'))
        self.assertTrue(any(item['payload'].get('Date of Enrollment') == '2021-13-13' for item in items))

    def test_batch_hash_is_deterministic(self):
        path = ROOT / 'data/samples/IND.xlsx'
        digest = file_hash(path)
        self.assertEqual(batch_id('SKYPOINTS_IND', 'MEMBER', digest), batch_id('SKYPOINTS_IND', 'MEMBER', digest))
        self.assertNotEqual(raw_id('a', 1), raw_id('a', 2))

    def test_redemption_feed(self):
        path = ROOT / 'data/samples/redemptions/example.json'
        parsed = list(redemption_rows(path))
        self.assertEqual(1, len(parsed))
        self.assertEqual(2, len(parsed[0][1]['redemptions']))

    def test_json_lines(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory) / 'feed.jsonl'
            p.write_text('{"a":1}\n{"a":2}\n', encoding='utf-8')
            self.assertEqual([1, 2], [value['a'] for _, value in redemption_rows(p)])


if __name__ == '__main__':
    unittest.main()
