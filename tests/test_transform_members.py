import copy
from datetime import date, datetime, timedelta
import unittest

from src.transform_members import transform, winners, run, parse_date, unique

AS_OF = date(2024, 3, 1)


def raw(rid='a', source='SKYPOINTS_IND', **payload):
    return dict(RAW_RECORD_ID=rid, BATCH_ID='batch', SOURCE_SYSTEM=source,
                SOURCE_COUNTRY='IND', SOURCE_UPDATE_TS=datetime(2024, 1, 1),
                SOURCE_SEQUENCE=0, INGESTED_AT=datetime(2024, 1, 2),
                PAYLOAD={'ID': 1, 'Name': 'Example', 'DOB': '2000-03-02',
                         'EnrollmentDate': '2020-01-01', **payload})


MAP = {('SKYPOINTS_IND', '1'): ['global-ind'], ('SKYPOINTS_USA', '1'): ['global-usa']}


class TransformTests(unittest.TestCase):
    def test_date_formats_and_invalid_dates(self):
        self.assertEqual(date(2022, 6, 15), parse_date(6152022, 'SKYPOINTS_USA'))
        self.assertEqual(date(2022, 1, 5), parse_date(1052022, 'SKYPOINTS_USA'))
        self.assertEqual(date(2022, 5, 11), parse_date('2022-05-11T00:00:00', 'SKYPOINTS_AUS'))
        for value in ('2021-13-13', '2023-02-29', '01/02/2022', '2022-01-01garbage'):
            with self.assertRaises(ValueError):
                parse_date(value, 'SKYPOINTS_IND')

    def test_age_birthday_and_leap_day(self):
        self.assertEqual(23, transform(raw(), MAP, AS_OF)['AGE'])
        self.assertEqual(24, transform(raw(), MAP, date(2024, 3, 2))['AGE'])
        leap = raw(DOB='2000-02-29')
        self.assertEqual(22, transform(leap, MAP, date(2023, 2, 28))['AGE'])
        self.assertEqual(23, transform(leap, MAP, date(2023, 3, 1))['AGE'])

    def test_stale_boundary_and_unknown(self):
        for days, expected in ((89, False), (90, False), (91, True)):
            row = transform(raw(FlightDate=(AS_OF-timedelta(days=days)).isoformat()), MAP, AS_OF)
            self.assertIs(expected, row['STALE_MEMBER'])
        self.assertIsNone(transform(raw(), MAP, AS_OF)['STALE_MEMBER'])

    def test_null_dob_is_optional_but_malformed_is_rejected(self):
        self.assertIsNone(transform(raw(DOB='NULL'), MAP, AS_OF)['AGE'])
        self.assertIn('INVALID_DOB', transform(raw(DOB='bad'), MAP, AS_OF)['VALIDATION_ERRORS'])

    def test_required_future_and_chronology(self):
        for payload, code in [({'Name': ''}, 'MISSING_OR_INVALID_MEMBER_NAME'),
                              ({'EnrollmentDate': ''}, 'MISSING_ENROLLMENT_DATE'),
                              ({'DOB': '2025-01-01'}, 'FUTURE_DOB'),
                              ({'FlightDate': '2019-01-01'}, 'FLIGHT_BEFORE_ENROLLMENT'),
                              ({'Name': 'x'*256}, 'TOO_LONG_MEMBER_NAME')]:
            self.assertIn(code, transform(raw(**payload), MAP, AS_OF)['VALIDATION_ERRORS'])

    def test_identity_collision_and_ambiguity(self):
        ind = transform(raw(), MAP, AS_OF)
        usa = transform(raw('b', 'SKYPOINTS_USA'), MAP, AS_OF)
        self.assertEqual(2, len(winners([ind, usa])))
        for mapping in ({}, {('SKYPOINTS_IND', '1'): ['x', 'x']}):
            self.assertEqual('REJECTED', transform(raw(), mapping, AS_OF)['VALIDATION_STATUS'])

    def test_source_version_required_and_approved_fallback(self):
        row = raw()
        row['SOURCE_UPDATE_TS'] = None
        self.assertIn('MISSING_SOURCE_UPDATE_TS', transform(row, MAP, AS_OF)['VALIDATION_ERRORS'])
        approved = {'SOURCE_UPDATE_TS': datetime(2024, 1, 1), 'SOURCE_SEQUENCE': 2}
        self.assertEqual('VALID', transform(row, MAP, AS_OF, approved)['VALIDATION_STATUS'])

    def test_invalid_versions(self):
        for key, value in [('SOURCE_UPDATE_TS', 'bad'), ('SOURCE_SEQUENCE', -1), ('SOURCE_SEQUENCE', 1.5)]:
            row = raw()
            row[key] = value
            self.assertIn('INVALID_SOURCE_VERSION', transform(row, MAP, AS_OF)['VALIDATION_ERRORS'])

    def test_latest_tie_break_late_arrival_and_invalid_newer(self):
        first = transform(raw('a'), MAP, AS_OF)
        tie = transform(raw('b'), MAP, AS_OF)
        invalid = transform(raw('z', Name=''), MAP, AS_OF)
        invalid['SOURCE_UPDATE_TS'] = datetime(2024, 2, 1)
        older = copy.deepcopy(first)
        older['SOURCE_UPDATE_TS'] = datetime(2023, 1, 1)
        for rows in ([first, tie, invalid, older], [older, invalid, tie, first]):
            self.assertEqual('b', winners(rows)['global-ind']['RAW_RECORD_ID'])
        first['SOURCE_SEQUENCE'] = 2
        self.assertEqual('a', winners([first, tie])['global-ind']['RAW_RECORD_ID'])

    def test_movement_requires_verified_mapping_and_stable_source_namespace(self):
        old = transform(raw('a'), MAP, AS_OF)
        moved = raw('b', CountryCode='AU')
        moved['SOURCE_UPDATE_TS'] = datetime(2024, 2, 1)
        new = transform(moved, MAP, AS_OF)
        self.assertEqual('AUS', winners([old, new])['global-ind']['COUNTRY_CODE'])
        self.assertEqual('GBR', transform(raw(Country='GB'), MAP, AS_OF)['COUNTRY_CODE'])
        self.assertEqual('REJECTED', transform(raw(Country='unknown'), MAP, AS_OF)['VALIDATION_STATUS'])

    def test_duplicates_fail_closed(self):
        with self.assertRaises(RuntimeError):
            unique([{'id': 1}, {'id': 1}], ['id'], 'raw')


class FakeCursor:
    """Control-flow double only. Does not emulate Snowflake SQL semantics."""
    def __init__(self, fail=False, done=False, invariant=False, moving=False):
        self.fail, self.done, self.invariant = fail, done, invariant
        self.moving = moving
        self.statements = []
        self.description = []
        self.rows = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        rows = []
        if sql.startswith('SELECT * FROM SKYPOINTS.INGEST_BATCH'):
            rows = [dict(STATUS='COMPLETED', FEED_TYPE='MEMBER', INPUT_ROWS=1)]
        elif sql.startswith('SELECT * FROM SKYPOINTS.RAW_MEMBER_PROFILE'):
            record = raw('b', CountryCode='AU') if self.moving else raw()
            if self.moving:
                record['SOURCE_UPDATE_TS'] = datetime(2024, 2, 1)
            rows = [record]
        elif sql.startswith('SELECT * FROM SKYPOINTS.MEMBER_IDENTITY_MAP'):
            rows = [dict(SOURCE_SYSTEM='SKYPOINTS_IND', SOURCE_MEMBER_ID='1', GLOBAL_MEMBER_ID='global-ind')]
        elif sql.startswith('SELECT MAX(AS_OF_DATE)'):
            rows = [{'D': None}]
        elif sql.startswith('SELECT * FROM SKYPOINTS.STG_MEMBER_PROFILE') and self.moving:
            previous = transform(raw(), MAP, AS_OF)
            previous['BATCH_ID'] = 'old-batch'
            rows = [previous]
        elif sql.startswith('SELECT * FROM SKYPOINTS.MEMBER_CURRENT') and self.moving:
            rows = [dict(GLOBAL_MEMBER_ID='global-ind', COUNTRY_CODE='IND')]
        elif sql.startswith('SELECT STATUS FROM SKYPOINTS.MEMBER_TRANSFORM_RUN') and self.done:
            rows = [{'STATUS': 'COMPLETED'}]
        elif sql.startswith('DELETE FROM SKYPOINTS.TABLE_USA') and self.fail:
            raise RuntimeError('simulated warehouse failure')
        elif sql.startswith('-- Every query') and self.invariant:
            rows = [{'GLOBAL_MEMBER_ID': 'broken'}]
        self.description = [(k,) for k in rows[0]] if rows else []
        self.rows = [tuple(r.values()) for r in rows]

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, **kwargs):
        self.cur = FakeCursor(**kwargs)
        self.commits = self.rollbacks = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class OrchestrationTests(unittest.TestCase):
    def test_move_records_audit_and_rebuilds_old_and_new_country(self):
        conn = FakeConnection(moving=True)
        run(conn, 'batch', AS_OF)
        movements = [(s,p) for s,p in conn.cur.statements if s.startswith('INSERT INTO SKYPOINTS.COUNTRY_MOVEMENT_AUDIT')]
        self.assertEqual(1, len(movements))
        self.assertEqual(('global-ind','IND','AUS'), movements[0][1][1:4])
        statements = [s for s,p in conn.cur.statements]
        for table in ('TABLE_INDIA','TABLE_AUSTRALIA'):
            self.assertIn('DELETE FROM SKYPOINTS.'+table, statements)
            self.assertTrue(any(s.startswith('INSERT INTO SKYPOINTS.'+table) for s in statements))

    def test_complete_includes_canonical_country_and_audit_counts(self):
        conn = FakeConnection()
        result = run(conn, 'batch', AS_OF)
        self.assertEqual((1, 0), (conn.commits, conn.rollbacks))
        self.assertEqual(1, result['accepted'])
        sql = [s for s, _ in conn.cur.statements]
        self.assertTrue(any(s.startswith('DELETE FROM SKYPOINTS.TABLE_INDIA') for s in sql))
        self.assertTrue(any(s.startswith('INSERT INTO SKYPOINTS.TABLE_OTHER_COUNTRIES') for s in sql))
        self.assertFalse(any(s.startswith(('CREATE ', 'ALTER ', 'TRUNCATE ')) for s in sql))

    def test_failure_rolls_back_and_can_retry(self):
        conn = FakeConnection(fail=True)
        with self.assertRaises(RuntimeError):
            run(conn, 'batch', AS_OF)
        self.assertEqual((0, 1), (conn.commits, conn.rollbacks))
        conn.cur.fail = False
        self.assertEqual('COMPLETED', run(conn, 'batch', AS_OF)['status'])

    def test_reconciliation_failure_rolls_back(self):
        conn = FakeConnection(invariant=True)
        with self.assertRaisesRegex(RuntimeError, 'reconciliation'):
            run(conn, 'batch', AS_OF)
        self.assertEqual((0, 1), (conn.commits, conn.rollbacks))

    def test_completed_replay_performs_no_writes(self):
        conn = FakeConnection(done=True)
        self.assertEqual('SKIPPED', run(conn, 'batch', AS_OF)['status'])
        self.assertFalse(any(s.startswith(('INSERT ', 'DELETE ', 'UPDATE ')) for s, _ in conn.cur.statements))


if __name__ == '__main__':
    unittest.main()
