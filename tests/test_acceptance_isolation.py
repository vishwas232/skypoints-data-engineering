import unittest
from src.snowflake_acceptance import rewrite,IsolatedCursor,InjectedFailure,TABLES


class AcceptanceIsolationTests(unittest.TestCase):
    def test_all_allowlisted_objects_rewrite_to_unique_fixture_names(self):
        prefix='ACCEPT_123456ABCDEF_'
        for table in TABLES:
            self.assertEqual('SELECT * FROM SKYPOINTS.'+prefix+table,
                             rewrite('SELECT * FROM SKYPOINTS.'+table,prefix))

    def test_unknown_objects_and_unsafe_prefixes_are_refused(self):
        with self.assertRaises(ValueError):
            rewrite('DELETE FROM SKYPOINTS.UNKNOWN','ACCEPT_123456ABCDEF_')
        with self.assertRaises(ValueError):
            rewrite('SELECT * FROM SKYPOINTS.MEMBER_CURRENT','bad;prefix')

    def test_failure_is_injected_before_audit_write(self):
        class Cursor:
            def execute(self,*args):
                raise AssertionError('Underlying write must not occur')
        cur=IsolatedCursor(Cursor(),'ACCEPT_123456ABCDEF_',True)
        with self.assertRaises(InjectedFailure):
            cur.execute('INSERT INTO SKYPOINTS.MEMBER_TRANSFORM_RUN VALUES (1)')


if __name__=='__main__':
    unittest.main()
