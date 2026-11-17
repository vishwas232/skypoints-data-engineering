import unittest
from unittest.mock import patch
from src.pipeline import Namespace,Pipeline


class AcceptanceIsolationTests(unittest.TestCase):
    def test_production_and_legacy_namespaces_are_refused(self):
        for name in ('SKYPOINTS','PUBLIC','PROD','SKYPOINTS_TEST_not_a_unique_id'):
            with self.assertRaises(ValueError): Namespace('ASSESSMENT',name)

    def test_existing_test_schema_is_never_adopted_or_modified(self):
        engine=Pipeline(None,Namespace('ASSESSMENT','SKYPOINTS_TEST_ABCDEF123456'))
        with patch.object(engine,'scalar',return_value=1),patch.object(engine,'execute') as execute:
            with self.assertRaisesRegex(RuntimeError,'already exists'): engine.deploy()
            execute.assert_not_called()
        with self.assertRaises(RuntimeError): engine.drop_owned_test_schema()

    def test_failure_injection_is_refused_in_persistent_development_schema(self):
        engine=Pipeline(None,Namespace('ASSESSMENT','SKYPOINTS_DEV'))
        with self.assertRaisesRegex(ValueError,'isolated test'):
            engine.process('a'*64,__import__('datetime').date(2026,11,18),fail_before_commit=True)


if __name__=='__main__': unittest.main()
