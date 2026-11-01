import unittest
import capacity_run


class BaselineEvidence(unittest.TestCase):
    def test_all_success_or_missing_proxy_error_cannot_prove_reproduction(self):
        for successes,errors in [(400,0),(400,30),(127,0)]:
            with self.subTest(successes=successes,errors=errors):
                with self.assertRaises(RuntimeError):
                    capacity_run.check_baseline({'logical_requests':400,'successes':successes,'concurrency_limit':200},errors)


if __name__=='__main__': unittest.main()
