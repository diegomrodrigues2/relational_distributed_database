import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster
from driver import Driver


class DriverTransactionWorkflowTest(unittest.TestCase):
    def test_commit_and_abort(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=1)
            driver = Driver(cluster)
            try:
                tx = driver.begin_transaction()
                driver.put("u", "k1", "v1", tx_id=tx)
                driver.put("u", "k2", "v2", tx_id=tx)

                # before commit nothing visible
                self.assertIsNone(driver.get("u", "k1"))
                self.assertIsNone(driver.get("u", "k2"))

                driver.commit(tx)
                time.sleep(0.5)

                self.assertEqual(driver.get("u", "k1"), "v1")
                self.assertEqual(driver.get("u", "k2"), "v2")

                tx2 = driver.begin_transaction()
                driver.put("u", "k3", "v3", tx_id=tx2)
                driver.put("u", "k4", "v4", tx_id=tx2)
                driver.abort(tx2)
                time.sleep(0.2)

                self.assertIsNone(driver.get("u", "k3"))
                self.assertIsNone(driver.get("u", "k4"))
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
