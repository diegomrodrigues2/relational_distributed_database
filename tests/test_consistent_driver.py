import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster
from driver import Driver


class DriverTest(unittest.TestCase):
    def test_read_your_own_writes_uses_leader(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2)
            driver = Driver(cluster, read_your_writes_timeout=5)

            driver.put("user1", "k1", "v1")
            value = driver.get("user1", "k1")
            self.assertEqual(value, "v1")
            cluster.shutdown()

    def test_monotonic_reads_same_follower(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=3)
            driver = Driver(cluster, read_your_writes_timeout=1)

            driver.put("setup", "key", "value")
            time.sleep(driver.read_your_writes_timeout + 0.1)

            user = "userB"
            first = driver.get(user, "key")
            self.assertEqual(first, "value")
            assigned = driver._sessions[user]["assigned_follower"]
            second = driver.get(user, "key")
            self.assertEqual(second, "value")
            cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
