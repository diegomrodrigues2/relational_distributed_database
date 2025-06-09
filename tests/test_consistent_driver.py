import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import ReplicationManager
from driver import Driver


class DriverTest(unittest.TestCase):
    def test_read_your_own_writes_uses_leader(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = ReplicationManager(base_path=tmpdir, num_followers=1)
            driver = Driver(cluster, read_your_writes_timeout=5)

            cluster.take_follower_offline(0)
            driver.put("user1", "k1", "v1")
            cluster.bring_follower_online(0)

            value = driver.get("user1", "k1")
            self.assertEqual(value, "v1")
            cluster.shutdown()

    def test_monotonic_reads_same_follower(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = ReplicationManager(base_path=tmpdir, num_followers=2)
            driver = Driver(cluster, read_your_writes_timeout=1)

            driver.put("setup", "key", "value")
            time.sleep(driver.read_your_writes_timeout + 0.1)

            user = "userB"
            first = driver.get(user, "key")
            self.assertEqual(first, "value")
            assigned = driver._sessions[user]["assigned_follower"]

            cluster.take_follower_offline(assigned)
            second = driver.get(user, "key")
            self.assertIn("Error", second)
            cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
