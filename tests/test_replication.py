import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import ReplicationManager

class ReplicationManagerTest(unittest.TestCase):
    def test_basic_replication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = ReplicationManager(base_path=tmpdir, num_followers=2)
            cluster.put('user:1', 'A')
            v_leader = cluster.get('user:1', read_from_leader=True)
            v_f0 = cluster.get('user:1', read_from_leader=False, follower_id=0)
            v_f1 = cluster.get('user:1', read_from_leader=False, follower_id=1)
            self.assertEqual(v_leader, 'A')
            self.assertEqual(v_f0, 'A')
            self.assertEqual(v_f1, 'A')
            cluster.shutdown()

    def test_offline_follower(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = ReplicationManager(base_path=tmpdir, num_followers=2)
            cluster.take_follower_offline(1)
            cluster.put('k2', 'v2')
            self.assertEqual(cluster.get('k2', True), 'v2')
            self.assertEqual(cluster.get('k2', False, 0), 'v2')
            self.assertIn('Error', cluster.get('k2', False, 1))
            cluster.bring_follower_online(1)
            # follower 1 did not receive previous write
            self.assertIsNone(cluster.get('k2', False, 1))
            cluster.shutdown()

if __name__ == '__main__':
    unittest.main()
