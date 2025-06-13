import os
import sys
import tempfile
import unittest
import multiprocessing
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster

class ReplicationManagerTest(unittest.TestCase):
    def test_basic_replication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=3)
            cluster.put(0, 'user:1', 'A')
            v0 = cluster.get(0, 'user:1')
            v1 = cluster.get(1, 'user:1')
            v2 = cluster.get(2, 'user:1')
            self.assertEqual(v0, 'A')
            self.assertEqual(v1, 'A')
            self.assertEqual(v2, 'A')
            cluster.shutdown()


if __name__ == '__main__':
    unittest.main()
