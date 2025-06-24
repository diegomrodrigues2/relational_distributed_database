import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster


class RouterServerTest(unittest.TestCase):
    def test_router_process_put_get(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, start_router=True)
            try:
                cluster.router_client.put("rkey", "v1")
                time.sleep(0.5)
                self.assertEqual(cluster.get(0, "rkey"), "v1")
                self.assertTrue(cluster.router_process.is_alive())
            finally:
                cluster.shutdown()
            self.assertFalse(cluster.router_process.is_alive())


if __name__ == "__main__":
    unittest.main()
