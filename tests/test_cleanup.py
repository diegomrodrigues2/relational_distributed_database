import tempfile
import unittest

from replica.grpc_server import NodeServer

class ReplicationLogCleanupTest(unittest.TestCase):
    def test_cleanup_removes_old_entries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            node.last_seen = {"p1": 3, "p2": 4}
            node.replication_log = {
                "node:1": ("k1", "v1", 1),
                "node:2": ("k2", "v2", 2),
                "node:3": ("k3", "v3", 3),
                "node:4": ("k4", "v4", 4),
            }
            node.cleanup_replication_log()
            self.assertIn("node:4", node.replication_log)
            self.assertNotIn("node:1", node.replication_log)
            self.assertNotIn("node:2", node.replication_log)
            self.assertNotIn("node:3", node.replication_log)
            node.stop()

if __name__ == "__main__":
    unittest.main()
