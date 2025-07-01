import os
import sys
import tempfile
import json
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication.replica.grpc_server import NodeServer

class ReplicationLogFileTest(unittest.TestCase):
    def test_log_persisted_and_loaded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir)
            node.load_replication_log()
            node.replication_log["node:1"] = ("k", "v", 1)
            node.save_replication_log()
            node.stop()

            path = os.path.join(tmpdir, "replication_log.json")
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.assertIn("node:1", data)

            node2 = NodeServer(db_path=tmpdir)
            node2.load_replication_log()
            self.assertIn("node:1", node2.replication_log)
            node2.stop()

if __name__ == "__main__":
    unittest.main()
