import os
import tempfile
import unittest

from database.replication.replica.grpc_server import NodeServer

class GlobalIndexManagerTest(unittest.TestCase):
    def test_rebuild_on_startup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, global_index_fields=["tag"])
            # add raw index entry and close to simulate prior data
            node.db.put("idx:tag:blue:k1", "1", timestamp=1)
            node.db.close()

            node = NodeServer(db_path=tmpdir, global_index_fields=["tag"])
            node.global_index_manager.rebuild(node.db)
            self.assertEqual(sorted(node.global_index_manager.query("tag", "blue")), ["k1"])
            node.db.close()

if __name__ == "__main__":
    unittest.main()
