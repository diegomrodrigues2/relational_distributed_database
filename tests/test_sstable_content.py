import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication.replica.grpc_server import NodeServer
from database.replication.replica.client import GRPCReplicaClient


class SSTableContentRPCTest(unittest.TestCase):
    def test_get_sstable_content(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            node = NodeServer(db_path=tmpdir, port=9110, node_id="A", peers=[])
            node.server.start()
            try:
                node.db.put("k1", "v1")
                node.db._flush_memtable_to_sstable()
                node.db.wait_for_compaction()
                seg = node.db.sstable_manager.sstable_segments[-1]
                sstable_id = os.path.basename(seg[1])

                client = GRPCReplicaClient("localhost", 9110)
                entries = client.get_sstable_content(sstable_id, node_id="A")
                client.close()

                self.assertEqual(len(entries), 1)
                self.assertEqual(entries[0][0], "k1")
                self.assertEqual(entries[0][1], "v1")
            finally:
                node.server.stop(0).wait()
                node.db.close()


if __name__ == "__main__":
    unittest.main()
