import os
import sys
import tempfile
import time
import json
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster
from replica.grpc_server import NodeServer


class ClusterIndexingTest(unittest.TestCase):
    def test_cluster_nodes_build_indexes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=1, index_fields=["name"])
            try:
                cluster.put(0, "u1", json.dumps({"name": "alice"}))
                # allow async replication/index update
                time.sleep(0.5)
            finally:
                cluster.shutdown()

            node = NodeServer(os.path.join(tmpdir, "node_0"), index_fields=["name"])
            try:
                self.assertEqual(sorted(node.query_index("name", "alice")), ["u1"])
            finally:
                node.db.close()


if __name__ == "__main__":
    unittest.main()
