import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class ListRecordsNodeDownTest(unittest.TestCase):
    def test_list_records_with_node_down(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ranges = [("a", "g"), ("g", "n"), ("n", "z")]
            cluster = NodeCluster(base_path=tmpdir, num_nodes=3, key_ranges=ranges)
            try:
                cluster.put(0, "alpha", "v1")
                cluster.put(0, "hotel", "v2")
                cluster.put(0, "zulu", "v3")
                time.sleep(0.5)
                cluster.stop_node("node_2")
                time.sleep(0.5)
                records = cluster.list_records()
                self.assertEqual(sorted(records), [("alpha", None, "v1"), ("hotel", None, "v2")])
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
