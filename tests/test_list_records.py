import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster


class ListRecordsTest(unittest.TestCase):
    def test_records_list_excludes_tombstones(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, replication_factor=2)
            try:
                # write records using different coordinator indices
                cluster.put(0, "user1", "profile", "alice")
                cluster.put(1, "user2", "profile", "bob")
                cluster.put(0, "settings", "darkmode")
                # allow async replication
                time.sleep(0.5)

                cluster.delete(1, "user2", "profile")
                time.sleep(0.5)

                records = cluster.list_records()
                self.assertEqual(
                    records,
                    [("settings", None, "darkmode"), ("user1", "profile", "alice")],
                )
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
