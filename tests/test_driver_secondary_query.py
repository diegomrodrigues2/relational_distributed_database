import os
import sys
import tempfile
import json
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.replication import NodeCluster
from driver import Driver


class DriverSecondaryQueryTest(unittest.TestCase):
    def test_secondary_query_across_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(base_path=tmpdir, num_nodes=2, index_fields=["color"])
            driver = Driver(cluster)
            try:
                cluster.put(0, "p1", json.dumps({"color": "red"}))
                cluster.put(1, "p2", json.dumps({"color": "red"}))
                # allow async replication/index update
                time.sleep(0.5)
                result = driver.secondary_query("color", "red")
                self.assertEqual(sorted(result), ["p1", "p2"])
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
