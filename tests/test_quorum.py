import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from replication import NodeCluster
from lsm_db import SimpleLSMDB


class NodeClusterInitTest(unittest.TestCase):
    def test_custom_quorum_parameters(self):
        configs = [
            (2, 2, 1),
            (3, 2, 2),
            (3, 3, 1),
        ]
        for rf, wq, rq in configs:
            with self.subTest(rf=rf, wq=wq, rq=rq):
                with tempfile.TemporaryDirectory() as tmpdir:
                    cluster = NodeCluster(
                        base_path=tmpdir,
                        num_nodes=4,
                        replication_factor=rf,
                        write_quorum=wq,
                        read_quorum=rq,
                    )
                    try:
                        self.assertEqual(cluster.replication_factor, rf)
                        self.assertEqual(cluster.write_quorum, wq)
                        self.assertEqual(cluster.read_quorum, rq)
                    finally:
                        cluster.shutdown()


class WriteQuorumTest(unittest.TestCase):
    def test_write_succeeds_and_fails_based_on_quorum(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=3,
                write_quorum=2,
                read_quorum=1,
            )
            try:
                key = "quorum:key"
                pref_nodes = cluster.ring.get_preference_list(key, 3)

                # stop one replica - should still succeed
                cluster.nodes_by_id[pref_nodes[1]].stop()
                time.sleep(0.5)
                cluster.put(0, key, "v1")
                for nid in pref_nodes:
                    if nid == pref_nodes[1]:
                        continue
                    self.assertEqual(cluster.nodes_by_id[nid].client.get(key)[0], "v1")

                # stop another replica - now quorum not met
                cluster.nodes_by_id[pref_nodes[2]].stop()
                time.sleep(0.5)
                with self.assertRaises(Exception):
                    cluster.put(0, key, "v2")
            finally:
                cluster.shutdown()


class ReadAfterWriteQuorumTest(unittest.TestCase):
    def test_read_returns_latest_when_r_plus_w_exceeds_n(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=3,
                write_quorum=2,
                read_quorum=2,
            )
            try:
                key = "latest:key"
                cluster.put(0, key, "v1")
                pref_nodes = cluster.ring.get_preference_list(key, 3)
                cluster.nodes_by_id[pref_nodes[2]].stop()
                time.sleep(0.5)
                cluster.put(0, key, "v2")
                time.sleep(0.5)
                value = cluster.get(0, key)
                self.assertEqual(value, "v2")
            finally:
                cluster.shutdown()


class AvailabilityScenarioTest(unittest.TestCase):
    def test_low_quorum_allows_stale_reads(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster = NodeCluster(
                base_path=tmpdir,
                num_nodes=3,
                replication_factor=3,
                write_quorum=1,
                read_quorum=1,
            )
            try:
                key = "stale:key"
                cluster.put(0, key, "v1")
                pref_nodes = cluster.ring.get_preference_list(key, 3)
                stale_id = pref_nodes[2]
                stale_node = cluster.nodes_by_id[stale_id]

                stale_node.stop()
                time.sleep(0.5)
                cluster.put(0, key, "v2")
                time.sleep(0.5)

                # read value directly from stale replica's storage
                db_path = os.path.join(tmpdir, stale_id)
                local_db = SimpleLSMDB(db_path=db_path)
                stale_val = local_db.get(key)
                fresh_val = cluster.nodes_by_id[pref_nodes[0]].client.get(key)[0]
                self.assertEqual(stale_val, "v1")
                self.assertEqual(fresh_val, "v2")
            finally:
                cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
