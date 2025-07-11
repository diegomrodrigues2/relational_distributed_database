import time
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from database.replication import NodeCluster


def test_ddl_replication(tmp_path):
    cluster = NodeCluster(base_path=tmp_path, num_nodes=2, replication_factor=2)
    try:
        ddl = "CREATE TABLE pets (id int primary key)"
        cluster.nodes[0].client.execute_ddl(ddl)
        time.sleep(1.0)
        val = cluster.get(1, "_meta:table:pets")
        assert val is not None
    finally:
        cluster.shutdown()


def test_ddl_consistency_after_restart(tmp_path):
    cluster = NodeCluster(base_path=tmp_path, num_nodes=2, replication_factor=2, write_quorum=1)
    try:
        cluster.nodes[0].client.execute_ddl("CREATE TABLE t1 (id int primary key)")
        time.sleep(0.5)
        cluster.stop_node("node_1")
        cluster.nodes[0].client.execute_ddl("CREATE TABLE t2 (id int primary key)")
        time.sleep(0.5)
        cluster.start_node("node_1")
        time.sleep(1.0)
        assert cluster.get(1, "_meta:table:t1") is not None
        assert cluster.get(1, "_meta:table:t2") is not None
    finally:
        cluster.shutdown()
