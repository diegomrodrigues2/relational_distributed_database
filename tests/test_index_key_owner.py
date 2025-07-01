import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from types import SimpleNamespace

from database.replication.replica.grpc_server import ReplicaService
from database.clustering.hash_ring import HashRing


class IndexKeyOwnerTest(unittest.TestCase):
    def test_same_value_owner_hash_ring(self):
        ring = HashRing()
        ring.add_node("node_a")
        ring.add_node("node_b")
        pmap = {i: nid for i, (_, nid) in enumerate(ring._ring)}
        peers = [("localhost", 0, "node_a"), ("localhost", 1, "node_b")]
        node = SimpleNamespace(
            hash_ring=ring,
            partition_map=pmap,
            range_table=None,
            partition_modulus=None,
            node_index=None,
            peers=peers,
            node_id="node_a",
        )
        service = ReplicaService(node)
        owner1 = service._owner_for_key("idx:tag:red:item1")
        owner2 = service._owner_for_key("idx:tag:red:item2")
        self.assertEqual(owner1, owner2)

    def test_same_value_owner_modulus(self):
        peers = [("localhost", i, f"node_{i}") for i in range(3)]
        node = SimpleNamespace(
            hash_ring=None,
            partition_map={},
            range_table=None,
            partition_modulus=3,
            node_index=0,
            peers=peers,
            node_id="node_0",
        )
        service = ReplicaService(node)
        owner1 = service._owner_for_key("idx:color:blue:1")
        owner2 = service._owner_for_key("idx:color:blue:2")
        self.assertEqual(owner1, owner2)


if __name__ == "__main__":
    unittest.main()
