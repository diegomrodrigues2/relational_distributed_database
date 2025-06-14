"""Simple multi-leader replication utilities."""

import os
import time
import shutil
import multiprocessing
from dataclasses import dataclass
from concurrent import futures

from replica.grpc_server import run_server
from replica.client import GRPCReplicaClient
from hash_ring import HashRing as ConsistentHashRing


@dataclass
class ClusterNode:
    node_id: str
    host: str
    port: int
    process: multiprocessing.Process
    client: GRPCReplicaClient

    def put(self, key, value):
        ts = int(time.time() * 1000)
        self.client.put(key, value, timestamp=ts, node_id=self.node_id)

    def delete(self, key):
        ts = int(time.time() * 1000)
        self.client.delete(key, timestamp=ts, node_id=self.node_id)

    def get(self, key):
        value, _ = self.client.get(key)
        return value

    def stop(self):
        if self.process.is_alive():
            self.process.terminate()
        self.process.join()
        self.client.close()


class NodeCluster:
    """Launches multiple nodes that replicate to each other."""

    def __init__(
        self,
        base_path: str,
        num_nodes: int = 3,
        topology: dict[int, list[int]] | None = None,
        *,
        consistency_mode: str = "lww",
        replication_factor: int = 3,
        write_quorum: int | None = None,
        read_quorum: int | None = None,
    ):
        self.base_path = base_path
        if os.path.exists(base_path):
            shutil.rmtree(base_path)
        os.makedirs(base_path)

        base_port = 9000
        self.nodes = []
        self.nodes_by_id: dict[str, ClusterNode] = {}
        self.consistency_mode = consistency_mode
        self.replication_factor = replication_factor
        self.write_quorum = write_quorum or (replication_factor // 2 + 1)
        self.read_quorum = read_quorum or (replication_factor // 2 + 1)
        self.ring = ConsistentHashRing()
        peers = [
            ("localhost", base_port + i, f"node_{i}")
            for i in range(num_nodes)
        ]
        for _, _, nid in peers:
            self.ring.add_node(nid)

        for i in range(num_nodes):
            node_id = f"node_{i}"
            db_path = os.path.join(base_path, node_id)
            port = base_port + i
            if topology is None:
                peers_i = peers
            else:
                peers_i = [
                    ("localhost", base_port + j, f"node_{j}")
                    for j in topology.get(i, [])
                ]
            p = multiprocessing.Process(
                target=run_server,
                args=(
                    db_path,
                    "localhost",
                    port,
                    node_id,
                    peers_i,
                    self.ring,
                    self.replication_factor,
                    self.write_quorum,
                    self.read_quorum,
                ),
                kwargs={"consistency_mode": self.consistency_mode},
                daemon=True,
            )
            p.start()
            time.sleep(0.2)
            client = GRPCReplicaClient("localhost", port)
            node = ClusterNode(node_id, "localhost", port, p, client)
            self.nodes.append(node)
            self.nodes_by_id[node_id] = node

        time.sleep(1)

    def _coordinator(self, key: str) -> ClusterNode:
        node_id = self.ring.get_preference_list(key, 1)[0]
        return self.nodes_by_id[node_id]

    def put(self, node_index: int, key: str, value: str):
        node = self._coordinator(key)
        node.put(key, value)

    def delete(self, node_index: int, key: str):
        node = self._coordinator(key)
        node.delete(key)

    def get(self, node_index: int, key: str):
        pref_nodes = self.ring.get_preference_list(key, self.replication_factor)
        nodes = [self.nodes_by_id[nid] for nid in pref_nodes]
        responses = []
        with futures.ThreadPoolExecutor(max_workers=len(nodes)) as ex:
            future_map = {ex.submit(n.client.get, key): n.node_id for n in nodes}
            for fut in futures.as_completed(future_map):
                try:
                    val, ts = fut.result()
                except Exception:
                    val = None
                    ts = -1
                if val is not None:
                    responses.append((ts, val))
                if len(responses) >= self.read_quorum:
                    break
        if not responses:
            return None
        _, value = max(responses, key=lambda x: x[0])
        return value

    def shutdown(self):
        for n in self.nodes:
            n.stop()

