"""Simple multi-leader replication utilities."""

import os
import time
import shutil
import multiprocessing
from dataclasses import dataclass

from replica.grpc_server import run_server
from replica.client import GRPCReplicaClient


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
        return self.client.get(key)

    def stop(self):
        if self.process.is_alive():
            self.process.terminate()
        self.process.join()
        self.client.close()


class NodeCluster:
    """Launches multiple nodes that replicate to each other."""

    def __init__(self, base_path: str, num_nodes: int = 3, topology: dict[int, list[int]] | None = None):
        self.base_path = base_path
        if os.path.exists(base_path):
            shutil.rmtree(base_path)
        os.makedirs(base_path)

        base_port = 9000
        self.nodes = []
        peers = [
            ("localhost", base_port + i, f"node_{i}")
            for i in range(num_nodes)
        ]

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
                args=(db_path, "localhost", port, node_id, peers_i),
                daemon=True,
            )
            p.start()
            time.sleep(0.2)
            client = GRPCReplicaClient("localhost", port)
            self.nodes.append(ClusterNode(node_id, "localhost", port, p, client))

        time.sleep(1)

    def put(self, node_index: int, key: str, value: str):
        self.nodes[node_index].put(key, value)

    def delete(self, node_index: int, key: str):
        self.nodes[node_index].delete(key)

    def get(self, node_index: int, key: str):
        return self.nodes[node_index].get(key)

    def shutdown(self):
        for n in self.nodes:
            n.stop()

