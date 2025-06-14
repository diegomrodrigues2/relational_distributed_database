"""Simple multi-leader replication utilities."""

import os
import time
import shutil
import multiprocessing
import threading
from dataclasses import dataclass
from concurrent import futures

from replica.grpc_server import run_server
from replica.client import GRPCReplicaClient
from hash_ring import HashRing as ConsistentHashRing
from vector_clock import VectorClock
from lsm_db import _merge_version_lists


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
        recs = self.client.get(key)
        return recs[0][0] if recs else None

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

    def get(self, node_index: int, key: str, *, merge: bool = True):
        pref_nodes = self.ring.get_preference_list(key, self.replication_factor)
        nodes = []
        for nid in pref_nodes:
            n = self.nodes_by_id[nid]
            try:
                n.client.ping(n.node_id)
                nodes.append(n)
            except Exception:
                continue

        if len(nodes) < self.read_quorum:
            all_nodes = self.ring.get_preference_list(key, len(self.nodes))
            for nid in all_nodes:
                if len(nodes) >= self.read_quorum:
                    break
                if nid in pref_nodes or any(x.node_id == nid for x in nodes):
                    continue
                n = self.nodes_by_id[nid]
                try:
                    n.client.ping(n.node_id)
                    nodes.append(n)
                except Exception:
                    continue

        if not nodes:
            return None

        responses = []
        with futures.ThreadPoolExecutor(max_workers=len(nodes)) as ex:
            future_map = {ex.submit(n.client.get, key): n for n in nodes}
            for fut in futures.as_completed(future_map):
                node = future_map[fut]
                try:
                    recs = fut.result()
                except Exception:
                    recs = []
                responses.append((node, recs))

        if not responses:
            return None

        merged = []
        for _, recs in responses:
            for val, ts, vc_dict in recs:
                merged = _merge_version_lists(merged, [(val, VectorClock(vc_dict))])

        if not merged:
            return None

        if merge:
            if self.consistency_mode in ("vector", "crdt"):
                best_val, best_vc = merged[0]
                best_ts = best_vc.clock.get("ts", 0)
                for val, vc in merged[1:]:
                    cmp = vc.compare(best_vc)
                    ts = vc.clock.get("ts", 0)
                    if cmp == ">" or (cmp is None and ts > best_ts):
                        best_val, best_vc, best_ts = val, vc, ts
            else:
                best_val = None
                best_ts = -1
                best_vc = None
                for val, vc in merged:
                    ts = vc.clock.get("ts", 0)
                    if ts > best_ts:
                        best_val, best_vc, best_ts = val, vc, ts

            stale_nodes = []
            for node, recs in responses:
                has = False
                for val, ts, vc_dict in recs:
                    if val == best_val and vc_dict == best_vc.clock:
                        has = True
                        break
                if not has:
                    stale_nodes.append(node)

            def _repair(n):
                try:
                    if self.consistency_mode in ("vector", "crdt"):
                        n.client.put(
                            key,
                            best_val,
                            timestamp=best_ts,
                            node_id=n.node_id,
                            vector=best_vc.clock,
                        )
                    else:
                        n.client.put(
                            key,
                            best_val,
                            timestamp=best_ts,
                            node_id=n.node_id,
                        )
                except Exception:
                    pass

            for sn in stale_nodes:
                t = threading.Thread(target=_repair, args=(sn,), daemon=True)
                t.start()

            return best_val
        else:
            return [(val, vc.clock) for val, vc in merged]

        def _repair(n):
            try:
                if self.consistency_mode in ("vector", "crdt"):
                    n.client.put(
                        key,
                        best_val,
                        timestamp=best_ts,
                        node_id=n.node_id,
                        vector=best_vc_dict,
                    )
                else:
                    n.client.put(
                        key,
                        best_val,
                        timestamp=best_ts,
                        node_id=n.node_id,
                    )
            except Exception:
                pass

        for sn in stale_nodes:
            t = threading.Thread(target=_repair, args=(sn,), daemon=True)
            t.start()

        return best_val

    def shutdown(self):
        for n in self.nodes:
            n.stop()

