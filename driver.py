import time
import random
import grpc
import threading
from database.replication import NodeCluster
from database.replication.replica import metadata_pb2, metadata_pb2_grpc, replication_pb2
from database.clustering.partitioning import compose_key

class Driver:
    """Interface entre usuários e o cluster garantindo certas consistências."""

    def __init__(
        self,
        cluster: NodeCluster,
        read_your_writes_timeout: int = 5,
        *,
        consistency_mode: str = "lww",
        load_balance_reads: bool | None = None,
        registry_host: str | None = None,
        registry_port: int | None = None,
    ) -> None:
        """Cria o driver e prepara o dicionário de sessões.

        :param load_balance_reads: distribui leituras entre as réplicas quando
            ``True``. ``None`` herda a configuração do cluster.
        """
        self.cluster = cluster
        self.consistency_mode = consistency_mode
        self.read_your_writes_timeout = read_your_writes_timeout
        if load_balance_reads is None:
            load_balance_reads = getattr(cluster, "load_balance_reads", False)
        self.load_balance_reads = bool(load_balance_reads)
        self._sessions = {}
        self._tx_nodes: dict[str, tuple[str, str, list[str]]] = {}
        self.partition_map = cluster.get_partition_map()
        self.registry_host = registry_host
        self.registry_port = registry_port
        self._registry_channel = None
        self._registry_stub = None
        self._watch_stop = threading.Event()
        self._watch_thread = None
        if registry_host and registry_port:
            self._registry_channel = grpc.insecure_channel(f"{registry_host}:{registry_port}")
            self._registry_stub = metadata_pb2_grpc.MetadataServiceStub(self._registry_channel)
            try:
                state = self._registry_stub.GetClusterState(replication_pb2.Empty())
                self.update_partition_map(state.partition_map.items)
            except Exception:
                pass
            self._start_watch_thread()
        cluster.register_driver(self)

    def _watch_loop(self) -> None:
        if not self._registry_stub:
            return
        while not self._watch_stop.is_set():
            try:
                stream = self._registry_stub.WatchClusterState(replication_pb2.Empty())
                for state in stream:
                    self.update_partition_map(state.partition_map.items)
                    if self._watch_stop.is_set():
                        break
            except Exception:
                time.sleep(1.0)

    def _start_watch_thread(self) -> None:
        if self._watch_thread and self._watch_thread.is_alive():
            return
        t = threading.Thread(target=self._watch_loop, daemon=True)
        self._watch_thread = t
        t.start()

    def update_partition_map(self, mapping: dict[int, str] | None = None) -> None:
        """Replace cached partition map with ``mapping`` or fetch from cluster."""
        if mapping is None:
            if self._registry_stub:
                try:
                    state = self._registry_stub.GetClusterState(replication_pb2.Empty())
                    mapping = state.partition_map.items
                except Exception:
                    mapping = self.cluster.get_partition_map()
            else:
                mapping = self.cluster.get_partition_map()
        self.partition_map = dict(mapping)

    def _get_or_create_session(self, user_id: str) -> dict:
        """Retorna a sessão existente ou cria uma nova."""
        if user_id not in self._sessions:
            num_nodes = len(self.cluster.nodes)
            follower = random.randint(0, num_nodes - 1) if num_nodes > 0 else 0
            self._sessions[user_id] = {
                "last_write_time": 0,
                "assigned_follower": follower,
            }
        return self._sessions[user_id]

    def put(
        self,
        user_id: str,
        partition_key: str,
        clustering_key: str,
        value: str | None = None,
        *,
        tx_id: str | None = None,
    ):
        """Escreve via líder e atualiza o tempo da sessão.

        Para retrocompatibilidade ``clustering_key`` pode ser omitido e o valor
        passado diretamente após ``partition_key``.
        """
        session = self._get_or_create_session(user_id)
        session["last_write_time"] = time.time()
        if value is None:
            value = clustering_key
            clustering_key = None
        if tx_id:
            mapping = self._tx_nodes.get(tx_id)
            if mapping is None:
                raise ValueError("unknown transaction")
            node_id, node_tx, _snapshot = mapping
        else:
            pid = self.cluster.get_partition_id(partition_key, clustering_key)
            node_id = self.partition_map.get(pid)
            if node_id is None:
                self.partition_map = self.cluster.get_partition_map()
                node_id = self.partition_map.get(pid)
        node_txid = node_tx if tx_id else ""
        node = self.cluster.nodes_by_id[node_id]
        key = compose_key(partition_key, clustering_key)
        try:
            node.client.put(key, value, node_id="", tx_id=node_txid)
        except grpc.RpcError as exc:
            if (
                not tx_id
                and exc.code() == grpc.StatusCode.FAILED_PRECONDITION
                and "NotOwner" in exc.details()
            ):
                self.partition_map = self.cluster.get_partition_map()
                pid = self.cluster.get_partition_id(partition_key, clustering_key)
                node_id = self.partition_map.get(pid)
                node = self.cluster.nodes_by_id[node_id]
                node.client.put(key, value, node_id="", tx_id="")
            else:
                raise

    def get(
        self,
        user_id: str,
        partition_key: str,
        clustering_key: str | None = None,
    ):
        """Lê aplicando read-your-own-writes e leituras monotônicas."""
        session = self._get_or_create_session(user_id)
        pid = self.cluster.get_partition_id(partition_key, clustering_key)
        node_id = self.partition_map.get(pid)
        if node_id is None:
            self.partition_map = self.cluster.get_partition_map()
            node_id = self.partition_map.get(pid)

        candidates: list[str]
        if self.load_balance_reads and getattr(self.cluster, "ring", None):
            candidates = self.cluster.ring.get_preference_list(
                partition_key, self.cluster.replication_factor
            )
            random.shuffle(candidates)
        else:
            candidates = [node_id]

        recs = None
        last_exc = None
        key = compose_key(partition_key, clustering_key)
        for nid in candidates:
            node = self.cluster.nodes_by_id[nid]
            try:
                recs = node.client.get(key)
                node_id = nid
                break
            except grpc.RpcError as exc:
                last_exc = exc
                if exc.code() == grpc.StatusCode.FAILED_PRECONDITION and "NotOwner" in exc.details():
                    continue
                else:
                    raise
        if recs is None:
            if last_exc is not None and last_exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                self.partition_map = self.cluster.get_partition_map()
                node_id = self.partition_map.get(pid)
                node = self.cluster.nodes_by_id[node_id]
                recs = node.client.get(key)
            else:
                raise last_exc
        
        if not recs:
            return None
        return recs[0][0]

    def secondary_query(self, field: str, value) -> list[str]:
        """Query secondary indexes across the cluster.

        Due to asynchronous replication, results may be stale or
        inconsistent across nodes."""
        results: set[str] = set()
        def _call(node):
            try:
                return node.client.list_by_index(field, value)
            except grpc.RpcError:
                return []

        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor() as ex:
            for keys in ex.map(_call, self.cluster.nodes_by_id.values()):
                results.update(keys)
        return sorted(results)

    # transaction methods -------------------------------------------------
    def begin_transaction(self) -> str:
        """Start a transaction on a random node and return its id."""
        node = random.choice(self.cluster.nodes)
        node_tx, snapshot = node.client.begin_transaction()
        tx_id = self.cluster.next_txid()
        self._tx_nodes[tx_id] = (node.node_id, node_tx, snapshot)
        return tx_id

    def commit(self, tx_id: str) -> None:
        """Commit a transaction via the node that started it."""
        mapping = self._tx_nodes.pop(tx_id, None)
        if mapping is None:
            return
        node_id, node_tx, _snapshot = mapping
        node = self.cluster.nodes_by_id[node_id]
        node.client.commit_transaction(node_tx)

    def abort(self, tx_id: str) -> None:
        """Abort a transaction discarding its buffered operations."""
        mapping = self._tx_nodes.pop(tx_id, None)
        if mapping is None:
            return
        node_id, node_tx, _snapshot = mapping
        node = self.cluster.nodes_by_id[node_id]
        node.client.abort_transaction(node_tx)
