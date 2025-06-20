import time
import random
import grpc
from replication import NodeCluster
from partitioning import compose_key

class Driver:
    """Interface entre usuários e o cluster garantindo certas consistências."""

    def __init__(
        self,
        cluster: NodeCluster,
        read_your_writes_timeout: int = 5,
        *,
        consistency_mode: str = "lww",
    ) -> None:
        """Cria o driver e prepara o dicionário de sessões."""
        self.cluster = cluster
        self.consistency_mode = consistency_mode
        self.read_your_writes_timeout = read_your_writes_timeout
        self._sessions = {}
        self.partition_map = cluster.get_partition_map()

    def update_partition_map(self, mapping: dict[int, str] | None = None) -> None:
        """Replace cached partition map with ``mapping`` or fetch from cluster."""
        if mapping is None:
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
        pid = self.cluster.get_partition_id(partition_key, clustering_key)
        node_id = self.partition_map.get(pid)
        if node_id is None:
            self.partition_map = self.cluster.get_partition_map()
            node_id = self.partition_map.get(pid)
        node = self.cluster.nodes_by_id[node_id]
        key = compose_key(partition_key, clustering_key)
        try:
            node.client.put(key, value, node_id="")
        except grpc.RpcError as exc:
            if exc.code() == grpc.StatusCode.FAILED_PRECONDITION and "NotOwner" in exc.details():
                self.partition_map = self.cluster.get_partition_map()
                node_id = self.partition_map.get(pid)
                node = self.cluster.nodes_by_id[node_id]
                node.client.put(key, value, node_id="")
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
        node = self.cluster.nodes_by_id[node_id]
        key = compose_key(partition_key, clustering_key)
        try:
            recs = node.client.get(key)
        except grpc.RpcError as exc:
            if exc.code() == grpc.StatusCode.FAILED_PRECONDITION and "NotOwner" in exc.details():
                self.partition_map = self.cluster.get_partition_map()
                node_id = self.partition_map.get(pid)
                node = self.cluster.nodes_by_id[node_id]
                recs = node.client.get(key)
            else:
                raise
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
