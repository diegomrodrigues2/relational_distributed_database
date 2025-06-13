import time
import random
from replication import NodeCluster

class Driver:
    """Interface entre usuários e o cluster garantindo certas consistências."""

    def __init__(self, cluster: NodeCluster, read_your_writes_timeout: int = 5) -> None:
        """Cria o driver e prepara o dicionário de sessões."""
        self.cluster = cluster
        self.read_your_writes_timeout = read_your_writes_timeout
        self._sessions = {}

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

    def put(self, user_id: str, key: str, value: str):
        """Escreve via líder e atualiza o tempo da sessão."""
        session = self._get_or_create_session(user_id)
        session["last_write_time"] = time.time()
        node = session["assigned_follower"]
        self.cluster.put(node, key, value)

    def get(self, user_id: str, key: str):
        """Lê aplicando read-your-own-writes e leituras monotônicas."""
        session = self._get_or_create_session(user_id)
        elapsed = time.time() - session["last_write_time"]
        if elapsed < self.read_your_writes_timeout:
            node = session["assigned_follower"]
            return self.cluster.get(node, key)
        node = session["assigned_follower"]
        return self.cluster.get(node, key)
