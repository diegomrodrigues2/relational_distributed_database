import time
import random
from replication import ReplicationManager

class Driver:
    """Interface entre usuários e o cluster garantindo certas consistências."""

    def __init__(self, replication_manager: ReplicationManager, read_your_writes_timeout: int = 5) -> None:
        """Cria o driver e prepara o dicionário de sessões."""
        self.replication_manager = replication_manager
        self.read_your_writes_timeout = read_your_writes_timeout
        self._sessions = {}

    def _get_or_create_session(self, user_id: str) -> dict:
        """Retorna a sessão existente ou cria uma nova."""
        if user_id not in self._sessions:
            num_followers = len(self.replication_manager._all_followers)
            follower = random.randint(0, num_followers - 1) if num_followers > 0 else 0
            self._sessions[user_id] = {
                "last_write_time": 0,
                "assigned_follower": follower,
            }
        return self._sessions[user_id]

    def put(self, user_id: str, key: str, value: str):
        """Escreve via líder e atualiza o tempo da sessão."""
        session = self._get_or_create_session(user_id)
        session["last_write_time"] = time.time()
        return self.replication_manager.put(key, value)

    def get(self, user_id: str, key: str):
        """Lê aplicando read-your-own-writes e leituras monotônicas."""
        session = self._get_or_create_session(user_id)
        elapsed = time.time() - session["last_write_time"]
        if elapsed < self.read_your_writes_timeout:
            return self.replication_manager.get(key, read_from_leader=True)
        follower_id = session["assigned_follower"]
        return self.replication_manager.get(key, read_from_leader=False, follower_id=follower_id)
