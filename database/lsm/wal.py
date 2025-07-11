import os
import json
from ..utils.vector_clock import VectorClock
from ..clustering.partitioning import compose_key

class WriteAheadLog(object):
    """Log de pré-escrita para garantir durabilidade."""

    def __init__(self, wal_file_path: str) -> None:
        self.wal_file_path = wal_file_path
        self._ensure_file_exists()
        print(f"WAL inicializado: {self.wal_file_path}")
    
    def _ensure_file_exists(self):
        """Cria o arquivo se não existir."""
        if not os.path.exists(self.wal_file_path):
            with open(self.wal_file_path, 'w') as f:
                pass # Apenas cria o arquivo
    
    def _write_entry(self, entry: dict) -> None:
        """Write ``entry`` to the WAL file and flush to disk."""
        with open(self.wal_file_path, "a", encoding="utf-8") as file:
            file.write(json.dumps(entry) + "\n")
            file.flush()
            os.fsync(file.fileno())

    def append(
        self, entry_type, key, value, vector_clock=None, *, clustering_key=None
    ):
        """Adiciona registro ao WAL com o vetor associado."""
        if vector_clock is None:
            vector_clock = VectorClock()
        composed = compose_key(key, clustering_key)
        entry = {
            "type": entry_type,
            "key": composed,
            "value": value,
            "vector": vector_clock.clock,
        }
        self._write_entry(entry)

    def append_update_with_index(
        self,
        key: str,
        old_value,
        new_value,
        index_fields: list[str],
        vector_clock: VectorClock | None = None,
    ) -> None:
        """Record an update that also touches secondary indexes."""
        if vector_clock is None:
            vector_clock = VectorClock()
        entry = {
            "type": "UPDATE_WITH_INDEX",
            "key": key,
            "old": old_value,
            "new": new_value,
            "indexes": list(index_fields),
            "vector": vector_clock.clock,
        }
        self._write_entry(entry)

    def read_all(self):
        """Retorna todas as entradas do WAL."""
        entries = []
        if not os.path.exists(self.wal_file_path):
            return entries

        with open(self.wal_file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    val_key = "value"
                    if data.get("type") == "UPDATE_WITH_INDEX":
                        val_key = "new"
                    entries.append(
                        (
                            0,
                            data.get("type"),
                            data.get("key"),
                            (
                                data.get(val_key),
                                VectorClock(data.get("vector", {})),
                            ),
                        )
                    )
                except json.JSONDecodeError:
                    continue

        return entries
    
    def clear(self):
        """Limpa o WAL."""
        open(self.wal_file_path, 'w').close()
