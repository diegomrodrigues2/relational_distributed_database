import os
import json
from vector_clock import VectorClock

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
    
    def append(self, entry_type, key, value, vector_clock=None):
        """Adiciona registro ao WAL com o vetor associado."""
        if vector_clock is None:
            vector_clock = VectorClock()
        entry = {
            "type": entry_type,
            "key": key,
            "value": value,
            "vector": vector_clock.clock,
        }
        with open(self.wal_file_path, "a", encoding="utf-8") as file:
            file.write(json.dumps(entry) + "\n")

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
                    entries.append(
                        (
                            0,
                            data.get("type"),
                            data.get("key"),
                            (data.get("value"), VectorClock(data.get("vector", {}))),
                        )
                    )
                except json.JSONDecodeError:
                    continue

        return entries
    
    def clear(self):
        """Limpa o WAL."""
        open(self.wal_file_path, 'w').close()
