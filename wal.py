import os
import time

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
    
    def append(self, entry_type, key, value, timestamp=None):
        """Adiciona registro ao WAL.

        Se ``timestamp`` não for fornecido, usa o horário atual em
        milissegundos. O timestamp é armazenado juntamente ao valor para
        permitir ordenação entre réplicas.
        """
        if timestamp is None:
            timestamp = int(time.time() * 1000)  # ms
        entry = f"{timestamp}|{entry_type}|{key}|{value}"
        with open(self.wal_file_path, 'a') as file:
            file.write(entry + "\n")

    def read_all(self):
        """Retorna todas as entradas do WAL."""
        entries = []
        if not os.path.exists(self.wal_file_path):
            return entries

        with open(self.wal_file_path, 'r') as f:
            for line in f:
                parts = line.strip().split('|', 3)
                if len(parts) == 4:
                    ts, entry_type, key, value = parts
                    ts = int(ts)
                    entries.append((ts, entry_type, key, (value, ts)))

        return entries
    
    def clear(self):
        """Limpa o WAL."""
        open(self.wal_file_path, 'w').close()
