import os
import json
import threading
import time
from mem_table import MemTable
from sstable import SSTableManager, TOMBSTONE
from wal import WriteAheadLog
from merkle import compute_segment_hashes
from vector_clock import VectorClock
from partitioning import compose_key


def _merge_version_lists(current, new_list):
    """Merge new version tuples into existing ones using vector clocks."""
    if not current:
        return list(new_list)
    result = list(current)
    for val, vc in new_list:
        add_new = True
        updated = []
        for c_val, c_vc in result:
            cmp = vc.compare(c_vc)
            if cmp == ">":
                # nova versão domina existente
                continue
            if cmp == "<":
                add_new = False
                updated.append((c_val, c_vc))
            else:
                if vc.clock == c_vc.clock and val == c_val:
                    add_new = False
                updated.append((c_val, c_vc))
        if add_new:
            updated.append((val, vc))
        result = updated
    return result


class SimpleLSMDB:
    """Banco de dados simples baseado em LSM."""

    def __init__(self, db_path: str = "simple_db_data", max_memtable_size: int = 1000):
        """Inicializa estruturas e carrega dados do WAL."""
        self.db_path = db_path
        self.wal_file = os.path.join(self.db_path, "write_ahead_log.txt")
        self.sstable_dir = os.path.join(self.db_path, "sstables")

        os.makedirs(self.sstable_dir, exist_ok=True)

        self.memtable = MemTable(max_memtable_size)
        self.wal = WriteAheadLog(self.wal_file)
        self.sstable_manager = SSTableManager(self.sstable_dir)
        self._compaction_thread = None
        self._recover_from_wal()
        print(f"\n--- Banco de Dados Iniciado em {self.db_path} ---")
        self.segment_hashes = compute_segment_hashes(self)

    def _start_compaction_async(self):
        """Inicia a compactação em uma thread de forma assíncrona."""
        if self._compaction_thread and self._compaction_thread.is_alive():
            self._compaction_thread.join()

        def _run():
            self.sstable_manager.compact_segments()

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        self._compaction_thread = t

    def wait_for_compaction(self):
        """Aguarda qualquer compactação assíncrona finalizar."""
        if self._compaction_thread:
            self._compaction_thread.join()
            self._compaction_thread = None

    def _recover_from_wal(self):
        """Recupera o MemTable a partir do WAL."""
        print("Iniciando recuperação do WAL...")
        wal_entries = self.wal.read_all()
        for _, entry_type, key, value_tuple in wal_entries:
            # Assumimos que o WAL contém apenas dados não persistidos.
            self.memtable.put(key, value_tuple)
        print(
            f"Recuperação do WAL concluída. MemTable agora tem {len(self.memtable)} itens."
        )

    def _flush_memtable_to_sstable(self):
        """Descarrega o MemTable para SSTable e limpa o WAL."""
        if not self.memtable:
            print("  FLUSH: MemTable está vazio, nada para descarregar.")
            return

        print(
            "  FLUSH: MemTable cheio ou trigger de flush manual. Descarregando para SSTable..."
        )

        # Prepara os dados para o SSTable (ordenados por chave). Pode haver
        # múltiplas versões por chave.
        sorted_data = []
        for k, versions in self.memtable.get_sorted_items():
            for val, vc in versions:
                sorted_data.append((k, val, vc))

        # Escreve o SSTable
        self.sstable_manager.write_sstable(sorted_data)

        # Limpa o MemTable e o WAL (os dados agora estão em disco)
        self.memtable.clear()
        self.wal.clear()
        print("  FLUSH: MemTable descarregado e WAL limpo.")

        # Inicia compactação de forma assíncrona
        self._start_compaction_async()
        self.segment_hashes = compute_segment_hashes(self)

    def put(self, key, value, *, timestamp=None, vector_clock=None, clustering_key=None):
        """Insere ou atualiza uma chave."""
        key = compose_key(str(key), clustering_key)
        value = str(value)
        if vector_clock is None:
            if timestamp is None:
                timestamp = int(time.time() * 1000)
            vector_clock = VectorClock({"ts": int(timestamp)})
        self.wal.append("PUT", key, value, vector_clock, clustering_key=None)
        self.memtable.put(key, (value, vector_clock))
        if self.memtable.is_full():
            self._flush_memtable_to_sstable()

    def get(self, key, *, clustering_key=None):
        """Retorna o(s) valor(es) associado(s) à chave."""
        key = compose_key(str(key), clustering_key)
        print(f"\nGET: Buscando chave '{key}'")

        versions = []
        record = self.memtable.get(key)
        if record:
            versions = _merge_version_lists(versions, record)

        for sstable_entry in reversed(self.sstable_manager.sstable_segments):
            rec = self.sstable_manager.get_from_sstable(sstable_entry, key)
            if rec:
                versions = _merge_version_lists(versions, rec)

        versions = [v for v in versions if v[0] != TOMBSTONE]

        if not versions:
            print(f"GET: Chave '{key}' não encontrada em nenhum lugar.")
            return None

        if len(versions) == 1:
            print(f"GET: '{key}' encontrado.")
            return versions[0][0]

        print(f"GET: '{key}' possui múltiplas versões.")
        return [v for v, _ in versions]

    def get_record(self, key, *, clustering_key=None):
        """Retorna lista de ``(valor, vector_clock)`` se presente."""
        key = compose_key(str(key), clustering_key)
        versions = []
        record = self.memtable.get(key)
        if record:
            versions = _merge_version_lists(versions, record)

        for sstable_entry in reversed(self.sstable_manager.sstable_segments):
            rec = self.sstable_manager.get_from_sstable(sstable_entry, key)
            if rec:
                versions = _merge_version_lists(versions, rec)

        versions = [v for v in versions if v[0] != TOMBSTONE]
        return versions

    def delete(self, key, *, timestamp=None, vector_clock=None, clustering_key=None):
        """Marca uma chave como removida."""
        key = compose_key(str(key), clustering_key)
        print(f"\nDELETE: Marcando chave '{key}' para exclusão.")
        if vector_clock is None:
            if timestamp is None:
                timestamp = int(time.time() * 1000)
            vector_clock = VectorClock({"ts": int(timestamp)})
        self.wal.append("DELETE", key, TOMBSTONE, vector_clock, clustering_key=None)
        self.memtable.put(key, (TOMBSTONE, vector_clock))
        if self.memtable.is_full():
            self._flush_memtable_to_sstable()

    def compact_all_data(self):
        """Força a compactação de todos os SSTables."""
        # Garante que qualquer coisa no memtable seja descarregada primeiro
        if len(self.memtable) > 0:
            print(
                "\nCompactação Manual: Descarregando MemTable antes de compactar todos os SSTables."
            )
            self._flush_memtable_to_sstable()
            # Aguarda a compactação automática disparada pelo flush
            self.wait_for_compaction()

        # Garante que não há compacções em andamento antes da manual
        self.wait_for_compaction()

        self.sstable_manager.compact_segments()

        self.segment_hashes = compute_segment_hashes(self)
    def recalc_merkle(self):
        self.segment_hashes = compute_segment_hashes(self)

    def get_segment_items(self, segment_id):
        if segment_id == "memtable":
            res = []
            for k, versions in self.memtable.get_sorted_items():
                for val, vc in versions:
                    res.append((k, val, vc))
            return res
        for ts, path, _ in self.sstable_manager.sstable_segments:
            name = os.path.basename(path)
            if name == segment_id:
                items = []
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = json.loads(line)
                            items.append(
                                (data.get("key"), data.get("value"), VectorClock(data.get("vector", {})))
                            )
                        except json.JSONDecodeError:
                            continue
                return items
        return []

    def close(self):
        """Descarrega dados pendentes e fecha o BD."""
        if len(self.memtable) > 0:
            print("\nFechando DB: Descarregando MemTable restante...")
            self._flush_memtable_to_sstable()
        # Garante que a compactação assíncrona termine antes de fechar
        self.wait_for_compaction()
        print("--- Banco de Dados Fechado ---")
