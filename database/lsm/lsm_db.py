import os
import json
import threading
import time
from .mem_table import MemTable
from .sstable import SSTableManager, TOMBSTONE
from .wal import WriteAheadLog
from ..utils.merkle import compute_segment_hashes
from ..utils.vector_clock import VectorClock
from ..clustering.partitioning import compose_key
from ..utils.event_logger import EventLogger
import logging

logger = logging.getLogger(__name__)


def _merge_version_lists(current, new_list):
    """Merge new version tuples into existing ones using vector clocks."""
    if not current:
        return list(new_list)
    result = list(current)
    for item in new_list:
        val, vc = item[0], item[1]
        created = item[2] if len(item) > 2 else None
        deleted = item[3] if len(item) > 3 else None
        add_new = True
        updated = []
        for cur in result:
            c_val, c_vc = cur[0], cur[1]
            c_created = cur[2] if len(cur) > 2 else None
            c_deleted = cur[3] if len(cur) > 3 else None
            cmp = vc.compare(c_vc)
            if cmp == ">":
                continue
            if cmp == "<":
                add_new = False
                updated.append((c_val, c_vc, c_created, c_deleted))
            else:
                if (
                    vc.clock == c_vc.clock
                    and val == c_val
                    and created == c_created
                    and deleted == c_deleted
                ):
                    add_new = False
                updated.append((c_val, c_vc, c_created, c_deleted))
        if add_new:
            updated.append((val, vc, created, deleted))
        result = updated
    return result


class SimpleLSMDB:
    """Banco de dados simples baseado em LSM."""

    def __init__(
        self,
        db_path: str = "simple_db_data",
        max_memtable_size: int = 1000,
        *,
        event_logger: EventLogger | None = None,
    ):
        """Inicializa estruturas e carrega dados do WAL."""
        self.event_logger = event_logger
        self.db_path = db_path
        self.wal_file = os.path.join(self.db_path, "write_ahead_log.txt")
        self.sstable_dir = os.path.join(self.db_path, "sstables")

        os.makedirs(self.sstable_dir, exist_ok=True)

        self.memtable = MemTable(max_memtable_size)
        self.wal = WriteAheadLog(self.wal_file)
        self.sstable_manager = SSTableManager(
            self.sstable_dir, event_logger=self.event_logger
        )
        self._compaction_thread = None
        self._last_timestamp = 0
        self._recover_from_wal()
        msg = f"--- Banco de Dados Iniciado em {self.db_path} ---"
        if self.event_logger:
            self.event_logger.log(msg)
        else:
            logger.info(msg)
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

    def _generate_timestamp(self, ts: int | None = None) -> int:
        """Return a monotonically increasing timestamp in milliseconds."""
        if ts is None:
            ts = int(time.time() * 1000)
        if ts <= self._last_timestamp:
            ts = self._last_timestamp + 1
        self._last_timestamp = ts
        return ts

    def _recover_from_wal(self):
        """Recupera o MemTable a partir do WAL."""
        msg = "Iniciando recuperação do WAL..."
        if self.event_logger:
            self.event_logger.log(msg)
        else:
            logger.info(msg)
        wal_entries = self.wal.read_all()
        for _, entry_type, key, value_tuple in wal_entries:
            # Assumimos que o WAL contém apenas dados não persistidos.
            self.memtable.put(key, value_tuple)
        msg = (
            f"Recuperação do WAL concluída. MemTable agora tem {len(self.memtable)} itens."
        )
        if self.event_logger:
            self.event_logger.log(msg)
        else:
            logger.info(msg)

    def _flush_memtable_to_sstable(self):
        """Descarrega o MemTable para SSTable e limpa o WAL."""
        if not self.memtable:
            msg = "  FLUSH: MemTable está vazio, nada para descarregar."
            if self.event_logger:
                self.event_logger.log(msg)
            else:
                logger.info(msg)
            return

        msg = (
            "  FLUSH: MemTable cheio ou trigger de flush manual. Descarregando para SSTable..."
        )
        if self.event_logger:
            self.event_logger.log(msg)
        else:
            logger.info(msg)

        # Prepara os dados para o SSTable (ordenados por chave). Pode haver
        # múltiplas versões por chave.
        sorted_data = []
        for k, versions in self.memtable.get_sorted_items():
            for val, vc, *_ in versions:
                sorted_data.append((k, val, vc))

        # Escreve o SSTable
        self.sstable_manager.write_sstable(sorted_data)

        # Limpa o MemTable e o WAL (os dados agora estão em disco)
        self.memtable.clear()
        self.wal.clear()
        msg = "  FLUSH: MemTable descarregado e WAL limpo."
        if self.event_logger:
            self.event_logger.log(msg)
        else:
            logger.info(msg)

        # Inicia compactação de forma assíncrona
        self._start_compaction_async()
        self.segment_hashes = compute_segment_hashes(self)

    def put(
        self,
        key,
        value,
        *,
        timestamp=None,
        vector_clock=None,
        clustering_key=None,
        tx_id=None,
        skip_wal: bool = False,
    ):
        """Insere ou atualiza uma chave."""
        key = compose_key(str(key), clustering_key)
        value = str(value)
        if vector_clock is None:
            timestamp = self._generate_timestamp(timestamp)
            vector_clock = VectorClock({"ts": int(timestamp)})
        if not skip_wal:
            self.wal.append("PUT", key, value, vector_clock, clustering_key=None)
        current = self.memtable.get(key) or []
        if tx_id is not None and current:
            updated = []
            for val_cur, vc_cur, *rest in current:
                created_cur = rest[0] if len(rest) > 0 else None
                deleted_cur = rest[1] if len(rest) > 1 else None
                if deleted_cur is None:
                    deleted_cur = tx_id
                updated.append((val_cur, vc_cur, created_cur, deleted_cur))
            self.memtable.set_versions(key, updated)
        self.memtable.put(key, (value, vector_clock, tx_id, None))
        if self.memtable.is_full():
            self._flush_memtable_to_sstable()

    def get(self, key, *, clustering_key=None):
        """Retorna o(s) valor(es) associado(s) à chave."""
        key = compose_key(str(key), clustering_key)
        msg = f"GET: Buscando chave '{key}'"
        if self.event_logger:
            self.event_logger.log(msg)
        else:
            logger.info(msg)

        versions = []
        record = self.memtable.get(key)
        if record:
            versions = _merge_version_lists(versions, record)

        # Protect sstable_segments access during potential compaction
        with self.sstable_manager._segments_lock:
            sstable_segments_copy = list(reversed(self.sstable_manager.sstable_segments))
        
        for sstable_entry in sstable_segments_copy:
            rec = self.sstable_manager.get_from_sstable(sstable_entry, key)
            if rec:
                versions = _merge_version_lists(versions, rec)

        versions = [v for v in versions if v[0] != TOMBSTONE]

        if not versions:
            msg = f"GET: Chave '{key}' não encontrada em nenhum lugar."
            if self.event_logger:
                self.event_logger.log(msg)
            else:
                logger.info(msg)
            return None

        if len(versions) == 1:
            msg = f"GET: '{key}' encontrado."
            if self.event_logger:
                self.event_logger.log(msg)
            else:
                logger.info(msg)
            return versions[0][0]

        msg = f"GET: '{key}' possui múltiplas versões."
        if self.event_logger:
            self.event_logger.log(msg)
        else:
            logger.info(msg)
        return [val for val, *_ in versions]

    def get_record(
        self,
        key,
        *,
        clustering_key=None,
        tx_id=None,
        in_progress: list[str] | None = None,
    ):
        """Retorna lista de ``(valor, vector_clock, created_txid, deleted_txid)`` se presente."""
        key = compose_key(str(key), clustering_key)
        versions = []
        record = self.memtable.get(key)
        if record:
            versions = _merge_version_lists(versions, record)

        # Protect sstable_segments access during potential compaction  
        with self.sstable_manager._segments_lock:
            sstable_segments_copy = list(reversed(self.sstable_manager.sstable_segments))
        
        for sstable_entry in sstable_segments_copy:
            rec = self.sstable_manager.get_from_sstable(sstable_entry, key)
            if rec:
                versions = _merge_version_lists(versions, rec)

        versions = [v for v in versions if v[0] != TOMBSTONE]

        if tx_id is not None:
            in_prog = set(in_progress or [])
            filtered = []
            for item in versions:
                val, vc = item[0], item[1]
                created = item[2] if len(item) > 2 else None
                deleted = item[3] if len(item) > 3 else None
                if created is not None and created in in_prog:
                    continue
                if deleted is not None and deleted not in in_prog:
                    continue
                filtered.append((val, vc, created, deleted))
            versions = filtered
        return versions

    def delete(
        self,
        key,
        *,
        timestamp=None,
        vector_clock=None,
        clustering_key=None,
        tx_id=None,
        skip_wal: bool = False,
    ):
        """Marca uma chave como removida."""
        key = compose_key(str(key), clustering_key)
        msg = f"DELETE: Marcando chave '{key}' para exclusão."
        if self.event_logger:
            self.event_logger.log(msg)
        else:
            logger.info(msg)
        if vector_clock is None:
            timestamp = self._generate_timestamp(timestamp)
            vector_clock = VectorClock({"ts": int(timestamp)})
        if not skip_wal:
            self.wal.append("DELETE", key, TOMBSTONE, vector_clock, clustering_key=None)
        current = self.memtable.get(key) or []
        if tx_id is not None and current:
            updated = []
            for val_cur, vc_cur, *rest in current:
                created_cur = rest[0] if len(rest) > 0 else None
                deleted_cur = rest[1] if len(rest) > 1 else None
                if deleted_cur is None:
                    deleted_cur = tx_id
                updated.append((val_cur, vc_cur, created_cur, deleted_cur))
            self.memtable.set_versions(key, updated)
        self.memtable.put(key, (TOMBSTONE, vector_clock, tx_id, None))
        if self.memtable.is_full():
            self._flush_memtable_to_sstable()

    def compact_all_data(self):
        """Força a compactação de todos os SSTables."""
        # Garante que qualquer coisa no memtable seja descarregada primeiro
        if len(self.memtable) > 0:
            msg = (
                "Compactação Manual: Descarregando MemTable antes de compactar todos os SSTables."
            )
            if self.event_logger:
                self.event_logger.log(msg)
            else:
                logger.info(msg)
            self._flush_memtable_to_sstable()
            # Aguarda a compactação automática disparada pelo flush
            self.wait_for_compaction()

        # Garante que não há compacções em andamento antes da manual
        self.wait_for_compaction()

        self.sstable_manager.compact_segments()

        self.segment_hashes = compute_segment_hashes(self)
    def recalc_merkle(self):
        self.segment_hashes = compute_segment_hashes(self)

    def scan_range(self, partition_key, start_ck, end_ck):
        """Return ordered items within [start_ck, end_ck] for partition_key."""
        start_key = compose_key(str(partition_key), start_ck)
        end_key = compose_key(str(partition_key), end_ck)
        items = {}

        prefix = f"{partition_key}|"
        for k, versions in self.memtable.get_sorted_items():
            if not k.startswith(prefix):
                continue
            if k < start_key or k > end_key:
                continue
            for val, vc, *_ in versions:
                items.setdefault(k, [])
                items[k] = _merge_version_lists(items[k], [(val, vc)])

        # Protect sstable_segments access during potential compaction
        with self.sstable_manager._segments_lock:
            sstable_segments_copy = list(reversed(self.sstable_manager.sstable_segments))
        
        for _, path, _ in sstable_segments_copy:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                        except Exception:
                            continue
                        key = data.get("key")
                        if not key or not key.startswith(prefix):
                            continue
                        if key < start_key or key > end_key:
                            continue
                        val = data.get("value")
                        vc = VectorClock(data.get("vector", {}))
                        items.setdefault(key, [])
                        items[key] = _merge_version_lists(items[key], [(val, vc)])
            except FileNotFoundError:
                # File may have been deleted by compaction, skip it
                pass

        result = []
        for k in sorted(items):
            ck = k.split("|", 1)[1] if "|" in k else ""
            versions = [v for v in items[k] if v[0] != TOMBSTONE]
            if not versions:
                continue
            best_val, best_vc, *_ = versions[0]
            best_ts = best_vc.clock.get("ts", 0)
            for val, vc, *_ in versions[1:]:
                cmp = vc.compare(best_vc)
                ts = vc.clock.get("ts", 0)
                if cmp == ">" or (cmp is None and ts > best_ts):
                    best_val, best_vc, best_ts = val, vc, ts
            result.append((ck, best_val, best_vc))

        return result

    def get_segment_items(self, segment_id):
        if segment_id == "memtable":
            res = []
            for k, versions in self.memtable.get_sorted_items():
                for val, vc, *_ in versions:
                    res.append((k, val, vc))
            return res
        
        # Protect sstable_segments access during potential compaction
        with self.sstable_manager._segments_lock:
            sstable_segments_copy = list(self.sstable_manager.sstable_segments)
        
        for ts, path, _ in sstable_segments_copy:
            name = os.path.basename(path)
            if name == segment_id:
                items = []
                try:
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
                except FileNotFoundError:
                    # File may have been deleted by compaction
                    return []
        return []

    def close(self):
        """Descarrega dados pendentes e fecha o BD."""
        if len(self.memtable) > 0:
            msg = "Fechando DB: Descarregando MemTable restante..."
            if self.event_logger:
                self.event_logger.log(msg)
            else:
                logger.info(msg)
            self._flush_memtable_to_sstable()
        # Garante que a compactação assíncrona termine antes de fechar
        self.wait_for_compaction()
        msg = "--- Banco de Dados Fechado ---"
        if self.event_logger:
            self.event_logger.log(msg)
        else:
            logger.info(msg)
