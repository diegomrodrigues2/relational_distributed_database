import os
import threading
from mem_table import MemTable
from sstable import SSTableManager, TOMBSTONE
from wal import WriteAheadLog

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

    def _start_compaction_async(self):
        """Inicia a compactação em uma thread de forma assíncrona."""
        if self._compaction_thread and self._compaction_thread.is_alive():
            return

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
        # Aplica as entradas do WAL no MemTable, garantindo a última escrita para cada chave
        for _, entry_type, key, value in wal_entries:
            # Em uma recuperação real, você também precisaria verificar se o SSTable mais recente
            # já contém essa entrada para evitar reprocessar dados já persistidos.
            # Para simplicidade didática, assumimos que o WAL contém apenas dados que ainda
            # não foram descarregados para o SSTable.
            if entry_type == "PUT":
                self.memtable.put(key, value)
            elif entry_type == "DELETE":
                self.memtable.put(key, TOMBSTONE) # DELETE é um PUT de um TOMBSTONE
        print(f"Recuperação do WAL concluída. MemTable agora tem {len(self.memtable)} itens.")

    def _flush_memtable_to_sstable(self):
        """Descarrega o MemTable para SSTable e limpa o WAL."""
        if not self.memtable:
            print("  FLUSH: MemTable está vazio, nada para descarregar.")
            return

        print("  FLUSH: MemTable cheio ou trigger de flush manual. Descarregando para SSTable...")
        
        # Prepara os dados para o SSTable (ordenados por chave)
        sorted_data = self.memtable.get_sorted_items()
        
        # Escreve o SSTable
        self.sstable_manager.write_sstable(sorted_data)
        
        # Limpa o MemTable e o WAL (os dados agora estão em disco)
        self.memtable.clear()
        self.wal.clear()
        print("  FLUSH: MemTable descarregado e WAL limpo.")

        # Inicia compactação de forma assíncrona
        self._start_compaction_async()

    def put(self, key, value):
        """Insere ou atualiza uma chave."""
        key = str(key)
        value = str(value)
        self.wal.append("PUT", key, value)
        self.memtable.put(key, value)
        if self.memtable.is_full():
            self._flush_memtable_to_sstable()

    def get(self, key):
        """Retorna o valor mais recente de uma chave."""
        key = str(key)
        print(f"\nGET: Buscando chave '{key}'")
        
        # 1. Tenta encontrar no MemTable (mais recente)
        value = self.memtable.get(key)
        if value is not None:
            if value == TOMBSTONE:
                print(f"GET: '{key}' encontrado como TOMBSTONE no MemTable. Não existe.")
                return None
            print(f"GET: '{key}' encontrado no MemTable.")
            return value

        # 2. Se não encontrado, procura nos SSTables, do mais novo para o mais antigo
        # Percorre a lista de SSTables do mais novo para o mais antigo
        # (A lista sstable_segments é ordenada do mais antigo para o mais novo, então invertemos)
        for sstable_entry in reversed(self.sstable_manager.sstable_segments):
            value = self.sstable_manager.get_from_sstable(sstable_entry, key)
            if value is not None:
                if value == TOMBSTONE:
                    print(f"GET: '{key}' encontrado como TOMBSTONE em SSTable. Não existe.")
                    return None
                print(f"GET: '{key}' encontrado em SSTable.")
                return value
            
        print(f"GET: Chave '{key}' não encontrada em nenhum lugar.")
        return None

    def delete(self, key):
        """Marca uma chave como removida."""
        key = str(key)
        print(f"\nDELETE: Marcando chave '{key}' para exclusão.")
        self.wal.append("DELETE", key, TOMBSTONE)
        self.memtable.put(key, TOMBSTONE) # Marca no MemTable como tombstone
        if self.memtable.is_full():
            self._flush_memtable_to_sstable()

    def compact_all_data(self):
        """Força a compactação de todos os SSTables."""
        # Garante que qualquer coisa no memtable seja descarregada primeiro
        if len(self.memtable) > 0:
            print("\nCompactação Manual: Descarregando MemTable antes de compactar todos os SSTables.")
            self._flush_memtable_to_sstable()
            # Aguarda a compactação automática disparada pelo flush
            self.wait_for_compaction()

        # Garante que não há compacções em andamento antes da manual
        self.wait_for_compaction()

        self.sstable_manager.compact_segments()

    def close(self):
        """Descarrega dados pendentes e fecha o BD."""
        if len(self.memtable) > 0:
            print("\nFechando DB: Descarregando MemTable restante...")
            self._flush_memtable_to_sstable()
        # Garante que a compactação assíncrona termine antes de fechar
        self.wait_for_compaction()
        print("--- Banco de Dados Fechado ---")
