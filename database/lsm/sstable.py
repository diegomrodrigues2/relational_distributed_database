import os
import json
import time
from ..utils.vector_clock import VectorClock
from ..clustering.partitioning import compose_key

SSTABLE_SPARSE_INDEX_INTERVAL = 100 # Intervalo para o índice esparso (a cada 100 linhas, por exemplo)
TOMBSTONE = "__TOMBSTONE__" # Marcador para exclusão

def bisect_left(array, value):
    """Retorna o índice de inserção ordenada."""
    left = 0
    right = len(array)

    while left < right:
        mid = (left + right) // 2

        if array[mid] < value:
            left = mid + 1
        else:
            right = mid
    
    return left


def _merge_version_lists(current, new_list):
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

class SSTableManager:

    def __init__(self, sstable_dir: str) -> None:
        """Gerencia arquivos SSTable no disco."""
        self.sstable_dir = sstable_dir
        self.sstable_segments = []

        self._load_existing_sstables()
        print(f"SSTableManager inicializado. {len(self.sstable_segments)} SSTables existentes carregados.")

    def _load_existing_sstables(self):
        """Carrega SSTables existentes e seus índices."""
        files = sorted(os.listdir(self.sstable_dir))
        for filename in files:
            if filename.endswith(".txt"):
                path = os.path.join(self.sstable_dir, filename)
                timestamp_str = filename.split('_')[1].split('.')[0] # Ex: sstable_16788899000.txt -> 16788899000
                try:
                    timestamp = int(timestamp_str)
                except ValueError:
                    timestamp = 0 # Fallback for malformed names
                
                sparse_index = self._build_sparse_index(path)
                self.sstable_segments.append((timestamp, path, sparse_index))
        # Ordena os segmentos do mais antigo para o mais novo
        self.sstable_segments.sort(key=lambda x: x[0])
        print(f"  SSTableManager: Carregou {len(self.sstable_segments)} SSTables do disco.")

    def _build_sparse_index(self, sstable_path):
        """Cria índice esparso para um SSTable."""
        sparse_index = []
        with open(sstable_path, "r", encoding="utf-8") as file:
            offset = 0
            for idx, line in enumerate(file):
                if idx % SSTABLE_SPARSE_INDEX_INTERVAL == 0:
                    try:
                        data = json.loads(line)
                        key_part = data["key"]
                    except Exception:
                        key_part = ""
                    sparse_index.append({"key": key_part, "offset": offset})
                offset += len(line.encode("utf-8"))
        print(
            f"  SSTableManager: Índice esparso construído para {os.path.basename(sstable_path)} com {len(sparse_index)} entradas."
        )
        return sparse_index
    
    def write_sstable(self, sorted_items):
        """Escreve itens ordenados em novo SSTable."""
        timestamp = int(time.time() * 1000)
        sstable_filename = f"sstable_{timestamp}.txt"
        sstable_path = os.path.join(self.sstable_dir, sstable_filename)

        os.makedirs(self.sstable_dir, exist_ok=True)

        with open(sstable_path, "w", encoding="utf-8") as f:
            for key, value, vector in sorted_items:
                entry = {"key": key, "value": value, "vector": vector.clock}
                f.write(json.dumps(entry) + "\n")

        sparse_index = self._build_sparse_index(sstable_path)
        # Adiciona o novo SSTable ao final (ele é o mais recente)
        self.sstable_segments.append((timestamp, sstable_path, sparse_index))
        self.sstable_segments.sort(key=lambda x: x[0]) # Re-ordena para garantir o mais novo no final
        print(f"  SSTableManager: Novo SSTable '{sstable_filename}' escrito com {len(sorted_items)} itens.")
        return sstable_path

    def get_from_sstable(self, sstable_entry, key, *, clustering_key=None):
        """Busca chave em um SSTable usando o índice esparso."""
        composed = compose_key(key, clustering_key)
        _, sstable_path, sparse_index = sstable_entry
        print(f"  SSTableManager: Buscando '{composed}' em {os.path.basename(sstable_path)}...")

        with open(sstable_path, 'r', encoding='utf-8') as f:
            start_offset = 0
            search_keys = [entry["key"] for entry in sparse_index]

            # bisect_left retorna um ponto de inserção que mantém a ordem
            # Se a chave for menor que a primeira entrada, start_idx será 0.
            # Se a chave for maior que a última, start_idx será len(sparse_index).
            start_idx = bisect_left(search_keys, composed)

            if start_idx > 0:
                # Se start_idx é maior que 0, significa que a chave pode estar a partir da entrada anterior no índice
                # Ou a partir da entrada em start_idx se ela for exatamente a chave buscada.
                # Para garantir que pegamos o bloco correto, buscamos a partir do último ponto de índice menor ou igual à chave.
                # Como bisect_left encontra o ponto de inserção, o elemento ANTES desse ponto é o maior <= key
                if start_idx == len(sparse_index) or search_keys[start_idx] != composed:
                    start_offset = sparse_index[start_idx - 1]["offset"]
                else: # key é exatamente um dos sparse_index keys
                    start_offset = sparse_index[start_idx]["offset"]
            
            f.seek(start_offset)

            # Varredura linear a partir do offset encontrado
            for line in f:
                try:
                    data = json.loads(line)
                    current_key = data.get("key")
                    value = data.get("value")
                    vector = VectorClock(data.get("vector", {}))
                except Exception:
                    continue

                if current_key == composed:
                    if value == TOMBSTONE:
                        print(f"  SSTableManager: Encontrado tombstone para '{composed}'.")
                        return [(TOMBSTONE, vector)]
                    print(f"  SSTableManager: '{composed}' encontrado em {os.path.basename(sstable_path)}.")
                    return [(value, vector)]
                elif current_key > composed:
                    # Como o arquivo é ordenado, se a chave atual é maior que a chave buscada,
                    # a chave buscada não está neste SSTable.
                    break
        
        print(f"  SSTableManager: '{composed}' não encontrado em {os.path.basename(sstable_path)}.")
        return None

    def compact_segments(self):
        """Compacta todos os SSTables em um novo."""
        if len(self.sstable_segments) <= 1:
            print("  SSTableManager: Não há segmentos suficientes para compactar.")
            return

        print(f"  SSTableManager: Iniciando compactação de {len(self.sstable_segments)} segmentos...")

        # Para garantir que a versão mais recente prevaleça,
        # iteramos sobre os segmentos do mais novo para o mais antigo.
        # Assim, o último valor encontrado para uma chave será o mais recente.
        merged_data = {}

        # Iterar do mais novo para o mais antigo para garantir que a versão mais recente seja mantida
        segments_to_merge = sorted(self.sstable_segments, key=lambda x: x[0], reverse=True)

        for _, sstable_path, _ in segments_to_merge:
            print(f"    SSTableManager: Lendo {os.path.basename(sstable_path)} para compactação...")

            with open(sstable_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        key = data.get('key')
                        value = data.get('value')
                        vc = VectorClock(data.get('vector', {}))
                    except Exception:
                        continue
                    merged_data[key] = _merge_version_lists(merged_data.get(key, []), [(value, vc)])

        # Remove tombstones da lista final de dados
        final_merged_data = {
            k: [tpl for tpl in v if tpl[0] != TOMBSTONE] for k, v in merged_data.items()
        }

        sorted_merged_items = []
        for k, vers in sorted(final_merged_data.items()):
            for val, vc in vers:
                sorted_merged_items.append((k, val, vc))

        # Escreve o novo SSTable compactado
        new_timestamp = int(time.time() * 1000)
        new_sstable_filename = f"sstable_compacted_{new_timestamp}.txt"
        new_sstable_path = os.path.join(self.sstable_dir, new_sstable_filename)

        with open(new_sstable_path, 'w', encoding='utf-8') as f:
            for key, value, vc in sorted_merged_items:
                entry = {"key": key, "value": value, "vector": vc.clock}
                f.write(json.dumps(entry) + "\n")

        new_sparse_index = self._build_sparse_index(new_sstable_path)

        # Atualiza a lista de segmentos: remove os antigos e adiciona o novo
        old_segments_paths = [s[1] for s in self.sstable_segments]
        self.sstable_segments = [(new_timestamp, new_sstable_path, new_sparse_index)]
        
        # Deleta os arquivos antigos
        for old_path in old_segments_paths:
            try:
                os.remove(old_path)
                print(f"    SSTableManager: Deletado SSTable antigo: {os.path.basename(old_path)}")
            except OSError as e:
                print(f"    SSTableManager: Erro ao deletar {os.path.basename(old_path)}: {e}")
        
        print(f"  SSTableManager: Compactação concluída. Novo SSTable: '{new_sstable_filename}'.")
        print(f"  SSTableManager: Agora temos {len(self.sstable_segments)} SSTables no disco.")





