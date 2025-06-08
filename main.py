import os
import shutil
from lsm_db import *
from replication import ReplicationManager

def test_replication():
 # Inicializa o cluster com 1 líder e 2 seguidores
    cluster = ReplicationManager(num_followers=2)
    
    # --- Teste 1: Escrita e leitura consistentes ---
    print("\n--- TESTE 1: Escrita e Leitura em Cluster Saudável ---")
    cluster.put("user:123:name", "Alice")
    
    # A leitura do líder e dos seguidores deve retornar "Alice"
    val_leader = cluster.get("user:123:name", read_from_leader=True)
    val_follower0 = cluster.get("user:123:name", read_from_leader=False, follower_id=0)
    val_follower1 = cluster.get("user:123:name", read_from_leader=False, follower_id=1)
    
    print(f"\nLeitura do Líder: {val_leader}")
    print(f"Leitura do Seguidor 0: {val_follower0}")
    print(f"Leitura do Seguidor 1: {val_follower1}")
    assert val_leader == val_follower0 == val_follower1 == "Alice"
    
    # --- Teste 2: Demonstrando Replicação Assíncrona com Falha ---
    print("\n--- TESTE 2: Simulação de Falha e Replicação Lag ---")
    
    # 1. Derrubar o seguidor 1
    cluster.take_follower_offline(follower_id=1)
    
    # 2. Fazer uma nova escrita. Ela só será replicada para o seguidor 0.
    cluster.put("user:123:name", "Bob")
    
    # 3. Ler de todos os nós
    val_leader = cluster.get("user:123:name", read_from_leader=True)
    val_follower0 = cluster.get("user:123:name", read_from_leader=False, follower_id=0)
    val_follower1 = cluster.get("user:123:name", read_from_leader=False, follower_id=1)

    print(f"\nApós escrita com Seguidor 1 offline:")
    print(f"Leitura do Líder: {val_leader}")             # Deve ser "Bob"
    print(f"Leitura do Seguidor 0: {val_follower0}")     # Deve ser "Bob"
    print(f"Leitura do Seguidor 1: {val_follower1}") # Deve dar erro, pois está offline

    assert val_leader == "Bob"
    assert val_follower0 == "Bob"
    assert "Error" in val_follower1

    # 4. Trazer o seguidor 1 de volta online
    cluster.bring_follower_online(follower_id=1)

    # 5. Ler do seguidor 1 novamente. Ele terá dados antigos (stale data).
    stale_val_follower1 = cluster.get("user:123:name", read_from_leader=False, follower_id=1)
    
    print(f"\nApós Seguidor 1 voltar online:")
    print(f"Leitura (desatualizada) do Seguidor 1: {stale_val_follower1}") # Deve ser "Alice"!
    assert stale_val_follower1 == "Alice" # Este é o efeito do "replication lag"
    
    # --- Teste 3: Nova escrita sincroniza todos os nós online ---
    print("\n--- TESTE 3: Nova Escrita Sincroniza Nós Online ---")
    cluster.put("user:456:email", "carol@example.com")

    # Agora todos os nós online devem ter o novo dado
    val_leader = cluster.get("user:456:email", read_from_leader=True)
    val_follower0 = cluster.get("user:456:email", read_from_leader=False, follower_id=0)
    val_follower1 = cluster.get("user:456:email", read_from_leader=False, follower_id=1)

    print(f"\nLeitura do Líder: {val_leader}")
    print(f"Leitura do Seguidor 0: {val_follower0}")
    print(f"Leitura do Seguidor 1: {val_follower1}") # Agora deve ter o novo dado
    assert val_leader == val_follower0 == val_follower1 == "carol@example.com"
    
    # No entanto, o seguidor 1 ainda não tem a atualização de "Bob"
    final_val_f1 = cluster.get("user:123:name", read_from_leader=False, follower_id=1)
    print(f"\nVerificação final do Seguidor 1 para 'user:123:name': {final_val_f1}")
    assert final_val_f1 == "Alice"
    print("\nSimulação concluída com sucesso!")    


def test_simple_lsm_database():
    # Limpa dados antigos para um teste limpo

    db = SimpleLSMDB()

    # --- Teste de PUT e GET ---
    print("\n--- Operações de PUT e GET ---")
    db.put("nome", "Alice") # MemTable
    db.put("idade", "30")   # MemTable
    db.put("cidade", "São Paulo") # MemTable

    print(f"Resultado GET 'nome': {db.get('nome')}") # Deve vir do MemTable
    print(f"Resultado GET 'idade': {db.get('idade')}") # Deve vir do MemTable
    print(f"Resultado GET 'pais': {db.get('pais')}")   # Não deve encontrar

    # Adicionar mais dados para forçar o flush do MemTable
    # Ajuste MAX_MEMTABLE_SIZE para testar o flush
    for i in range(1, 10000 + 5):
        db.put(f"chave_{i}", f"valor_{i}")

    print(f"\nResultado GET 'chave_1': {db.get('chave_1')}") # Deve vir de um SSTable
    print(f"Resultado GET 'cidade': {db.get('cidade')}") # Deve vir de um SSTable

    # --- Teste de Atualização (Overwrite) ---
    print("\n--- Operações de Atualização ---")
    db.put("nome", "Bob") # Atualiza Alice para Bob no MemTable
    print(f"Resultado GET 'nome' após atualização: {db.get('nome')}") # Deve vir do MemTable (Bob)

    # Forçar outro flush para mover 'nome:Bob' para um novo SSTable
    db._flush_memtable_to_sstable()
    print(f"Resultado GET 'nome' após flush: {db.get('nome')}") # Deve vir do SSTable mais recente (Bob)

    # --- Teste de DELETE ---
    print("\n--- Operações de DELETE ---")
    db.delete("idade") # Marca 'idade' com tombstone no MemTable
    print(f"Resultado GET 'idade' após DELETE: {db.get('idade')}") # Deve retornar None (tombstone no MemTable)

    # Forçar flush para mover 'idade:TOMBSTONE' para um SSTable
    db._flush_memtable_to_sstable()
    print(f"Resultado GET 'idade' após flush de DELETE: {db.get('idade')}") # Deve retornar None (tombstone no SSTable)

    # --- Teste de Compactação ---
    print("\n--- Operações de Compactação ---")
    # Agora 'idade' está em um SSTable antigo e um novo com tombstone
    # 'nome' está em um SSTable antigo ('Alice') e um novo ('Bob')
    
    # Adicionar mais dados para garantir que haja mais SSTables
    for i in range(1000 + 5, 1000 * 2 + 5):
        db.put(f"outra_chave_{i}", f"outro_valor_{i}")
    db._flush_memtable_to_sstable()

    print(f"Número de SSTables antes da compactação: {len(db.sstable_manager.sstable_segments)}")
    db.compact_all_data() # Força a compactação de todos os SSTables
    print(f"Número de SSTables após a compactação: {len(db.sstable_manager.sstable_segments)}")

    # Verificar dados após compactação
    print(f"Resultado GET 'nome' após compactação: {db.get('nome')}") # Deve ser 'Bob'
    print(f"Resultado GET 'idade' após compactação: {db.get('idade')}") # Deve ser None
    print(f"Resultado GET 'chave_1' após compactação: {db.get('chave_1')}") # Deve ser 'valor_1'
    print(f"Resultado GET 'outra_chave_{1000 + 10}' após compactação: {db.get(f'outra_chave_{1000 + 10}')}")

    # --- Teste de Recuperação (Simulando uma queda) ---
    print("\n--- Teste de Recuperação (Simulando uma queda) ---")
    db.put("chave_recuperacao", "valor_em_wal")
    db.put("outra_chave_recuperacao", "valor_temporario")
    # Não flushes (simula queda com dados no MemTable e WAL)
    print("Simulando queda (não chamando close() ou flush())...")
    del db 
    
    print("\nReiniciando o banco de dados para testar recuperação...")
    db_restarted = SimpleLSMDB()
    print(f"Resultado GET 'chave_recuperacao' após reinício: {db_restarted.get('chave_recuperacao')}") # Deve ser recuperado do WAL
    print(f"Resultado GET 'outra_chave_recuperacao' após reinício: {db_restarted.get('outra_chave_recuperacao')}") # Deve ser recuperado do WAL
    
    db_restarted.close()


if __name__ == "__main__":
    test_replication()