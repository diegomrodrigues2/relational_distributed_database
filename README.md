# py_distributed_database

Este projeto demonstra uma implementação simplificada de um banco de dados distribuído em Python. A combinação de uma LSM Tree local e a replicação assíncrona entre nós usando gRPC ilustra conceitos presentes em sistemas de banco de dados modernos.

## Principais componentes

- **Write-Ahead Log (WAL)** – registra cada operação de escrita antes que seja aplicada, garantindo durabilidade.
- **MemTable** – estrutura em memória baseada em Árvore Rubro-Negra para inserções e leituras rápidas.
- **SSTables** – arquivos ordenados e imutáveis no disco que armazenam os dados de forma permanente, incluindo tombstones para deleções.
- **Compactação** – mescla SSTables mais antigas, removendo registros obsoletos e otimizando a leitura.
- **Replicador líder-seguidor** – o líder recebe as escritas e as propaga para os seguidores por gRPC sem esperar confirmação (replicação assíncrona).
- **Heartbeat com failover sequencial** – monitora a disponibilidade dos nós e promove o próximo seguidor se o líder falhar.
- **Encerramento limpo** – o cluster pode ser iniciado e finalizado repetidamente sem deixar processos órfãos.
- **Driver** – interface opcional que direciona leituras de forma a garantir "read-your-own-writes" e leituras monotônicas para cada usuário.

## Executando

1. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
2. Rode o exemplo principal:
   ```bash
   python main.py
   ```
   O script inicializa um pequeno cluster com um líder e dois seguidores, grava uma chave e realiza a leitura no líder e em um dos seguidores para evidenciar a replicação.
Para simular failover automatico, invoque `cluster.simulate_leader_failure()` no exemplo.

Se desejar garantias de consistência para cada usuário, utilize o `Driver`:
```python
from replication import ReplicationManager
from driver import Driver

cluster = ReplicationManager(num_followers=2)
driver = Driver(cluster)
driver.put("alice", "k", "v")
value = driver.get("alice", "k")
cluster.shutdown()
```

## Testes

Para rodar a bateria de testes unitários:
```bash
python -m unittest discover -s tests -v
```

## Estrutura dos arquivos

- `main.py` – exemplo de inicialização do `ReplicationManager`.
- `lsm_db.py`, `mem_table.py`, `wal.py` e `sstable.py` – compõem a LSM Tree.
- `replication.py` e diretório `replica/` – implementação do cluster e serviços gRPC.
- `tests/` – testes unitários do projeto.
