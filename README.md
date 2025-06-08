# py_distributed_database

Este projeto demonstra de forma simplificada um banco de dados baseado em **Log-Structured Merge Tree (LSM)** com replicação assíncrona. O código implementa conceitos de WAL, MemTable, SSTables e compactação, além de um gerenciador de réplica com um líder e seguidores que podem ficar offline.

## Como executar

O projeto utiliza apenas Python 3, sem dependências externas. Para rodar a simulação padrão, execute:

```bash
python main.py
```

Por padrão, o script chama `test_replication()` ao final, mostrando as operações de escrita, leitura e a replicação entre nós.

### Testes disponíveis

- **`test_replication()`** – Simula um cluster com um líder e seguidores. Demonstra a replicação assíncrona, a leitura de seguidores offline e a reintegração de um nó.
- **`test_simple_lsm_database()`** – Exercita apenas a implementação do banco LSM local. Realiza operações de PUT/GET, atualização, DELETE, compactação e teste de recuperação a partir do WAL.

Para executar o segundo teste, edite o bloco final de `main.py` substituindo a chamada a `test_replication()` por `test_simple_lsm_database()`.

## Requisitos

- Python 3.x (bibliotecas padrão)
