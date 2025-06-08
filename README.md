# py_distributed_database

Este projeto demonstra de forma simplificada um banco de dados baseado em **Log-Structured Merge Tree (LSM)** com replicação assíncrona. A comunicação entre nós utiliza **gRPC** para reduzir a latência. O código implementa conceitos de WAL, MemTable, SSTables e compactação, além de um gerenciador de réplica com um líder e seguidores que podem ficar offline.

## Como executar

O projeto utiliza Python 3 e requer as bibliotecas `grpcio` e `grpcio-tools` listadas em `requirements.txt`. Para ver um exemplo rápido de uso, execute:

```bash
python main.py
```

O script inicializa um pequeno cluster, grava uma chave e exibe a leitura do líder e de um seguidor.

Os arquivos de definição do gRPC encontram-se no diretório `replica/`.

## Heartbeat

O gerenciador implementa um mecanismo simples de heartbeat para detectar falhas.
Cada seguidor executa um pequeno servidor gRPC que recebe `Ping` do líder e também envia heartbeats periodicamente.
Se um lado deixar de receber heartbeats por alguns segundos, o nó é considerado offline.

## Rodando os testes


Os testes unitários ficam no diretório `tests`. Para executá-los utilize:

```bash
python -m unittest discover -s tests -v
```

## Requisitos

- Python 3.x
- grpcio
- grpcio-tools
