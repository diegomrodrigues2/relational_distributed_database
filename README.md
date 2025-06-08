# py_distributed_database

Este projeto demonstra de forma simplificada um banco de dados baseado em **Log-Structured Merge Tree (LSM)** com replicação assíncrona. O código implementa conceitos de WAL, MemTable, SSTables e compactação, além de um gerenciador de réplica com um líder e seguidores que podem ficar offline.

## Como executar

O projeto utiliza apenas Python 3, sem dependências externas. Para ver um exemplo rápido de uso, execute:

```bash
python main.py
```

O script inicializa um pequeno cluster, grava uma chave e exibe a leitura do líder e de um seguidor.

## Rodando os testes

Os testes unitários ficam no diretório `tests`. Para executá-los utilize:

```bash
python -m unittest discover -s tests -v
```

## Requisitos

- Python 3.x (bibliotecas padrão)
