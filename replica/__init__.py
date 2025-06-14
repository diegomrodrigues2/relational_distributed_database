"""Componentes relacionados ao servidor de réplica usando gRPC."""

# Garantimos que os módulos gerados pelo protobuf possam ser importados
# usando o nome simples ``replication_pb2`` utilizado nos arquivos gerados
# pelo gRPC.
from importlib import import_module
import sys

_pb2 = import_module("replica.replication_pb2")
sys.modules.setdefault("replication_pb2", _pb2)
