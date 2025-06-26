"""Componentes relacionados ao servidor de réplica usando gRPC."""

# Garantimos que os módulos gerados pelo protobuf possam ser importados
# usando o nome simples ``replication_pb2`` utilizado nos arquivos gerados
# pelo gRPC.
from importlib import import_module
import sys

_pb2 = import_module("replica.replication_pb2")
sys.modules.setdefault("replication_pb2", _pb2)
_router_pb2 = import_module("replica.router_pb2")
sys.modules.setdefault("router_pb2", _router_pb2)
_metadata_pb2 = import_module("replica.metadata_pb2")
sys.modules.setdefault("metadata_pb2", _metadata_pb2)
