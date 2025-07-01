"""Componentes relacionados ao servidor de réplica usando gRPC."""

# Garantimos que os módulos gerados pelo protobuf possam ser importados
# usando o nome simples ``replication_pb2`` utilizado nos arquivos gerados
# pelo gRPC.
from importlib import import_module
import sys

_pkg = __package__
sys.modules.setdefault("replica", sys.modules[__name__])
_pb2 = import_module(f"{_pkg}.replication_pb2")
sys.modules.setdefault("replication_pb2", _pb2)
_router_pb2 = import_module(f"{_pkg}.router_pb2")
sys.modules.setdefault("router_pb2", _router_pb2)
_metadata_pb2 = import_module(f"{_pkg}.metadata_pb2")
sys.modules.setdefault("metadata_pb2", _metadata_pb2)
