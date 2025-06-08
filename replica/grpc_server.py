"""Servidor gRPC simplificado para expor operacoes do banco de dados.

Esta implementacao eh utilizada pelos seguidores no processo de replicacao.
"""

import grpc
from concurrent import futures
from lsm_db import SimpleLSMDB
from . import replication_pb2
from . import replication_pb2_grpc


db_instance = None

class ReplicaService(replication_pb2_grpc.ReplicaServicer):
    """gRPC service exposing the database operations."""
    def Put(self, request, context):
        db_instance.put(request.key, request.value)
        return replication_pb2.Empty()

    def Delete(self, request, context):
        db_instance.delete(request.key)
        return replication_pb2.Empty()

    def Get(self, request, context):
        value = db_instance.get(request.key)
        if value is None:
            value = ""
        return replication_pb2.ValueResponse(value=value)


def run_server(db_path, host='localhost', port=8000):
    global db_instance
    db_instance = SimpleLSMDB(db_path=db_path)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_ReplicaServicer_to_server(ReplicaService(), server)
    server.add_insecure_port(f'{host}:{port}')
    server.start()
    print(f'gRPC node server running on {host}:{port}')
    server.wait_for_termination()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run gRPC replica server')
    parser.add_argument('--path', required=True, help='Database path')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8000)
    args = parser.parse_args()
    run_server(args.path, args.host, args.port)
