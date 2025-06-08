"""Servidor gRPC simplificado para expor operacoes do banco de dados.

Esta implementacao eh utilizada pelos seguidores no processo de replicacao.
"""

import grpc
from concurrent import futures
import threading
import time
from lsm_db import SimpleLSMDB
from . import replication_pb2
from . import replication_pb2_grpc


db_instance = None
last_leader_heartbeat = time.time()

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


class HeartbeatService(replication_pb2_grpc.HeartbeatServiceServicer):
    """Servico simples para heartbeat."""

    def Ping(self, request, context):
        global last_leader_heartbeat
        # Quando o líder envia um heartbeat, registramos a hora
        if request.node_id == 'leader':
            last_leader_heartbeat = time.time()
        return replication_pb2.Empty()


def run_server(db_path, host='localhost', port=8000, leader_host=None, leader_port=None, node_id='follower'):
    """Inicializa o servidor gRPC do seguidor e threads de heartbeat."""

    global db_instance
    db_instance = SimpleLSMDB(db_path=db_path)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_ReplicaServicer_to_server(ReplicaService(), server)
    replication_pb2_grpc.add_HeartbeatServiceServicer_to_server(HeartbeatService(), server)
    server.add_insecure_port(f'{host}:{port}')
    server.start()
    print(f'gRPC node server running on {host}:{port}')

    # Se informado, inicia thread que envia heartbeats ao líder
    if leader_host and leader_port:
        channel = grpc.insecure_channel(f'{leader_host}:{leader_port}')
        stub = replication_pb2_grpc.HeartbeatServiceStub(channel)

        def _send_heartbeat():
            req = replication_pb2.Heartbeat(node_id=node_id)
            while True:
                try:
                    stub.Ping(req)
                except Exception as e:
                    print(f'Falha ao enviar heartbeat ao líder: {e}')
                time.sleep(1)

        threading.Thread(target=_send_heartbeat, daemon=True).start()

    # Monitora recebimento de heartbeat do líder
    def _check_leader():
        while True:
            time.sleep(2)
            if time.time() - last_leader_heartbeat > 3:
                print('Líder inativo para este seguidor.')

    threading.Thread(target=_check_leader, daemon=True).start()

    server.wait_for_termination()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run gRPC replica server')
    parser.add_argument('--path', required=True, help='Database path')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--leader_host', default=None)
    parser.add_argument('--leader_port', type=int, default=None)
    parser.add_argument('--node_id', default='follower')
    args = parser.parse_args()
    run_server(args.path, args.host, args.port, args.leader_host, args.leader_port, args.node_id)
