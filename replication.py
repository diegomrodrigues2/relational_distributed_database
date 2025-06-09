import os
import shutil
import multiprocessing
import grpc
from lsm_db import SimpleLSMDB
from replica.grpc_server import run_server, HeartbeatService
from concurrent import futures
import threading
import time
from grpc_health.v1 import health, health_pb2_grpc, health_pb2
from replica import replication_pb2
from replica import replication_pb2_grpc


class GRPCReplicaClient:
    """Cliente gRPC para interagir com uma réplica."""

    def __init__(self, host: str, port: int) -> None:
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = replication_pb2_grpc.ReplicaStub(self.channel)
        self.hb_stub = replication_pb2_grpc.HeartbeatServiceStub(self.channel)

    def put(self, key, value):
        request = replication_pb2.KeyValue(key=key, value=value)
        self.stub.Put(request)

    def delete(self, key):
        request = replication_pb2.KeyRequest(key=key)
        self.stub.Delete(request)

    def get(self, key):
        request = replication_pb2.KeyRequest(key=key)
        response = self.stub.Get(request)
        return response.value if response.value else None

    def heartbeat(self, node_id: str):
        """Envia mensagem de heartbeat para este nó."""
        request = replication_pb2.Heartbeat(node_id=node_id)
        self.hb_stub.Ping(request)

    def close(self):
        self.channel.close()


def _start_leader_heartbeat_server(host: str, port: int, manager):
    """Inicia um pequeno servidor gRPC para receber heartbeats dos seguidores."""

    class ManagerNode:
        def __init__(self, mgr):
            self._mgr = mgr

        def record_heartbeat(self, node_id: str):
            self._mgr.last_heartbeat_from[node_id] = time.time()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    replication_pb2_grpc.add_HeartbeatServiceServicer_to_server(
        HeartbeatService(ManagerNode(manager)), server
    )
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    print(f"Heartbeat server do líder escutando em {host}:{port}")
    return server

class ReplicationManager:
    """Gerencia um cluster com replicação assíncrona."""
    def __init__(self, base_path: str = "base_path", num_followers: int = 2) -> None:
        """Cria líder e seguidores para o cluster."""
        print(f"--- Iniciando Gerenciador de Replicação com {num_followers} seguidores ---")
        self.base_path = base_path
        # Garante um ambiente limpo para o teste
        if os.path.exists(self.base_path):
            shutil.rmtree(self.base_path)
        os.makedirs(self.base_path)

        # 1. Inicializar o Líder
        leader_path = os.path.join(self.base_path, "leader")
        self.leader = SimpleLSMDB(db_path=leader_path)
        
        # 2. Inicializar os Seguidores em processos separados
        self._all_followers = []
        self._follower_processes = []
        base_port = 9000

        # Porta usada para o servidor de heartbeat do líder
        self.leader_hb_port = base_port + num_followers
        self.last_heartbeat_from = {}
        self._leader_hb_server = None
        self._stopped = False

        for i in range(num_followers):
            follower_path = os.path.join(self.base_path, f"follower_{i}")
            port = base_port + i
            p = multiprocessing.Process(target=run_server, args=(follower_path, 'localhost', port, 'localhost', self.leader_hb_port, f'follower_{i}'), daemon=True)
            p.start()
            client = GRPCReplicaClient('localhost', port)
            self._all_followers.append(client)
            self._follower_processes.append(p)

        # Servidor de heartbeat do líder é iniciado após os forks para evitar
        # problemas de threads compartilhados pelo gRPC
        self._leader_hb_server = _start_leader_heartbeat_server('localhost', self.leader_hb_port, self)

        # dá tempo para os servidores iniciarem
        import time
        time.sleep(1)

        # A lista `online_followers` contém os seguidores que estão ativos
        self.online_followers = list(self._all_followers)

        # Inicia threads de heartbeat para monitorar seguidores
        self._threads = []
        for idx, follower in enumerate(self._all_followers):
            t = threading.Thread(target=self._send_heartbeat_loop, args=(follower, idx))
            t.start()
            self._threads.append(t)

        monitor = threading.Thread(target=self._monitor_followers)
        monitor.start()
        self._threads.append(monitor)

    def _replicate_operation(self, operation, key, value):
        """Replica a operação para seguidores online."""
        print(f"  REPLICATING: Enviando '{operation}({key})' para {len(self.online_followers)} seguidores online.")
        for i, follower in enumerate(self.online_followers):
            try:
                if operation == "PUT":
                    follower.put(key, value)
                elif operation == "DELETE":
                    follower.delete(key)
                print(f"    -> Réplica para Seguidor {i} concluída.")
            except Exception as e:
                print(f"    -> FALHA na réplica para Seguidor {i}: {e}")

    def put(self, key, value):
        """Escreve no líder e replica a operação."""
        print(f"\n>>> MANAGER: Recebido PUT('{key}', '{value}')")
        
        # 1. Escreve no líder. Esta é a única operação que o cliente espera.
        self.leader.put(key, value)
        print(">>> MANAGER: Escrita no Líder concluída e confirmada para o cliente (simulado).")
        
        # 2. Replicação assíncrona: A função retorna ao cliente antes que isso termine.
        # Em nossa simulação, chamamos isso sequencialmente, mas o conceito é o mesmo.
        self._replicate_operation("PUT", key, value)
        
        return "OK (acknowledged by leader)"

    def delete(self, key):
        """Exclui chave via líder e replica."""
        print(f"\n>>> MANAGER: Recebido DELETE('{key}')")
        self.leader.delete(key)
        print(">>> MANAGER: Exclusão no Líder concluída e confirmada.")
        
        self._replicate_operation("DELETE", key, None) # Valor não importa para delete
        return "OK (acknowledged by leader)"

    def get(self, key, read_from_leader=True, follower_id=0):
        """Lê chave do líder ou de um seguidor."""
        if read_from_leader:
            print(f"\n>>> MANAGER: Lendo '{key}' do LÍDER.")
            return self.leader.get(key)
        else:
            if follower_id < len(self._all_followers):
                follower = self._all_followers[follower_id]
                # Verifica se o seguidor está online para a leitura
                if follower in self.online_followers:
                    print(f"\n>>> MANAGER: Lendo '{key}' do SEGUIDOR {follower_id} (ONLINE).")
                    return follower.get(key)
                else:
                    print(f"\n>>> MANAGER: Tentativa de ler '{key}' do SEGUIDOR {follower_id} (OFFLINE).")
                    return "Error: Follower is offline."
            else:
                return "Error: Invalid follower ID."

    def take_follower_offline(self, follower_id):
        """Simula a queda de um seguidor."""
        if follower_id < len(self._all_followers):
            follower_to_disable = self._all_followers[follower_id]
            if follower_to_disable in self.online_followers:
                self.online_followers.remove(follower_to_disable)
                print(f"\n*** SIMULAÇÃO: Seguidor {follower_id} está agora OFFLINE. ***")
            else:
                print(f"\n*** SIMULAÇÃO: Seguidor {follower_id} já estava offline. ***")
    
    def bring_follower_online(self, follower_id):
        """Reativa um seguidor sem recuperar dados."""
        if follower_id < len(self._all_followers):
            follower_to_enable = self._all_followers[follower_id]
            if follower_to_enable not in self.online_followers:
                self.online_followers.append(follower_to_enable)
                print(f"\n*** SIMULAÇÃO: Seguidor {follower_id} está agora ONLINE. Ele pode ter dados desatualizados. ***")
            else:
                print(f"\n*** SIMULAÇÃO: Seguidor {follower_id} já estava online. ***")

    def shutdown(self):
        """Encerra todos os processos de seguidores."""
        self._stopped = True
        if hasattr(self, '_leader_hb_server'):
            # Aguarda parada do servidor de heartbeat para evitar threads
            # pendentes que podem causar falhas ao final dos testes
            self._leader_hb_server.stop(0).wait()
        # Aguarda termino das threads auxiliares
        for t in getattr(self, '_threads', []):
            t.join(timeout=1)
        for p in getattr(self, '_follower_processes', []):
            if p.is_alive():
                p.terminate()
            p.join()
        for f in self._all_followers:
            f.close()

    # --- Métodos auxiliares para heartbeat ---
    def _send_heartbeat_loop(self, follower, idx):
        while not self._stopped:
            try:
                follower.heartbeat('leader')
            except Exception as e:
                print(f'Falha ao enviar heartbeat para seguidor {idx}: {e}')
            time.sleep(1)

    def _monitor_followers(self):
        while not self._stopped:
            time.sleep(2)
            now = time.time()
            for i, follower in enumerate(self._all_followers):
                last = self.last_heartbeat_from.get(f'follower_{i}', 0)
                if now - last > 3:
                    if follower in self.online_followers:
                        self.online_followers.remove(follower)
                        print(f'Seguidor {i} considerado OFFLINE pelo heartbeat')
                else:
                    if follower not in self.online_followers:
                        self.online_followers.append(follower)
                        print(f'Seguidor {i} voltou ONLINE pelo heartbeat')


