import os
import shutil
import multiprocessing
import urllib.request
import urllib.parse
from lsm_db import SimpleLSMDB, TOMBSTONE
from replica_server import run_server


class HTTPReplicaClient:
    """Cliente simples para interagir com um replica_server."""

    def __init__(self, host: str, port: int) -> None:
        """Configura o endereço base do servidor."""
        self.base = f"http://{host}:{port}"

    def put(self, key, value):
        """Envia PUT ao servidor."""
        params = urllib.parse.urlencode({"key": key, "value": value})
        url = f"{self.base}/put?{params}"
        req = urllib.request.Request(url, method="POST")
        urllib.request.urlopen(req)

    def delete(self, key):
        """Envia DELETE ao servidor."""
        params = urllib.parse.urlencode({"key": key})
        url = f"{self.base}/delete?{params}"
        req = urllib.request.Request(url, method="POST")
        urllib.request.urlopen(req)

    def get(self, key):
        """Executa GET no servidor."""
        params = urllib.parse.urlencode({"key": key})
        url = f"{self.base}/get?{params}"
        with urllib.request.urlopen(url) as resp:
            data = resp.read().decode()
        return data if data else None

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
        for i in range(num_followers):
            follower_path = os.path.join(self.base_path, f"follower_{i}")
            port = base_port + i
            p = multiprocessing.Process(target=run_server, args=(follower_path, 'localhost', port), daemon=True)
            p.start()
            client = HTTPReplicaClient('localhost', port)
            self._all_followers.append(client)
            self._follower_processes.append(p)

        # dá tempo para os servidores iniciarem
        import time
        time.sleep(1)

        # A lista `online_followers` contém os seguidores que estão ativos
        self.online_followers = list(self._all_followers)

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
        for p in getattr(self, '_follower_processes', []):
            if p.is_alive():
                p.terminate()

