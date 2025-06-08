from replication import ReplicationManager
def main():
    """Exemplo simples de uso do banco de dados replicado."""
    cluster = ReplicationManager(num_followers=2)
    cluster.put("exemplo:chave", "valor")
    print("Valor no líder:", cluster.get("exemplo:chave", read_from_leader=True))
    print("Valor em um seguidor:", cluster.get("exemplo:chave", read_from_leader=False, follower_id=0))
    cluster.shutdown()


if __name__ == "__main__":
    main()
