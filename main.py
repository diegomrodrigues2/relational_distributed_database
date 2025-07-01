from database.replication import NodeCluster
def main():
    """Exemplo simples de uso do banco de dados replicado."""
    cluster = NodeCluster(base_path="example_cluster", num_nodes=3)
    cluster.put(0, "exemplo", "chave", "valor")
    print("Valor no nó 0:", cluster.get(0, "exemplo", "chave"))
    print("Valor no nó 1:", cluster.get(1, "exemplo", "chave"))
    cluster.shutdown()


if __name__ == "__main__":
    main()
