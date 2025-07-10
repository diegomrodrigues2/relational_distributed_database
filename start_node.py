"""Simple CLI for running a single NodeServer.

Usage:
    python start_node.py [--id N] [--port P] [--http-port HP] \
        [--data-dir PATH] [--peers H:P,H2:P2] [--registry-host HOST] [--registry-port PORT]

Values default to environment variables NODE_ID, GRPC_PORT, API_PORT,
DATA_DIR, PEERS, REGISTRY_HOST and REGISTRY_PORT when set.
"""

import argparse
import os
from typing import List, Tuple

from database.replication.replica.grpc_server import NodeServer


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    env = os.environ
    parser = argparse.ArgumentParser(description="Start a single NodeServer")
    parser.add_argument("--id", default=env.get("NODE_ID", "node"))
    parser.add_argument("--port", type=int, default=int(env.get("GRPC_PORT", 8000)))
    parser.add_argument(
        "--http-port",
        dest="http_port",
        type=int,
        default=int(env.get("API_PORT", 8001)),
    )
    parser.add_argument("--data-dir", default=env.get("DATA_DIR", "."))
    parser.add_argument("--peers", default=env.get("PEERS", ""))
    parser.add_argument("--registry-host", default=env.get("REGISTRY_HOST"))
    parser.add_argument(
        "--registry-port",
        type=lambda v: int(v) if v is not None else None,
        default=env.get("REGISTRY_PORT"),
    )
    return parser.parse_args(argv)


def _parse_peers(peers_str: str) -> List[Tuple[str, int]]:
    peers: List[Tuple[str, int]] = []
    for item in filter(None, peers_str.split(",")):
        host, port = item.split(":", 1)
        peers.append((host, int(port)))
    return peers


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    peers = _parse_peers(args.peers)
    node = NodeServer(
        db_path=args.data_dir,
        host="0.0.0.0",
        port=args.port,
        node_id=args.id,
        peers=peers,
        registry_host=args.registry_host,
        registry_port=args.registry_port,
    )
    node.start()


if __name__ == "__main__":
    main()
