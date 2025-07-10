import os
import sys
import tempfile
import time
import socket
import subprocess
import threading
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import grpc
from database.clustering.metadata_service import run_metadata_service
from database.replication.replica import metadata_pb2_grpc, replication_pb2


def _get_free_port():
    s = socket.socket()
    s.bind(("localhost", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class StartNodeRegistryTest(unittest.TestCase):
    def test_node_registers_with_registry(self):
        registry_port = _get_free_port()
        thread = threading.Thread(
            target=run_metadata_service,
            kwargs={"host": "localhost", "port": registry_port},
            daemon=True,
        )
        thread.start()
        time.sleep(0.5)

        with tempfile.TemporaryDirectory() as tmpdir:
            env = os.environ.copy()
            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            env["PYTHONPATH"] = repo_root + os.pathsep + env.get("PYTHONPATH", "")
            env.update(
                {
                    "NODE_ID": "test_node",
                    "GRPC_PORT": "0",
                    "API_PORT": "0",
                    "DATA_DIR": tmpdir,
                    "REGISTRY_HOST": "localhost",
                    "REGISTRY_PORT": str(registry_port),
                }
            )
            proc = subprocess.Popen(
                [sys.executable, "-c", "import start_node; start_node.main()"],
                env=env,
            )
            try:
                time.sleep(1.0)
                channel = grpc.insecure_channel(f"localhost:{registry_port}")
                stub = metadata_pb2_grpc.MetadataServiceStub(channel)
                state = stub.GetClusterState(replication_pb2.Empty())
                node_ids = {n.node_id for n in state.nodes}
                self.assertIn("test_node", node_ids)
            finally:
                proc.terminate()
                proc.wait(timeout=5)
                channel.close()


if __name__ == "__main__":
    unittest.main()
