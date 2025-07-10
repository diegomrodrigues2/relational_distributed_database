import os
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import start_node


class StartNodeArgsTest(unittest.TestCase):
    def test_env_and_cli_parsing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env = {
                "NODE_ID": "env_node",
                "GRPC_PORT": "9100",
                "API_PORT": "8001",
                "DATA_DIR": tmpdir,
                "PEERS": "a:1,b:2",
                "REGISTRY_HOST": "reg",
                "REGISTRY_PORT": "7000",
            }
            args = [
                "--id",
                "cli",
                "--port",
                "9200",
                "--data-dir",
                os.path.join(tmpdir, "data"),
                "--peers",
                "c:3",
                "--registry-port",
                "8000",
            ]
            with patch.dict(os.environ, env, clear=True):
                with patch("start_node.NodeServer") as MockServer:
                    start_node.main(args)
                    MockServer.assert_called_once_with(
                        db_path=os.path.join(tmpdir, "data"),
                        host="0.0.0.0",
                        port=9200,
                        node_id="cli",
                        peers=[("c", 3)],
                        registry_host="reg",
                        registry_port=8000,
                    )
                    MockServer.return_value.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
