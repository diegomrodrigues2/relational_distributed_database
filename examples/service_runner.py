import os
import subprocess

try:
    from pyngrok import ngrok  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    ngrok = None


def start_services(tunnel: bool = False):
    """Start API and frontend servers, optionally tunneling via ngrok."""
    api_proc = subprocess.Popen([
        "uvicorn",
        "api.main:app",
        "--port",
        "8000",
    ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT
    )
    frontend_proc = subprocess.Popen(
        ["npm", "run", "dev"],
        cwd=os.path.join(os.path.dirname(__file__), "..", "app"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    if tunnel and ngrok:
        api_url = ngrok.connect(8000, bind_tls=True).public_url
        ui_url = ngrok.connect(5173, bind_tls=True).public_url
    else:
        api_url = "http://localhost:8000"
        ui_url = "http://localhost:5173"
    print(f"API running at {api_url}")
    print(f"Frontend running at {ui_url}")
    return api_proc, frontend_proc
