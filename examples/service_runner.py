import os
import subprocess

try:
    from pyngrok import ngrok  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    ngrok = None


def start_frontend(tunnel: bool = False):
    """Start the React UI in the background and optionally tunnel it via ngrok."""
    proc = subprocess.Popen(
        ["npm", "run", "dev"],
        cwd=os.path.join(os.path.dirname(__file__), "..", "app"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    if tunnel and ngrok:
        ui_url = ngrok.connect(5173, bind_tls=True).public_url
    else:
        ui_url = "http://localhost:5173"
    print(f"Frontend running at {ui_url}")
    return proc


def start_services(tunnel: bool = False):
    """Start API and frontend servers, tunneling only the UI if requested."""
    api_proc = subprocess.Popen([
        "uvicorn",
        "api.main:app",
        "--port",
        "8000",
    ])
    frontend_proc = start_frontend(tunnel)
    api_url = "http://localhost:8000"
    print(f"API running at {api_url}")
    return api_proc, frontend_proc
