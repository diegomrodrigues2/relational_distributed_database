import os
import subprocess


def start_frontend() -> subprocess.Popen:
    """Start the React UI in the background."""
    proc = subprocess.Popen(
        ["npm", "run", "dev"],
        cwd=os.path.join(os.path.dirname(__file__), "..", "app"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    ui_url = "http://localhost:5173"
    print(f"Frontend running at {ui_url}")
    return proc


def start_services() -> tuple[subprocess.Popen, subprocess.Popen]:
    """Start API and frontend servers."""
    api_proc = subprocess.Popen([
        "uvicorn",
        "api.main:app",
        "--port",
        "8000",
    ])
    frontend_proc = start_frontend()
    api_url = "http://localhost:8000"
    print(f"API running at {api_url}")
    return api_proc, frontend_proc
