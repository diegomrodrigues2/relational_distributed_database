import os
import platform
import subprocess


def start_frontend() -> subprocess.Popen:
    """Start the React UI in the background."""
    cmd = ["npm", "run", "dev"]
    proc = subprocess.Popen(
        cmd,
        cwd=os.path.join(os.path.dirname(__file__), "..", "app"),
        shell=platform.system() == "Windows",
    )
    ui_url = "http://localhost:5173"
    print(f"Frontend running at {ui_url}")
    return proc


def start_services() -> tuple[subprocess.Popen, subprocess.Popen]:
    """Start API and frontend servers."""
    cmd = ["uvicorn", "api.main:app", "--port", "8000"]
    api_proc = subprocess.Popen(cmd, shell=platform.system() == "Windows")
    frontend_proc = start_frontend()
    api_url = "http://localhost:8000"
    print(f"API running at {api_url}")
    return api_proc, frontend_proc
