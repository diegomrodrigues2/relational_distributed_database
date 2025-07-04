import os
import time
import threading

class EventLogger:
    """Thread-safe event logger writing timestamped messages to a file."""

    def __init__(self, log_path: str) -> None:
        self.log_path = log_path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        self._fp = open(log_path, "a", encoding="utf-8")

    def log(self, message: str) -> None:
        """Append ``message`` to the log file with timestamp."""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with self._lock:
            self._fp.write(f"[{timestamp}] {message}\n")
            self._fp.flush()
