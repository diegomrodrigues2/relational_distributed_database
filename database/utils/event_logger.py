import os
import time
import threading
from collections import deque

class EventLogger:
    """Thread-safe event logger writing timestamped messages to a file.

    In addition to persisting events to disk, the logger keeps the most
    recent messages in memory so they can be served quickly via the API.
    """

    def __init__(self, log_path: str, *, max_events: int = 1000) -> None:
        self.log_path = log_path
        self._lock = threading.Lock()
        self._events = deque(maxlen=max_events)
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        # allow reading appended lines from other processes
        self._fp = open(log_path, "a+", encoding="utf-8")
        self._fp.seek(0, os.SEEK_END)
        self._read_pos = self._fp.tell()

    def close(self) -> None:
        """Close the underlying log file."""
        with self._lock:
            self._fp.close()

    def log(self, message: str) -> None:
        """Append ``message`` to the log file with timestamp."""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        entry = f"[{timestamp}] {message}"
        with self._lock:
            self._fp.write(entry + "\n")
            self._fp.flush()
            self._events.append(entry)

    def sync(self) -> None:
        """Read any new log lines written by other processes."""
        with self._lock:
            self._fp.flush()
            self._fp.seek(self._read_pos)
            for line in self._fp:
                self._events.append(line.rstrip("\n"))
            self._read_pos = self._fp.tell()
            self._fp.seek(0, os.SEEK_END)

    def get_events(self, offset: int = 0, limit: int | None = None) -> list[str]:
        """Return recent log entries stored in memory."""
        with self._lock:
            entries = list(self._events)
        if offset < 0:
            offset = 0
        end = offset + limit if limit is not None else None
        return entries[offset:end]
