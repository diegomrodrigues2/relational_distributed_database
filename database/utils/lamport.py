class LamportClock:
    """Simple Lamport logical clock."""
    def __init__(self, start: int = 0) -> None:
        self.time = int(start)

    def tick(self) -> int:
        """Advance the clock for a local event."""
        self.time += 1
        return self.time

    def update(self, external_timestamp: int) -> int:
        """Merge an external timestamp and advance."""
        self.time = max(self.time, int(external_timestamp)) + 1
        return self.time
