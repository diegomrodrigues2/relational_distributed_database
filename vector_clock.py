class VectorClock:
    """Simple vector clock implementation."""

    def __init__(self, initial=None):
        self.clock = dict(initial) if initial else {}

    def increment(self, node_id: str) -> int:
        """Increment counter for given node and return new value."""
        self.clock[node_id] = self.clock.get(node_id, 0) + 1
        return self.clock[node_id]

    def merge(self, other: "VectorClock") -> None:
        """Merge another vector clock into this one taking max of counters."""
        for node, counter in other.clock.items():
            if counter > self.clock.get(node, 0):
                self.clock[node] = counter

    def compare(self, other: "VectorClock") -> str | None:
        """Compare two vector clocks.

        Returns '>' if this clock is causally after ``other``,
        '<' if it is causally before ``other`` or ``None`` if
        they are concurrent or equal.
        """
        greater = False
        less = False
        nodes = set(self.clock) | set(other.clock)
        for node in nodes:
            a = self.clock.get(node, 0)
            b = other.clock.get(node, 0)
            if a > b:
                greater = True
            elif a < b:
                less = True
        if greater and not less:
            return ">"
        if less and not greater:
            return "<"
        return None
