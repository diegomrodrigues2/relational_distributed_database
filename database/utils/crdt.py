import time
import json

class GCounter:
    """Grow-only counter with per-replica state."""

    def __init__(self, replica_id: str, state: dict | None = None) -> None:
        self.replica_id = replica_id
        self.state = dict(state) if state else {}

    @property
    def value(self) -> int:
        return sum(self.state.values())

    def apply(self, amount: int = 1) -> None:
        """Apply a local increment operation."""
        self.state[self.replica_id] = self.state.get(self.replica_id, 0) + int(amount)

    def merge(self, other: "GCounter") -> None:
        """Merge another counter by taking maximum counts per replica."""
        for rid, val in other.state.items():
            if val > self.state.get(rid, 0):
                self.state[rid] = val

    def to_dict(self) -> dict:
        return dict(self.state)

    @classmethod
    def from_dict(cls, replica_id: str, data: dict) -> "GCounter":
        return cls(replica_id, state=data)


class ORSet:
    """Observed-remove set."""

    def __init__(self, replica_id: str, adds=None, removes=None) -> None:
        self.replica_id = replica_id
        self.adds = {e: set(tags) for e, tags in (adds or {}).items()}
        self.removes = {e: set(tags) for e, tags in (removes or {}).items()}

    def _next_tag(self) -> str:
        return f"{self.replica_id}:{int(time.time()*1000)}"

    @property
    def value(self) -> set:
        res = set()
        for e, tags in self.adds.items():
            if tags - self.removes.get(e, set()):
                res.add(e)
        return res

    def apply(self, op: dict) -> None:
        """Apply a local add/remove operation."""
        if op.get("op") == "add":
            elem = op["element"]
            tag = op.get("tag", self._next_tag())
            self.adds.setdefault(elem, set()).add(tag)
        elif op.get("op") == "remove":
            elem = op["element"]
            tags = self.adds.get(elem, set())
            if tags:
                self.removes.setdefault(elem, set()).update(tags)

    def merge(self, other: "ORSet") -> None:
        """Merge another OR-Set by uniting add/remove sets."""
        for e, tags in other.adds.items():
            self.adds.setdefault(e, set()).update(tags)
        for e, tags in other.removes.items():
            self.removes.setdefault(e, set()).update(tags)

    def to_dict(self) -> dict:
        return {
            "adds": {e: list(tags) for e, tags in self.adds.items()},
            "removes": {e: list(tags) for e, tags in self.removes.items()},
        }

    @classmethod
    def from_dict(cls, replica_id: str, data: dict) -> "ORSet":
        adds = {e: set(tags) for e, tags in data.get("adds", {}).items()}
        removes = {e: set(tags) for e, tags in data.get("removes", {}).items()}
        return cls(replica_id, adds=adds, removes=removes)
