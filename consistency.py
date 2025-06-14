from enum import Enum, auto

class Consistency(Enum):
    ONE = auto()
    QUORUM = auto()
    ALL = auto()

def level_to_quorum(level: 'Consistency', replication_factor: int) -> tuple[int, int]:
    if level == Consistency.ONE:
        return 1, 1
    if level == Consistency.ALL:
        return replication_factor, replication_factor
    q = replication_factor // 2 + 1
    return q, q
