import json
import random
from database.clustering.partitioning import compose_key


COLORS = ["red", "blue", "green", "yellow", "purple"]


def generate_index_items(num: int = 10):
    """Yield keys and JSON values with a random color field."""
    for i in range(1, num + 1):
        key = f"p{i}"
        value = json.dumps({"color": random.choice(COLORS)})
        yield key, value


def generate_hash_items(num: int = 10):
    """Yield simple key/value pairs."""
    for i in range(1, num + 1):
        yield f"k{i}", f"v{i}"


def generate_range_items(num: int = 10):
    """Yield composed keys suitable for range partitioning."""
    for i in range(1, num + 1):
        letter = chr(ord('a') + (i - 1) % 26)
        yield compose_key(letter, str(i)), f"v{i}"
