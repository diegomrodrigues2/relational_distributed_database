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


def generate_cart_items(num: int = 10):
    """Yield cart id keys mapping to a JSON list of product ids and quantities."""
    for i in range(1, num + 1):
        key = f"cart{i}"
        items = []
        for _ in range(random.randint(1, 5)):
            pid = f"p{random.randint(1, num)}"
            qty = random.randint(1, 3)
            items.append({"id": pid, "qty": qty})
        yield key, json.dumps(items)


def generate_product_catalog(num: int = 10):
    """Yield product id keys mapping to JSON objects with name and price."""
    for i in range(1, num + 1):
        key = f"p{i}"
        price = round(random.uniform(5.0, 100.0), 2)
        value = json.dumps({"name": f"Product {i}", "price": price})
        yield key, value
