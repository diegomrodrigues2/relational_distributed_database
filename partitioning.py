import hashlib


def hash_key(key: str) -> int:
    """Return a stable integer hash for ``key`` using SHA-1."""
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return int(h, 16)

