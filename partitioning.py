import hashlib


def hash_key(key: str) -> int:
    """Return a stable integer hash for ``key`` using SHA-1."""
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return int(h, 16)


def compose_key(partition_key: str, clustering_key: str | None = None) -> str:
    """Compose partition and clustering components into a sortable key.

    The representation preserves lexicographic ordering for the pair. When
    no ``clustering_key`` is provided, the function simply returns the
    ``partition_key`` to keep backwards compatibility with existing
    single-part keys.
    """
    if clustering_key is None or clustering_key == "":
        return str(partition_key)
    return f"{partition_key}|{clustering_key}"

