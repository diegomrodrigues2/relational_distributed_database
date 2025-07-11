import msgpack


class RowSerializer:
    """Simple MessagePack based row serializer."""

    @staticmethod
    def dumps(row_dict: dict) -> bytes:
        """Serialize a row dictionary to bytes using MessagePack."""
        return msgpack.packb(row_dict, use_bin_type=True)

    @staticmethod
    def loads(data: bytes) -> dict:
        """Deserialize MessagePack bytes back into a dictionary."""
        if data is None:
            return {}
        return msgpack.unpackb(data, raw=False)
