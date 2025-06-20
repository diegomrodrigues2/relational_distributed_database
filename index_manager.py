class IndexManager:
    """Simple in-memory secondary index manager."""

    def __init__(self) -> None:
        self._fields = set()
        self._indexes = {}
        self._records = {}

    def register_field(self, field: str) -> None:
        """Register ``field`` for indexing."""
        if not field:
            raise ValueError("field name required")
        if field in self._fields:
            return
        self._fields.add(field)
        self._indexes[field] = {}
        # Index existing records for the new field
        for rec_id, rec in self._records.items():
            if field in rec:
                value = rec[field]
                self._indexes[field].setdefault(value, set()).add(rec_id)

    def add_record(self, record_id: str, record: dict) -> None:
        """Add ``record`` with identifier ``record_id`` to indexes."""
        if record_id in self._records:
            self.remove_record(record_id)
        self._records[record_id] = dict(record)
        for field in self._fields:
            if field in record:
                value = record[field]
                self._indexes[field].setdefault(value, set()).add(record_id)

    def remove_record(self, record_id: str) -> None:
        """Remove a record from indexes."""
        record = self._records.pop(record_id, None)
        if not record:
            return
        for field in self._fields:
            if field in record:
                value = record[field]
                ids = self._indexes[field].get(value)
                if ids:
                    ids.discard(record_id)
                    if not ids:
                        del self._indexes[field][value]

    def query(self, field: str, value) -> set:
        """Return set of record ids with ``field`` equal to ``value``."""
        if field not in self._fields:
            raise KeyError(f"Unregistered field: {field}")
        return set(self._indexes.get(field, {}).get(value, set()))
