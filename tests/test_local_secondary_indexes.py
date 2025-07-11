import json
import tempfile
import unittest

from database.clustering.index_manager import IndexManager
from database.lsm.lsm_db import SimpleLSMDB


class LocalSecondaryIndexTest(unittest.TestCase):
    def test_add_record(self):
        im = IndexManager(["name"])
        im.add_record("users||1", json.dumps({"name": "alice"}))
        self.assertIn("1", im.indexes["users"]["name"]["alice"])

    def test_remove_record(self):
        im = IndexManager(["name"])
        im.add_record("users||1", json.dumps({"name": "alice"}))
        im.remove_record("users||1", json.dumps({"name": "alice"}))
        self.assertNotIn("alice", im.indexes.get("users", {}).get("name", {}))

    def test_rebuild(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = SimpleLSMDB(db_path=tmpdir, max_memtable_size=10)
            db.put("users||1", json.dumps({"name": "alice"}))
            db.put("users||2", json.dumps({"name": "bob"}))
            db.close()

            db2 = SimpleLSMDB(db_path=tmpdir, max_memtable_size=10)
            im = IndexManager(["name"])
            im.rebuild(db2)
            db2.close()

            self.assertEqual(im.indexes["users"]["name"]["alice"], {"1"})
            self.assertEqual(im.indexes["users"]["name"]["bob"], {"2"})


if __name__ == "__main__":
    unittest.main()
