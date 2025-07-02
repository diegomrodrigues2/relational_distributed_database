import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.lsm.lsm_db import SimpleLSMDB


class DurabilityTest(unittest.TestCase):
    def test_recover_after_crash(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db1 = SimpleLSMDB(db_path=tmpdir, max_memtable_size=10)
            db1.put("k1", "v1")
            # Simulate crash by not calling close()
            db2 = SimpleLSMDB(db_path=tmpdir, max_memtable_size=10)
            self.assertEqual(db2.get("k1"), "v1")
            db2.close()

if __name__ == "__main__":
    unittest.main()
