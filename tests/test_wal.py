import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.lsm.wal import WriteAheadLog
from database.utils.vector_clock import VectorClock

class WriteAheadLogTest(unittest.TestCase):
    def test_append_writes_newlines_and_read_all(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "wal.txt")
            wal = WriteAheadLog(wal_path)
            wal.append("PUT", "k1", "v1")
            wal.append("PUT", "k2", "v2")

            # Ensure each entry was written on its own line
            with open(wal_path, "r") as f:
                lines = f.readlines()
            self.assertEqual(len(lines), 2)

            # Ensure read_all parses entries correctly
            entries = wal.read_all()
            self.assertEqual(len(entries), 2)
            t1, e1, k1, v1 = entries[0]
            t2, e2, k2, v2 = entries[1]
            self.assertEqual((e1, k1, v1[0]), ("PUT", "k1", "v1"))
            self.assertIsInstance(v1[1], VectorClock)
            self.assertEqual((e2, k2, v2[0]), ("PUT", "k2", "v2"))
            self.assertIsInstance(v2[1], VectorClock)

if __name__ == "__main__":
    unittest.main()
