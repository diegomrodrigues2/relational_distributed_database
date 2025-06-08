import os
import tempfile
import unittest

from wal import WriteAheadLog

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
            self.assertEqual(entries[0][1:], ("PUT", "k1", "v1"))
            self.assertEqual(entries[1][1:], ("PUT", "k2", "v2"))

if __name__ == "__main__":
    unittest.main()
