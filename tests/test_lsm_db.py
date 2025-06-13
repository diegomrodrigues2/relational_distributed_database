import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lsm_db import SimpleLSMDB
from vector_clock import VectorClock

class SimpleLSMDBTest(unittest.TestCase):
    def test_put_get_and_flush(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = SimpleLSMDB(db_path=tmpdir, max_memtable_size=2)
            db.put('k1', 'v1')
            db.put('k2', 'v2')
            self.assertEqual(db.get('k1'), 'v1')
            self.assertEqual(db.get('k2'), 'v2')

            # Trigger flush by exceeding max_memtable_size
            db.put('k3', 'v3')
            self.assertEqual(db.get('k3'), 'v3')
            wal_path = os.path.join(tmpdir, 'write_ahead_log.txt')
            with open(wal_path, 'r') as f:
                lines = f.readlines()
            # After the flush for k1 and k2, only the entry for k3 should remain
            self.assertEqual(len(lines), 1)
            db.close()

    def test_delete_and_compaction(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = SimpleLSMDB(db_path=tmpdir, max_memtable_size=2)
            db.put('k1', 'v1')
            db._flush_memtable_to_sstable()
            db.delete('k1')
            db._flush_memtable_to_sstable()
            db.wait_for_compaction()
            self.assertIsNone(db.get('k1'))
            self.assertEqual(len(db.sstable_manager.sstable_segments), 1)
            db.close()

    def test_recovery_from_wal(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = SimpleLSMDB(db_path=tmpdir, max_memtable_size=10)
            db.put('k1', 'v1')
            # Do not close or flush to keep data only in WAL
            del db
            db2 = SimpleLSMDB(db_path=tmpdir, max_memtable_size=10)
            self.assertEqual(db2.get('k1'), 'v1')
            db2.close()

    def test_get_record(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = SimpleLSMDB(db_path=tmpdir, max_memtable_size=10)
            db.put('k1', 'v1')
            records = db.get_record('k1')
            self.assertEqual(len(records), 1)
            value, vc = records[0]
            self.assertEqual(value, 'v1')
            self.assertIsInstance(vc, VectorClock)
            db.close()

if __name__ == '__main__':
    unittest.main()
