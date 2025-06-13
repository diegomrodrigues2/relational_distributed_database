import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lsm_db import SimpleLSMDB
from vector_clock import VectorClock


class VectorClockMergeTest(unittest.TestCase):
    def test_concurrent_writes_merge(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = SimpleLSMDB(db_path=tmpdir, max_memtable_size=10)

            vc_a = VectorClock({'A': 1})
            vc_b = VectorClock({'B': 1})
            db.put('k', 'va', vector_clock=vc_a)
            db.put('k', 'vb', vector_clock=vc_b)

            records = db.get_record('k')
            self.assertEqual(len(records), 2)
            values = sorted(v for v, _ in records)
            self.assertEqual(values, ['va', 'vb'])

            vc_merge = VectorClock({'A': 1, 'B': 1})
            db.put('k', 'va', vector_clock=vc_merge)

            records = db.get_record('k')
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0][0], 'va')

            db.close()


if __name__ == '__main__':
    unittest.main()
