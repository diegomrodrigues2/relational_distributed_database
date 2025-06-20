import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from index_manager import IndexManager


class IndexManagerTest(unittest.TestCase):
    def test_register_and_query(self):
        idx = IndexManager()
        idx.register_field("name")
        idx.add_record("1", {"name": "alice", "age": 30})
        self.assertEqual(idx.query("name", "alice"), {"1"})

        idx.register_field("age")
        idx.add_record("2", {"name": "bob", "age": 30})
        self.assertEqual(idx.query("age", 30), {"1", "2"})

    def test_remove_record(self):
        idx = IndexManager()
        idx.register_field("name")
        idx.add_record("1", {"name": "alice"})
        idx.add_record("2", {"name": "alice"})
        idx.remove_record("1")
        self.assertEqual(idx.query("name", "alice"), {"2"})
        idx.remove_record("2")
        self.assertEqual(idx.query("name", "alice"), set())


if __name__ == "__main__":
    unittest.main()
