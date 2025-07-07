import os
import tempfile
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.utils.event_logger import EventLogger


class EventLoggerSyncTest(unittest.TestCase):
    def test_sync_reads_appended_lines(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = os.path.join(tmpdir, "log.txt")
            writer = EventLogger(log_path)
            reader = EventLogger(log_path)
            writer.log("first")
            reader.sync()
            events = reader.get_events()
            self.assertTrue(any("first" in e for e in events))
            writer.close()
            reader.close()


if __name__ == "__main__":
    unittest.main()
