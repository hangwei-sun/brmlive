import datetime
import pathlib
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from monitor import observations, recording_health


class MonitorTest(unittest.TestCase):
    def test_unavailable_is_not_zero_and_no_private_paths_exported(self):
        with tempfile.TemporaryDirectory() as root, patch('monitor.subprocess.check_output', side_effect=OSError()):
            result = observations({'cache': root})
            self.assertTrue(result['disks']['cache']['available'])
            self.assertFalse(result['gpu']['available'])
            self.assertNotIn(root, str(result))

    def test_enabled_schedule_without_file_raises_missing_signal(self):
        with tempfile.TemporaryDirectory() as root:
            path = pathlib.Path(root)/'fixture.sqlite'
            db = sqlite3.connect(path)
            db.executescript('CREATE TABLE program(id,enabled); CREATE TABLE recording_schedule(program_id,weekday,start_time,end_time,enabled); CREATE TABLE recording_file(program_id,size_bytes,created_at); CREATE TABLE recording_execution(status,started_at); INSERT INTO program VALUES(1,1); INSERT INTO recording_schedule VALUES(1,6,"19:33:00","20:15:00",1);')
            db.commit()
            db.close()
            now = datetime.datetime(2026,10,10,13,tzinfo=datetime.timezone.utc)
            self.assertEqual(recording_health(path, now)['missing_scheduled_windows'], 1)
            db = sqlite3.connect(path)
            db.execute('INSERT INTO recording_file VALUES(1,100,"2026-10-10T12:15:00Z")'); db.commit(); db.close()
            self.assertEqual(recording_health(path, now)['missing_scheduled_windows'], 0)


if __name__ == '__main__': unittest.main()
