import concurrent.futures
import pathlib
import tempfile
import threading
import time
import subprocess
import sys
import os
import unittest
from resource_gate import Gate


class GateTest(unittest.TestCase):
    def test_combined_budget_and_cancel(self):
        with tempfile.TemporaryDirectory() as root:
            gate = Gate(root)
            entered = threading.Event()
            cancel = threading.Event()
            with gate.acquire('asr'), gate.acquire('encode'):
                def blocked():
                    with gate.acquire('encode', cancel.is_set, lambda _: entered.set()):
                        self.fail('CPU budget exceeded')
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    f = pool.submit(blocked)
                    self.assertTrue(entered.wait(3))
                    cancel.set()
                    with self.assertRaises(RuntimeError):
                        f.result(timeout=3)
            with gate.db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM admission').fetchone()[0], 0)

    def test_two_encoders_and_timeout(self):
        with tempfile.TemporaryDirectory() as root:
            gate = Gate(root)
            with gate.acquire('encode'), gate.acquire('encode'):
                with self.assertRaises(TimeoutError):
                    with gate.acquire('encode', timeout=0.1): pass

    def test_dead_queue_entry_reaped(self):
        with tempfile.TemporaryDirectory() as root:
            gate = Gate(root)
            key = 'a' * 32
            (pathlib.Path(root)/(key+'.lock')).write_bytes(b'0')
            with gate.db() as db:
                db.execute("INSERT INTO admission(key,profile,state) VALUES(?,'asr','running')", (key,))
            with gate.acquire('asr'):
                with gate.db() as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM admission').fetchone()[0], 1)

    def test_crashed_process_releases_allocation(self):
        with tempfile.TemporaryDirectory() as root:
            code = "import sys,time\nfrom resource_gate import Gate\nwith Gate(sys.argv[1]).acquire('asr'):\n print('READY',flush=True)\n time.sleep(60)"
            p = subprocess.Popen([sys.executable, '-u', '-c', code, root],
                                 cwd=pathlib.Path(__file__).parent, stdout=subprocess.PIPE, text=True)
            try:
                self.assertEqual(p.stdout.readline().strip(), 'READY')
                gate = Gate(root)
                self.assertEqual(gate.snapshot()['running'], 1)
                p.kill(); p.wait(timeout=5)
                with gate.acquire('asr', timeout=3):
                    self.assertEqual(gate.snapshot()['running'], 1)
            finally:
                if p.poll() is None: p.kill(); p.wait(timeout=5)
                p.stdout.close()

    def test_snapshot_and_waiting_callback_failure_cleaned(self):
        with tempfile.TemporaryDirectory() as root:
            gate = Gate(root)
            def fail(_): raise ValueError('callback failed')
            with gate.acquire('asr'):
                with self.assertRaises(ValueError):
                    with gate.acquire('asr', waiting=fail): pass
                self.assertEqual(gate.snapshot()['waiting'], 0)

    def test_unknown_profile_and_symlink_root_fail_closed(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(ValueError):
                with Gate(root).acquire('unknown'): pass

    @unittest.skipUnless(os.name == 'posix', 'Linux flock/pass_fds acceptance required')
    def test_orphan_child_keeps_parent_allocation_until_exit(self):
        with tempfile.TemporaryDirectory() as root:
            code = "import os,sys,time,subprocess\nfrom resource_gate import Gate\nwith Gate(sys.argv[1]).acquire('asr') as fd:\n child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],pass_fds=(fd,),stdout=subprocess.DEVNULL)\n print(child.pid,flush=True)\n os._exit(0)"
            p = subprocess.Popen([sys.executable, '-u', '-c', code, root], cwd=pathlib.Path(__file__).parent,
                                 stdout=subprocess.PIPE, text=True)
            child = None
            try:
                child = int(p.stdout.readline().strip()); p.wait(timeout=5)
                gate = Gate(root)
                self.assertEqual(gate.snapshot()['running'], 1)
                with self.assertRaises(TimeoutError):
                    with gate.acquire('asr', timeout=.1): pass
                os.kill(child, 15)
                deadline = time.monotonic()+5
                while gate.snapshot()['running'] and time.monotonic()<deadline: time.sleep(.1)
                self.assertEqual(gate.snapshot()['running'], 0)
            finally:
                p.stdout.close()
                if p.poll() is None: p.kill(); p.wait(timeout=5)
                if child:
                    try: os.kill(child, 15)
                    except ProcessLookupError: pass


if __name__ == '__main__': unittest.main()
