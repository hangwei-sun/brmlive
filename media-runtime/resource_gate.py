"""Host-local FIFO admission shared by ASR, news and material workers.

Install this single module on PYTHONPATH for all three services. SQLite and lock
files must be on a local filesystem, owned by their shared service group.
"""
import contextlib
import json
import os
import pathlib
import re
import sqlite3
import time
import uuid

PROFILES = {"asr": (4, 0, 1), "encode": (2, 1, 0), "cpu": (2, 0, 0)}
CAPACITY = (6, 2, 1)  # CPU thread budget, NVENC tasks, ASR tasks


def lock_file(file, blocking=False):
    if os.name == "nt":
        import msvcrt
        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        fcntl.flock(file, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))


class Gate:
    def __init__(self, root):
        self.root = pathlib.Path(root)
        if self.root.is_symlink() or not self.root.is_dir():
            raise RuntimeError("Shared resource directory is not provisioned")
        if (self.root / 'admission.sqlite').is_symlink():
            raise RuntimeError('Unsafe resource database')
        with self.db() as db:
            db.execute("CREATE TABLE IF NOT EXISTS admission (seq INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE, profile TEXT, state TEXT, queued_at REAL, started_at REAL)")
            columns = {row[1] for row in db.execute('PRAGMA table_info(admission)')}
            for column in ('queued_at', 'started_at'):
                if column not in columns:
                    db.execute('ALTER TABLE admission ADD COLUMN ' + column + ' REAL')

    @contextlib.contextmanager
    def db(self):
        path = self.root / 'admission.sqlite'
        db = sqlite3.connect(str(path), timeout=5)
        if os.name != 'nt':
            try:
                os.chmod(path, 0o660)
            except PermissionError:
                if path.stat().st_mode & 0o777 != 0o660:
                    db.close()
                    raise RuntimeError('Resource database needs shared group permissions') from None
        db.execute("PRAGMA busy_timeout=5000")
        try:
            with db:
                yield db
        finally:
            db.close()

    def reap(self, db):
        # Kernel lock remains held by an inherited FFmpeg descriptor even if its
        # Python parent crashes. Never recycle a live/possibly orphaned process.
        for key, in db.execute("SELECT key FROM admission").fetchall():
            if not isinstance(key, str) or not re.fullmatch('[a-f0-9]{32}', key):
                raise RuntimeError('Invalid resource allocation')
            p = self.root / (key + ".lock")
            try:
                with os.fdopen(os.open(p, os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0)), 'r+b') as f:
                    lock_file(f)
                db.execute("DELETE FROM admission WHERE key=?", (key,))
                p.unlink(missing_ok=True)
            except FileNotFoundError:
                db.execute("DELETE FROM admission WHERE key=?", (key,))
            except (BlockingIOError, PermissionError, OSError):
                pass

    @contextlib.contextmanager
    def acquire(self, profile, cancel=None, waiting=None, timeout=1200):
        if profile not in PROFILES or timeout <= 0:
            raise ValueError("Unknown resource profile")
        key = uuid.uuid4().hex
        p = self.root / (key + ".lock")
        file = os.fdopen(os.open(p, os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o660), 'r+b')
        if os.name != 'nt':
            os.fchmod(file.fileno(), 0o660)
        file.write(b"0"); file.flush()
        lock_file(file)
        deadline = time.monotonic() + timeout
        try:
            with self.db() as db:
                db.execute("BEGIN IMMEDIATE")
                self.reap(db)
                if db.execute("SELECT COUNT(*) FROM admission").fetchone()[0] >= 32:
                    raise RuntimeError("Shared media queue is full")
                db.execute("INSERT INTO admission(key,profile,state,queued_at) VALUES(?,?,'waiting',?)", (key, profile, time.time()))
            while True:
                if cancel and cancel():
                    raise RuntimeError("Media admission cancelled")
                if time.monotonic() >= deadline:
                    raise TimeoutError("Shared media queue timeout")
                with self.db() as db:
                    db.execute("BEGIN IMMEDIATE")
                    self.reap(db)
                    rows = db.execute("SELECT key,profile,state FROM admission ORDER BY seq").fetchall()
                    queued = [r[0] for r in rows if r[2] == "waiting"]
                    used = tuple(sum(PROFILES[r[1]][i] for r in rows if r[2] == "running") for i in range(3))
                    if queued and queued[0] == key and all(used[i] + PROFILES[profile][i] <= CAPACITY[i] for i in range(3)):
                        db.execute("UPDATE admission SET state='running',started_at=? WHERE key=?", (time.time(), key))
                        break
                    position = queued.index(key) + 1
                if waiting:
                    waiting(position)
                time.sleep(0.25)
            yield file.fileno()
        finally:
            # Keep the lock until the allocation is removed transactionally.
            try:
                with self.db() as db:
                    db.execute("DELETE FROM admission WHERE key=?", (key,))
            finally:
                file.close()
                p.unlink(missing_ok=True)

    def snapshot(self):
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            self.reap(db)
            rows = db.execute('SELECT profile,state,queued_at,started_at FROM admission').fetchall()
        now = time.time()
        return {'capacity': dict(zip(('cpu_threads', 'nvenc_tasks', 'asr_tasks'), CAPACITY)),
                'waiting': sum(r[1] == 'waiting' for r in rows),
                'running': sum(r[1] == 'running' for r in rows),
                'oldest_wait_seconds': max([max(0, now-r[2]) for r in rows if r[1] == 'waiting' and r[2]] or [0]),
                'longest_running_seconds': max([max(0, now-r[3]) for r in rows if r[1] == 'running' and r[3]] or [0])}


@contextlib.contextmanager
def admission(profile, cancel=None, waiting=None, timeout=1200):
    root = os.environ.get("MEDIA_RESOURCE_ROOT", "")
    if not root:
        yield None  # explicit backwards-compatible rollout switch
    else:
        with Gate(root).acquire(profile, cancel, waiting, timeout) as fd:
            yield fd


def inherited(fd):
    return {"pass_fds": (fd,)} if fd is not None and os.name != "nt" else {}


if __name__ == "__main__":
    gate = Gate(os.environ["MEDIA_RESOURCE_ROOT"])
    print(json.dumps(gate.snapshot()))
