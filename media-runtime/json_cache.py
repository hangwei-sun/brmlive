"""Private, bounded JSON cache. No credentials, user jobs or editable results."""
import hashlib
import json
import os
import pathlib
import re
import threading
import time
import uuid


class JsonCache:
    def __init__(self, root, ttl=86400, max_bytes=64*1024*1024):
        self.root = pathlib.Path(root)
        if self.root.is_symlink(): raise ValueError('Unsafe cache root')
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.ttl, self.max_bytes = ttl, max_bytes
        self.lock = threading.RLock()

    @staticmethod
    def key(path, revision):
        digest = hashlib.sha256(revision.encode() + b'\0')
        with open(path, 'rb') as source:
            while chunk := source.read(1024*1024): digest.update(chunk)
        return digest.hexdigest()

    def path(self, key):
        if not re.fullmatch('[a-f0-9]{64}', key): raise ValueError('Invalid cache key')
        return self.root/(key+'.json')

    def get(self, key):
        with self.lock:
            path = self.path(key)
            try:
                if path.is_symlink() or path.stat().st_size > 4*1024*1024 or time.time()-path.stat().st_mtime > self.ttl:
                    return None
                return json.loads(path.read_text())
            except (OSError, ValueError): return None

    def clean(self):
        with self.lock:
            files = []
            for p in self.root.glob('*.json'):
                if not re.fullmatch(r'[a-f0-9]{64}\.json', p.name) or p.is_symlink(): continue
                stat = p.stat()
                if time.time()-stat.st_mtime > self.ttl: p.unlink()
                else: files.append((stat.st_mtime, stat.st_size, p))
            used = sum(row[1] for row in files)
            for _, size, p in sorted(files):
                if used <= self.max_bytes: break
                p.unlink(); used -= size

    def put(self, key, value):
        raw = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
        if len(raw) > min(4*1024*1024, self.max_bytes): return
        with self.lock:
            target = self.path(key)
            temp = self.root/('.'+uuid.uuid4().hex+'.partial')
            try:
                fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, 'wb') as f: f.write(raw)
                os.replace(temp, target)
            finally: temp.unlink(missing_ok=True)
            self.clean()
