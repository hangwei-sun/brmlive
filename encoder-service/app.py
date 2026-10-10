"""Private, bounded NVENC worker. One API process, two export jobs maximum."""
import concurrent.futures
import asyncio
import hashlib
import hmac
import json
import math
import os
import pathlib
import re
import shutil
import subprocess
import threading
import time
import sys
from contextlib import asynccontextmanager, nullcontext

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

KEY = os.environ.get("ENCODER_API_KEY", "")
ALLOWED_HOSTS = set(os.environ.get("ENCODER_ALLOWED_HOSTS", "127.0.0.1,::1").split(","))
ROOT = pathlib.Path(os.environ.get("ENCODER_ROOT", "/data/brm-news-encoder"))
MAX_SOURCE = 8 * 1024**3
QUOTA = 32 * 1024**3
HEADROOM = 4 * 1024**3
manager = None


def resource_admission(cancel, waiting):
    if not os.environ.get('MEDIA_RESOURCE_ROOT'):
        return nullcontext(None)
    sys.path.insert(0, os.environ.get('MEDIA_RUNTIME_PATH', '/opt/brm-media-runtime'))
    from resource_gate import admission
    return admission('encode', cancel, waiting)


def encode_args(source, output, start, length):
    profile = os.environ.get('ENCODER_PROFILE', 'legacy')
    decoder = os.environ.get('ENCODER_DECODE', 'cpu')
    if profile not in ('legacy', 'capped') or decoder not in ('cpu', 'nvdec'):
        raise ValueError('Invalid encoder profile')
    args = ['ffmpeg', '-hide_banner', '-loglevel', 'error', '-nostdin', '-y', '-threads', '2']
    if decoder == 'nvdec':
        args += ['-hwaccel', 'cuda', '-hwaccel_output_format', 'cuda']
    args += ['-ss', f'{start:.3f}', '-i', str(source), '-t', f'{length:.3f}',
             '-map', '0:v:0', '-map', '0:a:0?', '-filter_threads', '1', '-c:v', 'h264_nvenc']
    if profile == 'legacy':
        args += ['-preset', 'p4', '-rc', 'vbr', '-cq', '20', '-b:v', '0']
    else:
        # Candidate only. CQ != x264 CRF; requires matching-source quality QA.
        args += ['-preset', 'p5', '-rc', 'vbr', '-cq', '23', '-b:v', '4M',
                 '-maxrate', '6M', '-bufsize', '12M', '-spatial_aq', '1']
    args += ['-vf', 'scale_cuda=format=nv12'] if decoder == 'nvdec' else ['-pix_fmt', 'yuv420p']
    return args + ['-c:a', 'aac', '-b:a', '128k', '-movflags', '+faststart',
                   '-progress', 'pipe:1', '-stats_period', '1', str(output)]


def authorize(authorization: str = Header(default="")):
    if len(KEY) < 24 or not hmac.compare_digest(authorization, "Bearer " + KEY):
        raise HTTPException(401, "Unauthorized")


def validate_spec(spec):
    parts = spec.get("parts", [])
    duration, size = spec.get("duration"), spec.get("size")
    if (not isinstance(size, int) or isinstance(size, bool) or not 0 < size <= MAX_SOURCE
            or isinstance(duration, bool) or not isinstance(duration, (int, float)) or not math.isfinite(duration) or not 0 < duration <= 10800
            or not isinstance(parts, list) or not 1 <= len(parts) <= 30):
        raise HTTPException(422, "Invalid export")
    total = 0
    clean = []
    for p in parts:
        if not isinstance(p, dict):
            raise HTTPException(422, "Invalid clip boundary")
        start, end = p.get("start"), p.get("end")
        if (isinstance(start, bool) or isinstance(end, bool) or not isinstance(start, (float, int)) or not isinstance(end, (float, int))
                or not math.isfinite(start) or not math.isfinite(end) or not 0 <= start < end <= duration
                or end-start > 3600):
            raise HTTPException(422, "Invalid clip boundary")
        total += end-start
        clean.append({"start": start, "end": end})
    if total > 7200:
        raise HTTPException(422, "Export too long")
    result = {"parts": clean, "duration": duration, "size": size}
    digest = spec.get('sourceSha256')
    if digest is not None:
        if not isinstance(digest, str) or not re.fullmatch('[a-f0-9]{64}', digest):
            raise HTTPException(422, 'Invalid source digest')
        result['sourceSha256'] = digest
    return result


class Manager:
    def __init__(self, root):
        self.root = root
        if root.is_symlink():
            raise RuntimeError("Unsafe cache root")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = threading.RLock()
        self.jobs = {}
        self.stops = {}
        self.active = set()
        self.cleanup_stop = threading.Event()
        self.restart_requested = threading.Event()
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        recovered = []
        for p in root.glob("*/state.json"):
            if not re.fullmatch(r"[a-f0-9]{32}", p.parent.name) or p.parent.is_symlink():
                continue
            j = json.loads(p.read_text())
            self.jobs[p.parent.name] = j
            if j["state"] in ("waiting", "uploading", "queued", "running"):
                source = p.parent/'source'
                if (os.environ.get('ENCODER_RECOVER_ON_RESTART', '0') == '1'
                        and j['state'] in ('queued', 'running') and source.is_file() and not source.is_symlink()
                        and source.stat().st_size == j.get('spec', {}).get('size')
                        and j.get('settings') == self.settings()):
                    self.stops[p.parent.name] = threading.Event()
                    # Redo only this job's outputs; never reuse a truncated MP4.
                    for name in j.get('files', []):
                        if re.fullmatch(r'news-\d{2}\.mp4', name): (p.parent/name).unlink(missing_ok=True)
                    (p.parent/'output.partial.mp4').unlink(missing_ok=True)
                    (p.parent/'source.partial').unlink(missing_ok=True)
                    self.update(p.parent.name, state='queued', files=[], progress=0,
                                stage='Recovered; waiting for shared budget', expires=time.time()+3600,
                                timings={'resource_queue_seconds': 0, 'encode_seconds': 0},
                                recoveries=j.get('recoveries', 0)+1)
                    recovered.append(p.parent.name)
                    continue
                self.update(p.parent.name, state="failed", stage="Worker restarted; resubmit")
                for name in ("source", "source.partial", "output.partial.mp4"):
                    (p.parent / name).unlink(missing_ok=True)
        for key in recovered: self.pool.submit(self.run, key)

    @staticmethod
    def settings():
        return {'profile': os.environ.get('ENCODER_PROFILE', 'legacy'), 'decode': os.environ.get('ENCODER_DECODE', 'cpu')}

    def update(self, key, **fields):
        with self.lock:
            self.jobs[key].update(fields)
            p = self.root/key
            (p/"state.tmp").write_text(json.dumps(self.jobs[key]))
            os.chmod(p/"state.tmp", 0o600)
            (p/"state.tmp").replace(p/"state.json")

    def clean(self):
        with self.lock:
            for key, j in list(self.jobs.items()):
                if key not in self.active and j["state"] in ("waiting", "completed", "failed", "cancelled") and time.time() > j["expires"]:
                    shutil.rmtree(self.root/key)
                    del self.jobs[key]
                    self.stops.pop(key, None)
            cache = self.root/'sources'
            if cache.is_dir() and not cache.is_symlink():
                for p in cache.iterdir():
                    if re.fullmatch('[a-f0-9]{64}', p.name) and not p.is_symlink() and time.time()-p.stat().st_mtime > 3600:
                        # Active jobs hold a hard link; unlinking the cache name
                        # cannot remove data being decoded by another task.
                        p.unlink()

    def periodic_clean(self):
        while not self.cleanup_stop.wait(60):
            try: self.clean()
            except OSError: pass  # Retry next tick; never export a private path.

    def room(self, extra=0):
        with self.lock:
            used = self.storage_used()
            free = shutil.disk_usage(self.root).free
            needed = max(0, used+extra-QUOTA, HEADROOM+extra-free)
            if needed:
                cache = self.root/'sources'
                candidates = []
                if cache.is_dir() and not cache.is_symlink():
                    for p in cache.iterdir():
                        try:
                            st = p.stat()
                            if re.fullmatch('[a-f0-9]{64}', p.name) and not p.is_symlink() and st.st_nlink == 1:
                                candidates.append((st.st_mtime, st.st_size, p))
                        except FileNotFoundError: pass
                for _, size, p in sorted(candidates):
                    p.unlink(missing_ok=True); needed -= size
                    if needed <= 0: break
                used = self.storage_used()
                free = shutil.disk_usage(self.root).free
            if used+extra > QUOTA or free-extra < HEADROOM:
                raise HTTPException(507, 'Encoder storage is full')

    def storage_used(self):
        used, inodes = 0, set()
        for p in self.root.glob('*/*'):
            try:
                if p.is_file() and not p.is_symlink():
                    stat = p.stat(); inode = (stat.st_dev, stat.st_ino)
                    if inode not in inodes: used += stat.st_size; inodes.add(inode)
            except FileNotFoundError: pass
        return used

    def create(self, key, spec):
        if not re.fullmatch(r"[a-f0-9]{32}", key):
            raise HTTPException(422, "Invalid job key")
        spec = validate_spec(spec)
        digest = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
        with self.lock:
            self.clean()
            if key in self.jobs:
                if self.jobs[key]["hash"] != digest:
                    raise HTTPException(409, "Job key already used")
                return dict(self.jobs[key])
            if sum(j["state"] in ("waiting", "uploading", "queued", "running") for j in self.jobs.values()) >= 4:
                raise HTTPException(429, "Encoder queue full")
            cached_source = self.root/'sources'/spec.get('sourceSha256', 'unused')
            cache_hit = bool(spec.get('sourceSha256') and os.environ.get('ENCODER_SOURCE_CACHE', '0') == '1'
                             and not cached_source.is_symlink() and cached_source.is_file()
                             and cached_source.stat().st_size == spec['size'])
            self.room(0 if cache_hit else spec['size'])
            if cache_hit and not cached_source.is_file(): self.room(spec['size'])
            (self.root/key).mkdir(mode=0o700)
            self.jobs[key] = {"state": "waiting", "progress": 0, "stage": "Waiting for upload", "files": [],
                              "spec": spec, "hash": digest, "expires": time.time()+3600,
                              "timings": {"resource_queue_seconds": 0, "encode_seconds": 0}}
            self.jobs[key]['settings'] = self.settings()
            self.stops[key] = threading.Event()
            self.update(key)
            digest = spec.get('sourceSha256')
            cache = self.root/'sources'
            if digest and os.environ.get('ENCODER_SOURCE_CACHE', '0') == '1':
                if cache.is_symlink(): raise HTTPException(507, 'Unsafe source cache')
                source = cache/digest
                if source.is_file() and not source.is_symlink() and source.stat().st_size == spec['size']:
                    os.link(source, self.root/key/'source')
                    os.utime(source, None)
                    self.update(key, state='queued', stage='Shared source cache hit', sourceCached=True)
                    self.pool.submit(self.run, key)
            return dict(self.jobs[key])

    def get(self, key):
        with self.lock:
            self.clean()
            if key not in self.jobs:
                raise HTTPException(404, "Unknown export")
            return dict(self.jobs[key])

    def run(self, key):
        p = self.root/key
        with self.lock:
            self.active.add(key)
        try:
            j = self.get(key)
            if self.stops[key].is_set():
                return
            self.update(key, state="running", stage="NVENC encoding")
            total = sum(v["end"]-v["start"] for v in j["spec"]["parts"])
            done = 0
            for i, part in enumerate(j["spec"]["parts"]):
                if self.stops[key].is_set():
                    raise RuntimeError("Cancelled")
                length = part["end"]-part["start"]
                # Yield the shared budget between clips, not after the entire
                # two-hour export. Kill/wait happens BEFORE releasing its lock.
                self.encode_part(key, part, length, done, total, i)
                name = f"news-{i+1:02d}.mp4"
                (p/"output.partial.mp4").replace(p/name)
                done += length
                files = self.get(key)["files"]+[name]
                self.update(key, files=files, progress=min(99, int(done/total*100)))
            self.update(key, state="completed", progress=100, stage="Ready", expires=time.time()+3600)
        except Exception as error:
            if self.restart_requested.is_set() and self.get(key)['state'] != 'cancelled':
                self.update(key, state='queued', stage='Service restarting; recovery pending')
            else:
                self.update(key, state="cancelled" if self.stops[key].is_set() else "failed", stage="Hardware export failed; resubmit", errorType=type(error).__name__)
        finally:
            keep_source = (self.restart_requested.is_set() and self.get(key)['state'] in ('queued', 'running')
                           and (p/'source').is_file())
            if keep_source:
                self.update(key, state='queued', stage='Service restarting; recovery pending')
            else:
                (p/"source").unlink(missing_ok=True)
            (p/"output.partial.mp4").unlink(missing_ok=True)
            with self.lock:
                self.active.discard(key)

    def encode_part(self, key, part, length, done, total, index):
        p = self.root/key
        queue_started = time.monotonic()
        with resource_admission(self.stops[key].is_set,
                                lambda n: self.update(key, stage=f'Shared media queue {n}')) as fd:
            acquired = time.monotonic()
            timings = dict(self.get(key).get('timings', {}))
            timings['resource_queue_seconds'] = timings.get('resource_queue_seconds', 0) + acquired-queue_started
            self.update(key, timings=timings, stage=f'NVENC {index+1}/{len(self.get(key)["spec"]["parts"])}')
            if self.stops[key].is_set():
                raise RuntimeError('Cancelled')
            inherit = {'pass_fds': (fd,)} if fd is not None and os.name != 'nt' else {}
            proc = subprocess.Popen(encode_args(p/'source', p/'output.partial.mp4', part['start'], length),
                                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, **inherit)
            timer = threading.Timer(3600, proc.kill)
            timer.start()
            watcher_stop = threading.Event()
            def watch_cancel():
                while not watcher_stop.wait(0.2):
                    if self.stops[key].is_set():
                        if proc.poll() is None: proc.kill()
                        return
            watcher = threading.Thread(target=watch_cancel, daemon=True)
            watcher.start()
            try:
                last = 0
                for line in proc.stdout:
                    if self.stops[key].is_set():
                        raise RuntimeError('Cancelled')
                    if line.startswith('out_time_us=') and time.monotonic()-last >= 0.8:
                        last = time.monotonic()
                        self.room()
                        try:
                            encoded = max(0, min(length, int(line.split('=', 1)[1])/1e6))
                        except ValueError:
                            continue
                        self.update(key, progress=min(99, int((done+encoded)/total*100)),
                                    stage=f'NVENC {index+1}/{len(self.get(key)["spec"]["parts"])}')
                if proc.wait() != 0:
                    raise RuntimeError('NVENC failed')
            finally:
                timer.cancel()
                watcher_stop.set()
                watcher.join(timeout=2)
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
                if proc.stdout:
                    proc.stdout.close()
                timings['encode_seconds'] = timings.get('encode_seconds', 0) + time.monotonic()-acquired
                self.update(key, timings=timings)


@asynccontextmanager
async def lifespan(app):
    global manager
    if len(KEY) < 24:
        raise RuntimeError("ENCODER_API_KEY must contain at least 24 characters")
    manager = Manager(ROOT)
    # Validate candidate flags before accepting a job.
    encode_args('input', 'output', 0, 1)
    cleaner = threading.Thread(target=manager.periodic_clean, daemon=True)
    cleaner.start()
    try:
        yield
    finally:
        manager.cleanup_stop.set()
        if os.environ.get('ENCODER_RECOVER_ON_RESTART', '0') == '1':
            manager.restart_requested.set()
        cleaner.join(timeout=5)
        for stop in manager.stops.values():
            stop.set()
        manager.pool.shutdown(wait=True)


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None,
              dependencies=[Depends(authorize)])


@app.middleware("http")
async def private_clients(request, call_next):
    if request.client is None or request.client.host not in ALLOWED_HOSTS:
        return JSONResponse({"detail": "Private clients only"}, status_code=403)
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/health")
def health():
    result = {"ready": manager is not None, "encoder": "h264_nvenc", "concurrency": 2}
    root = os.environ.get('MEDIA_RESOURCE_ROOT')
    if root:
        path = pathlib.Path(root)/'monitor.json'
        try:
            if not path.is_symlink() and path.stat().st_size < 65536:
                result['monitor'] = json.loads(path.read_text())
                result['monitorAgeSeconds'] = max(0, time.time()-path.stat().st_mtime)
        except (OSError, ValueError): pass
    return result


@app.post("/v1/exports/{key}")
async def create(key: str, request: Request):
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 65536:
            raise HTTPException(413, "Export specification too large")
    try:
        spec = json.loads(body)
    except (ValueError, TypeError):
        raise HTTPException(422, "Invalid export") from None
    if not isinstance(spec, dict):
        raise HTTPException(422, "Invalid export")
    return manager.create(key, spec)


@app.put("/v1/exports/{key}/source")
async def upload(key: str, request: Request):
    j = manager.get(key)
    if request.headers.get("content-length") != str(j["spec"]["size"]):
        raise HTTPException(422, "Source length mismatch")
    with manager.lock:
        if manager.jobs[key]["state"] != "waiting":
            raise HTTPException(409, "Source already submitted")
        manager.update(key, state="uploading")
    p = manager.root/key
    count = 0
    digest = hashlib.sha256()
    try:
        with (p/"source.partial").open("xb") as f:
            os.chmod(p/"source.partial", 0o600)
            async with asyncio.timeout(1800):
                async for chunk in request.stream():
                    if manager.stops[key].is_set():
                        raise HTTPException(409, "Cancelled")
                    count += len(chunk)
                    if count > j["spec"]["size"]:
                        raise HTTPException(413, "Source too large")
                    manager.room(len(chunk))
                    f.write(chunk)
                    digest.update(chunk)
        if count != j["spec"]["size"]:
            raise HTTPException(422, "Source incomplete")
        expected = j['spec'].get('sourceSha256')
        if expected and not hmac.compare_digest(digest.hexdigest(), expected):
            raise HTTPException(422, 'Source digest mismatch')
        (p/"source.partial").replace(p/"source")
        if expected and os.environ.get('ENCODER_SOURCE_CACHE', '0') == '1':
            with manager.lock:
                cache = manager.root/'sources'
                if cache.is_symlink(): raise HTTPException(507, 'Unsafe source cache')
                cache.mkdir(mode=0o700, exist_ok=True)
                try: os.link(p/'source', cache/expected)
                except FileExistsError:
                    if (cache/expected).is_symlink() or (cache/expected).stat().st_size != count:
                        raise HTTPException(507, 'Invalid cached source')
                os.utime(cache/expected, None)
        manager.update(key, state="queued")
        manager.pool.submit(manager.run, key)
        return {"accepted": True}
    except Exception:
        (p/"source.partial").unlink(missing_ok=True)
        (p/'source').unlink(missing_ok=True)
        manager.update(key, state="failed", stage="Upload failed; resubmit")
        raise HTTPException(400, "Source upload failed") from None


@app.get("/v1/exports/{key}")
def status(key: str):
    j = manager.get(key)
    return {k: j.get(k) for k in ("state", "progress", "stage", "files", "timings", "recoveries")}


@app.get("/v1/exports/{key}/files/{name}")
def download(key: str, name: str):
    j = manager.get(key)
    if j["state"] != "completed" or name not in j["files"] or not re.fullmatch(r"news-\d{2}\.mp4", name):
        raise HTTPException(404, "Export not ready")
    return FileResponse(manager.root/key/name, media_type="video/mp4", headers={"Cache-Control": "no-store"})


@app.delete("/v1/exports/{key}")
def cancel(key: str):
    manager.get(key)
    with manager.lock:
        manager.stops.setdefault(key, threading.Event()).set()
        # Active files are removed by the worker/TTL, never under a live process.
        manager.update(key, state="cancelled", expires=time.time()+3600)
    return {"cancelled": True}
