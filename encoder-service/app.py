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
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

KEY = os.environ.get("ENCODER_API_KEY", "")
ALLOWED_HOSTS = set(os.environ.get("ENCODER_ALLOWED_HOSTS", "127.0.0.1,::1").split(","))
ROOT = pathlib.Path(os.environ.get("ENCODER_ROOT", "/data/brm-news-encoder"))
MAX_SOURCE = 8 * 1024**3
QUOTA = 32 * 1024**3
HEADROOM = 4 * 1024**3
manager = None


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
    return {"parts": clean, "duration": duration, "size": size}


class Manager:
    def __init__(self, root):
        self.root = root
        if root.is_symlink():
            raise RuntimeError("Unsafe cache root")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = threading.RLock()
        self.jobs = {}
        self.stops = {}
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        for p in root.glob("*/state.json"):
            if not re.fullmatch(r"[a-f0-9]{32}", p.parent.name) or p.parent.is_symlink():
                continue
            j = json.loads(p.read_text())
            self.jobs[p.parent.name] = j
            if j["state"] in ("waiting", "uploading", "queued", "running"):
                self.update(p.parent.name, state="failed", stage="Worker restarted; resubmit")
                for name in ("source", "source.partial", "output.partial.mp4"):
                    (p.parent / name).unlink(missing_ok=True)

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
                if j["state"] in ("waiting", "completed", "failed", "cancelled") and time.time() > j["expires"]:
                    shutil.rmtree(self.root/key)
                    del self.jobs[key]
                    self.stops.pop(key, None)

    def room(self, extra=0):
        used = sum(p.stat().st_size for p in self.root.glob("*/*") if p.is_file() and not p.is_symlink())
        if used+extra > QUOTA or shutil.disk_usage(self.root).free-extra < HEADROOM:
            raise HTTPException(507, "Encoder storage is full")

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
            self.room(spec["size"])
            (self.root/key).mkdir(mode=0o700)
            self.jobs[key] = {"state": "waiting", "progress": 0, "stage": "Waiting for upload", "files": [],
                              "spec": spec, "hash": digest, "expires": time.time()+3600}
            self.stops[key] = threading.Event()
            self.update(key)
            return dict(self.jobs[key])

    def get(self, key):
        with self.lock:
            self.clean()
            if key not in self.jobs:
                raise HTTPException(404, "Unknown export")
            return dict(self.jobs[key])

    def run(self, key):
        p = self.root/key
        proc = None
        timer = None
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
                # Keep decode/encode seek behaviour identical to the existing
                # exact exporter. NVENC accelerates encode, not GOP-only copying.
                args = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-threads", "2",
                        "-ss", f'{part["start"]:.3f}', "-i", str(p/"source"), "-t", f"{length:.3f}",
                        "-map", "0:v:0", "-map", "0:a:0?", "-c:v", "h264_nvenc", "-preset", "p4",
                        "-rc", "vbr", "-cq", "20", "-b:v", "0", "-pix_fmt", "yuv420p",
                        "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-progress", "pipe:1",
                        "-stats_period", "1", str(p/"output.partial.mp4")]
                proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
                timer = threading.Timer(3600, proc.kill)
                timer.start()
                last = 0
                for line in proc.stdout:
                    if self.stops[key].is_set():
                        proc.kill()
                        raise RuntimeError("Cancelled")
                    if line.startswith("out_time_us=") and time.monotonic()-last >= 0.8:
                        last = time.monotonic()
                        self.room()
                        # FFmpeg may report N/A before the first output packet,
                        # particularly after an accurate seek into AAC audio.
                        try:
                            encoded = max(0, min(length, int(line.split("=", 1)[1])/1e6))
                        except ValueError:
                            continue
                        self.update(key, progress=min(99, int((done+encoded)/total*100)),
                                    stage=f"NVENC {i+1}/{len(j['spec']['parts'])}")
                if proc.wait() != 0:
                    raise RuntimeError("NVENC failed")
                timer.cancel()
                name = f"news-{i+1:02d}.mp4"
                (p/"output.partial.mp4").replace(p/name)
                done += length
                files = self.get(key)["files"]+[name]
                self.update(key, files=files, progress=min(99, int(done/total*100)))
            self.update(key, state="completed", progress=100, stage="Ready", expires=time.time()+3600)
        except Exception as error:
            self.update(key, state="cancelled" if self.stops[key].is_set() else "failed", stage="Hardware export failed; resubmit", errorType=type(error).__name__)
        finally:
            if timer:
                timer.cancel()
            if proc and proc.poll() is None:
                proc.kill()
                proc.wait()
            (p/"source").unlink(missing_ok=True)
            (p/"output.partial.mp4").unlink(missing_ok=True)


@asynccontextmanager
async def lifespan(app):
    global manager
    if len(KEY) < 24:
        raise RuntimeError("ENCODER_API_KEY must contain at least 24 characters")
    manager = Manager(ROOT)
    yield
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
    return {"ready": manager is not None, "encoder": "h264_nvenc", "concurrency": 2}


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
        if count != j["spec"]["size"]:
            raise HTTPException(422, "Source incomplete")
        (p/"source.partial").replace(p/"source")
        manager.update(key, state="queued")
        manager.pool.submit(manager.run, key)
        return {"accepted": True}
    except Exception:
        (p/"source.partial").unlink(missing_ok=True)
        manager.update(key, state="failed", stage="Upload failed; resubmit")
        raise HTTPException(400, "Source upload failed") from None


@app.get("/v1/exports/{key}")
def status(key: str):
    j = manager.get(key)
    return {k: j[k] for k in ("state", "progress", "stage", "files")}


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
