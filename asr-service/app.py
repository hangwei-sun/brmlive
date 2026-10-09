"""Private news ASR adapter. One GPU worker; no source files or text are retained."""
import hmac
import os
import tempfile
import threading
import wave
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import JSONResponse

_model = None
_slot = threading.BoundedSemaphore(1)
MODEL = os.environ.get("ASR_MODEL", "large-v3")
KEY = os.environ.get("ASR_API_KEY", "")


@asynccontextmanager
async def lifespan(app):
    global _model
    if len(KEY) < 24:
        raise RuntimeError("ASR_API_KEY must contain at least 24 characters")
    if _model is None:
        from faster_whisper import WhisperModel
        _model = WhisperModel(
            MODEL, device=os.environ.get("ASR_DEVICE", "cuda"),
            compute_type=os.environ.get("ASR_COMPUTE_TYPE", "int8_float16"),
            download_root=os.environ.get("ASR_MODEL_CACHE", "/models"),
            cpu_threads=4, num_workers=1,
        )
    yield


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


@app.middleware("http")
async def body_limit(request, call_next):
    if request.method == "POST":
        try:
            length = int(request.headers.get("content-length", "-1"))
        except ValueError:
            length = -1
        if length < 0:
            return JSONResponse({"message": "Content-Length required"}, status_code=411)
        if length > 26 * 1024 * 1024:
            return JSONResponse({"message": "Audio upload too large"}, status_code=413)
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    return response


def authorize(authorization: str = Header(default="")):
    if not KEY or not hmac.compare_digest(authorization, "Bearer " + KEY):
        raise HTTPException(401, "Unauthorized")


@app.get("/health", dependencies=[Depends(authorize)])
def health():
    return {"ready": _model is not None, "model": MODEL}


@app.post("/v1/audio/transcriptions", dependencies=[Depends(authorize)])
def transcribe(
    file: UploadFile = File(...), model: str = Form(...),
    response_format: str = Form("verbose_json"), language: str = Form("zh"),
):
    if model != MODEL or response_format != "verbose_json" or language != "zh":
        raise HTTPException(422, "Use the configured model, verbose_json and zh")
    if _model is None:
        raise HTTPException(503, "Speech model is not ready")
    if not _slot.acquire(timeout=120):
        raise HTTPException(429, "Speech worker is busy")
    path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as target:
            path = target.name
            received = 0
            while chunk := file.file.read(1024 * 1024):
                received += len(chunk)
                if received > 25 * 1024 * 1024:
                    raise HTTPException(413, "Audio upload too large")
                target.write(chunk)
        try:
            with wave.open(path, "rb") as audio:
                if audio.getframerate() != 16000 or audio.getnchannels() != 1 or audio.getsampwidth() != 2:
                    raise ValueError("Invalid audio format")
                duration = audio.getnframes() / audio.getframerate()
                if not 0 < duration <= 601:
                    raise ValueError("Audio chunk must be at most 10 minutes")
        except (wave.Error, EOFError, ValueError):
            raise HTTPException(422, "Use mono 16 kHz PCM WAV, at most 10 minutes") from None
        segments, _ = _model.transcribe(
            path, language="zh", beam_size=5, vad_filter=True,
            condition_on_previous_text=False,
            initial_prompt="以下是电视新闻节目，包含主播导语、记者报道、采访和节目结束语。请使用简体中文转写。",
        )
        rows = [
            {"start": round(item.start, 3), "end": round(item.end, 3), "text": item.text.strip()}
            for item in segments if item.text.strip()
        ]
        return {"text": "".join(row["text"] for row in rows), "language": "zh", "duration": duration, "segments": rows}
    except HTTPException:
        raise
    except Exception:
        # Model errors must not expose local paths, input text, or credentials.
        raise HTTPException(503, "Speech transcription failed") from None
    finally:
        file.file.close()
        if path:
            os.unlink(path)
        _slot.release()
