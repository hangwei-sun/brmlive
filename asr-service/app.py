"""Private news ASR adapter. One GPU worker; no source files or text are retained."""
import hmac
import os
import tempfile
import threading
import wave
import math
import time
import logging
import sys
from contextlib import asynccontextmanager, nullcontext

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import JSONResponse

_model = None
_cache = None
_slot = threading.BoundedSemaphore(1)
MODEL = os.environ.get("ASR_MODEL", "large-v3")
KEY = os.environ.get("ASR_API_KEY", "")


def resource_admission():
    if not os.environ.get('MEDIA_RESOURCE_ROOT'):
        return nullcontext(None)
    sys.path.insert(0, os.environ.get('MEDIA_RUNTIME_PATH', '/opt/brm-media-runtime'))
    from resource_gate import admission
    return admission('asr', timeout=110)


@asynccontextmanager
async def lifespan(app):
    global _model, _cache
    if len(KEY) < 24:
        raise RuntimeError("ASR_API_KEY must contain at least 24 characters")
    inference_settings()  # Fail at startup, not halfway through a news task.
    if os.environ.get('ASR_CACHE_ROOT'):
        if not os.environ.get('ASR_CACHE_REVISION'):
            raise RuntimeError('ASR cache requires an immutable model/prompt revision')
        sys.path.insert(0, os.environ.get('MEDIA_RUNTIME_PATH', '/opt/brm-media-runtime'))
        from json_cache import JsonCache
        _cache = JsonCache(os.environ['ASR_CACHE_ROOT'])
    if _model is None:
        from faster_whisper import WhisperModel
        _model = WhisperModel(
            os.environ.get("ASR_MODEL_PATH", MODEL), device=os.environ.get("ASR_DEVICE", "cuda"),
            compute_type=os.environ.get("ASR_COMPUTE_TYPE", "int8_float16"),
            download_root=os.environ.get("ASR_MODEL_CACHE", "/models"),
            cpu_threads=4, num_workers=1,
        )
    stop = threading.Event()
    def clean_cache():
        while not stop.wait(60):
            if _cache is not None:
                try: _cache.clean()
                except OSError: logging.getLogger('brm.asr').warning('ASR cache cleanup failed')
    cleaner = threading.Thread(target=clean_cache, daemon=True)
    cleaner.start()
    try:
        yield
    finally:
        stop.set(); cleaner.join(timeout=5)


def valid_cached_result(cached, duration):
    if not isinstance(cached, dict) or cached.get('duration') != duration or not isinstance(cached.get('text'), str):
        return False
    rows = cached.get('segments')
    if not isinstance(rows, list) or not 0 < len(rows) <= 2000: return False
    last = 0
    for row in rows:
        if not isinstance(row, dict): return False
        start, end, text = row.get('start'), row.get('end'), row.get('text')
        if (isinstance(start, bool) or isinstance(end, bool) or not isinstance(start, (int, float))
                or not isinstance(end, (int, float)) or not math.isfinite(start) or not math.isfinite(end)
                or start < last or end <= start or end > duration+1 or not isinstance(text, str) or not text.strip()):
            return False
        speech = row.get('speech_start', start)
        if not isinstance(speech, (int, float)) or not math.isfinite(speech) or not start <= speech < end: return False
        last = end
    return cached['text'] == ''.join(row['text'] for row in rows)


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
    engine, batch = inference_settings()
    return {"ready": _model is not None, "model": MODEL, "engine": engine, "batch_size": batch}


@app.post("/v1/audio/transcriptions", dependencies=[Depends(authorize)])
def transcribe(
    file: UploadFile = File(...), model: str = Form(...),
    response_format: str = Form("verbose_json"), language: str = Form("zh"),
):
    if model != MODEL or response_format != "verbose_json" or language != "zh":
        raise HTTPException(422, "Use the configured model, verbose_json and zh")
    if _model is None:
        raise HTTPException(503, "Speech model is not ready")
    started = time.monotonic()
    if not _slot.acquire(timeout=120):
        raise HTTPException(429, "Speech worker is busy")
    slot_acquired = time.monotonic()
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
        validated = time.monotonic()
        key = None
        result = None
        if _cache is not None:
            engine, batch = inference_settings()
            revision = '|'.join((os.environ['ASR_CACHE_REVISION'], MODEL,
                                 os.environ.get('ASR_COMPUTE_TYPE', 'int8_float16'), engine, str(batch), 'sentence-v2'))
            key = _cache.key(path, revision)
            cached = _cache.get(key)
            if valid_cached_result(cached, duration):
                result = cached
                result['timings'] = {'prepare_seconds': 0, 'inference_seconds': 0, 'audio_seconds': duration,
                                     'engine': engine, 'batch_size': batch if engine == 'batched' else 1, 'fallback': False}
        resource_wait_started = time.monotonic()
        try:
            resource_acquired = resource_wait_started
            cache_hit = result is not None
            if not cache_hit:
                with resource_admission():
                    resource_acquired = time.monotonic()
                    result = recognise(path, duration)
                if key is not None and not result['timings']['fallback']:
                    try: _cache.put(key, {k: result[k] for k in ('text', 'language', 'duration', 'segments')})
                    except OSError: logging.getLogger('brm.asr').warning('ASR cache write failed')
        except TimeoutError:
            raise HTTPException(429, 'Shared media queue timed out', headers={'Retry-After': '30'}) from None
        result['timings']['resource_queue_seconds'] = round(resource_acquired-resource_wait_started, 3)
        result['timings']['cache_hit'] = cache_hit
        result['timings']['queue_seconds'] = round(slot_acquired - started, 3)
        result['timings']['upload_validate_seconds'] = round(validated - slot_acquired, 3)
        result['timings']['request_seconds'] = round(time.monotonic() - started, 3)
        # Operational metrics only: no audio, transcript, path or credential.
        logging.getLogger('brm.asr').info('ASR metrics %s', result['timings'])
        return result
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(503, "Speech transcription failed") from None
    finally:
        file.file.close()
        if path:
            os.unlink(path)
        _slot.release()


def inference_settings():
    # Real news batches can overlap aligned sentences. Keep the proven engine
    # as production default until batch timing and editorial QA both pass.
    engine = os.environ.get('ASR_ENGINE', 'legacy')
    batch = int(os.environ.get('ASR_BATCH_SIZE', '4'))
    if engine not in ('legacy', 'batched') or not 1 <= batch <= 8:
        raise ValueError('Invalid ASR inference settings')
    return engine, batch


def sentence_rows(segments, duration, split_sentences=False):
    rows, last = [], 0.0
    for item in segments:
        if not item.text.strip():
            continue
        if (not math.isfinite(item.start) or not math.isfinite(item.end)
                or not split_sentences and item.start + 1e-6 < last
                or item.start < 0 or item.end <= item.start or item.end > duration + 1):
            raise ValueError('Invalid ASR timestamp sequence')
        if split_sentences:
            words = getattr(item, 'words', None) or []
            if not words or ''.join(w.word for w in words).strip().replace(' ', '') != item.text.strip().replace(' ', ''):
                raise ValueError('Incomplete batch word alignment')
            pieces, current = [], []
            for word in words:
                if (not math.isfinite(word.start) or not math.isfinite(word.end)
                        or word.start < item.start or word.end > item.end + 0.001 or word.end < word.start
                        or current and word.start < current[-1].end):
                    raise ValueError('Invalid batch word alignment')
                current.append(word)
                if word.word.rstrip().rstrip('”’"\'').endswith(('。', '！', '？', '；', '!', '?', ';')):
                    pieces.append(current)
                    current = []
            if current:
                pieces.append(current)
            for piece in pieces:
                start, end = round(piece[0].start, 3), round(piece[-1].end, 3)
                if start < last or end <= start:
                    raise ValueError('Invalid batch sentence alignment')
                rows.append({'start': start, 'end': end, 'text': ''.join(w.word for w in piece).strip(), 'speech_start': start})
                last = end
            continue
        row = {'start': round(item.start, 3), 'end': round(item.end, 3), 'text': item.text.strip()}
        if row['end'] <= row['start']:
            raise ValueError('Invalid rounded ASR timestamp')
        last = item.end
        for word in (getattr(item, 'words', None) or []):
            if (word.word.strip() and math.isfinite(word.start) and math.isfinite(word.end)
                    and word.end > word.start and item.start <= word.start < item.end):
                row['speech_start'] = round(word.start, 3)
                break
        rows.append(row)
    return rows


def recognise(path, duration):
    import numpy as np
    started = time.monotonic()
    # Input is already validated mono PCM16; avoid an additional PyAV decoder.
    with wave.open(path, 'rb') as audio:
        samples = np.frombuffer(audio.readframes(audio.getnframes()), dtype='<i2').astype(np.float32) / 32768.0
    prepared = time.monotonic()
    engine, batch = inference_settings()
    options = dict(language='zh', beam_size=5, vad_filter=True,
                   condition_on_previous_text=False, word_timestamps=True,
                   initial_prompt='以下是电视新闻节目，包含主播导语、记者报道、采访和节目结束语。请使用简体中文转写。')
    fallback = False
    if engine == 'batched':
        from faster_whisper import BatchedInferencePipeline
        # Pipeline holds word-alignment state. Never reuse it across requests.
        pipeline = BatchedInferencePipeline(_model)
        try:
            segments, _ = pipeline.transcribe(samples, batch_size=batch, without_timestamps=False,
                                              vad_parameters={'min_silence_duration_ms': 2000}, **options)
            rows = sentence_rows(segments, duration, split_sentences=True)
        except (ValueError, RuntimeError) as error:
            # Invalid alignment/OOM may use the proven path once, never discard
            # a presenter's opening words or return a partially collected batch.
            if not isinstance(error, ValueError) and 'out of memory' not in str(error).lower():
                raise
            fallback = True
            pipeline = None
            segments = None
            error.__traceback__ = None
            import gc
            gc.collect()
            segments, _ = _model.transcribe(samples, **options)
            rows = sentence_rows(segments, duration)
    else:
        segments, _ = _model.transcribe(samples, **options)
        rows = sentence_rows(segments, duration)
    timings = {'prepare_seconds': round(prepared-started, 3),
               'inference_seconds': round(time.monotonic()-prepared, 3),
               'audio_seconds': round(duration, 3), 'engine': engine,
               'batch_size': batch if engine == 'batched' else 1, 'fallback': fallback}
    return {'text': ''.join(row['text'] for row in rows), 'language': 'zh',
            'duration': duration, 'segments': rows, 'timings': timings}
