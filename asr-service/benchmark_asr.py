"""Offline GPU acceptance only; never calls a production API or logs text.

Use a prepared mono16k PCM16 WAV <=601s and a locally pinned model directory.
Run in an agreed GPU test window, not alongside production inference.
"""
import argparse
import hashlib
import json
import os
import time
import wave


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('audio')
    parser.add_argument('--model-path', required=True)
    parser.add_argument('--batches', default='4,8')
    args = parser.parse_args()
    batches = [int(x) for x in args.batches.split(',')]
    if any(not 1 <= x <= 8 for x in batches):
        parser.error('batch size must be 1..8')
    with wave.open(args.audio, 'rb') as audio:
        duration = audio.getnframes() / audio.getframerate()
        if (audio.getframerate(), audio.getnchannels(), audio.getsampwidth()) != (16000, 1, 2) or not 0 < duration <= 601:
            parser.error('use mono16k PCM16 WAV <=601s')
    from faster_whisper import WhisperModel
    import app
    # Separate process/model, does not load the resource gate or service key.
    loaded = time.monotonic()
    app._model = WhisperModel(args.model_path, device='cuda', compute_type='int8_float16', cpu_threads=4, num_workers=1)
    print(json.dumps({'model_load_seconds': round(time.monotonic()-loaded, 3)}))
    baseline = None
    for engine, batch in [('legacy', 1)] + [('batched', n) for n in batches]:
        os.environ['ASR_ENGINE'], os.environ['ASR_BATCH_SIZE'] = engine, str(batch)
        started = time.monotonic()
        result = app.recognise(args.audio, duration)
        seconds = time.monotonic()-started
        if baseline is None: baseline = seconds
        print(json.dumps({'engine': engine, 'batch_size': batch, 'seconds': round(seconds, 3),
                          'speedup_vs_first_legacy': round(baseline/seconds, 3),
                          'segments': len(result['segments']),
                          'segments_with_word_start': sum('speech_start' in r for r in result['segments']),
                          'transcript_sha256': hashlib.sha256(result['text'].encode()).hexdigest(),
                          'timings': result['timings'],
                          'note': 'First legacy includes warmup; repeat separately before claiming speedup. Hash/segment counts are not accuracy.'}, ensure_ascii=False))


if __name__ == '__main__': main()
