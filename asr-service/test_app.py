import importlib
import io
import os
import types
import unittest
import wave
import sys
from unittest.mock import patch

from fastapi.testclient import TestClient


def wav():
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(16000)
        audio.writeframes(b"\0\0" * 16000)
    return buffer.getvalue()


class SpeechAdapterTest(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ, {"ASR_API_KEY": "test-only-key-not-a-real-secret-123", 'ASR_ENGINE': 'legacy', 'MEDIA_RESOURCE_ROOT': ''})
        self.environment.start()
        import app
        self.app = importlib.reload(app)
        self.app._model = types.SimpleNamespace(transcribe=lambda *args, **kwargs: (
            iter([types.SimpleNamespace(start=0, end=0.8, text=" 新闻正文 ")]), None))
        self.client = TestClient(self.app.app)
        self.client.__enter__()
        self.headers = {"Authorization": "Bearer " + self.app.KEY}

    def tearDown(self):
        self.client.__exit__(None, None, None)
        self.environment.stop()

    def test_auth_and_timestamp_contract(self):
        self.assertEqual(self.client.get("/health").status_code, 401)
        response = self.client.post("/v1/audio/transcriptions", headers=self.headers,
                                    files={"file": ("sample.wav", wav(), "audio/wav")},
                                    data={"model": "large-v3", "response_format": "verbose_json", "language": "zh"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["segments"], [{"start": 0, "end": 0.8, "text": "新闻正文"}])
        self.assertEqual(response.headers["cache-control"], "no-store")

    def test_reject_unknown_models_and_non_audio(self):
        for model, data in [("unknown", wav()), ("large-v3", b"not audio")]:
            response = self.client.post("/v1/audio/transcriptions", headers=self.headers,
                                        files={"file": ("sample.wav", data)}, data={"model": model})
            self.assertEqual(response.status_code, 422)

    def test_first_word_alignment_preserves_sentence_contract(self):
        def transcribe(*args, **kwargs):
            self.assertTrue(kwargs["word_timestamps"])
            return iter([types.SimpleNamespace(start=0, end=0.8, text=" 新闻正文 ", words=[
                types.SimpleNamespace(start=0.06, end=0.3, word="新闻"),
                types.SimpleNamespace(start=0.3, end=0.8, word="正文"),
            ])]), None
        self.app._model = types.SimpleNamespace(transcribe=transcribe)
        response = self.client.post("/v1/audio/transcriptions", headers=self.headers,
                                    files={"file": ("sample.wav", wav())}, data={"model": "large-v3"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["segments"], [{"start": 0, "end": 0.8, "text": "新闻正文", "speech_start": 0.06}])
        self.assertNotIn("words", response.json()["segments"][0])

    def test_sanitized_model_failures(self):
        self.app._model = types.SimpleNamespace(transcribe=lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("private path and secret")))
        response = self.client.post("/v1/audio/transcriptions", headers=self.headers,
                                    files={"file": ("sample.wav", wav())}, data={"model": "large-v3"})
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("private", response.text)

    def batch_request(self, factory):
        with patch.dict(os.environ, {'ASR_ENGINE': 'batched', 'ASR_BATCH_SIZE': '4'}), patch.dict(sys.modules, {
                'faster_whisper': types.SimpleNamespace(BatchedInferencePipeline=factory)}):
            return self.client.post('/v1/audio/transcriptions', headers=self.headers,
                                    files={'file': ('sample.wav', wav())}, data={'model': 'large-v3'})

    def test_batch_contract_and_fresh_pipeline(self):
        instances = []
        class Pipeline:
            def __init__(inner, model): instances.append(inner)
            def transcribe(inner, samples, **kwargs):
                self.assertEqual(samples.dtype.name, 'float32')
                self.assertEqual(len(samples), 16000)
                self.assertEqual(kwargs['batch_size'], 4)
                self.assertEqual(kwargs['beam_size'], 5)
                self.assertTrue(kwargs['word_timestamps'])
                self.assertFalse(kwargs['without_timestamps'])
                return iter([types.SimpleNamespace(start=0, end=0.8, text='新闻', words=[
                    types.SimpleNamespace(start=0.06, end=0.8, word='新闻')])]), None
        for _ in range(2):
            r = self.batch_request(Pipeline)
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()['segments'][0]['speech_start'], 0.06)
            self.assertFalse(r.json()['timings']['fallback'])
            self.assertIn('request_seconds', r.json()['timings'])
        self.assertEqual(len(instances), 2)

    def test_invalid_batch_timestamps_retry_legacy(self):
        class Pipeline:
            def __init__(self, model): pass
            def transcribe(self, *args, **kwargs):
                return iter([types.SimpleNamespace(start=0.9, end=0.1, text='bad')]), None
        r = self.batch_request(Pipeline)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['segments'][0]['text'], '新闻正文')
        self.assertTrue(r.json()['timings']['fallback'])

    def test_batch_sentences_keep_word_boundaries_and_all_text(self):
        segment = types.SimpleNamespace(start=0, end=2, text='新闻一。新闻二。', words=[
            types.SimpleNamespace(start=0.05, end=0.8, word='新闻一。'),
            types.SimpleNamespace(start=1.1, end=1.9, word='新闻二。')])
        rows = self.app.sentence_rows(iter([segment]), 2, split_sentences=True)
        self.assertEqual([r['start'] for r in rows], [0.05, 1.1])
        self.assertEqual(''.join(r['text'] for r in rows), segment.text)
        segment.words[1].word = '遗漏'
        with self.assertRaises(ValueError):
            self.app.sentence_rows(iter([segment]), 2, split_sentences=True)

    def test_lazy_batch_oom_retry_and_unrelated_error_sanitized(self):
        for message, status in [('CUDA out of memory', 200), ('private driver path', 503)]:
            class Pipeline:
                def __init__(self, model): pass
                def transcribe(self, *args, **kwargs):
                    def rows():
                        raise RuntimeError(message)
                        yield
                    return rows(), None
            r = self.batch_request(Pipeline)
            self.assertEqual(r.status_code, status)
            self.assertNotIn('private', r.text)


if __name__ == "__main__":
    unittest.main()
