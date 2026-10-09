import importlib
import io
import os
import types
import unittest
import wave
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
        self.environment = patch.dict(os.environ, {"ASR_API_KEY": "test-only-key-not-a-real-secret-123"})
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


if __name__ == "__main__":
    unittest.main()
