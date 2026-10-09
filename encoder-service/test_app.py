import importlib
import os
import pathlib
import tempfile
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient


class EncoderTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {"ENCODER_ROOT": self.tmp.name, "ENCODER_API_KEY": "test-only-encoder-key-0123456789", "ENCODER_ALLOWED_HOSTS": "testclient"})
        self.env.start()
        import app
        self.module = importlib.reload(app)
        self.client = TestClient(self.module.app)
        self.client.__enter__()
        self.headers = {"Authorization": "Bearer "+self.module.KEY}
        self.key = "a"*32
        self.spec = {"size": 4, "duration": 10, "parts": [{"start": 0.25, "end": 2.5}]}

    def tearDown(self):
        self.client.__exit__(None, None, None)
        self.env.stop()
        self.tmp.cleanup()

    def create(self, key=None, spec=None):
        return self.client.post("/v1/exports/"+(key or self.key), headers=self.headers, json=spec or self.spec)

    def test_auth_validation_and_idempotence(self):
        self.assertEqual(self.client.post('/v1/exports/'+self.key, json=self.spec).status_code, 401)
        self.assertEqual(self.create().status_code, 200)
        self.assertEqual(self.create().status_code, 200)
        self.assertEqual(self.create(spec={**self.spec, "size": 5}).status_code, 409)
        self.assertEqual(self.create('not-a-key').status_code, 422)
        self.assertEqual(self.create('b'*32, {**self.spec, "parts": [None]}).status_code, 422)
        self.assertEqual(self.create('b'*32, {**self.spec, "parts": [{"start": True, "end": 2}]}).status_code, 422)
        with patch.object(self.module, 'ALLOWED_HOSTS', set()):
            self.assertEqual(self.client.get('/health', headers=self.headers).status_code, 403)

    def test_queue_cap_and_partial_upload(self):
        for ch in 'abcd':
            self.assertEqual(self.create(ch*32).status_code, 200)
        self.assertEqual(self.create('e'*32).status_code, 429)
        response = self.client.put('/v1/exports/'+self.key+'/source', headers={**self.headers, "Content-Length": "4"}, content=b'bad')
        self.assertEqual(response.status_code, 400)
        self.assertFalse((pathlib.Path(self.tmp.name)/self.key/'source.partial').exists())

    def test_cancel_and_download_allowlist(self):
        self.create()
        self.assertEqual(self.client.get('/v1/exports/'+self.key+'/files/state.json', headers=self.headers).status_code, 404)
        self.assertEqual(self.client.delete('/v1/exports/'+self.key, headers=self.headers).status_code, 200)
        self.module.manager.update(self.key, expires=time.time()-1)
        self.assertEqual(self.client.get('/v1/exports/'+self.key, headers=self.headers).status_code, 404)

    def test_upload_and_restart_sanitization(self):
        self.create()
        with patch.object(self.module.manager.pool, 'submit') as submit:
            response = self.client.put('/v1/exports/'+self.key+'/source', headers=self.headers, content=b'test')
            self.assertEqual(response.status_code, 200)
            submit.assert_called_once()
        self.module.manager.update(self.key, state='running')
        recovered = self.module.Manager(pathlib.Path(self.tmp.name))
        try:
            self.assertEqual(recovered.get(self.key)['state'], 'failed')
            self.assertFalse((pathlib.Path(self.tmp.name)/self.key/'source').exists())
        finally:
            recovered.pool.shutdown()

    def test_nvenc_command_progress_and_source_cleanup(self):
        self.create()
        p = pathlib.Path(self.tmp.name)/self.key
        (p/'source').write_bytes(b'test')
        class Process:
            stdout = iter(['out_time_us=1000000\n', 'progress=end\n'])
            def wait(self): return 0
            def poll(self): return 0
            def kill(self): pass
        def start(args, **kwargs):
            self.assertEqual(args[args.index('-c:v')+1], 'h264_nvenc')
            self.assertEqual(args[args.index('-ss')+1], '0.250')
            self.assertIn('-progress', args)
            pathlib.Path(args[-1]).write_bytes(b'encoded-fixture')
            return Process()
        with patch.object(self.module.subprocess, 'Popen', side_effect=start):
            self.module.manager.run(self.key)
        j = self.module.manager.get(self.key)
        self.assertEqual(j['state'], 'completed')
        self.assertEqual(j['progress'], 100)
        self.assertEqual(j['files'], ['news-01.mp4'])
        self.assertFalse((p/'source').exists())

    def test_model_failure_never_discloses_inputs(self):
        self.create()
        p = pathlib.Path(self.tmp.name)/self.key
        (p/'source').write_bytes(b'test')
        with patch.object(self.module.subprocess, 'Popen', side_effect=ValueError('secret local input')):
            self.module.manager.run(self.key)
        status = self.client.get('/v1/exports/'+self.key, headers=self.headers)
        self.assertEqual(status.json()['state'], 'failed')
        self.assertNotIn('secret', status.text)
        self.assertFalse((p/'source').exists())


if __name__ == '__main__':
    unittest.main()
