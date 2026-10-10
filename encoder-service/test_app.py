import importlib
import io
import hashlib
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
        self.env = patch.dict(os.environ, {"ENCODER_ROOT": self.tmp.name, "ENCODER_API_KEY": "test-only-encoder-key-0123456789", "ENCODER_ALLOWED_HOSTS": "testclient", 'MEDIA_RESOURCE_ROOT': '', 'ENCODER_PROFILE': 'legacy', 'ENCODER_DECODE': 'cpu'})
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
            stdout = io.StringIO('out_time_us=N/A\nout_time_us=1000000\nprogress=end\n')
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
        self.assertIn('encode_seconds', j['timings'])

    def test_candidate_flags_default_safe_and_validated(self):
        args = self.module.encode_args('source', 'output', 0.04, 1)
        self.assertNotIn('-hwaccel', args)
        self.assertEqual(args[args.index('-cq')+1], '20')
        with patch.dict(os.environ, {'ENCODER_PROFILE': 'capped', 'ENCODER_DECODE': 'nvdec'}):
            args = self.module.encode_args('source', 'output', 0.04, 1)
            self.assertEqual(args[args.index('-hwaccel')+1], 'cuda')
            self.assertIn('-maxrate', args)
            self.assertNotIn('-pix_fmt', args)
        with patch.dict(os.environ, {'ENCODER_DECODE': 'invalid'}):
            with self.assertRaises(ValueError):
                self.module.encode_args('source', 'output', 0, 1)

    def test_cleanup_does_not_delete_active_cancelled_job(self):
        self.create()
        m = self.module.manager
        m.active.add(self.key)
        m.update(self.key, state='cancelled', expires=0)
        m.clean()
        self.assertTrue((pathlib.Path(self.tmp.name)/self.key).exists())
        m.active.discard(self.key)
        m.clean()
        self.assertFalse((pathlib.Path(self.tmp.name)/self.key).exists())

    def test_shared_source_cache_is_hash_verified_and_skips_second_upload(self):
        spec = {**self.spec, 'sourceSha256': hashlib.sha256(b'test').hexdigest()}
        with patch.dict(os.environ, {'ENCODER_SOURCE_CACHE': '1'}), patch.object(self.module.manager.pool, 'submit') as submit:
            self.create(spec=spec)
            r = self.client.put('/v1/exports/'+self.key+'/source', headers=self.headers, content=b'test')
            self.assertEqual(r.status_code, 200)
            r = self.create('b'*32, spec)
            self.assertTrue(r.json()['sourceCached'])
            self.assertEqual(submit.call_count, 2)
            self.assertEqual((pathlib.Path(self.tmp.name)/('b'*32)/'source').read_bytes(), b'test')
            self.create('c'*32, {**spec, 'sourceSha256': 'd'*64})
            r = self.client.put('/v1/exports/'+('c'*32)+'/source', headers=self.headers, content=b'test')
            self.assertEqual(r.status_code, 400)
            self.assertFalse((pathlib.Path(self.tmp.name)/('c'*32)/'source').exists())

    def test_restart_recovery_requires_complete_source_and_same_settings(self):
        self.create()
        p = pathlib.Path(self.tmp.name)/self.key
        (p/'source').write_bytes(b'test')
        self.module.manager.update(self.key, state='running')
        with patch.dict(os.environ, {'ENCODER_RECOVER_ON_RESTART': '1'}), patch.object(self.module.concurrent.futures.ThreadPoolExecutor, 'submit') as submit:
            recovered = self.module.Manager(pathlib.Path(self.tmp.name))
            try:
                self.assertEqual(recovered.get(self.key)['state'], 'queued')
                submit.assert_called_once()
                self.assertTrue((p/'source').exists())
            finally: recovered.pool.shutdown()

    def test_quota_evicts_only_unreferenced_sources(self):
        m = self.module.manager
        cache = pathlib.Path(self.tmp.name)/'sources'; cache.mkdir()
        old = cache/('a'*64); old.write_bytes(b'x'*2048)
        with patch.object(self.module, 'QUOTA', 1024):
            m.room()
        self.assertFalse(old.exists())
        live = cache/('b'*64); live.write_bytes(b'x'*2048)
        import os
        job = pathlib.Path(self.tmp.name)/('c'*32); job.mkdir()
        os.link(live, job/'source')
        with patch.object(self.module, 'QUOTA', 1024):
            with self.assertRaises(self.module.HTTPException): m.room()
        self.assertTrue(live.exists())

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

    def test_shutdown_preserves_source_but_user_cancel_does_not(self):
        self.create()
        m = self.module.manager
        p = pathlib.Path(self.tmp.name)/self.key
        (p/'source').write_bytes(b'test')
        m.restart_requested.set()
        with patch.object(m, 'encode_part', side_effect=RuntimeError('service stopping')):
            m.run(self.key)
        self.assertEqual(m.get(self.key)['state'], 'queued')
        self.assertTrue((p/'source').exists())
        m.update(self.key, state='cancelled')
        m.stops[self.key].set()
        m.run(self.key)
        self.assertEqual(m.get(self.key)['state'], 'cancelled')
        self.assertFalse((p/'source').exists())


if __name__ == '__main__':
    unittest.main()
