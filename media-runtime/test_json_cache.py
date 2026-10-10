import pathlib
import tempfile
import unittest
from json_cache import JsonCache


class CacheTest(unittest.TestCase):
    def test_content_and_revision_key_and_bounded_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            source = root/'sample'; source.write_bytes(b'fixture')
            cache = JsonCache(root/'cache', max_bytes=100)
            a, b = cache.key(source, 'model-v1'), cache.key(source, 'model-v2')
            self.assertNotEqual(a, b)
            self.assertIsNone(cache.get(a))
            cache.put(a, {'value': 'fixture'})
            self.assertEqual(cache.get(a), {'value': 'fixture'})
            cache.put(b, {'value': 'x'*200})
            self.assertIsNone(cache.get(b))
            cache.ttl = -1
            self.assertIsNone(cache.get(a))
            cache.clean(); self.assertFalse(cache.path(a).exists())

    def test_corruption_and_path_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = JsonCache(tmp)
            cache.path('a'*64).write_text('invalid json')
            self.assertIsNone(cache.get('a'*64))
            with self.assertRaises(ValueError): cache.get('../secret')


if __name__ == '__main__': unittest.main()
