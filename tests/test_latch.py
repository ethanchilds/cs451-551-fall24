import unittest
import time
from utilities.latch import Latch

class TestLatch(unittest.TestCase):
    def setUp(self):
        self.latch = Latch()

    def test_one_shared(self):
        self.assertTrue(self.latch.request_shared())
        self.assertEqual(self.latch._exclusive_lock, False)
        self.assertEqual(self.latch._shared_count, 1)

        self.assertFalse(self.latch.request_exclusive())
        self.latch.release()

        self.assertEqual(self.latch._exclusive_lock, False)
        self.assertEqual(self.latch._shared_count, 0)

    def test_two_shared(self):
        self.assertTrue(self.latch.request_shared())
        self.assertTrue(self.latch.request_shared())
        self.assertFalse(self.latch.request_exclusive())

        self.latch.release()
        self.assertFalse(self.latch.request_exclusive())
        self.latch.release()
        self.assertTrue(self.latch.request_exclusive())

    def test_exlusive(self):
        self.assertTrue(self.latch.request_exclusive())
        self.assertFalse(self.latch.request_exclusive())
        self.latch.release()
        self.assertTrue(self.latch.request_exclusive())
        self.assertFalse(self.latch.request_shared())

    def test_whole_lotta_latches(self):
        self.assertTrue(self.latch.request_shared())
        self.assertTrue(self.latch.request_shared())
        self.assertTrue(self.latch.request_shared())
        self.assertFalse(self.latch.request_exclusive())
        self.assertTrue(self.latch.request_shared())
        self.latch.release()
        self.latch.release()
        self.assertTrue(self.latch.request_shared())
        self.latch.release()
        self.assertFalse(self.latch.request_exclusive())
        self.latch.release()
        self.latch.release()
        self.assertTrue(self.latch.request_exclusive())
        self.assertFalse(self.latch.request_exclusive())
        self.assertFalse(self.latch.request_shared())
        self.latch.release()
        with self.assertRaises(RuntimeError):
            self.latch.release()