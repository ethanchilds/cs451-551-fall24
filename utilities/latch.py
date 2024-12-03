import threading
import unittest
import time

import threading

class Latch:
    def __init__(self):
        self._lock = threading.Lock()
        self._shared_count = 0
        self._exclusive_lock = False
        self._condition = threading.Condition(self._lock)

    def request_exclusive(self):
        with self._condition:
            if self._exclusive_lock or self._shared_count > 0:
                return False
            self._exclusive_lock = True
            return True

    def request_shared(self):
        with self._condition:
            if self._exclusive_lock:
                return False
            self._shared_count += 1
            return True

    def release(self):
        with self._condition:
            if self._exclusive_lock:
                self._exclusive_lock = False
            elif self._shared_count > 0:
                self._shared_count -= 1
            else:
                # Someone called release when they weren't supposed to :(
                raise RuntimeError("Release called without an active lock")
            
            self._condition.notify_all()

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
