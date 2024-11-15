import threading
import unittest
import random
from data_structures.linked_list import LinkedList

class Queue:

    def __init__(self, elements = None):
        self.queue = LinkedList()
        self.lock = threading.Lock()
        self.size = 0
        
        if elements:
            self.size = len(elements)
            for element in elements:
                self.queue.push(element)

    def push(self, data):
        with self.lock:
            self.queue.push(data)
            self.size += 1

    def pop(self):
        with self.lock:
            self.size -= 1
            return self.queue.pop_head()

    def isEmpty(self):
        if self.size == 0:
            return True
        return False
    
    def __len__(self):
        return self.size
    
class TestQueue(unittest.TestCase):

    def setUp(self):
        self.q = Queue()
        self.n = 1000

    def tearDown(self):
        self.q = None

    def test_pop_no_value(self):
        value = self.q.pop()

        self.assertFalse(value)

    def test_push_one(self):
        self.q.push(1)

        self.assertEqual(self.q.pop(), 1)
        self.assertEqual(self.q.size, 0)

    def test_add_several_elements(self):

        elements = range(1,self.n+1)
        for element in elements:
            self.q.push(element)

        idx = 0
        while not self.q.isEmpty():
            self.assertEqual(elements[idx], self.q.pop())
            idx += 1

    def test_len(self):
        rand_num = random.randint(self.n // 2, self.n)

        for i in range(rand_num):
            self.q.push(i)

        self.assertEqual(len(self.q), rand_num)

        for i in range(rand_num):
            self.q.pop()

        self.assertEqual(len(self.q), 0)

    def test_passed_in_elements(self):
        elements = range(self.n)
        test_queue = Queue(elements)

        self.assertEqual(self.n, len(test_queue))

        for element in elements:
            value = test_queue.pop()
            self.assertEqual(value, element)

        self.assertEqual(0, len(test_queue))

if __name__ == '__main__':
    unittest.main()