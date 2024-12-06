from data_structures.linked_list import LinkedList
import unittest
import random

class TestLinkedList(unittest.TestCase):

    def setUp(self):
        self.ll = LinkedList()
        self.n = 1000

    def tearDown(self):
        self.ll = None

    def test_pop_no_head(self):
        head = self.ll.pop_head()

        self.assertFalse(head)

    def test_push_one(self):
        self.ll.push(1)

        self.assertEqual(self.ll.head.data, 1)

    def test_pop_head_one(self):
        self.ll.push(1)

        head = self.ll.pop_head()

        self.assertEqual(head, 1)
        self.assertFalse(self.ll.head)
        self.assertFalse(self.ll.tail)

    def test_add_several_elements(self):

        elements = range(1,self.n+1)
        for element in elements:
            self.ll.push(element)

        last_node = self.ll.head
        idx = 0

        while last_node.next:
            self.assertEqual(last_node.data, elements[idx])
            idx += 1
            last_node = last_node.next

    def test_pop_several_elements(self):
        elements = range(1,self.n+1)

        for element in elements:
            self.ll.push(element)

        for element in elements:
            node = self.ll.pop_head()

            self.assertEqual(node, element)

        head = self.ll.pop_head()

        self.assertFalse(head)
        self.assertFalse(self.ll.tail)

    def test_len(self):
        rand_num = random.randint(self.n // 2, self.n)

        for i in range(rand_num):
            self.ll.push(i)

        self.assertEqual(len(self.ll), rand_num)

        for i in range(rand_num):
            self.ll.pop_head()

        self.assertEqual(len(self.ll), 0)

if __name__ == '__main__':
    unittest.main()