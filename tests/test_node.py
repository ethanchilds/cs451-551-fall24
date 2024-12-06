import unittest
from data_structures.b_plus_tree import Node
from errors import *

class TestNode(unittest.TestCase):

    def test_initialization(self):
        node = Node()
        self.assertEqual(node.minimum_degree, 16)
        self.assertFalse(node.is_leaf)
        self.assertEqual(node.keys, [])
        self.assertEqual(node.values, [])

        node = Node(minimum_degree=4, is_leaf=True)
        self.assertEqual(node.minimum_degree, 4)
        self.assertTrue(node.is_leaf)

    def test_is_maintained_leaf_node(self):
        node = Node(minimum_degree=2, is_leaf=True)
        node.keys = [1, 2, 3]
        node.values = [10, 20, 30]
        node.parent = Node()

        self.assertTrue(node.is_maintained(is_root=False))

    def test_is_maintained_internal_node(self):
        node = Node(minimum_degree=2, is_leaf=False)
        node.keys = [1]
        node.parent = Node()

        child_node1 = Node(minimum_degree=2, is_leaf=True, parent=node)
        child_node2 = Node(minimum_degree=2, is_leaf=True, parent=node)

        child_node1.keys = [0]
        child_node2.keys = [1]

        node.values = [child_node1, child_node2]

        self.assertTrue(node.is_maintained(is_root=False))

    def test_is_maintained_invalid_leaf_node(self):
        node = Node(minimum_degree=2, is_leaf=True)
        node.keys = [1, 2]
        node.values = [10]  # len(values) != len(keys)

        with self.assertRaises(LeafNodeValueCountError):
            node.is_maintained(is_root=False)

    def test_is_maintained_invalid_internal_node(self):
        child_node = Node(minimum_degree=2, is_leaf=True)
        node = Node(minimum_degree=2, is_leaf=False)
        node.keys = []
        node.values = [child_node]

        with self.assertRaises(NonRootNodeKeyCountError):
            node.is_maintained(is_root=False)

    def test_keys_order(self):
        node = Node(minimum_degree=2, is_leaf=False)
        node.keys = [1, 3, 2]  # This should be an invalid case
        node.values = [Node()] * 4

        with self.assertRaises(NonDecreasingOrderError):
            node.is_maintained(is_root=False)

    def test_keys_within_limits(self):
        node = Node(minimum_degree=2, is_leaf=False)
        node.keys = [1, 2, 3]
        node.values = [Node(parent=node) for _ in range(4)]
        node.parent = Node()
        for i in range(4):
            node.values[i].keys = [i]

        self.assertTrue(node.is_maintained(is_root=False))

        node.keys = list(range(1, 10))  # 9 keys, invalid if minimum_degree is 5
        with self.assertRaises(MaxKeysExceededError):
            node.is_maintained(is_root=False)