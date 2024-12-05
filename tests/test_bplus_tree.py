import unittest
from data_structures.b_plus_tree import BPlusTree, Node
from errors import *
from random import random
from config import Config



class TestBPlusTree(unittest.TestCase):
    def setUp(self):
        self.tree = BPlusTree(minimum_degree=2, unique_keys=False, return_keys=True)

    # I used https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html to help me design this. -Kai
    # Its what a tree should look like after inserting 1 through 10 in order.
    def make_generic_tree(self):
        # Root Node
        tree = BPlusTree(2)

        tree.root.keys = [7]
        tree.root.is_leaf = False  # On by default. Manually turn off.
        
        # Internal Nodes
        internal1 = Node(2, False)
        internal2 = Node(2, False)
        internal1.parent = tree.root
        internal2.parent = tree.root
        tree.root.values = [internal1, internal2]
        
        # Leaf Nodes
        leaf11 = Node(2, True)
        leaf12 = Node(2, True)
        leaf13 = Node(2, True)

        leaf21 = Node(2, True)
        leaf22 = Node(2, True)

        internal1.keys = [3, 5]
        internal1.values = [leaf11, leaf12, leaf13]
        leaf11.parent = internal1
        leaf12.parent = internal1
        leaf13.parent = internal1

        internal2.keys = [9]
        internal2.values = [leaf21, leaf22]
        leaf21.parent = internal2
        leaf22.parent = internal2
        

        # Leaf Node values
        leaf11.keys = [1, 2]
        leaf11.values = [[1], [2]]
        leaf11.link = leaf12

        leaf12.keys = [3, 4]
        leaf12.values = [[3], [4]]
        leaf12.link = leaf13

        leaf13.keys = [5, 6]
        leaf13.values = [[5], [6]]
        leaf13.link = leaf21

        leaf21.keys = [7, 8]
        leaf21.values = [[7], [8]]
        leaf21.link = leaf22

        leaf22.keys = [9, 10]
        leaf22.values = [[9], [10]]
        leaf22.link = None

        tree.length = 10
        tree.height = 2

        return tree
    
    def test_generic_tree_is_maintained(self):
        tree = self.make_generic_tree()
        self.assertTrue(tree.is_maintained())

    def test_eq(self):
        tree1 = self.make_generic_tree()
        tree2 = self.make_generic_tree()
        self.assertEqual(tree1, tree2)

    def test_not_eq(self):
        tree1 = self.make_generic_tree()
        tree2 = self.make_generic_tree()
        tree2._minimum_leaf().keys[0] = -10
        self.assertTrue(tree2.is_maintained())
        self.assertNotEqual(tree1, tree2)

    def test_generated_tree_is_maintained_and_equals_generic_tree(self):
        tree = self.tree
        for i in range(1, 11):
            tree.insert(i, i)

        for i in range(1, 11):
            self.assertEqual(tree.get(i), [(i, i)])

        tree2 = self.make_generic_tree()

        self.assertTrue(tree.is_maintained())
        self.assertEqual(tree, tree2)

    def test_insert_no_splits(self):
        tree = self.tree
        for i in range(1, 4):
            tree.insert(i, i)

        self.assertTrue(tree.is_maintained())

        # The root node can hold all three so it shouldn't split
        self.assertEqual(tree.root.keys, [1, 2, 3])

    def test_insert_one_leaf_split(self):
        tree = self.tree
        for i in range(1, 5):
            tree.insert(i, i)

        tree.is_maintained()
        self.assertEqual(tree.root.keys, [3])
        self.assertEqual(tree.root.values[0].keys, [1, 2])
        self.assertEqual(tree.root.values[1].keys, [3, 4])

    def test_insert_two_leaf_split(self):
        tree = self.tree
        for i in range(6):
            tree.insert(i, i)

        tree.is_maintained()
        self.assertEqual(tree.root.keys, [2, 4])
        self.assertEqual(tree.root.values[0].keys, [0, 1])
        self.assertEqual(tree.root.values[1].keys, [2, 3])
        self.assertEqual(tree.root.values[2].keys, [4, 5]) 

    def test_insert_duplicate_keys_1(self):
        tree = BPlusTree(minimum_degree=2, unique_keys=False)
        for i in range(10):
            tree.insert(i, i)
        
        for i in range(10):
            tree.insert(i, i)

    def test_insert_duplicate_keys_2(self):
        tree = BPlusTree(minimum_degree=2, unique_keys=True)
        for i in range(10):
            tree.insert(i, i*i)

        with self.assertRaises(NonUniqueKeyError):
            tree.insert(0, 0)


    def test_valid_leaf_link(self):
        tree = self.tree
        for i in range(101):
            message = ""
            if i % 3 == 0:
                message += "fizz"
            if i % 5 == 0:
                message += "buzz"
            tree.insert(i, message)

        leaf = tree._minimum_leaf() 

        while leaf.link is not None:
            leaf = leaf.link

        # The last value in the link should be the maximum value.
        self.assertEqual([(leaf.keys[-1], leaf.values[-1][0])], tree.maximum())

        # The items iterator should contain as many items as are in the tree.
        self.assertEqual(len(list(tree.items())), len(tree))

    def test_min_and_max_item(self):
        from random import shuffle
        keys = [i for i in range(1_000)]
        shuffle(keys)

        for key in keys:
            self.tree.insert(key, key)

        self.assertEqual(self.tree.minimum(), [(0, 0)])
        self.assertEqual(self.tree.maximum(), [(999, 999)])

        
    def test_get_range_1(self):
        tree = self.tree
        for i in range(100):
            tree.insert(i, None)

        self.assertEqual(tree.get_range(10, 15.5), [(10, None), (11, None), (12, None), (13, None), (14, None), (15, None)])

    def test_get_range_2(self):
        tree = self.tree
        for i in range(100):
            tree.insert(i, None)

        self.assertEqual(tree.get_range(None, 4), [(0, None), (1, None), (2, None), (3, None), (4, None)])

    def test_get_range_3(self):
        tree = self.tree
        for i in range(100):
            tree.insert(i, None)

        self.assertEqual(tree.get_range(97, None), [(97, None), (98, None), (99, None)])

    # This test revealed a flaw in my duplicate key approach.
    # As it was, we insert duplicate keys at will, and when we look for one of the items
    #   , we find the furthest left occurence of the key, and follow the link until we 
    #   get the key-value pair. 
    # As it turns out, b+trees can't return the furthest left occurence of a duplicate key.
    # With a few hours to go before the assignment was due, we made the leafes a doubly linked list,
    #   and wen't backwards to find the furthest left occurence of a key.
    # It ended up working correctly!
    # However, we had to change the approach for a3 because having duplicate keys spread out like this
    #   is not latch friendly. 
    def test_get_duplicates(self):
        from random import random
        tree = self.tree
        tree.return_keys = False
        tree.unique_keys = False
        for i in range(50):
            if i % 10 == 0:
                tree.insert(0, i)
            else:
                tree.insert(random(), i)
        self.assertEqual(sorted(tree.get(0)), [0, 10, 20, 30, 40])

    def test_in_operator(self):
        tree = self.tree
        for i in range(1000):
            if i == 42:
                continue

            tree.insert(i, i ^ 10)
        
        self.assertFalse(42 in tree)
        self.assertTrue(10 in tree)

    def test_many_trees(self):
        for size in range(100):
            self._test_many_trees(size * 3)

    def _test_many_trees(self, size):
        tree = BPlusTree(minimum_degree=2)
        for i in range(size):
            tree.insert(random(), i)
        tree.is_maintained()

    def test_parent_pointer(self):
        tree = self.tree
        for i in range(50):
            tree.insert(i, i*i)

        node = tree.root
        while not node.is_leaf:
            self.assertTrue(node.values[-1].parent == node)
            node = node.values[-1]

    def test_remove_once(self):
        tree = self.tree
        tree.unique_keys = True
        for i in range(50):
            tree.insert(i, i*i)

        tree.remove(42)

        self.assertTrue(tree.is_maintained())
        self.assertTrue(len(tree.get(42)) == 0)
        self.assertTrue(len(tree) == 49)

    def test_fill_and_empty_from_left(self):
        tree = self.tree
        tree.unique_keys = True
        for i in range(100):
            tree.insert(i, i*i)

        for i in range(100):
            tree.remove(i)

        self.assertEqual(len(tree), 0)

    def test_fill_and_empty_from_right(self):
        tree = self.tree
        tree.unique_keys = True

        try:
            for i in range(28):
                tree.insert(i, i*i)

            for i in range(27, -1, -1):
                tree.remove(i)
        except Exception as e:
            print("TEST FILL AND EMPTY FROM RIGHT FAILED. PRINTING TREE STATE")
            print(tree)
            raise e

        self.assertEqual(len(tree), 0)


    def test_random_insert_and_remove(self):
        tree = self.tree
        tree.unique_keys = True
        items = [[random(), random()] for _ in range(64)]

        for i in range(32):
            tree.insert(items[i][0], items[i][1])

        for i in range(16):
            tree.remove(items[i][0])

        for i in range(32, 64):
            tree.insert(items[i][0], items[i][1])

        for i in range(32, 48):
            tree.remove(items[i][0])

        self.assertTrue(tree.is_maintained())

    def test_erratic_insert_and_remove(self):
        from random import shuffle
        from random import seed
        seed(99)
        
        tree = self.tree
        tree.unique_keys = True

        # Make a list of insert and remove operations in a random order, with insert before remove 
        items = []
        for i in range(5_000):
            key = i
            value = None
            items.append([key, value])
            items.append([key, value])

        shuffle(items)

        item_set = set()
        for i, item in enumerate(items):
            item[1] = i
            if item[0] not in item_set:
                item.append("Insert")
                item_set.add(item[0])
            else:
                item.append("Remove")
        try:
            for item in items:
                # print(item)
                if item[2] == "Insert":
                    tree.insert(item[0], item[1])
                else:
                    tree.remove(item[0])
        except Exception as e:
            print("TEST FAILED. PRINTING TREE STATE")
            print(tree)
            raise e

        self.assertTrue(tree.is_maintained())
        self.assertEqual(len(tree), 0)

    def test_large_tree(self):
        tree = BPlusTree(minimum_degree=Config.b_plus_tree_minimum_degree)
        for i in range(50_000):
            tree.insert(random(), i)

        self.assertTrue(tree.is_maintained())

    def test_bulk_insert(self):
        tree1 = BPlusTree(minimum_degree=2)
        tree2 = BPlusTree(minimum_degree=2)

        items = [(i, i << 2) for i in range(10000)]

        for key, value in items:
            tree1.insert(key, value)

        tree2.bulk_insert(items)

        self.assertTrue(tree2.is_maintained())

        self.assertTrue(tree1 == tree2)

    def test_many_bulk_inserts(self):
        for i in range(20):
            tree1 = BPlusTree(minimum_degree=2, unique_keys=False)
            tree2 = BPlusTree(minimum_degree=2, unique_keys=False)
            items = [(j, j) for j in range(i)]
            # So we have some duplicate key in here
            items.append((i//2, i//2))
            tree1.bulk_insert(items)
            for key, value in items:
                tree2.insert(key, value)

            self.assertEqual(tree1, tree2)

    def test_rev_link(self):
        items = [(i, i) for i in range(100)]
        self.tree.bulk_insert(items)
        self.assertEqual(len(self.tree), len(list(self.tree.items_rev())))
        
        for i in range(10, 20):
            self.tree.remove(i, i)

        self.assertEqual(len(self.tree), len(list(self.tree.items_rev())))

        for i in range(50, 80):
            self.tree.insert(i, i+7)

        self.assertEqual(len(self.tree), len(list(self.tree.items_rev())))
        


    def test_bulk_insert_on_existing_tree(self):
        tree1 = BPlusTree(minimum_degree=2, unique_keys=True)
        tree2 = BPlusTree(minimum_degree=2, unique_keys=True)
        
        items1 = [(i, i | 8) for i in range(20)]
        items2 = [(i, i | 8) for i in range(20, 40)]
        tree1.bulk_insert(items1)    # we have established that this works
        tree2.bulk_insert(items1)    # just part of the setup

        tree1.bulk_insert(items2)
        for item in items2:
            tree2.insert(*item)

        self.assertEqual(tree1, tree2)