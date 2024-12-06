import unittest
from random import shuffle
from errors import *
from data_structures.b_plus_tree import BPlusTree
from data_structures.hash_map import HashMap

class TestIndexDataStructures(unittest.TestCase):
    def setUp(self):
        self.ordered = BPlusTree(unique_keys=True, minimum_degree=2, return_keys=False)
        self.unordered = HashMap(unique_keys=True)

        items = [(i, i + 2) for i in range(10)]
        shuffle(items)

        self.ordered.bulk_insert(items)
        self.unordered.bulk_insert(items)

    def make_duplicate_keys(self):
        self.ordered.unique_keys = False
        self.unordered.unique_keys = False
        items = [(i, -i) for i in range(10)]
        self.ordered.bulk_insert(items)
        self.unordered.bulk_insert(items)

    def test_get(self):
        results1 = self.ordered.get(1)
        results2 = self.unordered.get(1)
        self.assertEqual(results1, [3])
        self.assertEqual(results1, results2)
    
    def test_get_non_unique(self):
        self.make_duplicate_keys()
        results1 = sorted(self.ordered.get(1))
        results2 = sorted(self.unordered.get(1))
        self.assertEqual(results1, [-1, 3])
        self.assertEqual(results1, results2)

    def test_get_range(self):
        results1 = sorted(self.ordered.get_range(0, 5))
        results2 = sorted(self.unordered.get_range(0, 5))
        self.assertEqual(results1, [2, 3, 4, 5, 6, 7])
        self.assertEqual(results1, results2)

    def test_get_range_non_unique(self):
        self.make_duplicate_keys()
        results1 = sorted(self.ordered.get_range(0, 2))
        results2 = sorted(self.unordered.get_range(0, 2))
        self.assertEqual(results1, [-2, -1, 0, 2, 3, 4])
        self.assertEqual(results1, results2)

    def test_insert_non_unique(self):
        with self.assertRaises(NonUniqueKeyError):
            self.ordered.insert(0, 10)

        with self.assertRaises(NonUniqueKeyError):
            self.unordered.insert(0, 10)

    def test_items(self):
        ordered_items = list(self.ordered.items())
        unordered_items = list(self.unordered.items())
        unordered_items.sort(key=lambda item: item[0])
        self.assertEqual(ordered_items, unordered_items)

    def test_remove(self):
        self.ordered.remove(1)
        self.unordered.remove(1)
        value1 = self.ordered.get(1)
        value2 = self.unordered.get(1)
        self.assertEqual(value1, [])
        self.assertEqual(value1, value2)

    def test_remove_duplicate_keys(self):
        self.make_duplicate_keys()
        value1 = sorted(self.ordered.get(1))[1]
        value2 = sorted(self.unordered.get(1))[1]
        self.ordered.remove(1, value1)
        self.unordered.remove(1, value2)
        new_values1 = self.ordered.get(1)
        new_values2 = self.unordered.get(1)
        self.assertEqual(new_values1, [-1])
        self.assertEqual(new_values1, new_values2)

    def test_update_bad_old_key(self):
        # Should raise KeyError
        with self.assertRaises(KeyError):
            self.ordered.update(-1, -2)

        with self.assertRaises(KeyError):
            self.unordered.update(-1, -2)

    def test_update_bad_old_key_non_unique(self):
        # Should raise KeyError
        self.make_duplicate_keys()
        with self.assertRaises(KeyError):
            self.ordered.update(-1, -2, value=0)

        with self.assertRaises(KeyError):
            self.unordered.update(-1, -2, value=0)

    def test_update_bad_new_key(self):
        # Should raise NotUniqueKeyError
        with self.assertRaises(NonUniqueKeyError):
            self.ordered.update(2, 0)

        with self.assertRaises(NonUniqueKeyError):
            self.unordered.update(2, 0)

    def test_update_bad_new_key_non_unique(self):
        # ALLOWED because keys can be duplicate
        self.make_duplicate_keys()
        self.ordered.update(2, 0, value=-2)
        self.unordered.update(2, 0, value=-2)

    def test_update_incorrect_value(self):
        # Not an issue because we don't use it when
        self.ordered.update(0, -1, value=300)
        self.unordered.update(0, -1, value=300)
        values1 = self.ordered.get(-1)
        values2 = sorted(self.unordered.get(-1))
        self.assertEqual(values1, [2])
        self.assertEqual(values1, values2)

    def test_update_invalid_value_non_unique(self):
        self.make_duplicate_keys()
        
        with self.assertRaises(KeyError):
            self.ordered.update(0, -1, value=10)    # There's no item (0, 10) in the datastructure
        
        with self.assertRaises(KeyError):
            self.unordered.update(0, -1, value=10)

    def test_update(self):
        self.ordered.update(0, -1)
        self.unordered.update(0, -1)
        values1 = self.ordered.get(-1)
        values2 = self.unordered.get(-1)
        self.assertEqual(values1, [2])
        self.assertEqual(values1, values2)

    def test_update_non_unique(self):
        self.make_duplicate_keys()
        self.ordered.update(0, -1, value=2)
        self.unordered.update(0, -1, value=2)
        values1 = self.ordered.get(-1)
        values2 = self.unordered.get(-1)
        self.assertEqual(values1, [2])
        self.assertEqual(values1, values2)

        self.ordered.insert(-0.5, "A NUMBER")

        v1 = self.ordered.get_range(0, 0)
        v2 = self.unordered.get_range(0, 0)
        self.assertEqual(v1, [0])
        self.assertEqual(v1, v2)