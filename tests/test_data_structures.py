from data_structures.b_plus_tree import BPlusTree
from data_structures.hash_map import HashMap
from data_structures.ordered_dict import OD
#from bintrees import *
from utilities.timer import timer
from random import *

import unittest
# from test import TestLstoreIndexUndo
from utilities.latch import TestLatch
unittest.main()
from tests.test_everything import UltimateLstoreTest
from lstore.index import TestIndexDataStructures
from data_structures.b_plus_tree import TestBPlusTree
from tests.test_everything import TestLstoreDB
from tests.test_everything import TestLstoreIndex
from tests.test_everything import UltimateLstoreTest
from lstore.table import Table

def test_data_structure_correctness(DataStructure, operations):
    data_structure = DataStructure()

    # TODO: add delete tests to this.

    # Test Empty data structure.
    assert(data_structure.get(0) is None)
    assert(not data_structure.contains_key(0))
    assert(data_structure.minimum() is None)
    assert(data_structure.maximum() is None)
    assert(data_structure.len() == 0)
    # assert(tree.delete is None or however this is implimented)
    # assert(also the transplant one shouldn't work)

    # Test data structure with one node.
    data_structure.insert(0, "a")
    # assert(data_structure.get(0) == "a")
    assert(data_structure.contains_key(0))
    assert(data_structure.minimum() == "a")
    assert(data_structure.maximum() == "a")
    assert(data_structure.len() == 1)

    # Test data structure with multiple nodes.
    data_structure.insert(1, "b")
    data_structure.insert(2, "c")
    data_structure.insert(-1, "d")
    assert(data_structure.contains_key(-1))
    assert(data_structure.get(2) == "c")
    assert(data_structure.minimum() == (-1, "d"))
    assert(data_structure.maximum() == (2, "c"))
    assert(len(data_structure) == 4)

    # Test data structure with duplicate nodes.
    # assert(data_structure.insert(2, "e"))
    # assert(data_structure.insert(2, "f"))
    # assert(data_structure.insert(2, "g"))
    # print(data_structure.get(2))

    # BENCHMARKS
    x = []

    # Add many items
    for i in range(operations):
        x.append((random(), random()))
        data_structure.insert(x[i][0], x[i][1])
        



    # Look up many items
    hit_rate = [0, 0]
    for i in range(operations):
        try:
            assert(data_structure.get(x[i][0]) == x[i][1])
            hit_rate[0] += 1
        except:
            # print(data_structure.get(x[i][0]), "!=", x[i][1])
            hit_rate[1] += 1

    # datastructure CAN NOT be used until this prints 100%.
    print(100 * hit_rate[0] / (hit_rate[0] + hit_rate[1]), "%", sep="")

    # Get all of the items at once
    # data_structure.keys()

    data_structure.insert(2, "apple")

    # TODO: delete all items
    
    print("Data Structure `"+data_structure.__class__.__name__+"` passed all tests!")

@timer
def test_data_structure_insert_speed(DataStructure, operations):
    data_structure = DataStructure(unique_keys=False)

    items = [(random(), "Value")] * operations
    sorted(items, key= lambda x: x[0])

    for item in items:
    # for i in range(operations):
        # key = random()
        # value = key * key
        # data_structure.insert(key, value)
        data_structure.insert(item[0], item[1])

    return data_structure

"""
A simple function to alter a number to make the map more interesting.
WARNING don't use this in a benchmark. It REALLY slows down the benchmark.
"""
def transform_number(number):
    number_str = str(number)
    binary_representation = []

    for char in number_str:
        if char.isdigit():
            if int(char) > 5:
                binary_representation.append('1')
            else:
                binary_representation.append('0')
        else:
            binary_representation.append('.')

    return ''.join(binary_representation)
    


def test_data_structure_get_speed(DataStructure, operations):
    data_structure = DataStructure()

    inputs = []

    for _ in range(operations):
        key = random()
        value = transform_number(key)
        data_structure.insert(key, value)

        inputs.append(key)

    return _test_data_structure_get_speed(data_structure, inputs)

@timer
def _test_data_structure_get_speed(data_structure, inputs):
    for input in inputs:
        if not data_structure.contains_key(input):
            print("{input} was false for some reason")

    return data_structure


# data_structure should already contain values.
# data_structure.get_range() must be defined.
@timer
def test_data_structure_get_range_speed(data_structure, operations, range_delta):
    for i in range(operations):
        low_key = random()
        high_key = low_key + range_delta

        data_structure.get_range(low_key, high_key)


        


def test_insert_speed(classes: list, operations: int):
    for c in classes:
        test_data_structure_insert_speed(c, 500_000)

def test_get_range_speed(classes, operations):
    for c in classes:
        test_data_structure_get_range_speed(c, 500_000)


#classes = [FastBinaryTree, BSTree, BPlusTree, HashMap]
# test_insert_speed(classes, 500_000)
# test_get_range_speed(classes, 500_000)

from config import Config






# Findings:
# Insert and get point:
#   Binary Search Tree: O(lg(h)) medium speed.
#   B Plus Tree:        O(min_deg*lg(h)) up to 20%ish faster than bst. Depends on minimum degree.
#   HashMap:            O(1) ave  10 times faster
#
# get range (not tested yet):
#   Binary Search Tree: O(range)    scan only the range, however, a weird tree walk is needed.
#   B Plus Tree:        O(range) efficient. Scan only what is in the range. Linked nodes means no weird tree walk.
#   HashMap:            O(n) always. linear scan no matter the range
#
# value of min or max key:
#   Binary Search Tree: O(lg(h)) pretty much free.
#   B Plus Tree:        O(b*lg(h)) pretty much free.
#   HashMap:            O(n) Very expensive.

# Consider using the bplustree library. It sores the tree in a file.












from lstore.page import Page

import unittest
# from lstore.db import Database, TestDatabase
# from lstore.page import Page# , TestPage
# from utilities.algorithms import TestAlgorithms
from data_structures.b_plus_tree import TestNode as TestBPlusNode
from data_structures.b_plus_tree import BPlusTree, TestBPlusTree
from lstore.index import TestIndexDataStructures


# map = test_data_structure_get_speed(HashMap, 1_000_000)


# print(map.get_range(0, 0.00001))
# test_data_structure_insert_speed(BPlusTree, 500_000) # executed in 4.7528 seconds. 4.2134 with better parent method
# test_data_structure_insert_speed(HashMap, 500_000)


from lstore.db import Database
from lstore.index import Index
from lstore.query import Query
import time

l = [1, 1, 2, 6, 24, 120, 720]
print(l)
exit()

tree = BPlusTree(minimum_degree=2, unique_keys=False, return_keys=False)
# tree.insert(1, 1)
# tree.insert(1, 2)
# tree.insert(2, 2)
# tree.insert(3, 3)
# tree.insert(3, 4)
# tree.insert(1, 3)
# tree.insert(1, 3)
items = [(i, i) for i in range(500_001)]
# items.extend([(i, i+1) for i in range(50000)])
s1 = time.time()
tree.bulk_insert(items)
e1 = time.time()
# print(tree)

tree2 = BPlusTree(minimum_degree=2, unique_keys=False, return_keys=False)
s2 = time.time()
for key, value in items:
    tree2.insert(key, value)
e2 = time.time()

print(f"t1: {e1 - s1}")
print(f"t2: {e2 - s2}")
print(tree == tree2)

exit()


# COMPARING INSERT VS BULK INSERT (min degree 128) (items already sorted)
# size          insert time     bulk insert time    speed up
# 100_000       0.2744          0.0277              9.9061
# 500_000       1.5082          0.0968              15.5805
# 1_000_000     3.0603          0.2426              12.6145
# 5_000_000     16.7839         0.9855              17.0308
# 10_000_000    35.8649         2.3495              15.2649

# 20_000_000                    4.7174
# 30_000_000                    12.4288
# 40_000_000                    20.1099
# 45_000_000                    27.4965
# 50_000_000    243.1192        41.6309             5.8398
# Bulk insert seems to use too much memory making it slow down fast with tens of millions of items

# TODO: make this same table but shuffle the items first. My first findings are that bulk insert is 6 times faster

from lstore.index import TestIndexDataStructures
# from lstore.index import TestIndex
# unittest.main()


import time
def tree_insert(tree, items):
    for key, value in items:
        tree.insert(key, value)

def tree_bulk_insert(tree, items):
    tree.bulk_insert(items)


def benchmark_start_from_zero(mix_items:bool):
    total_items = 5_000_000
    trials = 20

    # for i in range(trials + 1):
    i = 20
    size = i * (total_items // trials) + 1

    items = [(i, i) for i in range(size)]
    if mix_items:
        shuffle(items)

    tree = BPlusTree(unique_keys=False)

    start = time.time()
    tree_bulk_insert(tree, items)
    end = time.time()
    duration = f"{end - start:.2f}"
    print(duration)


benchmark_start_from_zero(mix_items=False)
exit()

def benchmark_inserts(bulk: bool, mix_items: bool):
    # CHANGE AROUND THE CONFIG.PY TO SWITCH INSERT MODES
    # database = Database()
    # table = database.create_table("Index Benchmark", 2, 0)
    # query = Query(table)


    initial_size = 1_000_000
    trials = 20

    for i in range(trials+1):
        final_size = initial_size + ((i * initial_size) // trials)

        items = [(i, i ^ (i << 13)) for i in range(final_size)]
        if mix_items:
            shuffle(items)
        
        tree = BPlusTree()
        tree.bulk_insert(items[:initial_size])

        start_time = time.time()
        if bulk:
            tree_bulk_insert(tree, items[initial_size:])
        else:
            tree_insert(tree, items[initial_size:])
        end_time = time.time()

        duration = end_time - start_time
        percent = 100 * (final_size - initial_size) / initial_size
        print(f"{duration:.4f}")

benchmark_inserts(bulk=True, mix_items=True)




exit()

# COMPARING INSERT VS BULK INSERT (min degree 128)
# size          insert time     bulk insert time    difference
# 100_000       0.2744          0.0259              10.594
# 500_000       1.5082          0.1573              9.588
# 1_000_000     3.0603          0.2500              12.241
# 5_000_000     16.7839         1.3660              12.286
# 10_000_000    35.8649         3.3714              10.637
# 50_000_000    203.3555        it never finishes for some reason


@timer
def build_tree_by_one_at_a_time_insert(minimum_degree, size, items):
    tree = BPlusTree(minimum_degree)
    for key, value in items:
        tree.insert(key, value)

    return tree

    return tree

@timer
def build_tree_by_batch_insert(minimum_degree, size, items):
    # Pass the items as an argument becuase making and storing them takes just as long as bulk insert
    tree = BPlusTree(minimum_degree)
    tree.bulk_insert(items)

    return tree

minimum_degree = 128

size = 5_000_000
items = [(i, i ^ 63) for i in range(size)]
shuffle(items)

print()
tree_insert = build_tree_by_one_at_a_time_insert(minimum_degree, size, items)
print()
tree_batch_insert = build_tree_by_batch_insert(minimum_degree, size, items)

# tree_batch_insert.is_maintained()

print(f"\nBoth trees are equivelant: {tree_batch_insert == tree_insert}")

size = 1_000_000

tree_insert = fn1(minimum_degree, size)
items = [(i, i+i) for i in range(size)]
print("starting fn2")
tree_batch_insert = fn2(minimum_degree, size, items)

# tree_batch_insert._maximum_leaf().values[0] = "AWOOOOOGAAA"
# tree_batch_insert.root.values[1].values[-1].keys[-1] += 1
tree_batch_insert.is_maintained()

print(tree_insert == tree_batch_insert)

# b = []
# limit = size - (minimum_degree * 2)
# i = 0

# while i <= limit:
#     # print(minimum_degree)
#     b.append(minimum_degree)
#     i += minimum_degree
# b.append(size - i)
# print(b)
    

exit()

seed = 123456789
m = 2 << 62
a = 1103515245
c = 12345

for i in range(100):
    seed = (a * seed + c) % m
    query.insert(i, seed, 100000 // (i + 1))

print(table.index.locate_range(0, 100000, 1))
table.index.create_index(1, ordered=False)
print(table.index.locate_range(0, 100000, 1))
print(table.index.locate(2, 99))
print(table.index.locate(2, 99))
print(table.index.usage_histogram)
print(table)
