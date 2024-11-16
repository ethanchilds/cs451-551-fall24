from lstore.index import TestIndexDataStructures
from lstore.db import TestDatabase
from lstore.page import TestPage
from data_structures.b_plus_tree import TestBPlusTree, TestNode
from data_structures.queue import TestQueue
from tests.test_block import TestBlock
from tests.test_priorityqueue import TestPriorityQueue
from test import TestLstoreIndex
from test import TestLstroreDB
from mergeTest import TestLstroreDB as the_first_one
from mergeThreadTest import TestLstroreDB as the_second_one

import unittest
unittest.main()