from tests.test_index_data_structures import TestIndexDataStructures
from tests.test_bplus_tree import TestBPlusTree
from tests.test_node import TestNode
from tests.test_queue import TestQueue
from tests.test_block import TestBlock
from tests.test_linked_list import TestLinkedList
from tests.test_priorityqueue import TestPriorityQueue
from tests.test_everything import TestLstoreIndex, TestLstoreDB, TestTransactionUndo, UltimateLstoreTest, UltimateLstoreConcurrencyTest
from tests.mergeTest import TestMerge
from tests.mergeThreadTest import TestMergeThread

import unittest
import argparse

#from tests.test_database import TestDatabase
#from tests.test_page import TestPage
# TestDatabase, TestPage are legacy code, currently not correct for our system


"""
To add tests to automatic run do the following:

1. Import test class, please save these to their own file in the "tests" directory.

2. Under main's else statement, add suite.addTests(loader.loadTestsFromTestCase(ExampleTest)),
where ExampleTest is the new imported test class.

3. To add test class to help command lt, add string test to TEST_LIST.
"""

TEST_LIST = [
    "TestIndexDataStructures", 
    "TestBPlusTree", 
    "TestNode", 
    "TestQueue",
    "TestBlock",
    "TestLinkedList",
    "TestPriorityQueue",
    "TestLstoreIndex",
    "TestLstoreDB",
    "TestTransactionUndo",
    "UltimateLstoreTest",
    "UltimateLstoreConcurrencyTest",
    "TestMerge",
    "TestMergeThread"
    ]

def main():
    # Argument parser for filtering test classes
    parser = argparse.ArgumentParser(description="Run tests")
    parser.add_argument(
        "-t", "--test",
        help="Specify a test class name (e.g., Example1Test) to run"
    )
    parser.add_argument(
        "-lt", "--list-tests",
        action="store_true",  # Flag that doesn't require a value
        help="List all available test classes and exit."
    )
    args = parser.parse_args()

    if args.list_tests:
        print("Available tests:")
        for test_name in TEST_LIST:
            print(f"- {test_name}")
        return

    # Create a test suite
    loader = unittest.TestLoader()

    if args.test:
        # Run the specific test class
        try:
            suite = loader.loadTestsFromTestCase(globals()[args.test])
        except KeyError:
            print(f"Test class '{args.test}' not found.")
            return
    else:
        # Run all test cases from imported classes
        suite = unittest.TestSuite()
        suite.addTests(loader.loadTestsFromTestCase(TestIndexDataStructures))
        #suite.addTests(loader.loadTestsFromTestCase(TestDatabase))
        #suite.addTests(loader.loadTestsFromTestCase(TestPage))
        suite.addTests(loader.loadTestsFromTestCase(TestBPlusTree))
        suite.addTests(loader.loadTestsFromTestCase(TestNode))
        suite.addTests(loader.loadTestsFromTestCase(TestQueue))
        suite.addTests(loader.loadTestsFromTestCase(TestBlock))
        suite.addTests(loader.loadTestsFromTestCase(TestPriorityQueue))
        suite.addTests(loader.loadTestsFromTestCase(TestLstoreIndex))
        suite.addTests(loader.loadTestsFromTestCase(TestLstoreDB))
        suite.addTests(loader.loadTestsFromTestCase(TestMerge))
        suite.addTests(loader.loadTestsFromTestCase(TestMergeThread))
        suite.addTest(loader.loadTestsFromTestCase(TestLinkedList))
        suite.addTests(loader.loadTestsFromTestCase(TestTransactionUndo))
        suite.addTests(loader.loadTestsFromTestCase(UltimateLstoreTest))
        suite.addTests(loader.loadTestsFromTestCase(UltimateLstoreConcurrencyTest))
        
    # Run the tests
    runner = unittest.TextTestRunner()
    runner.run(suite)

if __name__ == "__main__":
    main()