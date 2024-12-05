import unittest
from utilities.algorithms import binary_search, linear_search

class TestAlgorithms(unittest.TestCase):
    
    def search_algorithms(self, lst, target):
        """ Helper function to test both search algorithms. """
        binary_result = binary_search(lst, target)
        linear_result = linear_search(lst, target)
        self.assertEqual(binary_result, linear_result)

    def test_found(self):
        self.search_algorithms([1, 2, 3, 4, 5], 3)
        self.search_algorithms([1, 2, 3, 4, 5], 1)
        self.search_algorithms([1, 2, 3, 4, 5], 5)

    def test_not_found(self):
        self.search_algorithms([1, 2, 3, 4, 5], 0)  
        self.search_algorithms([1, 2, 3, 4, 5], 6)  
        self.search_algorithms([1, 2, 3, 4, 5], 2.5)
        self.search_algorithms([1, 2, 3, 4, 5], 4.5)

    def test_empty_list(self):
        self.search_algorithms([], 1)  # Should return index 0

    def test_single_element(self):
        self.search_algorithms([1], 1) 
        self.search_algorithms([1], 0) 
        self.search_algorithms([1], 2) 

    def test_duplicates(self):
        self.search_algorithms([1, 2, 2, 2, 3], 2) 
        self.search_algorithms([1, 2, 2, 2, 3], 1) 
        self.search_algorithms([1, 2, 2, 2, 3], 3) 
        self.search_algorithms([1, 2, 2, 2, 3], 0) 
        self.search_algorithms([1, 2, 2, 2, 3], 2.5) 
        self.search_algorithms([1, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3], 3)