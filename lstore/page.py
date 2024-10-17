"""
The Page class provides low-level physical storage capabilities. In the provided skeleton, each 
page has a fixed size of 4096 KB. This should provide optimal performance when persisting to 
disk, as most hard drives have blocks of the same size. You can experiment with different sizes. 
This class is mostly used internally by the Table class to store and retrieve records. While working 
with this class, keep in mind that tail and base pages should be identical from the hardware’s point 
of view. The config.py file is meant to act as centralized storage for all the configuration options
and the constant values used in the code. It is good practice to organize such information into a
Singleton object accessible from every file in the project. This class will find more use when 
implementing persistence in the next milestone.
"""
from config import Config
from errors import PageNoCapacityError, PageValueTooLargeError, PageKeyError

class Page:
    # DANIEL QUESTION why dont we inherit size of the column data type we are using as the cell_size?
    def __init__(self, page_size=Config.page_size, cell_size=Config.page_cell_size):
        self.num_cells = 0
        self.data = bytearray(page_size)
        self.cell_size = cell_size
        self.page_id = id(self)

    # Locate does not check if the cell_number is valid.
    def __locate(self, cell_number):
        index = self.cell_size * cell_number
        return index
        

    def has_capacity(self):
        has_capacity = len(self.data) >= (self.num_cells + 1) * self.cell_size
        return has_capacity

    def write(self, value):
        if not self.has_capacity():
            raise PageNoCapacityError

        # DANIEL QUESTION are we sure len is correct here? maybe sys.getsizeof ???
        if len(value) > self.cell_size:      # Consider if this should be len(value) != self.cell_size instead
            raise PageValueTooLargeError
        
        padding_len = self.cell_size - len(value)   # Ensures value is exactly self.cell_size bytes long.
        value += (b"\x00" * padding_len)            # Prevents a really nasty bug.
        
        start_index = self.__locate(self.num_cells)
        end_index = start_index + self.cell_size
        self.data[start_index:end_index] = value
        self.num_cells += 1

    def read(self, cell_number: int) -> bytes:
        if cell_number >= self.num_cells or cell_number < 0:
            raise PageKeyError

        start_index = self.__locate(cell_number)
        return self.data[start_index:start_index + self.cell_size]
    
    def print(self, start_cell, end_cell):
        start_index = self.__locate(start_cell)
        end_index = self.__locate(end_cell) + self.cell_size
        print(repr(self.data[start_index:end_index]))


import unittest
class TestPage(unittest.TestCase):
    def setUp(self):
        # Small page to make testing easier.
        self.page = Page(page_size=64, cell_size=8)

    def test_write(self):
        self.page.write(b"\xff" * 8)

    def test_write_value_too_large(self):
        with self.assertRaises(PageValueTooLargeError):
            self.page.write(b"\xee" * 9)

    def test_write_no_capacity(self):
        for _ in range(8):
            self.page.write(b"01234567")

        with self.assertRaises(PageNoCapacityError):
            self.page.write(b"overflow")

    def test_read(self):
        value = b"page!"
        self.page.write(value)
        self.assertEqual(self.page.read(0), value + b"\x00\x00\x00")    # The page should pad any value smaller than the cell size

    def test_uninitialized_read(self):
        with self.assertRaises(PageKeyError):
            self.page.read(0)

    def test_locate(self):
        self.assertEqual(self.page._Page__locate(0), 0) # _Page__locate was requested by the interpreter instead of __locate
        self.assertEqual(self.page._Page__locate(1), 8)
        self.assertEqual(self.page._Page__locate(4), 4 * 8)
        

    def test_has_capacity(self):
        self.assertTrue(self.page.has_capacity())
        for i in range(7):
            self.page.write(f"i{i}".encode())
        self.assertTrue(self.page.has_capacity())
        self.page.write(b'hello')
        self.assertFalse(self.page.has_capacity())

