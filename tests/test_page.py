import unittest
from lstore.page import Page
from errors import PageNoCapacityError, PageValueTooLargeError, PageKeyError

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

