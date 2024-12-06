import unittest
import os
import shutil
from errors import TableNotUniqueError, TableDoesNotExistError
from lstore.db import Database

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.db.create_table("foo", 3, 0)

    def tearDown(self):
        self.db = None
        db_path = './TEMP'
        if (os.path.exists(db_path)):
            shutil.rmtree(db_path, ignore_errors=True)

    def test_create_table(self):
        self.assertTrue(self.db.tables.get("foo"))

    def test_create_existing_table(self):
        with self.assertRaises(TableNotUniqueError):
            self.db.create_table("foo", 2, 1)

    def test_drop_table(self):
        self.db.drop_table("foo")
        with self.assertRaises(TableDoesNotExistError):
            self.db.get_table("foo")

    def test_drop_non_existant_table(self):
        with self.assertRaises(TableDoesNotExistError):
            self.db.drop_table("bar")

    def test_double_drop_table(self):
        self.db.drop_table("foo")
        with self.assertRaises(TableDoesNotExistError):
            self.db.drop_table("foo")

    def test_get_table(self):
        self.db.get_table("foo")

    def test_get_non_existant_table(self):
        with self.assertRaises(TableDoesNotExistError):
            self.db.get_table("bar")

