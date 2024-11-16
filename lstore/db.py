"""
The Database class is a general interface to the database and handles high-level operations such as 
starting and shutting down the database instance and loading the database from stored disk files. 
This class also handles the creation and deletion of tables via the create and drop function. The 
create function will create a new table in the database. The Table constructor takes as input the 
name of the table, the number of columns, and the index of the key column. The drop function 
drops the specified table
"""

# System imports
import os
import lzma

# Local imports
from lstore.table import Table
from errors import TableNotUniqueError, TableDoesNotExistError
from config import Config

class Database():
    def __init__(self):
        self.path = './TEMP'  # Path to the saved database
        self.tables = {}  # Dictionary of name - Table pairs

    def open(self, path):
        """Open an existing database
        
        Parameters
        ----------
        path : str
            The path to the database file.  This will
            be created if it doesn't already exist.
        """

        # Check if path is not empty
        if (path != ''):
            self.path = path

            # Check if path already exists
            if (not os.path.exists(path)):
                # Create the folder
                os.makedirs(path)
        else:
            pass # TODO: Throw an error for invalid path
    

    def close(self):
        """Close the current database

        This manages all of the bookkeeping
        methods that need to be called to
        keep the current database persistent
        on disk.
        """
        
        # Only close out if the stored path is proper
        if (self.path != ''):
            for table_name,t in self.tables.items():
                # Create a folder for each table
                    
                pname = os.path.join(self.path, table_name)
                if (not os.path.exists(pname)):
                    os.makedirs(pname)
                
                #close table
                t.close()


    def create_table(self, name, num_columns, key_index, force_merge=True):
        """Creates a new table

        Parameters
        ----------
        name : str
            Table name
        num_columns : int
            Number of Columns: all columns are integer
        key : int
            Index of table key in columns

        Returns
        -------
        table : Table
            The newly created table

        Raises
        ------
        
        """
        # Check that the name doesn't exist already
        if (name in self.tables):
            raise TableNotUniqueError
        

        table = Table(self.path, name, num_columns, key_index, force_merge)
        self.tables[name] = table

        return table

    
    def drop_table(self, name):
        """Deletes the specified table

        Parameters
        ----------
        name : str
            The name of the table to delete
        
        Raises
        ------
        
        """
        if (name not in self.tables):
            raise TableDoesNotExistError(f"cannot drop table `{name}` because it does not exist")
        
        del self.tables[name]

    
    def get_table(self, name):
        """Returns table with the passed name

        Parameters
        ----------
        name : str
            The name of the table to get

        Returns
        -------
        table : Table
            The table that was found in the current database
            or `None` if not found.
        """
        
        table_path = os.path.exists(os.path.join(self.path, name))
        
        if (name not in self.tables) and not os.path.exists(table_path):
            raise TableDoesNotExistError(f"cannot get table `{name}` because it does not exist")
        
        if os.path.exists(table_path):
            table = Table(self.path, name)
            self.tables[name] = table
            
        return self.tables.get(name)
    

import unittest
class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.db.create_table("foo", 3, 0)

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

