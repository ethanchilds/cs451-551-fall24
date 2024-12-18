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
import shutil

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

    def create_table(self, name, num_columns, key_index, force_merge=False, merge_interval=30):
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
        TableNotUniqueError
        """

        # Extract the table path on disk
        table_path = os.path.join(self.path, name)

        # Check that the name doesn't exist already
        if (name in self.tables or os.path.exists(table_path)):
            raise TableNotUniqueError
        
        # Create a new table
        table = Table(self.path, name, num_columns, key_index, force_merge, merge_interval)
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
        TableDoesNotExistError
        """

        # Extract the table path on disk
        table_path = os.path.join(self.path, name)

        # Check if the table does not exist
        if (name not in self.tables or not os.path.exists(table_path)):
            raise TableDoesNotExistError(f"cannot drop table `{name}` because it does not exist")
        
        # Delete the table on disk
        shutil.rmtree(table_path, ignore_errors=True)

        # Delete the table from memory
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

        Raises
        ------
        TableDoesNotExistError
        """
        
        table_path = os.path.join(self.path, name)

        if (name not in self.tables) and (not os.path.exists(table_path)):
            raise TableDoesNotExistError(f"cannot get table `{name}` because it does not exist")
        
        if os.path.exists(table_path):
            table = Table(self.path, name)
            self.tables[name] = table
            
        return self.tables.get(name)
