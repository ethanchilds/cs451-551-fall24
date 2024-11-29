"""
This contains various wrappers for handling
threaded queries.
"""

from lstore.query import Query
from config import Config


class QueryWrapper():
    """
    A QueryWrapper object is a way of wrapping
    a query into its threaded counterpart to allow
    concurrent queries to take place seamlessly.
    """

    def __init__(self, table, query_function, transaction, args):
        """
        Initialize a new ThreadedQuery which wraps
        a specific query function.  This allows
        determining the data dependencies, locking
        and other operations to facilitate concurrency.

        Parameters
        ----------
        table : Table
            The Table object to control access to
        query_function : function
            A specific Query function to try to execute
        transaction : Transaction
            The Transaction object associated with the Query
        args : list
            A list of arguments to be passed into the query_function
        """
        
        # Set up internal variables
        self.table = table
        self.query_function = query_function
        self.transaction = transaction
        self.args = args
        self.lock_manager = self.table.lock_manager

        # While we could find these values at initialization
        # it requires the index lock, so wait until try_run.
        # Also saves some work.

        # In case of delete roll back
        if self.query_function == Query.delete:
            self.delete_rid = None

        # in case of update roll back
        if self.query_function == Query.update:
            self.update_schema = None
        # Determine hooks

    def try_run(self):
        """Try run

        This function will try to run
        a given query function. First
        gathering locks, returning false
        if required locks are unavailable,
        then running and returning query.

        Parameters
        ----------
        args: List
            The list of arguments for the query.
        """

        # Find which resources are accessed

        resources = self.__find_resources(*self.args)

        
        # request index lock first
        # Do this seperately from the rest in case query is delete
        # and we have to find RID before query in case of roll back
        lock_type, unique_id, transaction = resources[0]
        lock = self.table.lock_manager.request(lock_type, unique_id, transaction)
        if lock == None:
            return False

        # Update the query rid
        if self.query_function == Query.delete:
            self.delete_rid = self.table.index.locate(column=self.table.primary_key, value=self.args[0])
        # update the query schema
        elif self.query_function == Query.update:
            rid = self.table.index.locate(column=self.table.primary_key, value=self.args[0])
            self.update_schema = self.table.page_directory.get_column_value(rid, Config.schema_encoding_column_idx, )

        for i in range(1, len(resources)):
            lock_type, unique_id, transaction = resources[i]
            lock = self.table.lock_manager.request(lock_type, unique_id, transaction)
        
            if lock == None:
                return False

        return self.query_function(*self.args)
    
    def __find_resources(self, *args):
        """Find resources

        For the given query in the function
        wrapper, this will specify the locks
        needed to safely run each query.

        Parameters
        ----------
        args: List
            The list of arguments for the query.

        Returns
        -------
        resources
            list of locks needed for query.
        """
        resources = []

        
        if self.query_function == Query.delete:
            # Write only that affects only one column
            resources.append((Config.EXCLUSIVE_LOCK, ('Index'), self.transaction))
            resources.append((Config.EXCLUSIVE_LOCK, (args[0], Config.rid_column_idx), self.transaction))

        elif self.query_function == Query.insert:
            primary = args[self.table.primary_key]

            # Write only on all columns
            resources.append((Config.EXCLUSIVE_LOCK, ('Index'), self.transaction))
            for i in range(len(args) + Config.column_data_offset):
                resources.append((Config.EXCLUSIVE_LOCK, (primary, i), self.transaction))

        elif self.query_function == Query.update:
            # IMPORTANT: In an update the exclusive lock might not always be needed
            resources.append((Config.EXCLUSIVE_LOCK, ('Index'), self.transaction))
            primary = args[0]
            columns = args[1]

            # As a record will need to be written in each column
            # an exclusive lock will eventually be needed on all columns
            # so just get exclusive instead of exclsuive and shared for 
            # the read operations
            for i in range(len(columns)+Config.column_data_offset):
                resources.append((Config.EXCLUSIVE_LOCK, (primary, i), self.transaction))

        elif self.query_function == Query.select:
            project_columns = args[2]
            primary = args[0]

            # read only on just the columns required in the select args
            resources.append((Config.SHARED_LOCK, ('Index'), self.transaction))
            for i in range(len(project_columns)):
                if project_columns[i]:
                    resources.append((Config.SHARED_LOCK, (primary, i+Config.column_data_offset), self.transaction))

        elif self.query_function == Query.sum:
            # read only on just the primary key column
            # WARNING: sum may function on a range that includes the final value
            # If so, must change range to (args[0], args[1]+1)
            resources.append((Config.SHARED_LOCK, ('Index'), self.transaction))
            for i in range(args[0], args[1]):
                resources.append((Config.SHARED_LOCK, (i, Config.rid_column_idx), self.transaction))
        
        return resources

    def roll_back(self, primary_key):
        if self.query_function == Query.delete:
            # Set deleted row rid back to original rid
            self.table.page_directory.set_column_value(self.delete_rid, Config.rid_column_idx, self.delete_rid)

        elif self.query_function == Query.insert:
            # Get rid of new record and set its rid to -1
            rid = self.table.index.locate(column=self.table.primary_key, value=primary_key)
            self.table.page_directory.set_column_value(rid, Config.rid_column_idx, -1)

        elif self.query_function == Query.update:
            pass

    def revert(self):
        """
        Revert the internal operations of the given
        query to prevent it from persisting in the
        database.
        """

        # TODO: Revert the query using stored internal args/state
        pass

    def __enter__(self):
        """
        When starting up the wrapper function,
        determine all possible dependencies
        and establish locks.
        """

        pass

    def __exit__(self, *args):
        pass