"""
This contains various wrappers for handling
threaded queries.
"""


class QueryWrapper():
    """
    A QueryWrapper object is a way of wrapping
    a query into its threaded counterpart to allow
    concurrent queries to take place seamlessly.
    """

    def __init__(self, table, query_function):
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
        """
        
        # Set up internal variables
        self.table = table
        self.query_function = query_function
        self.lock_manager = self.table.lock_manager

        # Determine hooks

    def try_run(self, *args):
        """
        
        """

        # Find which resources are accessed


        # Try to obtain a lock on resources


        return self.query_function(*args)

    def __enter__(self):
        """
        When starting up the wrapper function,
        determine all possible dependencies
        and establish locks.
        """

        pass

    def __exit__(self, *args):
        pass