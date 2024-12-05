# System Imports
import time

# Local Imports
from lstore.table import Table, Record
from lstore.index import Index
from lstore.wrapper import QueryWrapper

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.log = []  # Log of each query in the order they were done
        self.lock_managers = set()

    def add_query(self, query, table, *args):
        """
        Add a given query function to the current transaction.

        Parameters
        ----------
        query : function
            The Query function to add
        table : Table
            The Table to operate on
        *args : any
            All arguments that are passed into the
            function

        Example
        -------
            q = Query(grades_table)
            t = Transaction()
            t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
        """

        # Add the query to the query list
        wrapper = QueryWrapper(table, query, self, args)
        self.queries.append(wrapper)

        # Add the lock manager to the lock manager set
        self.lock_managers.add(table.lock_manager)

        # use grades_table for aborting
        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):# use grades_table for aborting
        """
        Run the given transaction, performing every query
        in the current list of queries.

        Returns
        -------
        status : bool
            Whether or not the transaction was successful
            False will be returned on abort
        """

        # Loop through all queries
        for wrapper in self.queries:
            # Try to run the wrapped query
            result = wrapper.try_run()
            
            # If the query has failed the transaction should abort
            if result == False:
                # The lock failed to be obtained
                return self.abort()
            elif result == None:
                # An actual error occurred in the transaction
                return self.abort(failure=True)
        
        return self.commit()
    
    def abort(self, failure=False):
        """
        Roll back any operations that were applied
        and abort the transaction, releasing any locks
        that were granted.

        Parameters
        ----------
        failure : bool (default=False)
            Whether or not an actual error occurred

        Returns
        -------
        status : False or None
            If the transaction aborted due to a lock issue
            this returns False, if there was an actual error
            then it returns None to signal that this transaction
            is actually invalid.
        """

        #TODO: do roll-back and any other necessary operations
        for wrapper in reversed(self.queries):
            wrapper.roll_back()

        # Release all held locks
        self.__release_all()

        # Force a context switch
        time.sleep(0)

        return (False if not failure else None)

    
    def commit(self):
        """
        Commit all actions to the database, releasing
        all locks that were granted.
        """

        # TODO: commit to database


        # Release all held locks
        self.__release_all()
        
        return True


    def __release_all(self):
        """Internal lock release

        Release all internally held locks.
        """

        # Loop through all lock managers
        for manager in self.lock_managers:
            manager.release_all(self)
