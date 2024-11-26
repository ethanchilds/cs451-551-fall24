from lstore.table import Table, Record
from lstore.index import Index
from lstore.threading import ThreadedQuery

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.locks = []  # A list of granted locks

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

        self.queries.append((query, table, args))
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

        for query, table, args in self.queries:
            # Wrap the query in a ThreadedQuery wrapper
            query_wrapper = ThreadedQuery(query, table)
            result = query_wrapper.try_run(*args)
            
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        """
        Roll back any operations that were applied
        and abort the transaction, releasing any locks
        that were granted.
        """

        #TODO: do roll-back and any other necessary operations
        return False

    
    def commit(self):
        """
        Commit all actions to the database, releasing
        all locks that were granted.
        """

        # TODO: commit to database
        return True

