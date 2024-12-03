# System Imports
from threading import Thread

# Local Importsimport threading
from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:
    def __init__(self, transactions = None):
        """
        Creates a transaction worker object.

        Parameters
        ----------
        transactions : list<Transaction>
            A list of transactions to add to the worker.
        """

        # Internal variables
        self.stats = []  # Which transactions failed and which succeeded
        self.transactions = [] if (transactions is None) else transactions
        self.result = 0
        self.commit_set = set()  # A set of successful Transaction objects
        self.fail_set = set()  # A set of failed Transaction objects (errored)

        # Current running thread
        self.current_thread = None
    
    def add_transaction(self, t):
        """
        Appends t to transactions

        Parameters
        ----------
        t : Transaction
            The transaction to add to the list
        """

        self.transactions.append(t)

    def run(self):
        """
        Runs all transactions as a thread
        """
        
        # Create a new thread and start its execution
        self.current_thread = Thread(target=self.__run)
        self.current_thread.start()
    
    def join(self):
        """
        Waits for the worker to finish
        """

        # Try to join the current thread
        if (self.current_thread is not None):
            self.current_thread.join()

    def __run(self):
        """Internal run

        This runs all transactions and stores a list
        of which transactions actually committed work.

        Whenever a transaction aborts, it either can be
        due to a resource conflict, or an actual query
        failure.  On query failures, the transaction should
        be completely removed from trying to run or else
        it will continuously loop trying to run the erroneous
        query forever.
        """

        # Continue running until there are no transactions left
        # WARNING: this can be the cause of an infinite loop
        while (len(self.transactions) > 0):
            # Loop through all transactions and try to run them
            for transaction in self.transactions:
                # each transaction returns True if committed or False if aborted
                result = transaction.run()
                self.stats.append(result)

                # Record which transactions committed/failed
                if (result == True):
                    # Commit condition
                    self.commit_set.add(transaction)
                elif (result == None):
                    # Failure condition
                    self.fail_set.add(transaction)
            
            # stores the number of transactions that committed
            self.result += len(list(filter(lambda x: x, self.stats)))

            # Remove all elements which committed successfully or failed with an error
            for t in self.commit_set:
                if (t in self.transactions):
                    self.transactions.remove(t)
            self.commit_set = set()
            for t in self.fail_set:
                if (t in self.transactions):
                    self.transactions.remove(t)
            self.fail_set = set()
