# System Imports
import threading

# Local Imports
from config import Config

class LockManager():
    """Lock Manager

    The lock manager handles requests
    for locks on specific resources,
    handing them off when available,
    constructing them if they do not
    exist, and deconstructing them when
    they are no longer needed.
    """

    def __init__(self):
        """Initialize a Lock Manger

        This initializes a lock manger which tracks
        lock resources in a dictionary and handles
        calls to it in a thread-safe manner.
        """

        # Internal variables
        self.x_locks = {}  # Exclusive locks which map a key to a single Transaction
        self.s_locks = {}  # Shared locks which map a key to a set of Transactions
        self.transaction_dictionary = {}  # Transactions mapped to a set of x/s lock keys
        self.__lock = threading.Lock()  # Internal mutex (Singular to prevent request/release race conditions)

    def __add_transaction(self, key, transaction):
        """Internal method to add transactions

        This maintains the transaction dictionary when
        a transaction is added.

        Parameters
        ----------
        key : tuple<int, any>
            The key used to uniquely identify the lock
        transaction : Transaction
            The specific Transaction to associate the lock with
        """

        # Check if the transaction already exists
        if (transaction not in self.transaction_dictionary):
            # Create a new set
            self.transaction_dictionary[transaction] = set()

        # Add the key to the transaction's set
        self.transaction_dictionary[transaction].add(key)

    def __remove_transaction(self, key, transaction):
        """Internal method to remove transactions

        This maintains the transaction dictionary when
        a transaction is removed.

        Parameters
        ----------
        key : tuple<int, any>
            The key used to uniquely identify the lock
        transaction : Transaction
            The specific Transaction to associate the lock with
        """

        # Check if the transaction actually exists
        if (transaction in self.transaction_dictionary):
            # Check if the key exists in the transaction's set
            t_set = self.transaction_dictionary[transaction]
            if (key in t_set):
                t_set.remove(key)

                # If the element was the last in the set, delete the transaction
                if (len(t_set) == 0):
                    del self.transaction_dictionary[transaction]

    def __add_shared_lock(self, key, transaction):
        """Internal method for adding shared lock

        Add a shared lock to the internal storage.

        Parameters
        ----------
        key : tuple<int, any>
            The key used to uniquely identify the lock
        transaction : Transaction
            The specific Transaction to associate the lock with

        Returns
        -------
        status : bool
            Whether or not the operation completed successfully
        """

        # Check if there already exists a set of shared locks
        if (key not in self.s_locks):
            self.s_locks[key] = set()

        # Add the element to the set
        self.s_locks[key].add(transaction)

        # Maintain the transaction dictionary
        self.__add_transaction(key, transaction)

        return True

    def __remove_shared_lock(self, key, transaction):
        """Internal method for removing shared lock

        Remove a shared lock from the internal storage.
        If no elements remain, the key is deleted.

        Parameters
        ----------
        key : tuple<int, any>
            The key used to uniquely identify the lock
        transaction : Transaction
            The specific Transaction to associate the lock with

        Returns
        -------
        status : bool
            Whether or not the operation completed successfully
        """

        # Check if the key actually exists in the shared locks
        if (key in self.s_locks):
            # Check if the transaction actually exists
            lock_set = self.s_locks[key]
            if (transaction in lock_set):
                # Remove the transaction from the set
                lock_set.remove(transaction)

                # If the set is empty, delete it from the dictionary
                if (len(lock_set) == 0):
                    del self.s_locks[key]

                # Maintain the transaction dictionary
                self.__remove_transaction(key, transaction)

                return True
            else:
                # The transaction does not exist
                return False
        else:
            # The key does not exist
            return False

    def __add_exclusive_lock(self, key, transaction):
        """Internal method for adding exclusive lock

        Add an exclusive lock to the internal storage.

        Parameters
        ----------
        key : tuple<int, any>
            The key used to uniquely identify the lock
        transaction : Transaction
            The specific Transaction to associate the lock with

        Returns
        -------
        status : bool
            Whether or not the operation completed successfully
        """

        # Only add the element if it doesn't exist in the exclusive lock dictionary
        if (key not in self.x_locks):
            self.x_locks[key] = transaction

            # Maintain the transaction dictionary
            self.__add_transaction(key, transaction)

            return True
        else:
            return False

    def __remove_exclusive_lock(self, key, transaction):
        """Internal method for removing exclusive lock

        Remove an exclusive lock from the internal storage.

        Parameters
        ----------
        key : tuple<int, any>
            The key used to uniquely identify the lock
        transaction : Transaction
            The specific Transaction to associate the lock with

        Returns
        -------
        status : bool
            Whether or not the operation completed successfully
        """

        # Only remove the element if it exists in the exclusive lock dictionary
        if (key in self.x_locks):
            # Check for proper ownership
            if (self.x_locks[key] == transaction):
                del self.x_locks[key]

                # Maintain the transaction dictionary
                self.__remove_transaction(key, transaction)

                return True
            else:
                # The lock is not owned by the requested transaction
                return False
        else:
            return False

    def request(self, lock_type, unique_id, transaction):
        """Request a lock

        This will handle all logic regarding a request 
        for a lock. If an exclusive lock on resource
        exists, deny request. If an exclusive lock is 
        requested but a shared lock exists, deny
        request. All other cases, construct lock if
        needed and return. Returns false for denied
        request and a lock otherwise.
        
        Parameters
        ----------
        lock_type: int
            The specific type of lock requested as
            defined in Config.
        unique_id : any
            Any value to uniquely identify a resource.
        transaction : Transaction
            The Transaction requesting the lock

        Returns
        -------
        key : any or None
            The key for the lock, or None if unsuccessful
        """

        with self.__lock:
            # Construct keys for different lock types
            s_key = (Config.SHARED_LOCK, unique_id)
            x_key = (Config.EXCLUSIVE_LOCK, unique_id)

            # Boolean checks
            shared_exists = s_key in self.s_locks
            exclusive_exists = x_key in self.x_locks

            # If an exclusive lock exists, further checks are needed
            if exclusive_exists:
                # Only allow the lock if it already belongs to the requesting Transaction
                if (self.x_locks[x_key] == transaction):
                    return x_key
                else:
                    return None

            # Construct the requesting lock key
            lock_key = (lock_type, unique_id)
            
            # Check the requested lock type
            if (lock_type == Config.EXCLUSIVE_LOCK):
                # If a shared lock already exists, further checks are needed
                if shared_exists:
                    # The lock can be upgraded to an X-Lock if it belongs to the requesting Transaction
                    if (transaction in self.s_locks[s_key]):
                        # Upgrade the lock
                        self.__remove_shared_lock(s_key, transaction)
                        self.__add_exclusive_lock(lock_key, transaction)

                        return lock_key
                    else:
                        # The lock is forbidden
                        return None
                else:
                    # The exclusive lock can be added
                    self.__add_exclusive_lock(lock_key, transaction)
                    return lock_key
            elif (lock_type == Config.SHARED_LOCK):
                # The shared lock can be added since no exclusive locks precede it
                self.__add_shared_lock(lock_key, transaction)
                return lock_key
            else:
                # Unhandled lock type
                return False


    def release(self, lock_type, unique_id, transaction):
        """Release a lock

        This will handle all logic regarding a
        release of a lock. If releasing an
        exclusive lock, just deconstruct it. If
        releasing a shared lock, if other threads
        are using it, decrement counter, else
        deconstruct lock. 

        Parameters
        ----------
        lock_type: int
            The specific type of lock requested as
            defined in Config.
        unique_id : any
            Any value to uniquely identify a resource.
        transaction : Transaction
            The Transaction trying to release the lock

        Returns
        -------
        status : bool
            Returns True if successful release,
            otherwise returns False.
        """

        with self.__lock:
            # Try to successfully release a lock
            try:
                # Construct the lock key
                lock_key = (lock_type, unique_id)

                # If releasing exclusive lock, deconstruct it
                if (lock_type == Config.EXCLUSIVE_LOCK):
                    return self.__remove_exclusive_lock(lock_key, transaction)
                
                # If releasing shared lock, decrement count if other
                # threads are using it, else deconstruct it
                elif (lock_type == Config.SHARED_LOCK):
                    return self.__remove_shared_lock(lock_key, transaction)
                else:
                    # Unhandled lock type
                    return False
            # Return False if a failure occured
            # WITH DEL, KEY ERROR IS RAISED FOR FAILURE
            except:
                return False

    def release_all(self, transaction):
        """
        Remove all locks for a given transaction.

        Parameters
        ----------
        transaction : Transaction
            The specific Transaction to remove all locks

        Returns
        -------
        status : bool
            Whether or not the operation completed successfully
        """

        with self.__lock:
            # Check if the transaction actually has any locks currently
            if (transaction in self.transaction_dictionary):
                # Loop through all keys in the transaction dictionary for the given transaction
                keys = list(self.transaction_dictionary[transaction])  # Prevents dictionary resize errors
                for key in keys:
                    # Check the lock type
                    if (key[0] == Config.SHARED_LOCK):
                        status = self.__remove_shared_lock(key, transaction)
                        if (status == False):
                            return False
                    elif (key[0] == Config.EXCLUSIVE_LOCK):
                        status = self.__remove_exclusive_lock(key, transaction)
                        if (status == False):
                            return False
                
            # All removals completed successfully
            return True
