import threading

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
        self.lock_dictionary = {}
        self.shared_counter = {}
        self.lock = threading.Lock()

    def request(self, type, unique_id):
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
        type: int
            Either 0 or 1, representing shared and 
            exclusive locks, respectively.
        unique_id
            Any value to uniquely identify a resource.
        """
        with self.lock:
            # Boolean checks
            shared_exists = (0, unique_id) in self.lock_dictionary
            exclusive_exists = (1, unique_id) in self.lock_dictionary

            # if an exclsuive lock exists, return false
            if exclusive_exists:
                return False
            
            # if requested an exclusive lock but a shared lock exists,
            # return false, else construct and return lock
            if type:
                if shared_exists:
                    return False
                else:
                    self.lock_dictionary[(type, unique_id)] = threading.Lock()
                    return self.lock_dictionary[(type, unique_id)]
                
            # if requested a shared lock, if it exists return the lock,
            # else construct and return lock
            # WARNING: shared lock has not yet been implemented
            if shared_exists:
                self.shared_counter[(type, unique_id)] += 1
                return self.lock_dictionary[(type, unique_id)]
            else:
                self.lock_dictionary[(type, unique_id)] = threading.Lock()
                self.shared_counter[(type, unique_id)] = 1
                return self.lock_dictionary[(type, unique_id)]


    def release(self, type, unique_id):
        """Release a lock

        This will handle all logic regarding a
        release of a lock. If releasing an
        exclusive lock, just deconstruct it. If
        releasing a shared lock, if other threads
        are using it, decrement counter, else
        deconstruct lock. 

        Parameters
        ----------
        type: int
            Either 0 or 1, representing shared and 
            exclusive locks, respectively.
        unique_id
            Any value to uniquely identify a resource.

        Returns
        -------
        bool
            Returns true if successful release,
            otherwise returns false.
        """
        with self.lock:
            # Try to successfully release a lock
            try:
                # If releasing exclusive lock, deconstruct it
                if type:
                    del self.lock_dictionary[(type, unique_id)]
                    return True
                
                # If releasing shared lock, decrement count if other
                # threads are using it, else deconstruct it
                else:
                    if self.shared_counter[(type, unique_id)] > 1:
                        self.shared_counter[(type, unique_id)] -= 1
                        return True
                    else:
                        del self.lock_dictionary[(type, unique_id)]
                        del self.shared_counter[(type, unique_id)]
                        return True
            # Return False if a failure occured
            # WITH DEL, KEY ERROR IS RAISED FOR FAILURE
            except:
                return False