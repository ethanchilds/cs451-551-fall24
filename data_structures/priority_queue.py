"""
This defines a data structure for allowing a
fixed number of items to exist within a queue
ordered by a priority number.  Fast retrieval
of individual items is achieved via a hash
map that stores unique keys to the item itself.
"""

# System imports
import heapq

# Local imports
from errors import PriorityQueueCapacityOutOfBoundsError, PriorityQueueInvalidPolicyError
from lstore.cache_policy import CachePolicy

class PriorityQueue():
    """A fixed-size priority queue

    This defines a fixed-size priority
    queue as a heap of a maximum number
    of elements that maintains sorted order
    and fast access to individual elements.

    Items in the internal queue itself are stored
    as follows:

    [priority, key, Item]
    """

    def __init__(self, capacity):
        """Initialize a PriorityQueue

        This initializes a new PriorityQueue
        with a fixed capacity.

        Parameters
        ----------
        capacity : int
            The maximum number of allowed elements

        Raises
        ------
        PriorityQueueCapacityOutOfBoundsError
            Whenever a given capacity is out of bounds (<=0)
        """

        # Check for valid capacity
        if (capacity <= 0):
            raise PriorityQueueCapacityOutOfBoundsError(capacity)

        self.capacity = capacity
        self.data = []  # Actual priority queue
        self.map = {}  # Used for fast finding a particular item
        heapq.heapify(self.data)
        self.policy = CachePolicy(self)  # Default cache policy

    def clear(self):
        """Clear the entire queue

        This removes all elements in the current queue
        and their corresponding entries in the hash map.
        """

        # Destroy all elements
        self.data = []
        self.map = {}
        heapq.heapify(self.data)

    def push(self, key, value, priority=0):
        """Try to push a value

        Attempt to push a specific key-value pair
        onto the PriorityQueue.  If the queue is
        at capacity, the lowest priority item
        is removed and returned.  If the item to
        insert is already in the queue, its
        priority increases according to the priority
        function.

        Note that trying to add an existing key
        will ignore the new value.  This may not
        be desirable depending on how values
        are expected to be stored. (This implementation
        assumes that the value itself is mutable and
        can be updated externally).

        Parameters
        ----------
        key : any
            The uniquely identifying key
        value : any
            The value to store
        priority : int (default=0)
            The initial priority of the stored item

        Returns
        -------
        item : list<int, any, any> or None
            If the queue is at capacity and the new item
            is unique, the lowest priority item is returned
            or else None will be returned
        """

        # Perform push policy update
        self.policy.on_push()

        # Check if the new item is already in the queue
        if (key in self.map):
            # Increment the priority
            old_priority = self.map[key][0]
            new_priority = self.policy.update_priority(old_priority)
            self.map[key][0] = new_priority
            #self.map[key][0] += 1  # Max queue increment
            heapq.heapify(self.data)  # TODO: Check performance

            return None

        # Create a new item and set it in the map
        new_item = [priority, key, value]
        self.map[key] = new_item

        # Check if the queue is at capacity
        if (len(self.data) < self.capacity):
            # Add the new item to the queue
            heapq.heappush(self.data, new_item)
            return None
        else:
            # Queue is at capacity, so remove before adding
            item = heapq.heappushpop(self.data, new_item)
            last_key = item[1]  # Extract key
            del self.map[last_key]
            return item

    def pop(self):
        """Try to pop a value

        This removes the current lowest priority item 
        from the queue and returns it.

        Returns
        -------
        item : list<int, any, any> or None
            The loweest priority item from the queue 
            if it exists or else None.
        """

        if (len(self.data) > 0):
            # Get the item from the queue
            item = heapq.heappop(self.data)

            # Remove the internal reference in the hash map
            key = item[1]
            del self.map[key]

            return item
        else:
            return None

    def get(self, key):
        """Try to retrieve an item

        This attemps to retrieve an item by its key
        from the internal queue.

        Parameters
        ----------
        key : any
            The uniquely identifying key

        Returns
        -------
        item : list<int, any, any> or None
            The item from the queue if it exists
            or else None.
        """

        # Quickly find the item using the hash map
        if (key in self.map):
            return self.map[key]
        else:
            return None

    def remove(self, key):
        """Try to remove an item

        This attempts to remove an item by its key
        from the internal queue.  If it is successful
        the item is returned, or else None is returned.

        Parameters
        ----------
        key : any
            The uniquely identifying key
        
        Returns
        -------
        item : list<int, any, any> or None
            The item from the queue that was removed
            or else None.
        """

        # Quickly find the item using the hash map
        if (key in self.map):
            # Remove the item from the internal queue and hash map
            item = self.map[key]
            self.data.remove(item)
            del self.map[key]

            return item
        else:
            return None

    def items(self, ordered=False):
        """Key value generator

        This is a key-value generator for iterating
        through all key value pairs of items within
        the current queue.  This does not guarantee
        iterating in priority order unless the
        ordered flag is set to True.

        Parameters
        ----------
        ordered : bool
            Whether or not the yielded items are
            in sorted priority order

        Yields
        ------
        key : any
            The uniquely identifying key
        item : list<int, any, any>
            The item from the queue
        """

        if (ordered):
            for d in self.data:
                yield d[1], d
        else:
            for k,v in self.map.items():
                yield k, v

    def set_policy(self, policy):
        """Set the cache priority update policy

        This sets the internal cache priority update
        policy which auto-applies any time that push
        is called.

        Parameters
        ----------
        policy : CachePolicy
            The policy for auto updating priorities in the cache

        Raises
        ------
        PriorityQueueInvalidPolicyError
            If the provided policy is invalid
        """

        if (isinstance(policy, CachePolicy)):
            self.policy = policy
        else:
            raise PriorityQueueInvalidPolicyError()

    def set_priority(self, key, priority):
        """Update the priority of an item

        This tries to set a specific priority on an
        item stored in the current queue.  If the
        operation is successful it will return True,
        or else it will return False.

        Parameters
        ----------
        key : any
            The uniquely identifying key
        priority : int
            The new priority value


        Returns
        -------
        status : bool
            Whether or not the operation succeeded
        """

        if (key in self.map):
            #prev_priority = self.map[key][0]
            self.map[key][0] = priority
            #if (priority < prev_priority):
            #    heapq._siftdown(self.data, )
            #elif (priority > prev_priority):
            #    heapq._siftup(self.data, )
            heapq.heapify(self.data)
            return True
        else:
            return False

    def __contains__(self, key):
        """Check for inclusion

        This checks if a specific key exists
        in the current queue.

        Parameters
        ----------
        key : any
            The uniquely identifying key

        Returns
        -------
        b : bool
            Whether or not the key is in the queue
        """

        return (key in self.map)

    def __getitem__(self, key):
        """Try to retrieve an item

        This attemps to retrieve an item by its key
        from the internal queue.

        Parameters
        ----------
        key : any
            The uniquely identifying key

        Returns
        -------
        item : list<int, any, any> or None
            The item from the queue if it exists
            or else None.
        """

        return self.get(key)

    def __len__(self):
        """Get the length of the queue

        This returns the current number of elements
        stored in the PriorityQueue.

        Returns
        -------
        length : int
            The total number of elements in the queue
        """

        return len(self.data)
