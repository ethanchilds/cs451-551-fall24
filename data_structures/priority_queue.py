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
from errors import PriorityQueueCapacityOutOfBoundsError

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

        # Check if the new item is already in the queue
        if (key in self.map):
            # Increment the priority
            self.map[key][0] += 1  # Max queue increment
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
            self.map[key][0] = priority
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
