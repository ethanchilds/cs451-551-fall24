"""
This is responsible for defining a pool of
Pages called the BufferPool which allows for
a fixed size chunk of memory to be allocated
for storing pages whereas the majority of
the rest of pages are stored on disk.

The cache policy determines exactly how Pages
are flushed back to disk depending on a specific
heuristic function.
"""

# System imports
import os

# Local imports
from config import Config
from lstore.block import Block
from data_structures.priority_queue import PriorityQueue

class BufferPool():
    """A fixed size pool of memory

    The BufferPool represents a fixed size pool
    of memory in RAM which reads/writes data to
    and from disk whenever elements are needed
    to be exchanged.
    """

    def __init__(self, base_path, max_blocks=Config.pool_max_blocks):
        """Initialize the BufferPool

        Initialize the BufferPool with a set of
        fixed parameters that determine its behavior.

        Parameters
        ----------
        base_path : str
            The path to the base directory where the
            data will be stored (<database>/<table>/<type>)
        max_blocks : int
            The maximum number of Blocks that will reside in memory
            at any given time
        """

        # Create the base path if it doesn't exist
        self.base_path = base_path
        if (not os.path.exists(base_path)):
            os.makedirs(base_path)
        
        # Create a priority queue corresponding to each Block
        self.queue = PriorityQueue(max_blocks)

        # Create a list of pins and dirty blocks
        self.pinned_blocks = []
        self.dirty_blocks = []

    def flush(self):
        """Flush all dirty blocks to disk

        This writes all remaining dirty blocks to disk
        to prevent data loss and also clears the queue.
        """

        # Loop through all dirty_blocks and write them to disk
        for b in self.dirty_blocks:
            b.write()

        # Remove all items in the queue and pinned/dirty lists
        self.queue.clear()
        self.pinned_blocks = []
        self.dirty_blocks = []

    def __policy__(self, old_value):
        """Compute the policy on a given priority

        Compute the internal policy on a given priority
        value to allow it to update a priority queue.

        Parameters
        ----------
        old_value : int
            The old priority value

        Returns
        -------
        new_value : int
            The new priority value
        """
        return (old_value + 1)  # TODO: Abstract default policy

    def __getitem__(self, key):
        """
        Try to retrieve an item given a specific key.

        Parameters
        ----------
        key : int
            The key corresponding to a stored item
        """

        # If the item is in the BufferPool, just return it
        # and apply the cache policy to the existing item
        if (key in self.queue):
            # Extract the item and its current priority
            item = self.queue.get(key)
            old_priority = item[0]

            # Set the new priority according to the internal policy
            new_priority = self.__policy__(old_priority)
            self.queue.set_priority(key, new_priority)

            # Return the item
            return item

        # If the item is not in the BufferPool, try to retrieve it from disk
        #b = Block()


class CachePolicy():
    """An abstract policy for cache behavior

    This represents a basic abstract class
    for cache behavior.
    """

    def __init__(self):
        pass
