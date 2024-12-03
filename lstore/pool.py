# System imports
import os

# Local imports
from config import Config
from lstore.block import Block
from lstore.page import Page
from lstore.cache_policy import LeakyBucketCachePolicy, LRUCachePolicy, MRUCachePolicy
from data_structures.priority_queue import PriorityQueue
from collections import defaultdict
import threading

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

class BufferPool():
    """A fixed size pool of memory

    The BufferPool represents a fixed size pool
    of memory in RAM which reads/writes data to
    and from disk whenever elements are needed
    to be exchanged.
    """

    def __init__(self, base_path, num_columns, max_blocks=Config.pool_max_blocks, block_size = Config.pages_per_block):
        """Initialize the BufferPool

        Initialize the BufferPool with a set of
        fixed parameters that determine its behavior.

        Parameters
        ----------
        base_path : str
            The path to the base directory where the
            data will be stored (<database>/<table>)
        max_blocks : int
            The maximum number of Blocks that will reside in memory
            at any given time
        """

        # Create the base path if it doesn't exist
        self.base_path = base_path
        self.max_blocks = max_blocks
        self.block_size = block_size
        if (not os.path.exists(os.path.join(base_path, 'base'))):
            os.makedirs(os.path.join(base_path, 'base'))
            os.makedirs(os.path.join(base_path, 'tail'))
        
        # Create a folder for each column
        for i in range(num_columns):
            if (not os.path.exists(os.path.join(base_path, 'base', str(i)))):
                os.makedirs(os.path.join(base_path, 'base', str(i)))
            if (not os.path.exists(os.path.join(base_path, 'tail', str(i)))):
                os.makedirs(os.path.join(base_path, 'tail', str(i)))
        
        # Create a priority queue corresponding to each Block
        self.queue = PriorityQueue(max_blocks)
        policy = LRUCachePolicy(self.queue)
        self.queue.set_policy(policy=policy)

        # Create a list of pins and dirty blocks
        # self.pinned_blocks
        self.dirty_blocks = set()
        self.pinned_blocks = defaultdict(int)
        self.__lock = threading.Lock()
        self.to_evict_flag = defaultdict(int)

    def flush(self):
        """Flush all dirty blocks to disk

        This writes all remaining dirty blocks to disk
        to prevent data loss and also clears the queue.
        """

        # Loop through all dirty_blocks and write them to disk
        for key in self.dirty_blocks:
            if key in self.queue:
                block = self.queue[key][2] 
                block.write()
            else:
                assert 1 == 0

        # Remove all items in the queue and pinned/dirty lists
        self.queue.clear()
        self.dirty_blocks = []
    
    def _pin_block(self, key):
        with self.__lock:
            self.pinned_blocks[key] += 1
            
    def _unpin_block(self, key):
        with self.__lock:
            self.pinned_blocks[key] -= 1
            # we never should go below zero
            assert self.pinned_blocks[key] >= 0
            
            # check if we should evict the block
            if self.pinned_blocks[key] == 0 and self.to_evict_flag[key]:
                # reverse eviction if key already in a queue
                if key not in self.queue:
                    block = self._get_block(*key)
                    block.write()   
                self.to_evict_flag[key] = 0
                    
                
    
    def _evict_block(self, key, block):
        with self.__lock:
            if self.pinned_blocks[key] == 0:
                block.write()
            else:
                self.to_evict_flag[key] = 1

    def add_page(self, page, page_num, column_id, tail_flg=0, cache_update=True):
        block_num = page_num // self.block_size
        key = (self.base_path, column_id, tail_flg, block_num)
        self._pin_block(key)
        
        # we use the combination of table path, column, tail and block_num as the unique identifier of the block
        block = self._get_block(self.base_path, column_id, tail_flg, block_num)
        
        block.append(page)
        
        self._unpin_block(key)
        
        if cache_update:
            # we use the combination of table path, column, tail and block_num as the unique identifier of the block
            self._maintain_cache(self.base_path, column_id, tail_flg, block_num, block)
            self.dirty_blocks.add((self.base_path, column_id, tail_flg, block_num))
        else:
            block.write()

    def get_page(self, page_num, column_id, tail_flg=0, cache_update=True) -> Page:
        # If the item is in the BufferPool, just return it
        # and apply the cache policy to the existing item
        block_num = page_num // self.block_size
        key = (self.base_path, column_id, tail_flg, block_num)
        self._pin_block(key)
        
        block = self._get_block(self.base_path, column_id, tail_flg, block_num)
        
        order_in_block = page_num % self.block_size
        assert order_in_block < len(block.pages)
        
        page = block.pages[order_in_block]
        
        self._unpin_block(key)
        if cache_update:
            # we use the combination of table path, column, tail and block_num as the unique identifier of the block
            self._maintain_cache(self.base_path, column_id, tail_flg, block_num, block)
        
        return page
        
    def update_page(self, page, page_num, column_id, tail_flg=0, cache_update=True):
        block_num = page_num // self.block_size
        key = (self.base_path, column_id, tail_flg, block_num)
        self._pin_block(key)
        block = self._get_block(self.base_path, column_id, tail_flg, block_num)
        
        order_in_block = page_num % self.block_size
        block.pages[order_in_block] = page
        
        self._unpin_block(key)
        if cache_update:
            # we use the combination of table path, column, tail and block_num as the unique identifier of the block
            self._maintain_cache(self.base_path, column_id, tail_flg, block_num, block)
            self.dirty_blocks.add((self.base_path, column_id, tail_flg, block_num))
        else:
            block.write()
            
    def _get_block(self, path, column_id, tail_flg, block_num):
        key = (path, column_id, tail_flg, block_num)
        
        if key in self.queue:
            block = self.queue[key][2]
        else:
            path = os.path.join(self.base_path, ('base' if tail_flg == 0 else 'tail'), str(column_id))
            block = Block(path, column=column_id, block_id=block_num, size=self.block_size)
            block.read()
        return block
    
    def _maintain_cache(self, path, column_id, tail_flg, block_num, block):
        key = (path, column_id, tail_flg, block_num)
        value = block
        result = self.queue.push(key, value)
        # check if the evicted block is dirty and remove it
        if result is not None and (result[1] in self.dirty_blocks):
            self._evict_block(result[1], result[2])
            self.dirty_blocks.remove(result[1])            
        


class CachePolicy():
    """An abstract policy for cache behavior

    This represents a basic abstract class
    for cache behavior.
    """

    def __init__(self):
        pass
