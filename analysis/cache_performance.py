"""
Test CachePolicy methods for performance.
"""

# System Imports
import csv
import os
import random
import shutil
from time import process_time

# Local Imports
from lstore.page import Page
import lstore.cache_policy as cache_policy
from lstore.pool import BufferPool

# Global parameters
SEED = 58582083
analysis_basepath = 'analysis/scratch/cache_performance/'
analysis_output = 'analysis/output/'
num_columns = 1
max_blocks = 10
block_size = 1
total_pages = 10000
steps = [100, 1000, 10000, 100000, 1000000]

def clean():
    # Remove any existing database
    if (os.path.exists(analysis_basepath)):
        shutil.rmtree(analysis_basepath)

def setup():
    # Clean existing data
    clean()

    # Create a new path
    os.makedirs(analysis_basepath)

def teardown():
    # Clean existing data
    clean()

def test_policy(policy_class, reads=True, strategy='random'):
    """
    Test a specific policy class.

    Parameters
    ----------
    policy_class : CachePolicy
        The specific CachePolicy class to implement
    reads : bool
        Whether to use a read (True) or write (False)
        workload
    strategy : str
        The particular strategy to use for selecting
        a page:
            'random' - Uniform random page selection
            'increment' - Linearly increasing
            'stripe' - Random stripes of increasing values
            'burst' - Random bursts of the same value
    """

    # Set seed
    random.seed(SEED)

    print("======================================")
    print("Testing: {0}".format(policy_class.__name__))

    # Saved outputs
    times = []

    # Perform setup
    setup()

    # Create a dataset
    blank_page = Page()
    base_pool = BufferPool(analysis_basepath, num_columns, max_blocks, block_size, policy_class)
    print("  Creating dataset...")
    for i in range(total_pages):
        base_pool.add_page(blank_page, i, 0)
    base_pool.flush()

    # Loop through increasing write sizes
    for s in steps:
        # Create a new buffer pool
        pool = BufferPool(analysis_basepath, num_columns, max_blocks, block_size, policy_class)

        # Perform operations
        print("  Testing {0} {1}...".format(s, ("reads" if reads else "writes")))
        start_time = process_time()
        for i in range(s):
            choice = random.randint(0, total_pages-1)
            if (reads is True):
                pool.get_page(choice, 0)
            else:
                pool.update_page(blank_page, 0, 0)
        end_time = process_time()
        total_time = end_time - start_time
        times.append(total_time)
        
        print("    Completed in {0} s.".format(total_time))

    # Perform teardown
    teardown()

    print("======================================")



if __name__ == "__main__":
    test_policy(cache_policy.LRUCachePolicy)
    test_policy(cache_policy.MRUCachePolicy)
    test_policy(cache_policy.ZeroWeightCachePolicy)
    test_policy(cache_policy.LeakyBucketCachePolicy)
    test_policy(cache_policy.InverseLeakyBucketCachePolicy)
    test_policy(cache_policy.StochasticCachePolicy)