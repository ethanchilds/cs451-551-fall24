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
steps = [10, 100, 1000, 10000, 100000, 1000000]
policy_list = [
    cache_policy.LRUCachePolicy,
    cache_policy.MRUCachePolicy,
    cache_policy.ZeroWeightCachePolicy,
    cache_policy.LeakyBucketCachePolicy,
    cache_policy.InverseLeakyBucketCachePolicy,
    cache_policy.StochasticCachePolicy
]
reads_choice = True
strategy_choice = 'single'

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
            'single' - One value repeating
            'increment' - Linearly increasing
            'stripe' - Random stripes of increasing values
            'burst' - Random bursts of the same value
    
    Returns
    -------
    times : list<float>
        A list of timings for the particular case
    """

    # Set seed
    random.seed(SEED)

    print("======================================")
    print("Testing: {0} with {1} strategy".format(policy_class.__name__, strategy.upper()))

    # Saved outputs
    times = []

    # Perform setup
    setup()

    # Create a dataset
    blank_page = Page()
    base_pool = BufferPool(analysis_basepath, num_columns, max_blocks, block_size)
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
        choice = 0
        for i in range(s):
            # Choose
            if (strategy == 'random'):
                choice = random.randint(0, total_pages-1)
            elif (strategy == 'single'):
                choice = 0
            elif (strategy == 'increment'):
                choice = (i % total_pages)
            elif (strategy == 'stripe'):
                choice = ((i % 5 + i//3) % total_pages)
            elif (strategy == 'burst'):
                if (random.randint(0, 9) == 0):
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

    return times



if __name__ == "__main__":
    if (not os.path.exists(analysis_output)):
        os.makedirs(analysis_output)

    res_fullpath = os.path.join(analysis_output, '{0}_{1}.csv'.format('READ' if reads_choice else 'WRITE', strategy_choice))
    with open(res_fullpath, 'w', newline='', encoding='utf-8') as fp:
        writer = csv.writer(fp, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        # Write header
        writer.writerow([''] + steps)

        # Run tests and log to csv file
        for policy in policy_list:
            tv = test_policy(policy, reads=reads_choice, strategy=strategy_choice)
            writer.writerow([str(policy.__name__)] + tv)