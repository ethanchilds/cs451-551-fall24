"""
A centralized spot to configure the database.
"""
# from data_structures.b_plus_tree import BPlusTree
from data_structures.binary_search_tree import BSTree
from data_structures.hash_map import HashMap

class Config:
    page_size = 2**12    #4KB
    page_cell_size = 8   # Thats what the adssignment description said.
    pages_per_block = 2**4  # Total pages that exist in a single block file
    pool_max_blocks = 2**12  # Total number of blocks that can be stored in the BufferPool at a time
    index_ordered_data_structure = BSTree    # Make sure this class passes test_data_structure_correctness(), and does well on it.
    index_unordered_data_structure = HashMap
    b_plus_tree_minimum_degree = 2**7   # 2**6 to 2**7 for fast insert. 2**8 to 2**9 for fast range query
    b_plus_tree_search_algorithm_threshold = 10 # Switch between a linear scan and binary search in b+ tree at this value. Might improve performance.
    b_plus_tree_bulk_insert_start_threshold = 100
    b_plus_tree_bulk_insert_ratio_threshold = 0.30
    lstore_is_cumulative = False    # Paper mentions there are two ways to do this.
    column_data_offset = 5
    byteorder = 'big'
    indirection_column_idx = 0
    rid_column_idx = 1
    timestamp_column_idx = 2
    schema_encoding_column_idx = 3
    tps_and_brid_column_idx = 4
    benchmark_mode = False

    # Lock types
    SHARED_LOCK = 0
    EXCLUSIVE_LOCK = 1

    # Best time to insert 100_000 random items into a b+ tree with minimum_degree.
    # 200 1.5865
    # 150 1.5878
    # 128 1.5161
    # 100 1.5156
    # 64  1.5255

    #                     HashMap  B+Tree
    # Insert 1_000_000   3x faster
    # Get 1_000_000     16x faster
    # Get range 1_000             100x faster

    @staticmethod
    def display_config():
        """Method to display current configuration settings"""
        # Grow this method as more is added to this class.

        print(f"Page Size: {Config.page_size}")
        print(f"Page Cell Size: {Config.page_cell_size}")
        print(f"Index data structure: {Config.index_ordered_data_structure.__name__}")
        print(f"B+ Tree minimum degree: {Config.b_plus_tree_minimum_degree}")
        print(f"L-Store is Cumulative: {Config.lstore_is_cumulative}")

        