"""
The Index class provides a data structure that allows fast processing of queries (e.g., select or 
update) by indexing columns of tables over their values. Given a certain value for a column, the 
index should efficiently locate all records having that value. The key column of all tables is 
required to be indexed by default for performance reasons. However, supporting secondary indexes 
is optional for this milestone. The API for this class exposes the two functions create_index and 
drop_index (optional for this milestone)
"""
from utilities.timer import timer
from config import Config
from data_structures.b_plus_tree import BPlusTree
from data_structures.hash_map import HashMap
from errors import *

POINT_QUERY = 0
RANGE_QUERY = 1

class Index:
    """
    Give the index a column and a target value, and the index will give you a list of row id's that match

    Don't forget to maintain the index using maintain_insert and maintain_delete.
    The index needs to be told when the table is changed.
    Take care of your index!!!

    methods:
    __init__

    locate
    locate_range

    create_index
    drop_index

    _locate_linear
    _locate_range_linear

    maintain_insert
    maintain_update
    maintain_delete

    _consider_new_index
    """
    def __init__(self, table, benchmark_mode=False, debug_mode=False, automatic_new_indexes=True):
        self.indices = [None] *  table.num_columns
        self.OrderedDataStructure = BPlusTree
        self.UnorderedDataStructure = HashMap
        self.usage_histogram = [[0 for i in range(2)] for j in range(table.num_columns)] # 0: point queries, 1: range queries
        self.table = table
        self.benchmark_mode = benchmark_mode
        self.debug_mode = debug_mode
        self.automatic_new_indexes = automatic_new_indexes
        self.has_unique_keys = [False] * table.num_columns

        # Default unique key index on the primary key.
        self.create_index(column=table.primary_key, unique_keys=True, ordered=True)
        

    def locate(self, column: int, value):
        """
        returns the location of all records with the given value on column "column"
        """
        if column >= len(self.indices) or column < 0:
            raise ColumnDoesNotExist
        
        self.usage_histogram[column][POINT_QUERY] += 1
        self._consider_new_index(column)

        if self.indices[column]:
            return self.indices[column].get(value)
        else:
            return list(self._locate_linear(column, target_value=value))


    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end"       
        returns list of (value, rid) pairs
        """
        if column >= len(self.indices) or column < 0:
            raise ColumnDoesNotExist
        
        self.usage_histogram[column][RANGE_QUERY] += 1
        self._consider_new_index(column)

        if self.indices[column]:
            return self.indices[column].get_range(begin, end)
        else:
            return list(self._locate_range_linear(column, low_target_value=begin, high_target_value=end))

    @timer
    def create_index(self, column, ordered:bool=False, unique_keys:bool=False):
        """
        Create index on specific column
        """
        if self.indices[column]:
            raise ValueError("Index at column ", column, " already exists")
        
        self.has_unique_keys[column] = unique_keys

        if ordered:
            data_structure = self.OrderedDataStructure(unique_keys=unique_keys)
        else:
            data_structure = self.UnorderedDataStructure(unique_keys=unique_keys)

        self.indices[column] = data_structure

        items = list(self.table.column_iterator(column))
        data_structure.bulk_insert(items)
        
    def drop_index(self, column_number):
        """
        Drop index of specific column
        """
        self.indices[column_number] = None
    
    def _locate_linear(self, column, target_value):
        """
        Returns the rid of every row with target_value in a column
        A linear scan point query
        """
        for attribute, rid in self.table.column_iterator(column):
            if attribute == target_value:
                yield rid

    def _locate_range_linear(self, column, low_target_value, high_target_value):
        """
        Returns the rid of every row with a value within range in a column
        A linear scan range query
        """
        for attribute, rid in self.table.column_iterator(column):
            if (not low_target_value or attribute >= low_target_value) and (not high_target_value or attribute <= high_target_value):
                yield rid
    
    def maintain_insert(self, tuple, rid):
        for column, attribute in enumerate(tuple):
            if self.indices[column] is None:
                continue

            self.indices[column].insert(attribute, rid)

    def maintain_update(self, rid, new_tuple):
        """
        rid is assumed to be valid
        otherwise, undefined behavior!!!
        """      
        for column, new_attribute in enumerate(new_tuple):
            index = self.indices[column]
            if (index is None) or (new_attribute is None):
                continue

            old_attribute = self.table.page_directory.get_data_attribute(rid, column)
            index.update(old_attribute, new_attribute, rid)        
    
    def maintain_delete(self, rid):
        """
        rid is assumed to be valid
        otherwise, undefined behavior!!!
        """
        for column, index in enumerate(self.indices):
            if index:
                attribute = self.table.page_directory.get_data_attribute(rid, column)
                index.remove(attribute, rid)
    
    def _consider_new_index(self, column):
        if self.automatic_new_indexes == False:
            return

        if self.indices[column] is not None:
            return
        
        if self.usage_histogram[column][RANGE_QUERY] >= 2 or self.usage_histogram[column][POINT_QUERY] >= 2:
            if self.debug_mode:
                print(f"AUTOMATICALLY CREATING INDEX ON COLUMN {column}")
            self.create_index(column, ordered=True, unique_keys=False)




"""
THE BEHAVIOR OF ALL INDEX DATA STRUCTURES
Should have a flag unique_keys
get method: always returns a list of values
get_range method: always returns a list of values
Stores key values (plural) pairs internally.
insert method: takes in a key value pair. 
    If unique_keys==True and key alread exists, Raises NonUniqueKeyError if the key
    If unique_keys==False and key already exists, 
bulk_insert method: quickly makes the datastructure from a list of items.
    raises error if called and unique_key=True 
    (Usually things get inserted one at a time. Program should fail if the key is not unique. 
    to properly get a list of items and insert them all at the same time, we need to check if the item is in the existing datastructure,
    or in the list of items, for each item at a time. This is slower than just inserting one at a time)
__contains__ method: determine if an item is already in 
remove: old_key, value(only needed if unique_key==False) -> takes value out of key values pair. Deletes whole item if len(values)==0
    raises error if old_key is not in the data structure.
update method: old_key, new_key -> gets old_key value (old_key must be valid). Removes old_key. Inserts new_key, value (new_key must be unique if unique_keys==True)
    note that update needs to properly abort if theres an error AFTER an item is removed
items: list of key, values (plural) pairs. (len(values)==1 for all items if unique_keys==True)

KNOWN BUG WITH MY B+TREE!!!!!!
THOUGH, I THINK IT'S A BUG WITH THE ALGORITHM ITSELF
anyways, I'm not sure if it's possible to easily find the furthest left occurance of a duplicate key.
Why? Look at this example
Node(keys=[0, 3])
~~~
Leaf(keys=[0, 0, 0])
Leaf(keys=[0, 1, 2])
Leaf(keys=[3, 4, 5])
len(tree.get_range(0, 0)) will return 1!!!!
Why? Because _find_index_in_keys returns the first occurance of a key.
The parent would have keys [0, 0, 3], but we drop the first one because len(keys) == len(values)-1
That means that _find_index_in_keys is actually unable to always find the first occurance of a key because the implicit first key is missing!


THE BEHAVIOR OF THE INDEX
locate method: column, value -> list of rid's; applies maintinance
locate range method: low_key, high_key, column -> list of rid's; applies maintenance
maintain_insert: doesn't apply immediately. Except pk. That needs to go in immediately (because it has to be unique)
maintain_update: applies maintinance. new pk can't exist
maintain_delete: applies maintinance. 
"""

    

    