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
    """
    def __init__(self, table, benchmark_mode=False, debug_mode=False):
        self.indices = [None] *  table.num_columns
        self.OrderedDataStructure = BPlusTree
        self.UnorderedDataStructure = HashMap
        self.usage_histogram = [[0 for i in range(2)] for j in range(table.num_columns)] # 0: point queries, 1: range queries, 2: inserts, 3: updates
        self.maintenance_list = [[] for _ in range(table.num_columns)]
        self.table = table
        self.benchmark_mode = benchmark_mode
        self.debug_mode = debug_mode

        self.create_index(column_number=table.primary_key, ordered=True)
        

    def locate(self, column: int, value):
        """
        returns the location of all records with the given value on column "column"
        """
        if column >= len(self.indices) or column < 0:
            raise ColumnDoesNotExist

        self.usage_histogram[column][0] += 1

        if self.indices[column]:
            index = self.indices[column]
            return index.get(value)
        else:
            return list(self._locate_linear(column, target_value=value))


    @timer
    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end"
        
        returns list of (value, rid) pairs
        """
        self.usage_histogram[column][1] += 1

        if self.indices[column]:
            index = self.indices[column]
            return index.get_range(begin, end)
        else:
            return list(self._locate_range_linear(column, low_target_value=begin, high_target_value=end))

    @timer
    def create_index(self, column_number, ordered:bool=False):
        """
        Create index on specific column
        """
        if self.indices[column_number]:
            raise ValueError("Index at column ", column_number, " already exists")
        
        data_structure = self.OrderedDataStructure() if ordered else self.UnorderedDataStructure()

        self.indices[column_number] = data_structure

        items = list(self.table.column_iterator(column_number))
        items.sort(key=lambda item: item[0])
    
        for rid, value in items:
            data_structure.insert(value, rid)

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
        for rid, value in self.table.column_iterator(column):
            if value == target_value:
                yield rid

    def _locate_range_linear(self, column, low_target_value, high_target_value):
        """
        Returns the rid of every row with a value within range in a column
        A linear scan range query
        """
        for rid, value in self.table.column_iterator(column):
            if (not low_target_value or value >= low_target_value) and (not high_target_value or value <= high_target_value):
                yield rid
    
    def maintain_insert(self, columns, rid):
        for column, value in enumerate(columns):
            if self.indices[column] is not None:
                self.maintenance_list[column].append((value, rid))
                # self.indices[column].insert(value, rid)

    def maintain_update(self, primary_key, new_columns):
        rid = self.locate(column=self.table.primary_key, value=primary_key)[0]
        for column, new_value in enumerate(new_columns):
            self._apply_maintenance(column)
            index = self.indices[column]
            if index and (new_value is not None):
                old_value = self.table.page_directory.get_column_value(rid, column + Config.column_data_offset)
                index.update(old_value, new_value)
    
    def maintain_delete(self, primary_key):
        rid = self.locate(column=self.table.primary_key, value=primary_key)[0]
        if rid is None:
            raise KeyError
        
        for column, index in enumerate(self.indices):
            self._apply_maintenance(column)
            if index:
                value = self.table.page_directory.get_column_value(rid, self.table.primary_key + Config.column_data_offset)
                index.remove(value)

        return
    
    def _apply_maintenance(self, column):
        # raise NotImplementedError
        if self.indices[column] is not None:
            if len(self.maintenance_list[column]) > 0:
                
                if self.debug_mode:
                    print(f"INDEX {column} IS APPLYING MAINTENANCE")

                self.maintenance_list[column].sort(key=lambda item: item[0])
                for value, rid in self.maintenance_list[column]:
                    self.indices[column].insert(value, rid)

                self.maintenance_list[column] = []