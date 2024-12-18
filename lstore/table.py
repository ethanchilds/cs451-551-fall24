"""
The Table class provides the core of our relational storage functionality. All columns are 64-bit
integers in this implementation. Users mainly interact with tables through queries. Tables provide 
a logical view of the actual physically stored data and mostly manage the storage and retrieval of 
data. Each table is responsible for managing its pages and requires an internal page directory that, 
given a RID, returns the actual physical location of the record. The table class should also manage 
the periodical merge of its corresponding page ranges.
"""

# System Imports
import copy
import math
import os
import struct
import threading
import time

# Local Imports
from config import Config
from data_structures.queue import Queue
from errors import ColumnDoesNotExist, PrimaryKeyOutOfBoundsError, TotalColumnsInvalidError
from lstore.index import Index
from lstore.lock_manager import LockManager
from lstore.page import Page
from lstore.pool import BufferPool
import lstore.utils as utils
import itertools

class Record:
    """A Record stores multiple columns for a single row
    """

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def info_print(self):
        print('RID:', self.rid)
        print('Columns:', self.columns)

    def __getitem__(self, index):
        return self.columns[index]
    
    def __str__(self):
        return str(self.columns)
    
    def __repr__(self):
        return "Record(" + str(self.columns) + ")"

class PageDirectory:
    """The PageDirectory controls access to different pages

    Pages can span multiple blocks of memory and the PageDirectory
    keeps track of these blocks so that they are easily
    indexable.
    """

    def __init__(self, db_path, table_name, num_columns, num_records=0, num_tail_records=0):
        self.db_path = db_path
        self.table_name = table_name
        self.num_records = num_records
        self.num_tail_records = num_tail_records
        self.num_columns = num_columns
        self.num_tail_pages = 0
            
        # assert self.num_columns == num_columns
        # self.data = []
        # for _ in range(0, num_columns):
        #     self.data.append({'Base':[], 'Tail':[]})
        self.bufferpool = BufferPool(
            base_path=os.path.join(db_path, table_name),
            num_columns=num_columns
        )

    def add_record(self, columns, tail_flg = 0):
        """
        Accepts list of column values and adds the values to the latest base page of each column
        """
        assert len(columns) == self.num_columns

        # page_class = 'Base' if tail_flg == 0 else 'Tail'

        new_page = False
        for i, column_value in enumerate(columns):
            # allocate new page if we are at full capacity
            # if (not self.data[i][page_class]) or (not self.data[i][page_class][-1].has_capacity()):
            #     self.data[i][page_class].append(Page())
            # self.data[i][page_class][-1].write(column_value)
            page_capacity = Config.page_size // 8
            num_records = (self.num_records if tail_flg == 0 else self.num_tail_records)
            # last_page = self.bufferpool.get_page(page_num=last_page_id, column_id=i, tail_flg=tail_flg)
            if num_records % page_capacity == 0: # this means we need to create a new page
                new_page = Page()
                new_page.write(column_value)
                page_num = num_records // page_capacity
                self.bufferpool.add_page(new_page, page_num, column_id=i, tail_flg=tail_flg)
                if not new_page:
                    new_page = True
            else:
                last_page_num = num_records // page_capacity
                last_page = self.bufferpool.get_page(page_num=last_page_num, column_id=i, tail_flg=tail_flg)
                last_page.write(column_value)
                self.bufferpool.update_page(last_page, last_page_num, column_id=i, tail_flg=tail_flg)

        if new_page and tail_flg:
            self.num_tail_pages += 1

        if tail_flg == 0:
            self.num_records += 1
        else:
            self.num_tail_records += 1

    def get_rid_for_version(self, rid, relative_version = 0):
        """
        Find the value rid corresponding to specified version, provided the rid in the base page
        Returns rid and flag whether it is in base or tail
        """
        assert rid < self.num_records
        
        current_rid = rid
        
        indirection = self.get_column_value(current_rid, Config.indirection_column_idx, tail_flg=0)

        # return base record if there is only one version
        if indirection == -1: 
            return 0, current_rid

        # get the latest version first
        current_rid = indirection

        # from now on we are definetely working with tail
        indirection = self.get_column_value(current_rid, Config.indirection_column_idx, tail_flg=1)
        
        # get the relative version by iterating backwards using the indirection pointer
        # we stop either if we reached the specified relative version or the primary tail record (in this case we return the base record)
        version = 0
        while version > relative_version and indirection != -1:
            current_rid = indirection

            # from now on we are definetely working with tail
            indirection = self.get_column_value(current_rid, Config.indirection_column_idx, tail_flg=1)
            version -= 1

        if version == relative_version:
            # return record from the tail
            return 1, current_rid
        
        elif indirection == -1:
            return 0, current_rid

        assert 1 == 0 # shouldn't reach this part
    
    def get_column_value(self, rid, column_id, tail_flg = 0, cache_update=True):
        assert column_id < self.num_columns

        page_capacity = Config.page_size // 8
        page_num = rid // page_capacity
        order_in_page = rid % page_capacity

        if tail_flg == 0:
            assert rid < self.num_records
            # return self.data[column_id]['Base'][page_num].read(order_in_page)
            return self.bufferpool.get_page(page_num, column_id, tail_flg=0, cache_update=cache_update).read(order_in_page)
        else:
            assert rid < self.num_tail_records
            # return self.data[column_id]['Tail'][page_num].read(order_in_page)
            return self.bufferpool.get_page(page_num, column_id, tail_flg=1, cache_update=cache_update).read(order_in_page)
        
    def get_data_attribute(self, rid, column):
        """
        Use this to get the newest version of a data attribute.
        column is a logical column. Don't use the column offset
        """
        assert column < self.num_columns
        indirection = self.get_column_value(rid, Config.indirection_column_idx, tail_flg=False)
        if indirection == -1:
            return self.get_column_value(rid, column+Config.column_data_offset, tail_flg=False)
        schema = self.get_column_value(indirection, Config.schema_encoding_column_idx, tail_flg=True)
        if utils.get_bit(schema, column):  
            return self.get_column_value(indirection, column+Config.column_data_offset, tail_flg=True)    
        return self.get_column_value(rid, column+Config.column_data_offset, tail_flg=False)          
        
    def set_column_value(self, rid, column_id, new_value, tail_flg = 0, cache_update=True):
        assert column_id >= 0 
        assert column_id < self.num_columns
        page_capacity = Config.page_size // 8
        page_num = rid // page_capacity
        order_in_page = rid % page_capacity

        if tail_flg == 0:
            assert rid < self.num_records
            page = self.bufferpool.get_page(page_num, column_id, tail_flg=0, cache_update=cache_update)
            page.write_at_location(new_value, order_in_page)
            self.bufferpool.update_page(page, page_num, column_id, tail_flg=0, cache_update=cache_update)
            # !!! Do we need to update explicitly here??? self.bufferpool.update
            # self.data[column_id]['Base'][page_num].write_at_location(new_value, order_in_page)
        else:
            assert rid < self.num_tail_records
            page = self.bufferpool.get_page(page_num, column_id, tail_flg=1, cache_update=cache_update)
            page.write_at_location(new_value, order_in_page)
            self.bufferpool.update_page(page, page_num, column_id, tail_flg=1, cache_update=cache_update)
        return True

'''
  def get_page_copy(self, column, page_idx, tail_flg = 0):
        if tail_flg == 0:
            copy_page = copy.deepcopy(self.data[column]['Base'][page_idx])
            return copy_page
        elif tail_flg == 1:
            copy_page = copy.deepcopy(self.data[column]['Tail'][page_idx])
            return copy_page
        
    def get_page(self, column, page_idx, tail_flg = 0):
        if tail_flg == 0:
            return self.data[column]['Base'][page_idx]
        elif tail_flg == 1:
            return self.data[column]['Tail'][page_idx]
        
    def overwrite_page(self, new_page, column, page_idx, tail_flg=0):
        if tail_flg == 0:
            self.data[column]['Base'][page_idx] = new_page
        elif tail_flg == 1:
            self.data[column]['Tail'][page_idx] = new_page
'''
  
class Table:
    """A Table defines a unique grouping of records

    An individual Table is responsible for storing and indexing
    a list of Records which are logically stored in Pages.
    Each column of a Record is stored in a separate list of Pages
    which is kept track by a PageDirectory.  An Index object allows
    for individual records to be retrieved by value.
    """

    def __init__(self, db_path, name, num_columns=None, primary_key=None, force_merge=Config.force_merge, merge_interval=Config.merge_interval):
        """Initialize a Table

        Parameters
        ----------
        name: string
            The name of the table
        num_columns: int
            The total number of columns to store in the table
        primary_key: int
            The index of the column to use as the primary key
        
        Raises
        ------
        PrimaryKeyOutOfBoundsError
        TotalColumnsInvalidError
        """

        # Set internal state
        self.db_path = db_path
        self.name = name
        self.primary_key = primary_key
        self.num_columns = num_columns
        self.lock_manager = LockManager()
        
        # restore num_records and num_tail_records if they exist
        meta_path = os.path.join(db_path, name, 'meta.data')
        if os.path.exists(meta_path):
            with open(meta_path, 'rb') as fp:
                num_records = struct.unpack('<i', fp.read(4))[0]
                num_tail_records = struct.unpack('<i', fp.read(4))[0]
                self.num_columns = struct.unpack('<i', fp.read(4))[0]
                self.primary_key = struct.unpack('<i', fp.read(4))[0]
        else:
            num_records = 0
            num_tail_records = 0
            self.num_columns_file = num_columns
            self.primary_key = primary_key
            
        # Validate that the primary key column is within the range of columns
        if (self.primary_key >= self.num_columns):
            raise PrimaryKeyOutOfBoundsError(primary_key, num_columns)

        # Validate that the total number of columns is greater than 0
        if (self.num_columns <= 0):
            raise TotalColumnsInvalidError(num_columns)
            
        self.force_merge = force_merge          
        self.page_directory = PageDirectory(
            self.db_path,
            self.name,
            self.num_columns + Config.column_data_offset,
            num_records=num_records,
            num_tail_records=num_tail_records,
        )
        

        self.index = Index(self)

        # Merge policy features

        if self.force_merge == False:
            # Only adjust interval and num tails to merge
            self.interval = merge_interval
            self.num_tails_to_merge = 1

            # DO NOT TOUCH
            self.running = True
            self.num_tail_pages = 0
            self.tail_queue = Queue()
            thread = threading.Thread(target=self.__run, daemon=True)
            thread.start()


    def __contains__(self, key):
        """Implements the contains operator
        
        Parameters
        ----------
        key : int
            The primary key to find within the table
        
        Returns
        -------
        b : bool
            Whether or not the primary key was found
        """

        # Search through the primary key column and try to find it
        v = self.index.locate(self.primary_key, key)  # TODO: double check this is the correct column
        return (v is not None)

    def __getitem__(self, primary_key):
        """Implements the get operator

        Parameters
        ----------
        primary_key : int
            The primary key to find within the table

        Returns
        -------
        r : Record
            The found Record object

        Raises
        ------
        IndexError
        """

        #if (key in self):
        #    return self.index.locate(self.key)
        #else:
        #    raise IndexError("Key {} does not exist.".format(key))

        # Use the internal Index to find a record
        res = self.index.locate(self.primary_key, primary_key)[0]
        return res

    def __len__(self):
        """Get the total number of Records

        Returns
        -------
        l : int
            The total number of records (regardless of deletion status)
        """

        return self.page_directory.num_records

    def __str__(self):
        """
        String representation of the logical Table

        Returns
        -------
        s : string
            The string representation of the logical Table
        """

        # Number of spaces
        nsp = 10

        s = ""

        # Top bar
        s += ("-"*(nsp+1))*self.num_columns + "-\n"

        # Header (indicates primary key column)
        s += "|"
        for c in range(self.num_columns):
            if (c == self.primary_key):
                s += "*"*nsp+"|"
            else:
                s += " "*nsp+"|"
        s += "\n"

        # Header bar
        s += ("-"*nsp+"-")*self.num_columns + "-\n"

        # Data
        #s += "|"
        num_logical_records = 0
        for r in range(self.page_directory.num_records):
            rid = self.page_directory.get_column_value(r, Config.rid_column_idx)
            if (rid == -1):
                continue

            s += "|"
            for c in range(self.num_columns):
                    v = self.page_directory.get_data_attribute(r, c)
                    s += f"{v: {nsp}}|"
                    num_logical_records += 1
            s += "\n"

        # Bottom Bar
        s += ("-"*nsp+"-")*self.num_columns + "-"

        return s

    def __repr__(self):
        """String representation of the Table

        Returns
        -------
        s : string
            The string representation of the Table
        """

        return self.__str__()
    
    def str_physical(self, base_limit=10, tail_limit=10) -> str:
        """
        Creates a string of the physical table representation

        Parameters
        ----------
        base_limit : int | None (Default=10)
            The limit of base tuples to represent
        tail_limit : int | None (Default=10)
            The limit of tail tuples to represent

        Returns
        ------
        str
            The pysical table representation string
        """
        # Construct Column Names
        total_columns = self.num_columns + Config.column_data_offset
        column_widths = [0] * total_columns
        column_names = [None] * total_columns

        column_names[Config.indirection_column_idx] = "indir"
        column_names[Config.rid_column_idx] = "rid"
        column_names[Config.timestamp_column_idx] = "time"
        column_names[Config.schema_encoding_column_idx] = "schema"
        column_names[Config.tps_and_brid_column_idx] = "tps/brid"
        for column in range(self.num_columns):
            physical_column = column + Config.column_data_offset
            column_names[physical_column] = str(column)
            if self.primary_key == column:
                column_names[physical_column] += ":pk"
        # End Construct Column Names

        # Construct Table Data
        base_page_data = []
        base_clipped = False
        num_records = self.page_directory.num_records
        if base_limit is not None and base_limit < num_records:
            base_clipped = True
            num_records = base_limit
        for rid in range(num_records):
            values = []
            for column in range(total_columns):
                values.append(str(self.page_directory.get_column_value(rid, column, tail_flg=False)))
            base_page_data.append(values)

        tail_page_data = []
        tail_clipped = False
        num_tail_records = self.page_directory.num_tail_records
        if tail_limit is not None and tail_limit < num_tail_records:
            tail_clipped = True
            num_tail_records = tail_limit
        for rid in range(num_tail_records):
            values = []
            for column in range(total_columns):
                values.append(str(self.page_directory.get_column_value(rid, column, tail_flg=True)))
            tail_page_data.append(values)
        # End Construct Table Data

        # Construct Column Widths
        for column, column_name in enumerate(column_names):
            column_widths[column] = len(column_name) + 2
        
        for values in base_page_data:
            for column, attribute in enumerate(values):
                column_widths[column] = max(column_widths[column], len(attribute)+2)

        for values in tail_page_data:
            for column, attribute in enumerate(values):
                column_widths[column] = max(column_widths[column], len(attribute)+2)
        # End Construct Column Widths

        # Construct String
        result = ""
        result += self._str_horizontal_line(widths=column_widths) + "\n"
        result += self._str_tuple(column_widths, column_names) + "\n"
        result += self._str_horizontal_line(widths=column_widths) + "\n"

        if len(base_page_data) == 0 and len(tail_page_data) == 0:
            result += self._str_message(widths=column_widths, message="EMPTY TABLE :(") + "\n"
            result += self._str_horizontal_line(widths=column_widths)
            return result

        for values in base_page_data:
            result += self._str_tuple(column_widths, values) + "\n"
        if base_clipped:
            result += self._str_dots(column_widths) + "\n"
        if len(tail_page_data) != 0 or tail_clipped:
            result += self._str_horizontal_line(column_widths) + "\n"

        for values in tail_page_data:
            result += self._str_tuple(column_widths, values) + "\n"
        if tail_clipped:
            result += self._str_dots(column_widths) + "\n"
        result += self._str_horizontal_line(column_widths)
        # End Construct String

        return result

    def _str_horizontal_line(self, widths) -> str:
        result = "+"
        for column, width in enumerate(widths):
            if column != 0:
                result += "+"
            result += "-" * width
        result += "+"
        return result
    
    def _str_tuple(self, widths, values) -> str:
        result = "|"
        for physical_column, info in enumerate(zip(widths, values)):
            width, attribute = info
            if physical_column != 0:
                result += "|"

            left_padding = width - (len(attribute) + 1)
            
            result += (" " * left_padding) + attribute + " "
        result += "|"
        return result
    
    def _str_dots(self, widths):
        dots = ["."*min(width-2, 3) for width in widths]
        return self._str_tuple(widths, dots)
    
    def _str_message(self, widths, message):
        total_width = sum(widths) + (len(widths) - 1)
        left_padding = 1
        right_padding = total_width - left_padding - len(message)
        assert right_padding >= 1
        result = "|" + (" "*left_padding) + message + (" "*right_padding) + "|"
        return result


    def column_iterator(self, column):
        """Iterate through all values in a column

        Parameters
        ----------
        column : int
            The column index
        tail_flg : int (Default=0)
            Whether the record is a tail record or not

        Yields
        ------
        value : any
            The value of the column at the specific RID
        rid : int
            The RID of the current column
        """

        # Check if the range is valid
        if (column >= self.num_columns):
            # Immediately return None to prevent looping
            return None

        # Loop through all possible rows and yield a value
        for i in range(len(self)):
            # Resolve the true RID
            rid_column = Config.rid_column_idx
            indirection_column = Config.indirection_column_idx
            rid = self.page_directory.get_column_value(rid=i, column_id=rid_column, tail_flg=0)
            if rid == -1:
                # if a tombstone is found, continue
                continue

            yield self.page_directory.get_data_attribute(rid, column), rid
        
    def get_column(self, column_index):
        if column_index >= self.num_columns or column_index < 0:
            raise ColumnDoesNotExist(column_index, self.num_columns)
        return self.page_directory.data[column_index]

    def delete(self, rid):
        """Remove a specific Record given its RID

        Parameters
        ----------
        rid : int
            The record ID to remove
        
        Returns
        -------
        status : bool
            Whether or not the operation completed successfully
        """
        # Set the RID column value to -1 (invalid)
        return self.page_directory.set_column_value(rid, Config.rid_column_idx, -1, 0)
        
    def close(self):
        # dump record data
        meta_path = os.path.join(self.db_path, self.name, 'meta.data')
        with open(meta_path, 'wb') as fp:
                fp.write(struct.pack('<i', self.page_directory.num_records))
                fp.write(struct.pack('<i', self.page_directory.num_tail_records))
                fp.write(struct.pack('<i', self.num_columns))
                fp.write(struct.pack('<i', self.primary_key))
            
        #flush the pool
        # TODO do we even need this? the object is deleted automatically
        self.page_directory.bufferpool.flush()
        

    def __merge(self, tail_page_indices):
        # Which tail pages are going to be merged
        # This needs to be discussed

        for tail_page_idx in tail_page_indices:
            page_capacity = Config.page_size // 8
            num_tail_pages = math.ceil(self.page_directory.num_tail_records / page_capacity)
            if tail_page_idx >= num_tail_pages:
                # we shouldn't be calling merge when there are no records to merge
                # raise error, we are trying to merge records that don't exist
                break

            # still need to include tps tracking
            # get the tail page rid and schema columms
            # page_rid = self.page_directory.get_page(Config.tps_and_brid_column_idx, tail_page_idx, 1)
            # page_schema = self.page_directory.get_page(Config.schema_encoding_column_idx, tail_page_idx, 1)
            page_rid = self.page_directory.bufferpool.get_page(tail_page_idx, Config.tps_and_brid_column_idx, tail_flg=1, cache_update=True)
            page_schema = self.page_directory.bufferpool.get_page(tail_page_idx, Config.schema_encoding_column_idx, tail_flg=1, cache_update=True)

            # initialize base_copies, this will be where we manage the copied pages
            tps_copies = {}
            base_copies = []
            for i in range(self.num_columns):
                base_copies.append({})

            for i in range(self.num_columns):
                # get the column we will work on
                # tail = self.page_directory.get_page(i + Config.column_data_offset, tail_page_idx, 1)
                tail = self.page_directory.bufferpool.get_page(tail_page_idx,i + Config.column_data_offset, tail_flg=1, cache_update=True)
                # track if an RID has been seen yet, we iterate backwards so we only merge the most recent update
                # instead of seen I would like to use TPS, this may prevent some possible errors
                seen = set() 

                for j in range(tail.num_cells-1, -1, -1):
                    record_value = tail.read(j) # value we might update
                    rid = page_rid.read(j) # rid of base record we might update
                    # get the page index so we know which base page will be updated
                    base_page_idx = int(rid // (Config.page_size / Config.page_cell_size))

                    # if the base page has not been copied and brought in, do so
                    if base_page_idx not in base_copies[i]:
                        # base = self.page_directory.get_page_copy(i + Config.column_data_offset, base_page_idx)
                        base_source = self.page_directory.bufferpool.get_page(
                            base_page_idx,
                            i + Config.column_data_offset,
                            tail_flg=0,
                            cache_update=True
                        )
                        base = copy.deepcopy(base_source)
                        
                        # tps = self.page_directory.get_page_copy(Config.tps_and_brid_column_idx, base_page_idx)
                        tps_source = self.page_directory.bufferpool.get_page(
                            base_page_idx,
                            Config.tps_and_brid_column_idx,
                            tail_flg=0,
                            cache_update=True
                        )
                        tps = copy.deepcopy(tps_source)
                        
                        tps_copies[base_page_idx] = tps
                        base_copies[i][base_page_idx] = base
                    
                    # if the record has not been updated yet
                    if rid not in seen:
                        seen.add(rid)
                        # if the column has been updated at this spot, update it
                        if utils.get_bit(page_schema.read(j), i):
                            location = int(rid % (Config.page_size / Config.page_cell_size))
                            base_copies[i][base_page_idx].write_at_location(record_value, location)

                    tps = self.page_directory.get_column_value(rid, Config.tps_and_brid_column_idx)
                    if not tps >= rid:
                        # tail_rid = self.page_directory.get_page(Config.rid_column_idx, tail_page_idx, 1)
                        tail_rid = self.page_directory.bufferpool.get_page(
                            tail_page_idx,
                            Config.rid_column_idx,
                            tail_flg=1,
                            cache_update=True
                        )
                        new_tps = tail_rid.read(j)
                        self.page_directory.set_column_value(rid, Config.tps_and_brid_column_idx, new_tps)
            
            # overwrite base page with new
            for i in range(len(base_copies)):
                for key in base_copies[i].keys():
                    # self.page_directory.overwrite_page(base_copies[i][key], i+Config.column_data_offset, key)
                    self.page_directory.bufferpool.update_page(
                        page=base_copies[i][key],
                        page_num=key,
                        column_id=i+Config.column_data_offset,
                        tail_flg=0,
                        cache_update=True
                    )
 
    def merge(self):
        self.__merge(tail_page_indices = [0, 1, 2])

    def __run(self):
        while self.running:
            old_num_tails = self.num_tail_pages 
            new_num_tails = self.page_directory.num_tail_pages
            if new_num_tails >= 2:
                self.num_tail_pages = new_num_tails
                for i in range(old_num_tails, new_num_tails):
                    self.tail_queue.push(i)

            if not self.tail_queue.isEmpty():
                tails_to_merge = []
                for i in range(self.num_tails_to_merge):
                    # BAD CODE REWRITE
                    if not self.tail_queue.isEmpty():
                        value = self.tail_queue.pop()
                        tails_to_merge.append(value)

                self.__merge(tails_to_merge)
            time.sleep(self.interval)
