"""
The Table class provides the core of our relational storage functionality. All columns are 64-bit
integers in this implementation. Users mainly interact with tables through queries. Tables provide 
a logical view of the actual physically stored data and mostly manage the storage and retrieval of 
data. Each table is responsible for managing its pages and requires an internal page directory that, 
given a RID, returns the actual physical location of the record. The table class should also manage 
the periodical merge of its corresponding page ranges.
"""


from lstore.index import Index
import lstore.utils as utils
import copy
import time
from lstore.page import Page
from errors import ColumnDoesNotExist, PrimaryKeyOutOfBoundsError, TotalColumnsInvalidError
from config import Config
from data_structures.queue import Queue
import threading


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

class PageDirectory:
    """The PageDirectory controls access to different pages

    Pages can span multiple blocks of memory and the PageDirectory
    keeps track of these blocks so that they are easily
    indexable.
    """

    def __init__(self, num_columns):
        self.num_records = 0
        self.num_tail_records = 0
        self.num_columns = num_columns
        self.data = []
        self.num_tail_pages = 0
        for _ in range(0, num_columns):
            self.data.append({'Base':[], 'Tail':[]})

    def add_record(self, columns, tail_flg = 0):
        """
        Accepts list of column values and adds the values to the latest base page of each column
        """
        assert len(columns) == self.num_columns

        page_class = 'Base' if tail_flg == 0 else 'Tail'

        new_page = False
        for i, column_value in enumerate(columns):
            # allocate new base page if we are at full capacity
            if (not self.data[i][page_class]) or (not self.data[i][page_class][-1].has_capacity()):
                self.data[i][page_class].append(Page())
                if not new_page:
                    new_page = True
            self.data[i][page_class][-1].write(column_value)

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
        
        # IMPORTANT TODO: this works for 64 bit integers, need to make some smart function for variable data types
        page_capacity = Config.page_size // 8
        current_rid = rid
        page_num = current_rid // page_capacity
        order_in_page = current_rid % page_capacity

        schema = self.data[Config.schema_encoding_column_idx]['Base'][page_num].read(order_in_page)
        indirection = self.data[Config.indirection_column_idx]['Base'][page_num].read(order_in_page)

        # return base record if there is only one version
        if indirection == -1: 
            return 0, current_rid

        # get the latest version first
        current_rid = indirection
        
        page_num = current_rid // page_capacity
        order_in_page = current_rid % page_capacity

        # from now on we are definetely working with tail
        schema = self.data[Config.schema_encoding_column_idx]['Tail'][page_num].read(order_in_page)
        indirection = self.data[Config.indirection_column_idx]['Tail'][page_num].read(order_in_page)
        
        # get the relative version by iterating backwards using the indirection pointer
        # we stop either if we reached the specified relative version or the primary tail record (in this case we return the base record)
        version = 0
        while version > relative_version and indirection != -1:
            current_rid = indirection
            
            page_num = current_rid // page_capacity
            order_in_page = current_rid % page_capacity

            # from now on we are definetely working with tail
            schema = self.data[Config.schema_encoding_column_idx]['Tail'][page_num].read(order_in_page)
            indirection = self.data[Config.indirection_column_idx]['Tail'][page_num].read(order_in_page)
            version -= 1

        if version == relative_version:
            # return record from the tail
            return 1, current_rid
        
        elif indirection == -1:
            return 0, current_rid

        assert 1 == 0 # shouldn't reach this part
    
    def get_column_value(self, rid, column_id, tail_flg = 0):
        assert column_id < self.num_columns

        page_capacity = Config.page_size // 8
        page_num = rid // page_capacity
        order_in_page = rid % page_capacity

        if tail_flg == 0:
            assert rid < self.num_records
            return self.data[column_id]['Base'][page_num].read(order_in_page)
        else:
            assert rid < self.num_tail_records
            return self.data[column_id]['Tail'][page_num].read(order_in_page)
        
    def set_column_value(self, rid, column_id, new_value, tail_flg = 0):
        assert column_id >= 0 
        assert column_id < self.num_columns

        page_capacity = Config.page_size // 8
        page_num = rid // page_capacity
        order_in_page = rid % page_capacity

        if tail_flg == 0:
            assert rid < self.num_records
            self.data[column_id]['Base'][page_num].write_at_location(new_value, order_in_page)
        else:
            assert rid < self.num_tail_records
            self.data[column_id]['Tail'][page_num].write_at_location(new_value, order_in_page)

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


class Table:
    """A Table defines a unique grouping of records

    An individual Table is responsible for storing and indexing
    a list of Records which are logically stored in Pages.
    Each column of a Record is stored in a separate list of Pages
    which is kept track by a PageDirectory.  An Index object allows
    for individual records to be retrieved by value.
    """

    def __init__(self, name, num_columns, primary_key, force_merge = True):
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

        # Validate that the primary key column is within the range of columns
        if (primary_key >= num_columns):
            raise PrimaryKeyOutOfBoundsError(primary_key, num_columns)

        # Validate that the total number of columns is greater than 0
        if (num_columns <= 0):
            raise TotalColumnsInvalidError(num_columns)

        # Set internal state
        self.name = name
        self.primary_key = primary_key
        self.num_columns = num_columns
        self.force_merge = force_merge
        self.page_directory = PageDirectory(num_columns + Config.column_data_offset)
        self.index = Index(self)

        # Merge policy features

        if self.force_merge == False:
            # Only adjust interval and num tails to merge
            self.interval = 1
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
        return res[0]

    def __len__(self):
        """Get the total number of Records

        Returns
        -------
        l : int
            The total number of records (regardless of deletion status)
        """

        return self.page_directory.num_records

    def __str__(self):
        """String representation of the Table

        Returns
        -------
        s : string
            The string representation of the Table
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
        for r in range(self.page_directory.num_records):
            s += "|"
            for c in range(self.num_columns):
                rid = self.page_directory.get_column_value(r, Config.rid_column_idx)
                if (rid != -1):
                    v = self.page_directory.get_column_value(rid, c+Config.column_data_offset)
                    s += f"{v: {nsp}}|"
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

    def column_iterator(self, column, tail_flg=0):
        """Iterate through all values in a column

        Parameters
        ----------
        column : int
            The column index
        tail_flg : int (Default=0)
            Whether the record is a tail record or not

        Yields
        ------
        rid : int
            The RID of the current column
        value : any
            The value of the column at the specific RID
        """

        # Check if the range is valid
        if (column >= self.num_columns):
            # Immediately return None to prevent looping
            return None

        # Loop through all possible rows and yield a value
        for i in range(len(self)):
            # Resolve the true RID
            rid_column = Config.rid_column_idx
            rid = self.page_directory.get_column_value(rid=i, column_id=rid_column, tail_flg=tail_flg)
            
            # Only yield if RID is valid
            if (rid != -1):
                yield rid, self.page_directory.get_column_value(rid, column+Config.column_data_offset, tail_flg)
        
    def get_column(self, column_index):
        if column_index >= self.num_columns or column_index < 0:
            raise ColumnDoesNotExist(column_index, self.num_columns)
        return self.page_directory.data[column_index + Config.column_data_offset]
    
    def add_base_page(self, column_index):
        if column_index >= self.num_columns or column_index < 0:
            raise ColumnDoesNotExist(column_index, self.num_columns)
        self.page_directory[column_index]['Base'].append(Page())

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
        self.page_directory.set_column_value(rid, Config.rid_column_idx, -1, 0)

    def __merge(self, tail_page_indices):
        #print("merge is happening")
        # Which tail pages are going to be merged
        # This needs to be discussed

        for tail_page_idx in tail_page_indices:

            if tail_page_idx >= len(self.page_directory.data[0]['Tail']):
                # we shouldn't be calling merge when there are no records to merge
                # raise error, we are trying to merge records that don't exist
                break

            # still need to include tps tracking
            # get the tail page rid and schema columms
            page_rid = self.page_directory.get_page(Config.tps_and_brid_column_idx, tail_page_idx, 1)
            page_schema = self.page_directory.get_page(Config.schema_encoding_column_idx, tail_page_idx, 1)

            # initialize base_copies, this will be where we manage the copied pages
            tps_copies = {}
            base_copies = []
            for i in range(self.num_columns):
                base_copies.append({})

            for i in range(self.num_columns):
                # get the column we will work on
                tail = self.page_directory.get_page(i + Config.column_data_offset, tail_page_idx, 1)
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
                        base = self.page_directory.get_page_copy(i + Config.column_data_offset, base_page_idx)
                        tps = self.page_directory.get_page_copy(Config.tps_and_brid_column_idx, base_page_idx)
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
                        tail_rid = self.page_directory.get_page(Config.rid_column_idx, tail_page_idx, 1)
                        new_tps = tail_rid.read(j)
                        self.page_directory.set_column_value(rid, Config.tps_and_brid_column_idx, new_tps)

            # overwrite base page with new
            for i in range(len(base_copies)):
                for key in base_copies[i].keys():
                    self.page_directory.overwrite_page(base_copies[i][key], i+Config.column_data_offset, key)
 
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
                    value = self.tail_queue.pop()
                    tails_to_merge.append(value)

                self.__merge(tails_to_merge)
            time.sleep(self.interval)
