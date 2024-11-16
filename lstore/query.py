"""
The Query class provides standard SQL operations such as insert, select, update, delete, and sum. 
The select function returns all the records matching the search key (if any), and only the projected 
columns of the matching records are returned. The insert function will insert a new record in the 
table. All columns should be passed a non-NULL value when inserting. The update function 
updates values for the specified set of columns. The delete function will delete the record with the
specified key from the table. The sum function will sum over the values of the selected column for 
a range of records specified by their key values. We query tables by direct function calls rather 
than parsing SQL queries.
"""

from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
import lstore.utils as utils
from config import Config
import datetime

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table : Table):
        self.table = table
        pass

    def delete(self, primary_key):
        """Delete a record given a primary_key

        Parameters
        ----------
        primary_key : any
            The primary key which specifies the record to delete
        
        Returns
        -------
        status : bool
            If the operation completed successfully, this will
            be True.  If the record doesn't exist or is locked
            due to 2PL, this will be False.
        """
        # Check if the requested primary key exists in the current table
        rids = self.table.index.locate(column=self.table.primary_key, value=primary_key)

        if len(rids) == 0:
            return False
        else:
            assert len(rids) == 1
            self.table.index.maintain_delete(primary_key)
            # assert self.table.index.locate(column=self.table.primary_key, value=columns[0])[0] == new_rid
            return self.table.delete(rids[0])
        if (primary_key in self.table):
            # TODO: Eventually check for LOCK state

            # Set the deletion flag on the specified record (Set RID to -1)
            return self.table.delete(self.table[primary_key])
        else:
            return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # as is, this SHOULD insert a new record, however, this needs to be tested first

        # in general we should return the whole column, not just the first page TODO: fix this

        # for each column: if the last base page is at full capacity -> add a new base page

        # Verify that the primary key doesn't exist already
        try:
            pk = columns[self.table.primary_key]
            pkv = self.table[pk]

            # If the value is not null, it exists already
            if (pkv is not None):
                return False
        except:
            pass

        columns_values = [None] * (len(columns) + Config.column_data_offset)

        new_rid = self.table.page_directory.num_records

        columns_values[Config.rid_column_idx] = new_rid
        columns_values[Config.schema_encoding_column_idx] = 0
        # get current timestamp as an integer
        columns_values[Config.timestamp_column_idx] = int(datetime.datetime.now().timestamp())
        columns_values[Config.indirection_column_idx] = -1
        
        columns_values[Config.column_data_offset:] = columns[:]

        try:
            self.table.page_directory.add_record(columns_values)
            self.table.index.maintain_insert(columns, new_rid)
    
            return True
        except Exception as e:
            print(e)
            return False
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version=0):
        # general idea:
        # if there is an index for this column:
        #     use index to find rids
        # else:
        #     use linear iteration through rid page directory to find corresponding rid's
        # for each found rid construct a Record object:
        #   go to final version of record with specified rid and get all the column values
        #   for _ in range(relative_version)
        #       go to the previous version of the record using pointer to prev version

        # dumb rid search for now TODO: Add search of rid's using the index
        
        # first find all the relevant rids:

        # relevant_rids = []

        # for rid in range(self.table.page_directory.num_records):
        #     if self.table.page_directory.get_column_value(rid, search_key_index + Config.column_data_offset) == search_key:
        #         relevant_rids.append(rid)  

        relevant_rids = self.table.index.locate(column=search_key_index, value=search_key)

        records = []

        for rid in relevant_rids:
            # check for tombstone
            if self.table.page_directory.get_column_value(rid, Config.rid_column_idx) == -1:
                continue

            res_columns = []
            # gather the column_values from the base record
            for column_id in range(len(projected_columns_index)):
                if projected_columns_index[column_id]:
                    res_columns.append(self.table.page_directory.get_column_value(rid, column_id + Config.column_data_offset, tail_flg=0))

            # get the rid corresponding to the relevant version
            tail_flg, target_rid = self.table.page_directory.get_rid_for_version(rid, relative_version)
            # if there is record in tail - do the updates, otherwise return the base record data since there is not updates
            if tail_flg:
                schema = self.table.page_directory.get_column_value(target_rid, Config.schema_encoding_column_idx, tail_flg)
                for column_id in range(len(projected_columns_index)):
                    if utils.get_bit(schema, column_id):
                        # this means that the column has been updated
                        res_columns[column_id] = self.table.page_directory.get_column_value(target_rid, column_id + Config.column_data_offset, tail_flg)

            records.append(
                Record(
                    rid = rid,
                    key = self.table.page_directory.get_column_value(rid, self.table.primary_key + Config.column_data_offset),
                    columns=res_columns)
            )

        return records



    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # found_rids = []

        # for rid in range(self.table.page_directory.num_records):
        #     if self.table.page_directory.get_column_value(rid, self.table.primary_key+Config.column_data_offset) == primary_key:
        #         found_rids.append(rid)

        found_rids = self.table.index.locate(column=self.table.primary_key, value=primary_key)

        # In this case, the primary key itself is being updated, which requires extra checks
        if (primary_key != columns[self.table.primary_key]):
            other_rids = self.table.index.locate(column=self.table.primary_key, value=columns[self.table.primary_key])
            
            # Verify that changing the primary key does not result in an existing primary key
            if (isinstance(other_rids, list)):
                if (len(other_rids) > 0):
                    if (other_rids[0] not in found_rids):
                        return False

        relevant_rids = []

        for rid in found_rids:
            # check for tombstone
            if self.table.page_directory.get_column_value(rid, Config.rid_column_idx) != -1:
                relevant_rids.append(rid)

        if not relevant_rids:
            return False

        # should be only one base rid
        assert len(relevant_rids) == 1

        columns_values = [-1] * (len(columns) + Config.column_data_offset)

        # need base meta information
        rid = relevant_rids[0]
 
        base_ind = self.table.page_directory.get_column_value(rid, Config.indirection_column_idx, tail_flg=0)
        base_schema = self.table.page_directory.get_column_value(rid, Config.schema_encoding_column_idx, tail_flg=0)

        # create new tail record based off of base record data
        new_rid = self.table.page_directory.num_tail_records

        columns_values[Config.rid_column_idx] = new_rid
        # get current timestamp as an integer
        columns_values[Config.timestamp_column_idx] = int(datetime.datetime.now().timestamp())
        columns_values[Config.indirection_column_idx] = base_ind

        # code below maintains cumulative approach to tail records


        # if there is another update
        if base_ind != -1:
            # get current values of record
            old_record = self.select(primary_key, self.table.primary_key, [1]*len(columns))[0]

            for i in range(len(columns)):
                if utils.get_bit(base_schema, i):
                    columns_values[i + Config.column_data_offset] = old_record.columns[i]

        # for all columns passed in check if they are Nonetype,
        # if not add it tail record and adjust schema accordingly
        # else, add -1 as place holder
        new_schema = base_schema
        for i in range(len(columns)):
            if columns[i] is not None:
                columns_values[i + Config.column_data_offset] = columns[i]
                new_schema = utils.set_bit(new_schema, i)
        
        columns_values[Config.schema_encoding_column_idx] = new_schema

        # add record to tail page
        self.table.page_directory.add_record(columns_values, tail_flg=1)

        self.table.page_directory.set_column_value(
            rid,
            Config.indirection_column_idx,
            new_value=new_rid,
            tail_flg=0
        )
        # assert self.table.page_directory.get_column_value(rid, Config.indirection_column_idx, tail_flg=0) == new_rid

        self.table.page_directory.set_column_value(
            rid,
            Config.schema_encoding_column_idx,
            new_value=columns_values[Config.schema_encoding_column_idx],
            tail_flg=0
        )
        # assert self.table.page_directory.get_column_value(rid, Config.schema_encoding_column_idx, tail_flg=0) == columns_values[Config.schema_encoding_column_idx]
        
        self.table.index.maintain_update(primary_key, columns)
        # assert len(self.table.index.locate(self.table.primary_key, primary_key)) == 1
        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, relative_version=0)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        # relevant_rids = []

        # for rid in range(self.table.page_directory.num_records):
        #     if start_range <= self.table.page_directory.get_column_value(rid, self.table.primary_key + Config.column_data_offset) <= end_range:
        #         relevant_rids.append(rid)       

        # if len(relevant_rids) == 0:
        #     return False

        relevant_rids = self.table.index.locate_range(begin=start_range, end=end_range, column=self.table.primary_key)
        
        result = 0

        for rid in relevant_rids:
            if self.table.page_directory.get_column_value(rid, Config.rid_column_idx) == -1:
                continue
            # get the column_value from the base record
            value = self.table.page_directory.get_column_value(rid, aggregate_column_index + Config.column_data_offset, tail_flg=0)

            # get the rid corresponding to the relevant version
            tail_flg, target_rid = self.table.page_directory.get_rid_for_version(rid, relative_version)
            # if there is record in tail - do the updates, otherwise return the base record data since there is not updates
            if tail_flg:
                schema = self.table.page_directory.get_column_value(target_rid, Config.schema_encoding_column_idx, tail_flg)
                if utils.get_bit(schema, aggregate_column_index):
                    # this means that the column has been updated
                    value = self.table.page_directory.get_column_value(target_rid, aggregate_column_index + Config.column_data_offset, tail_flg)

            result += value

        return result
    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.primary_key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False