"""
This contains various wrappers for handling
threaded queries.
"""

from lstore.query import Query
from config import Config


class QueryWrapper():
    """
    A QueryWrapper object is a way of wrapping
    a query into its threaded counterpart to allow
    concurrent queries to take place seamlessly.
    """

    def __init__(self, table, query_function, transaction, args):
        """
        Initialize a new ThreadedQuery which wraps
        a specific query function.  This allows
        determining the data dependencies, locking
        and other operations to facilitate concurrency.

        Parameters
        ----------
        table : Table
            The Table object to control access to
        query_function : function
            A specific Query function to try to execute
        transaction : Transaction
            The Transaction object associated with the Query
        args : list
            A list of arguments to be passed into the query_function
        """
        
        # Set up internal variables
        self.table = table
        self.index = self.table.index
        self.query_function = query_function
        self.query_function_type = query_function.__func__
        self.transaction = transaction
        self.args = args
        self.lock_manager = self.table.lock_manager
        self.work_flag = False

        # While we could find these values at initialization
        # it requires the index lock, so wait until try_run.
        # Also saves some work.

        # In case of delete roll back
        if self.query_function_type == Query.insert:
            self.primary_key = self.args[self.table.primary_key]

        if self.query_function_type == Query.delete:
            self.delete_rid = None

        # in case of update roll back
        if self.query_function_type == Query.update:
            self.update_schema = None
            primary_key_location_in_args = self.table.primary_key + 1
            if self.args[primary_key_location_in_args] is None:
                self.primary_key = self.args[0]
            else:
                self.primary_key = self.args[primary_key_location_in_args]
            self.delete_rid = None
            self.old_record = [None] * self.table.num_columns
        # Determine hooks

    def try_run(self):
        """Try run

        This function will try to run
        a given query function. First
        gathering locks, returning false
        if required locks are unavailable,
        then running and returning query.

        Parameters
        ----------
        args: List
            The list of arguments for the query.
        """

        # Find which resources are accessed

        resources = self.__find_resources(*self.args)

        
        # request index lock first
        # Do this seperately from the rest in case query is delete
        # and we have to find RID before query in case of roll back
        lock_type, unique_id, transaction = resources[0]
        lock = self.table.lock_manager.request(lock_type, unique_id, transaction)
        if lock == None:
            return False

        # Update the query rid
        if self.query_function_type == Query.delete:
            rids = self.table.index.locate(column=self.table.primary_key, value=self.args[0])
            if len(rids) != 1:
                return None
            self.delete_rid = rids[0]

        # update the query schema
        elif self.query_function_type == Query.update:
            rid = self.table.index.locate(column=self.table.primary_key, value=self.args[0])[0]
            self.delete_rid = rid
            self.update_schema = self.table.page_directory.get_column_value(rid, Config.schema_encoding_column_idx, )

            for column in range(self.table.num_columns):
                self.old_record[column] = self.table.page_directory.get_data_attribute(rid, column)

        for i in range(1, len(resources)):
            lock_type, unique_id, transaction = resources[i]
            lock = self.table.lock_manager.request(lock_type, unique_id, transaction)
        
            if lock == None:
                return False

        query_result = self.query_function(*self.args)
        if not query_result:
            return None
        else:
            self.work_flag = True
            return query_result
    
    def __find_resources(self, *args):
        """Find resources

        For the given query in the function
        wrapper, this will specify the locks
        needed to safely run each query.

        Parameters
        ----------
        args: List
            The list of arguments for the query.

        Returns
        -------
        resources
            list of locks needed for query.
        """

        # Store a list of resources
        resources = []
        
        # Compare which function to use
        if self.query_function_type == Query.delete:
            # Write only that affects only one column
            resources.append((Config.EXCLUSIVE_LOCK, ('Index'), self.transaction))
            resources.append((Config.EXCLUSIVE_LOCK, (args[0], Config.rid_column_idx), self.transaction))

        elif self.query_function_type == Query.insert:
            primary = args[self.table.primary_key]

            # Write only on all columns
            resources.append((Config.EXCLUSIVE_LOCK, ('Index'), self.transaction))
            for i in range(len(args) + Config.column_data_offset):
                resources.append((Config.EXCLUSIVE_LOCK, (primary, i), self.transaction))

        elif self.query_function_type == Query.update:
            # IMPORTANT: In an update the exclusive lock might not always be needed
            resources.append((Config.EXCLUSIVE_LOCK, ('Index'), self.transaction))
            primary = args[0]
            columns = args[1:]

            # As a record will need to be written in each column
            # an exclusive lock will eventually be needed on all columns
            # so just get exclusive instead of exclsuive and shared for 
            # the read operations
            for i in range(len(columns)+Config.column_data_offset):
                resources.append((Config.EXCLUSIVE_LOCK, (primary, i), self.transaction))

        elif self.query_function_type == Query.select or self.query_function_type == Query.select_version:
            project_columns = args[2]
            primary = args[0]

            # read only on just the columns required in the select args
            resources.append((Config.SHARED_LOCK, ('Index'), self.transaction))
            for i in range(len(project_columns)):
                if project_columns[i]:
                    resources.append((Config.SHARED_LOCK, (primary, i+Config.column_data_offset), self.transaction))

        elif self.query_function_type == Query.sum or self.query_function_type == Query.sum_version:
            # read only on just the primary key column
            # WARNING: sum may function on a range that includes the final value
            # If so, must change range to (args[0], args[1]+1)
            resources.append((Config.SHARED_LOCK, ('Index'), self.transaction))
            for i in range(args[0], args[1]):
                resources.append((Config.SHARED_LOCK, (i, Config.rid_column_idx), self.transaction))
        
        return resources

    def roll_back(self):
        """Roll back changes

        If the query is one that must
        be reverted if transaction
        aborts, roll back changes.
        """        
        if self.work_flag:

            if self.query_function_type == Query.delete:
                # Set deleted row rid back to original rid
                # WARNING: I don't know if set_column_value will find the location given it's gravestone
                # however, logically the location should still be able to be found based on how rid is made
                self.table.page_directory.set_column_value(self.delete_rid, Config.rid_column_idx, self.delete_rid)
                
                num_columns = self.table.num_columns
                record = [None] * num_columns
                for column in range(num_columns):
                    record[column] = self.table.page_directory.get_data_attribute(self.delete_rid, column)
                self.index.maintain_insert(record, self.delete_rid)

            elif self.query_function_type == Query.insert:
                # Get rid of new record and set its rid to -1
                rid = self.table.index.locate(column=self.table.primary_key, value=self.primary_key)[0]

                self.table.page_directory.set_column_value(rid, Config.rid_column_idx, -1)
                self.index.maintain_delete(rid)

            elif self.query_function_type == Query.update:                
                # Get necessary data for roll back
                rid = self.table.index.locate(column=self.table.primary_key, value = self.primary_key)[0]

                ind = self.table.page_directory.get_column_value(rid, Config.indirection_column_idx)
                old_ind = self.table.page_directory.get_column_value(ind, Config.indirection_column_idx, tail_flg=1)

                # Update the index
                self.index.maintain_update(rid, self.old_record)

                # gravestone tail page
                self.table.page_directory.set_column_value(ind, Config.rid_column_idx, -1, tail_flg=1)

                # revert old base meta data back to original
                self.table.page_directory.set_column_value(rid, Config.schema_encoding_column_idx, self.update_schema)
                self.table.page_directory.set_column_value(rid, Config.indirection_column_idx, old_ind)

