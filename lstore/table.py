"""
The Table class provides the core of our relational storage functionality. All columns are 64-bit
integers in this implementation. Users mainly interact with tables through queries. Tables provide 
a logical view of the actual physically stored data and mostly manage the storage and retrieval of 
data. Each table is responsible for managing its pages and requires an internal page directory that, 
given a RID, returns the actual physical location of the record. The table class should also manage 
the periodical merge of its corresponding page ranges.
"""


from lstore.index import Index
from time import time
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)

        # Very rough implementaion of base and tail coupling, consider changing later
        for i in range(num_columns):
            # DANIEL COMMENT maybe {'Base': [Page(), ... ], 'Tail': [Page(), ... ]}
            self.page_directory[i] = [Page(), Page()]

        self.page_directory['RID'] = [Page(), Page()]
        self.page_directory['indirection'] = [Page(), Page()]
        self.page_directory['schema'] = [Page(), Page()]
        # DANIEL COMMENT Also need a pointer for latest version in case of base page and ptr to prev version in case of tail page
        

    def __merge(self):
        print("merge is happening")
        pass
 
