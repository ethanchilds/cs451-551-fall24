"""
This is responsible for storing a chunk of
pages referred to as a "Block".  The Block
is responsible for many operations on this
group of pages.

Data is stored on disk with the following format:

n_pages (4 Bytes)
[
    data (M Bytes)
] x n_pages

Where:
  * M is defined as the total number of bytes per page
"""

# System imports
import os
#import lzma
import struct

# Local imports
from config import Config

class Block():
    """A Block is a group of pages

    A Block is a group of pages that are read from disk
    and written back to disk.  This allows pages to be
    quickly retrieved and written between disk and RAM.
    """

    def __init__(self, base_path, column, block_id, size):
        """Initialize the Block

        This sets up an initial Block with internal properties.

        Parameters
        ----------
        base_path : str
            The path to the base directory where the
            data will be stored (database/table)
        column : int
            The column number that this block corresponds to
        block_id : int
            The unique block ID that this block corresponds to
        size : int
            The size of the block in number of pages
        """

        self.base_path = base_path
        self.column = column
        self.block_id = block_id
        self.size = size
        self.pages = []

        # Compute the full path
        self.full_path = os.path.join(base_path, f"{0}.{1}.data".format(column, block_id))

    def read(self):
        """Read data from disk if it exists.

        Fill the internal page pool with data if
        there exists a stored data file on disk.

        Returns
        -------
        status : bool
            Whether or not the operation was successful
            (False implies that this block is completely new)
        """

        # Check if the file exists on disk already
        if (os.path.exists(self.full_path)):
            # Open the Block file
            with open(self.full_path, 'rb') as fp:
                # Read metadata
                n_pages = struct.unpack('<i', fp.read(4))[0]

                # Read each page into the internal array
                for _ in range(n_pages):
                    barray = fp.read(Config.page_size)
                    
                    # TODO pass bytes into decompression stream

                    # Add data to the page list
                    self.pages.append(barray)
            
            return True
        else:
            # No existing data
            return False

    def write(self):
        """Write data to the disk and discard pages

        Write all pages in the current Block to disk
        and delete them from memory to save on RAM.
        It is important to note that this is a
        destructive action and will not preserve
        any data in RAM after writing to disk successfully.

        Returns
        -------
        status : bool
            Whether or not the operation was successful
        """

        # Fast reject
        if (len(self.pages) == 0):
            return False

        # Write all data to the disk
        with open(self.full_path, 'wb') as fp:
            # Write metadata
            n_pages = len(self.pages)
            fp.write(struct.pack('<i', n_pages))

            # Write each page into the file
            for i in range(n_pages):
                barray = self.pages[i]

                # TODO: Pass bytes into compression stream

                # Write data to the file
                fp.write(barray)

        # Delete internal data
        del self.pages
        self.pages = []

        return True

    def get(self):
        """Get all pages

        This returns the list of pages that the
        current Block is responsible for.

        Returns
        -------
        pages : list<bytearray>
            The list of pages (bytearrays)
        """

        return self.pages

    def get_page(self, page_number):
        """Get a specific page

        This returns a specific page in the Block
        or None if the specified page doesn't
        exist.

        Parameters
        ----------
        page_number : int
            The index of the page to find in the Block

        Returns
        -------
        page : bytearray
            The specified page or None if it doesn't exist
        """

        if (page_number < len(self.pages)):
            return self.pages[page_number]
        else:
            return None
