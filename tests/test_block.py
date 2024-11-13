# Imports
import os
import shutil
import unittest

# Local imports
from lstore.block import Block, Page
from config import Config

class TestBlock(unittest.TestCase):
    """Unit testing Block class

    This tests various functions of the Block class.
    """

    def setUp(self):
        self.full_path = 'tests/scratch/block_test001'
        self.block = Block(self.full_path, 0, 0)
        if (os.path.exists(self.full_path)):
            shutil.rmtree(self.full_path, ignore_errors=True)
        
        os.makedirs(self.full_path)

    def tearDown(self):
        self.block = None
        if (os.path.exists(self.full_path)):
            shutil.rmtree(self.full_path, ignore_errors=True)

    def test_read_non_existing(self):
        """
        Test that reading a block that doesn't exist
        results in a False read condition.
        """
        value = self.block.read()
        self.assertFalse(value)

    def test_read_existing(self):
        """
        Test that reading an existing block results in a True read
        """

        p = Page()
        self.block.append(p)
        self.block.write()
        value = self.block.read()
        self.assertTrue(value)

    def test_read_nonempty(self):
        """
        Test that reading a non empty block results
        in the data that was written.
        """

        # Create a new page
        p = Page()

        # Set the data
        data = [2,3,5,7,11]
        for d in data:
            p.write(d)
        
        # Add the page to the block
        self.block.append(p)

        # Write the data to disk
        self.block.write()

        # Reconstruct the block
        self.block = None
        self.block = Block(self.full_path, 0, 0)

        # Read data from disk
        self.block.read()

        # Check the read data from the block matches
        p = self.block.get_page(0)
        for i, d in enumerate(data):
            tvalue = p.read(i)
            self.assertEqual(tvalue, d)



    #def test_write(self):
    #    pass

if __name__ == "__main__":
    unittest.main()
