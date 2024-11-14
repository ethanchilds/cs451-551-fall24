# System imports
import unittest

# Internal imports
from data_structures.priority_queue import PriorityQueue
from errors import PriorityQueueCapacityOutOfBoundsError

class TestPriorityQueue(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_invalid_capacity(self):
        """
        Test that an invalid capacity is not allowed
        """

        with self.assertRaises(PriorityQueueCapacityOutOfBoundsError) as context:
            PriorityQueue(0)
            
        self.assertTrue('out of bounds' in str(context.exception))

    def test_single_insert_in_capacity(self):
        """
        Test inserting a single item while
        within capacity.
        """

        p = PriorityQueue(1)
        p.push(3, "Test")

        self.assertTrue(p.get(3) == [0, 3, "Test"])

    def test_multi_insert_in_capacity(self):
        """
        Test inserting multiple items while
        within capacity.
        """

        data = ["A", "B", "C"]
        p = PriorityQueue(len(data))

        # Insert all items
        for i,d in enumerate(data):
            p.push(i, d)

        # Read each item
        for i,d in enumerate(data):
            self.assertTrue(p.get(i) == [0, i, d])

    def test_multi_insert_out_capacity(self):
        """
        Test inserting multiple items when
        out of capacity.
        """

        data = ["A", "B", "C", "D"]
        p = PriorityQueue(len(data)-1)

        # Insert all items
        for i,d in enumerate(data):
            p.push(i, d)

        # Read each item
        for i,d in enumerate(data):
            if (i == 0):
                # The first item should not exist
                self.assertTrue(p.get(i) is None)
            else:
                self.assertTrue(p.get(i) == [0, i, d])

    def test_multi_insert_pop(self):
        """
        Test that while inserting multiple items
        the popped items are in order of insertion.
        """

        data = ["A", "B", "C", "D"]
        p = PriorityQueue(1)

        # Insert all items
        for i,d in enumerate(data):
            if (i > 0):
                self.assertTrue(p.push(i, d) == [0, i-1, data[i-1]])
            else:
                self.assertTrue(p.push(i,d) is None)

    def test_multi_insert_priority_increment(self):
        """
        Test that inserting the same item results
        in incrementing the priority.
        """

        data = ["A", "A", "A"]
        p = PriorityQueue(1)

        # Insert all items
        for _,d in enumerate(data):
            p.push(0, d)
        
        # Get the item and verify its priority
        item = p.get(0)
        self.assertTrue(item[0] == 2)

    def test_pop(self):
        """
        Test that popping an item results in the item.
        """

        data = ["A", "B", "C"]
        p = PriorityQueue(3)

        # Insert all items
        for i,d in enumerate(data):
            p.push(i, d)

        # Pop all items
        for i,d in enumerate(data):
            item = p.pop()
            self.assertTrue(item == [0, i, d])
    
    def test_pop_with_priority(self):
        """
        Test that popping a sequence of items
        with different priorities happens in the
        expected order (min priority first).
        """

        N = 3  # Unique values
        data = ["A", "A", "B", "B", "B", "C"]
        keys = [0, 0, 1, 1, 1, 2]
        p = PriorityQueue(N)

        # Insert all items
        for i,d in enumerate(data):
            p.push(keys[i], d)

        # Pop all items
        pop_order = ["C", "A", "B"]
        for i,d in enumerate(pop_order):
            self.assertTrue(p.pop()[2] == d)


if __name__ == '__main__':
    unittest.main()