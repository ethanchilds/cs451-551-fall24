from lstore.db import Database
from lstore.query import Query
import unittest
import os
import shutil
import random


class TestLstroreDB(unittest.TestCase):

    def setUp(self):
        self.db = Database()
        self.test_table = self.db.create_table('Test', 5, 0)
        self.query = Query(self.test_table)  

    def tearDown(self):
        self.db = None
        self.test_table = None
        self.query = None
        db_path = './TEMP'
        if (os.path.exists(db_path)):
            shutil.rmtree(db_path, ignore_errors=True)

    def test_single_insert(self):
        test_values = [0,1,2,3,4]
        self.query.insert(*test_values)
        
        record = self.query.select(0, 0, [1,1,1,1,1])[0]

        self.assertListEqual(test_values, record.columns)

    def test_single_update(self):
        test_values = [0,1,2,3,4]
        self.query.insert(*test_values)

        self.query.update(0, *[None,None,5,6,7])

        record = self.query.select(0,0,[1,1,1,1,1])[0]

        self.assertListEqual([0,1,5,6,7], record.columns)
    
    def test_select_non_existent(self):
        record = self.query.select(0,0,[1,1,1,1,1])
        
        self.assertFalse(record)

    def test_select_version_any_number_updates(self):
        # test if select version is running properly
        # update record with primary key 0 however many times and test if every version is correct
        test_values = [0,1,2,3,4]
        self.query.insert(*test_values)
        test_dict = {}
        num_trials = 514
        
        for i in range(1,1+num_trials):
            change_index = (i % 4) + 1

            test_values[change_index] = i
            test_dict[i] = test_values.copy()
            self.query.update(0, *test_values)
            
        for key in test_dict:
            version = key - num_trials
            # print(version)
            record = self.query.select_version(0,0, [1]*5, version)[0]
            self.assertListEqual(test_dict[key], record.columns)

    def test_update_non_existent_record(self):
        value = self.query.update(0, *[None]*5)

        self.assertFalse(value)

    def test_select_version_too_far_back(self):
        # testing if select version were to be given a version that does not exist, will it return base version
        # if there is only 2 versions but asked for 3 versions back, return base
        test_values = [0,1,2,3,4]
        self.query.insert(*test_values)
        
        self.query.update(0, *[None,None,5,6,7])

        record = self.query.select_version(0, 0, [1,1,1,1,1], -3)[0]

        self.assertListEqual(test_values, record.columns)

    def test_update_record_doesnt_exist(self):
        truth = self.query.update(0, [1,2,3,4,5])
        
        self.assertFalse(truth)

    def test_single_delete(self):
        test_values = [0,1,2,3,4]
        self.query.insert(*test_values)
        
        self.query.delete(0)
        record = self.query.select(0, 0, [1,1,1,1,1])
        self.assertFalse(record)

    def test_sum_integers(self):
        n = 514

        for i in range(1,n+1):
            self.query.insert(*[i]*5)

        sum = self.query.sum(1,n+1, 2)

        calc_sum = (n*(n+1)) / 2
        self.assertEqual(sum, calc_sum)

    def test_sum_version_integers(self):
        n = 5

        for i in range(1, n+1):
            self.query.insert(*([i]*5))

        for i in range(1, n+1):
            self.query.update(i, *[i]*5)

        sum = self.query.sum_version(1, n+1, 2, relative_version=-1)

        calc_sum = (n*(n+1)) / 2
        self.assertEqual(sum, calc_sum)

    def test_increment(self):
        test_values = [0,1,2,3,4]
        self.query.insert(*test_values)

        for i in range(1, 5):
            truth = self.query.increment(0, i)
            self.assertTrue(truth)
        
        record = self.query.select(0,0,[1]*5)[0]
        self.assertListEqual([0,2,3,4,5], record.columns)

    def test_mostly_all_functions(self):
        n = 5000
        numbers = [i for i in range(1, n + 1)] * 3
        random.shuffle(numbers)
        instance_dict = {}

        for number in numbers:
            if number not in instance_dict:
                instance_dict[number] = 1

                self.query.insert(*[number]*5)

                record = self.query.select(number, 0, [1]*5)[0]
                self.assertListEqual([number]*5, record.columns)

                break

            elif instance_dict[number] == 1:
                instance_dict[number] = 2

                num_updates = random.randint(1,3)
                num_version = random.randint(-num_updates, 0)

                for i in range(1, num_updates+1):
                    new_record = [number+i]*5
                    new_record[0] = None

                    self.query.update(number, *new_record)

                record = self.query.select_version(number, 0, [1]*5, num_version)[0]

                assert_list = [number + num_version + 3]*5
                assert_list[0] = number

                self.assertListEqual(assert_list, record.columns)

                break
            
            elif instance_dict[number] == 2:
                self.query.delete(number)

                record = self.query.select(number, 0, 0)

                self.assertFalse(record)

                break

    # def test_lazy_maintenance(self):
    #     for i in range(1000):
    #         self.query.insert(*[i]*5)

    #         self.query.update(i, *[None, 2,2,2,2])

    #     column = self.test_table.get_column(0)
    #     self.assertEqual(len(column['Base']), len(column['Tail']))

    def test_update_conflicting_primary_key(self):
        self.query.insert(*[1]*5)
        self.query.insert(*[2]*5)

        value = self.query.update(2, *[1]*5)

        self.assertFalse(value)

    def test_insert_conflicting_primary_key(self):
        self.query.insert(*[1]*5)
        value = self.query.insert(*[1]*5)

        self.assertFalse(value)

    def test_update_non_conflicting_primary_key(self):
        self.query.insert(*[1]*5)
        self.query.insert(*[2]*5)

        passed = self.query.update(2, *[3]*5)

        self.assertTrue(passed)

class TestLstoreIndex(unittest.TestCase):
    """
    THE BEHAVIOR OF THE INDEX

    locate method: column, value -> list of rid's; applies maintinance
    locate range method: low_key, high_key, column -> list of rid's; applies maintenance
    maintain_insert: doesn't apply immediately. Except pk. That needs to go in immediately (because it has to be unique)
    maintain_update: applies maintinance. new pk can't exist
    maintain_delete: applies maintinance. 
    _apply_maintainance: loops through maintenance lists and bulk inserts. Can't do on pk.
    """
    def setUp(self):
        self.database = Database()
        self.table = self.database.create_table("Table", 4, 0)
        self.index = self.table.index
        self.index.automatic_new_indexes = False
        self.table.index.create_index(1, ordered=True)
        self.table.index.create_index(2, ordered=False)
        self.query = Query(self.table)

    def test_insert_duplicate_pk(self):
        self.query.insert(0, 0, 0, 0)
        self.assertFalse(self.query.insert(0, 1, 1, 1))
        
    def test_insert_duplicate_non_pk(self):
        self.query.insert(0, 0, 0, 0)
        self.assertTrue(self.query.insert(1, 0, 0, 0))
        rids1 = self.index.locate(1, 0)
        rids2 = self.index.locate(2, 0)
        self.assertEqual(rids1, [0, 1])
        self.assertEqual(rids1, rids2)

    def test_locate_on_empty_index(self):
        v1 = self.index.locate_range(-1000, 1000, 1)
        v2 = self.index.locate_range(-1000, 1000, 2)
        self.assertEqual(v1, [])
        self.assertEqual(v1, v2)

    def test_basic_locate(self):
        self.assertTrue(self.query.insert(1, 10, 10, 10))
        self.assertTrue(self.query.insert(2, 20, 20, 10))

        v1 = self.index.locate(1, 10)
        v2 = self.index.locate(2, 10)
        
        self.assertEqual(v1, v2)
        self.assertEqual(self.index.locate_range(10, 20, 1), self.index.locate_range(10, 20, 2))

    def test_basic_locate_duplicates(self):
        # TODO: only the primary key needs unique keys. MAKE SURE LAZY INDEXING DOESN'T MESS EVERYTHING UP!!!
        self.query.insert(1, 8, 8, 8)
        self.query.insert(2, 8, 8, 8)
        self.assertFalse(self.query.insert(1, 9, 9, 9))

        v1 = self.index.locate(1, 8)
        v2 = self.index.locate(2, 8)
        
        self.assertEqual(len(v1), 2)  
        self.assertEqual(v1, v2)

    def test_locate_duplicates(self):
        num_items = 1_000
        for i in range(num_items):
            self.query.insert(i, 8, 8, 8)

        len1 = len(self.index.locate(1, 8))
        len2 = len(self.index.locate(2, 8))

        # KNOWN BUG WITH B+TREE!!! THIS TEST FAILS!!!
        self.assertEqual(len1, num_items)
        self.assertEqual(len1, len2)

    def test_updates_pk(self):
        for i in range(5):
            if i != 3:
                self.query.insert(i, i, i, i)

        self.assertTrue(self.query.update(0, *[3, None, None, None]))
        values = self.index.locate_range(0, 7, 0)
        self.assertEqual(values, [1, 2, 0, 3])

    def test_updates_pk_doesnt_exist(self):
        for i in range(5):
            self.query.insert(i, i, i, i)

        self.assertFalse(self.query.update(-1, *[10, None, None, None]))

    def test_updates_new_pk_exists(self):
        for i in range(5):
            self.query.insert(i, i, i, i)

        self.assertFalse(self.query.update(0, *[1, None, None, None]))
        
from random import shuffle
class UltimateLstoreTest(unittest.TestCase):
    """
    This is the ultimate lstore test
    Every query
    Every column
    Every order
    """
    def setUp(self):
        self.database = Database()
        self.table = self.database.creaet_table("Test Table", 0, 3)
        self.query = Query(self.table)

    def test_everything_all_at_once(self):
        queries = []
        for i in range(1_000_000):
            if i < 900_000:
                queries.append((i, -i, 2*i))
            else:
                queries.append((i, i % 31, i % 727))

        queries += queries
        
        for q in queries:
            self.query.insert(*q)

        print(self.query.select())
        
        

    
if __name__ == '__main__':
    unittest.main()

