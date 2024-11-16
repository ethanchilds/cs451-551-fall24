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

        value = self.query.update(2, *[3]*5)

        self.assertTrue(value)
    
if __name__ == '__main__':
    unittest.main()

