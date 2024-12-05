from lstore.db import Database
from lstore.query import Query
import unittest
import random
import time
import os
import shutil
from config import Config

class TestMergeThread(unittest.TestCase):

    def setUp(self):
        self.db = Database()
        self.test_table = self.db.create_table('Test', 5, 0, False, merge_interval=0.5)
        self.query = Query(self.test_table)  

    def tearDown(self):
        self.db = None
        self.test_table = None
        self.query = None 

        db_path = './TEMP'
        if (os.path.exists(db_path)):
            shutil.rmtree(db_path, ignore_errors=True)

    def test_merge_page_thread(self):
        self.query.insert(*[0]*5)

        for i in range(513):
            updated_record = [i+1]*5
            updated_record[0] = None
            self.query.update(0, *updated_record)

        time.sleep(self.test_table.interval)

        updated_record = [0, 512, 512, 512, 512]
        
        record = []
        for i in range(5):
            value = self.test_table.page_directory.get_column_value(0, i + Config.column_data_offset)
            record.append(value)

        self.assertListEqual(record, updated_record)

    def test_one_record_no_updates(self):
        self.query.insert(*[0]*5)

        time.sleep(self.test_table.interval)

        record = []
        for i in range(5):
            value = self.test_table.page_directory.get_column_value(0, i + Config.column_data_offset)
            record.append(value)

        self.assertListEqual(record, [0]*5)

    def test_one_record_many_pages_update_merge(self):
        self.query.insert(*[0]*5)

        for i in range(1025):
            updated_record = [i+1]*5
            updated_record[0] = None
            self.query.update(0, *updated_record)

        time.sleep(self.test_table.interval)

        updated_record = [0, 512, 512, 512, 512]
        
        record = []
        for i in range(5):
            value = self.test_table.page_directory.get_column_value(0, i + Config.column_data_offset)
            record.append(value)

        self.assertListEqual(record, updated_record)

        time.sleep(self.test_table.interval)

        updated_record = [0, 1024, 1024, 1024, 1024]
        
        record = []
        for i in range(5):
            value = self.test_table.page_directory.get_column_value(0, i + Config.column_data_offset)
            record.append(value)

        self.assertListEqual(record, updated_record)

    def test_several_records_one_update_merge(self):
        for i in range(514):
            self.query.insert(*[i]*5)

        random_key = random.randint(0,511)
        new_update = [random_key]*5
        new_update[0] = None
        self.query.update(random_key, *new_update)

        time.sleep(self.test_table.interval)

        for i in range(514):
            record = []
            for j in range(5):
                value = self.test_table.page_directory.get_column_value(i, j + Config.column_data_offset)
                record.append(value)

            if i == random_key:
                new_update[0] = random_key
                self.assertListEqual(record, new_update)
            else:
                self.assertListEqual(record, [i]*5)

    def test_several_records_several_updates_back_to_back_merge(self):
        for i in range(513):
            self.query.insert(*[i]*5)

        for i in range(513):
            for j in range(1,3):
                update_record = [(i+1)*j]*5
                update_record[0] = None

                self.query.update(i, *update_record)

        time.sleep(2*self.test_table.interval)

        for i in range(512):
            test_record = [(i+1)*2]*5
            test_record[0] = i

            record = []
            for j in range(5):
                value = self.test_table.page_directory.get_column_value(i, j + Config.column_data_offset)
                record.append(value)

            self.assertListEqual(test_record, record)

    def test_several_records_several_updates_different_page_merge(self):
        for i in range(512):
            self.query.insert(*[i]*5)

        for i in range(1,3):
            for j in range(512):
                update_record = [(j+1)*i]*5
                update_record[0] = None

                self.query.update(j, *update_record)

        self.query.update(0, *[0]*5)

        time.sleep(self.test_table.interval)

        for i in range(512):
            test_record = [(i+1)]*5
            test_record[0] = i

            record = []
            for j in range(5):
                value = self.test_table.page_directory.get_column_value(i, j + Config.column_data_offset)
                record.append(value)

            self.assertListEqual(test_record, record)

        time.sleep(self.test_table.interval)

        for i in range(512):
            test_record = [(i+1)*2]*5
            test_record[0] = i

            record = []
            for j in range(5):
                value = self.test_table.page_directory.get_column_value(i, j + Config.column_data_offset)
                record.append(value)

            self.assertListEqual(test_record, record)

    def test_update_not_back_to_back_base_pages_merge(self):
        for i in range(1536):
            self.query.insert(*[i]*5)

        for i in range(512):
            updated_record = [i+1]*5
            updated_record[0] = None
            self.query.update(i, *updated_record)

        for i in range(1024, 1536):
            updated_record = [i+1]*5
            updated_record[0] = None
            self.query.update(i, *updated_record)

        time.sleep(2*self.test_table.interval)
        for i in range(1536):
            updated = None

            if i >= 1024 or i < 512:
                updated = True
            else:
                updated = False

            record = []
            for j in range(5):
                value = self.test_table.page_directory.get_column_value(i, j + Config.column_data_offset)
                record.append(value)

            if updated:
                test_record = [i+1]*5
                test_record[0] = i
                self.assertListEqual(record, test_record)
            elif not updated:
                test_record = [i]*5
                self.assertListEqual(record, test_record)

    def test_tps_update(self):
        for i in range(512):
            self.query.insert(*[i]*5)
    
        for i in range(512):
            updated_record = [i+1]*5
            updated_record[0] = None
            self.query.update(i, *updated_record)
        
        self.query.update(0, *[0]*5)

        time.sleep(self.test_table.interval)

        for i in range(512):
            tps = self.test_table.page_directory.get_column_value(i, Config.tps_and_brid_column_idx)
            self.assertEqual(tps, i)

    def test_wait_with_no_full_page_occuring(self):
        self.query.insert(*[0]*5)

        for i in range(10):
            updated_record = [i+1]*5
            updated_record[0] = None
            self.query.update(i, *updated_record)

        time.sleep(5*self.test_table.interval)

        record = []
        for j in range(5):
            value = self.test_table.page_directory.get_column_value(0, j + Config.column_data_offset)
            record.append(value)
        
        self.assertListEqual(record, [0]*5)

if __name__ == '__main__':
    unittest.main()