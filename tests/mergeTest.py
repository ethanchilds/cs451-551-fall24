from lstore.db import Database
from lstore.query import Query
import unittest
import random
import os
import shutil
from config import Config

class TestMerge(unittest.TestCase):

    def setUp(self):
        self.db = Database()
        self.test_table = self.db.create_table('Test', 5, 0, merge_interval=0.5)
        self.query = Query(self.test_table)  

    def tearDown(self):
        self.db = None
        self.test_table = None
        self.query = None
        db_path = './TEMP'
        if (os.path.exists(db_path)):
            shutil.rmtree(db_path, ignore_errors=True)

    def test_one_record_no_update_merge(self):
        self.query.insert(*[1]*5)

        self.test_table.merge()

        for i in range(5):
            value = self.test_table.page_directory.get_column_value(0, i + Config.column_data_offset)
            self.assertEqual(value, 1)

    def test_one_record_one_update_merge(self):
        self.query.insert(*[1]*5)

        self.query.update(1, *[None,2,2,2,2])

        self.test_table.merge()

        record = []
        for i in range(5):
            value = self.test_table.page_directory.get_column_value(0, i + Config.column_data_offset)
            record.append(value)

        self.assertListEqual(record, [1,2,2,2,2])

    def test_one_records_several_update_merge(self):
        self.query.insert(*[0]*5)

        for i in range(1000):
            self.query.update(0, *[None, None, i, i, i])

        self.test_table.merge()

        record = []
        for i in range(5):
            value = self.test_table.page_directory.get_column_value(0, i + Config.column_data_offset)
            record.append(value)

        self.assertListEqual([0,0,999,999,999], record)

    def test_several_records_one_update_merge(self):
        # be aware this may be a faulty test case
        for i in range(514):
            self.query.insert(*[i]*5)

        random_key = random.randint(0,511)
        new_update = [random_key]*5
        new_update[0] = None
        self.query.update(random_key, *new_update)

        self.test_table.merge()

        record = []
        for i in range(5):
            value = self.test_table.page_directory.get_column_value(random_key, i + Config.column_data_offset)
            record.append(value)

        new_update[0] = random_key
        self.assertListEqual(record, new_update)

    def test_several_records_several_updates_back_to_back_merge(self):
        for i in range(513):
            self.query.insert(*[i]*5)

        for i in range(513):
            for j in range(1,3):
                update_record = [(i+1)*j]*5
                update_record[0] = None

                self.query.update(i, *update_record)

        self.test_table.merge()

        for i in range(513):
            test_record = [(i+1)*2]*5
            test_record[0] = i

            record = []
            for j in range(5):
                value = self.test_table.page_directory.get_column_value(i, j + Config.column_data_offset)
                record.append(value)

            self.assertListEqual(test_record, record)

    def test_several_records_several_updates_different_page_merge(self):
        for i in range(513):
            self.query.insert(*[i]*5)

        for i in range(1,3):
            for j in range(513):
                update_record = [(j+1)*i]*5
                update_record[0] = None

                self.query.update(j, *update_record)

        self.test_table.merge()

        for i in range(513):
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
            update_record = [i+1]*5
            update_record[0] = None
            self.query.update(i, *update_record)

        for i in range(1024,1536):
            update_record = [i+1]*5
            update_record[0] = None
            self.query.update(i, *update_record)

        self.test_table.merge()


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

    def test_merge_after_merge(self):
        for i in range(513):
            self.query.insert(*[i]*5)

        for i in range(513):
            updated_record = [i+1]*5
            updated_record[0] = None
            self.query.update(i, *updated_record)

        self.test_table.merge()
        self.test_table.merge()

        for i in range(513):
            record = []
            for j in range(5):
                value = self.test_table.page_directory.get_column_value(i, j + Config.column_data_offset)
                record.append(value)

            test_record = [i+1]*5
            test_record[0] = i

            self.assertListEqual(record, test_record)

    def test_merge_update_loop(self):
        N = 513

        self.query.insert(*[1]*5)

        for i in range(N):
            updated_record = [1+i+2]*5
            updated_record[0] = None
            self.query.update(1, *updated_record)

            self.test_table.merge()

            record = []
            for j in range(5):
                value = self.test_table.page_directory.get_column_value(0, j + Config.column_data_offset)
                record.append(value)

            updated_record[0] = 1
            self.assertListEqual(updated_record, record)

    def test_tps_update(self):
        for i in range(513):
            self.query.insert(*[i]*5)

        for i in range(513):
            updated_record = [i+1]*5
            updated_record[0] = None
            self.query.update(i, *updated_record)

        self.test_table.merge()

        for i in range(513):
            tps = self.test_table.page_directory.get_column_value(i, Config.tps_and_brid_column_idx)
            self.assertEqual(tps, i)


if __name__ == '__main__':
    unittest.main()