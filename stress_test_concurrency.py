from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from collections import defaultdict
import sys

from random import choice, randint, sample, seed

db = Database()
db.open('./CS451')

# Getting the existing Grades table
grades_table = db.get_table('Grades')

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory

number_of_records = 1000
number_of_transactions = 10
number_of_queries = 100000
num_threads = 8
current_seed = 3562901

if len(sys.argv) >= 2:
    number_of_records = int(sys.argv[1])
    number_of_transactions = int(sys.argv[2])
    number_of_queries = int(sys.argv[3])
    num_threads = int(sys.argv[4])
    current_seed = int(sys.argv[5])

seed(current_seed)
    
# print(number_of_records, number_of_transactions, number_of_queries, num_threads)

keys = []
records = {}

# re-generate records for testing
for i in range(0, number_of_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [[key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]]

transaction_workers = []
transactions = []

for i in range(number_of_transactions):
    transactions.append(Transaction())

for i in range(num_threads):
    transaction_workers.append(TransactionWorker())

# print(records)
# x update on every column

query_stats = defaultdict(int)
for j in range(number_of_queries):
    queries = ["update", "select", "insert", 'delete']
    selected_query = choice(queries)
    selected_transaction = randint(0, number_of_transactions-1)
    
    if selected_query == "update":
        if not records:
            continue
        query_stats[selected_query] += 1
        key = choice(list(records.keys()))
        updated_columns = [None, None, None, None, None]    
        new_values = records[key][-1].copy()
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            new_values[i] = value
            updated_columns[i] = value
        records[key].append(new_values)
        transactions[selected_transaction].add_query(query.update, grades_table, key, *updated_columns)
    
    elif selected_query == "select":
        if not records:
            continue
        query_stats[selected_query] += 1
        key = choice(list(records.keys()))
        transactions[selected_transaction].add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
        
    elif selected_query == "delete":
        if not records:
            continue
        query_stats[selected_query] += 1
        key = choice(list(records.keys()))
        del records[key]
        transactions[selected_transaction].add_query(query.delete, grades_table, key)
        
    elif selected_query == "insert":
        query_stats[selected_query] += 1
        number_of_records += 1
        key = 92106429 + number_of_records - 1
        keys.append(key)
        records[key] = [[key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]]
        
        transactions[selected_transaction].add_query(query.insert, grades_table, *records[key][0])
        

print("Queries distributed")


# add trasactions to transaction workers  
for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(transactions[i])



# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()

print("Queries succceeded")
# for j in range(number_of_operations_per_record + 1):
#     score = len(keys)
#     for key in keys:
#         correct = records[key][-j - 1]
#         query = Query(grades_table)
        
        
#         result = query.select_version(key, 0, [1, 1, 1, 1, 1], -j)[0].columns
#         if correct != result:
#             print('select error on primary key', key, ':', result, ', correct:', correct)
#             score -= 1
#     print(f'Version {-j} Score:', score, '/', len(keys))

db.close()
