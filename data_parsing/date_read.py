"""
plan to parse it:
tables:
ORDERS: {'_id': ObjectId('599cba295b2d8bd743ae875e'),
 'days_since_prior_order': '',
 'eval_set': 'prior',
 'order_dow': '2',
 'order_hour_of_day': '08',
 'order_id': '2539329',
 'order_number': '1',
 'user_id': '1'}

 PRODS: {'_id': ObjectId('59a485dc5b2d8b964cb6c34d'),
 'aisle_id': '61',
 'department_id': '19',
 'product_id': '1',
 'product_name': 'Chocolate Sandwich Cookies'}
TRAIN: {'_id': ObjectId('599c95615b2d8bd743aa7d7a'),
 'add_to_cart_order': '1',
 'order_id': '1',
 'product_id': '49302',
 'reordered': '1'}


1. get all data as input for a particular user. -> get data for the user

for x in products:
    user[x] + = 1
prior
"""

import numpy as np
import gzip
import os
from util import timer
from tqdm import tqdm
import csv
import pickle
import shutil


def build_matrix(db, eval_set, path='/home/jupyter/instacart_2017_05_01/numpy'):
    user_per_file = 10000
    user_coordinates = dict()
    prod_total = db.products.count()
    path = path + '/' + eval_set

    shutil.rmtree(path, ignore_errors=True)
    os.mkdir(path)
    user_total = len(db.orders.distinct(key="user_id", filter={'eval_set': eval_set}))

    for qiter in tqdm(range(1, 1 + user_total // user_per_file)):
        matrix = np.zeros((user_per_file, prod_total))
        begin = (qiter - 1) * user_per_file
        end = min(user_total, qiter * user_per_file)
        cursor = db.orders.find({"eval_set": eval_set})

        for order in cursor:
            if not (int(order['user_id']) < end and int(order['user_id']) >= begin):
                continue
            user_shifted = int(order['user_id']) - (qiter - 1) * user_per_file
            user_coordinates[order['user_id']] = (qiter, user_shifted)
            items = db['order_products__' + eval_set].find({"order_id": order['order_id']})
            for item in items:
                matrix[user_shifted][int(item['product_id']) - 1] += 1
        with open(path + '/coords.dict', 'wb') as f:
            pickle.dump(user_coordinates, f)
        with gzip.GzipFile(path + '/user_prod_{0}_{1}.np.gz'.format(begin, end), 'wb') as f:
            np.save(f, matrix)
        del (matrix)


def open_matrix(eval_set, path='/home/jupyter/instacart_2017_05_01/numpy'):
    path = path + '/' + eval_set
    files = [x for x in os.listdir(path) if 'np.gz' in x]
    for file in tqdm(files):
        with gzip.GzipFile(path + '/' + file, 'rb') as f:
            matrix = np.load(f)
            yield matrix
            del (matrix)


def add_to_lib(file, db):
    full_name = '/home/jupyter/instacart_2017_05_01/' + file
    tablename = file.split('.')[0]
    input_list = list()
    with open(full_name) as f:
        print('open file %s' % file)
        field_names = (f.readline().split('\n')[0]).split(',')
        print(field_names)
        collection = db.prods
        collection.drop()
        collection.create_index(field_names[0], unique=False)
        if 'user_id' in field_names:
            collection.create_index('user_id', unique=False)
        for iterator, entry in enumerate(csv.reader(f)):
            entry_dict = dict()
            if iterator % 10000 == 0:
                print(iterator)
            for x, y in zip(field_names, entry):
                if x in ('product_name', 'eval_set', 'aisle', 'department'):
                    entry_dict[x] = y
                else:
                    entry_dict[x] = int(y)
            collection.insert_one(entry_dict)
            del (entry_dict)

