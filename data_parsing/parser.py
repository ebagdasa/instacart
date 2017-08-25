import json

import requests
from pymongo import MongoClient
import json
import datetime
import numpy as np
import time


# '''
# Type:
#
# {'_id': ObjectId('599cc2815b2d8bd743e37dae'),
#  'aisle_id': '47',
#  'department_id': '11',
#  'link': 'https://www.enfamil.com/products/enfamil-tri-vi-sol',
#  'product_id': '24',
#  'product_name': 'Tri-Vi-Sol Vitamins A-C-and D Supplement Drops for Infants',
#  'title': 'Enfamil Tri-Vi-Sol: Tri Vitamin Drops | Enfamil US'}
# '''


with open('/home/jupyter/dev/instacart/secret.json') as f:
    res = json.load(f)
    keys = res['keys']
    proxies = res['proxies']



client = MongoClient()
db = client.insta

def get_ndbno(name, key, proxies):
    res = requests.get('http://api.nal.usda.gov/ndb/search',
                       params={"format":"json","q":name,"max":"25","offset":"0", 'api_key': key }, proxies=proxies)
    if not res.content:
        return {1: 0}
    # print(res.content)
    try:
        js = json.loads(res.content.decode('utf-8'))
    except json.decoder.JSONDecodeError:
        print('Exception: requested: {0} {1} {2}'.format(name, key, proxies))
        # raise ValueError('stop')
        return {1:0}
    return js
    # return {1:0}

def get_descr(ndbno, key, proxies):
    res = requests.get('https://api.nal.usda.gov/ndb/reports/',
                       params={"format":"json",'ndbno': ndbno, 'type': 'f',  'api_key': key }, proxies=proxies)

    if not res.content:
        return {1: 0}
    # print(res.content)
    return json.loads(res.content.decode('utf-8'))


def save_mongo(coll, res):
    coll.update_one(filter={'_id': res['_id']}, update={'$set': res}, upsert=False)

def get_proxies(pr):
    if pr=='localhost':
        return None
    return {'http': pr, 'https': pr}

# def get_next_item(db):
#     prods = db.products.find()
#     for item in prods:
#         item['product_name'] =  item['product_name'].encode('ascii','ignore').decode('ascii')
#         get_ndbno(item['product_name'])
#
#
# prods = db.products.find()
# ndbno = list()
# for item in prods[1:10]:
#     if item.get('ndbno', False):
#         continue
#     item['product_name'] = item['product_name'].encode('ascii', 'ignore').decode('ascii')
#     res = get_ndbno(item['product_name']).get('list', dict()).get('item', [{1: 0}])[0].get('ndbno', False)
#     if res:
#         item['ndbno'] = res
#         report = get_descr(res)['report']['food']
#         item['report'] = report
#
#     else:
#         item['ndbno'] = "NAN"
#     db.products.update_one(filter={'_id': item['_id']}, update={'$set': item}, upsert=False)


def parse(keys, proxies):
    count_per_hour = [873, 874]
    total_count = 0
    neg_count = 0
    pos_count = 0
    skipped = 0
    cursor = db.products.find()
    for item in cursor:
        print('Total: {0}. Positive: {4} Negative: {1}. Skipped: {2}. Hour limit: {3}.'.format(total_count, neg_count,
                                                                                             skipped, count_per_hour, pos_count))
        total_count += 1

        if item.get('ndbno', False):
            if item['ndbno'] == 'NAN':
                neg_count += 1
            else:
                pos_count += 1
            skipped += 1
            # print('skipping: {0}. no: {1}'.format(item['product_id'], skipped))
            continue


        if np.sum(count_per_hour)<=2:
            print('waiting for new hour')
            time.sleep((60 - datetime.datetime.now().minute)*60) # wait for new hour
            count_per_hour = [999,999]

        cur_id = int(np.argmax(count_per_hour))
        key = keys[cur_id]
        proxy = get_proxies(proxies[cur_id])


        item['product_name'] = item['product_name'].encode('ascii', 'ignore').decode('ascii')
        res = get_ndbno(item['product_name'], key=key, proxies=proxy)
        count_per_hour[cur_id] -= 1

        #check if remains
        if count_per_hour[cur_id] == 0:
            cur_id = int(np.argmax(count_per_hour))
            key = keys[cur_id]
            proxy = get_proxies([cur_id])


        res_parsed = res.get('list', dict()).get('item', [{1: 0}])[0].get('ndbno', False)
        if res_parsed:
            item['ndbno'] = res_parsed
            report = get_descr(res_parsed, key=key, proxies=proxy)['report']['food']
            count_per_hour[cur_id] -= 1
            pos_count+= 1
            item['report'] = report

        elif res.get('errors', False):
            item['ndbno'] = "NAN"
            neg_count += 1
        else:
            print(res)
            raise ValueError('probably a wrong data')


        db.products.update_one(filter={'_id': item['_id']}, update={'$set': item}, upsert=False)
        # time.sleep(1)



parse(keys, proxies)

