import pandas as pd
from pymongo import MongoClient

client = MongoClient(host='localhost',
                     port=27017)
# 指定数据库
db = client['stock']


def find_all_data(coll_name):
    collection = db[coll_name]
    rows = collection.find({})
    df = pd.DataFrame([basic for basic in rows])
    return df


def insert_mongo(df, database):
    collection = db[database]
    # 格式转换
    collection.insert_many(df.to_dict('records'))


def get_basic_by_ts_code(ts_code=None, trade_date=None, database=None):
    collection = db[database]
    result = collection.find({'ts_code': ts_code, 'trade_date': trade_date})
    result = [doc for doc in result]
    print(result)
