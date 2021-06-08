import time

import tuapi.tu_share_api as tu_share_api
import mongo.mongodb_util as mongodb_util


# 获取所有上市公司利润表
def sync_all_stock_save_income():
    df = mongodb_util.find_all_data('stockBasic')
    slice_data = df['_id'].copy()

    for ts_code in slice_data:
        try:
            # if mongodb_util.exist_data('income', ts_code) != 0:
            #     continue
            tu_share_api.save_income(ts_code, None, None, None, 'income')
            print(ts_code)
            time.sleep(2)
        except BaseException:
            print('发生异常:' + ts_code)
            time.sleep(60)
            tu_share_api.save_income(ts_code, None, None, None, 'income')


if __name__ == '__main__':
    sync_all_stock_save_income()
