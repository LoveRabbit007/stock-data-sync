import time

import tuapi.tu_share_api as tu_share_api
import mongo.mongodb_util as mongodb_util


# 获取所有上市公司现金流量表
def sync_all_stock_cash_flow():
    df = mongodb_util.find_all_data('stockBasic')
    slice_data = df['_id'].copy()

    for ts_code in slice_data:
        try:
            if mongodb_util.exist_data('cashFlow', ts_code) != 0:
                continue
            tu_share_api.save_cash_flow(ts_code, None, None, None, 'cashFlow')
            print(ts_code)
            time.sleep(2)
        except BaseException:
            print('发生异常:' + ts_code)
            time.sleep(60)
            tu_share_api.save_cash_flow(ts_code, None, None, None, 'cashFlow')


if __name__ == '__main__':
    sync_all_stock_cash_flow()
