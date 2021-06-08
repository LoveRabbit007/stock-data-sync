import time

import tuapi.tu_share_api as tu_share_api
import mongo.mongodb_util as mongodb_util


# 获取所有上市公司现金流量表
def sync_all_stock_cash_flow():
    df = mongodb_util.find_all_data('stockBasic')
    slice_data = df['_id'].copy()

    for ts_code in slice_data:
        tu_share_api.save_cash_flow(ts_code, None, None, None, 'cashFlow')
        time.sleep(2)


if __name__ == '__main__':
    sync_all_stock_cash_flow()
