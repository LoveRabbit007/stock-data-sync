import time

import tuapi.tu_share_api as tu_share_api
import mongo.mongodb_util as mongodb_util


# 获取所有上市公司资产负债表
def sync_all_stock_balancesheet():
    df = mongodb_util.find_all_data('stockBasic')
    slice_data = df['_id'].copy()

    for ts_code in slice_data:
        tu_share_api.save_balance_sheet(ts_code, None, None, None, 'balanceSheet')
        time.sleep(2)


if __name__ == '__main__':
    # tushareapi.get_balancesheet('300085.SZ', None, None, None, 'balancesheet')
    sync_all_stock_balancesheet()
