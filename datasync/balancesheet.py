import time

import tuapi.tushareapi as  tushareapi
import mongo.mongodbutil as mongodbutil


# 获取所有上市公司资产负债表
def sync_all_stock_balancesheet():
    df = mongodbutil.find_all_data('stockBasic')
    slice_data = df['_id'].copy()

    for ts_code in slice_data:
        tushareapi.get_balancesheet(ts_code, None, None, None, 'balancesheet')
        time.sleep(0.5)


if __name__ == '__main__':
    # tushareapi.get_balancesheet('300085.SZ', None, None, None, 'balancesheet')
    sync_all_stock_balancesheet()
