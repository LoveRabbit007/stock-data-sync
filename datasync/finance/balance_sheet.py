import time

import tuapi.tu_share_api as tu_share_api
import mongo.mongodb_util as mongodb_util


# 获取所有上市公司资产负债表
def sync_all_stock_balance_sheet():
    df = mongodb_util.find_all_data('stockBasic')
    slice_data = df['_id'].copy()
    # 300771 600371
    for ts_code in slice_data:
        try:
            if mongodb_util.exist_data('balanceSheet', ts_code) != 0:
                continue
            tu_share_api.save_balance_sheet(ts_code, None, None, None, 'balanceSheet')
            print(ts_code)
            time.sleep(3)
        except BaseException:
            print('发生异常:' + ts_code)
            time.sleep(60)
            tu_share_api.save_balance_sheet(ts_code, None, None, None, 'balanceSheet')


if __name__ == '__main__':
    sync_all_stock_balance_sheet()
