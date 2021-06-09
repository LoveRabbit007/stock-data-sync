import tuapi.tu_share_api as tu_share_api
import mongo.mongodb_util as mongodb_util
import time


def sync_all_stock_dividend():
    df = mongodb_util.find_all_data('stockBasic')
    slice_data = df['_id'].copy()
    for ts_code in slice_data:
        # try:
            if mongodb_util.exist_data('dividend', ts_code) != 0:
                continue
            tu_share_api.save_stock_dividend(ts_code, None, 'dividend')
            print(ts_code)
            time.sleep(2)
        # except BaseException:
        #     print('发生异常:' + ts_code)
        #     time.sleep(60)
        #     tu_share_api.save_stock_dividend(ts_code, None, None, None, 'dividend')


if __name__ == '__main__':
    sync_all_stock_dividend()
