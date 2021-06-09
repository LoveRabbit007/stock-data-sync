import time
from datetime import datetime
from datetime import timedelta
import tuapi.tu_share_api as tu_share_api
import mongo.mongodb_util as mongodb_util


def sync_all_daily_data():
    begin = add_date('1990-12-09', 1)
    now_date = datetime.today().date()
    while now_date > datetime.date(begin):
        try:
            str_begin = begin.strftime('%Y-%m-%d')
            if mongodb_util.exist_daily_data('daily', str_begin) != 0:
                continue
            tu_share_api.save_daily(str_begin, 'daily')
            print(str_begin)
            begin = add_date(str_begin, 1)
            time.sleep(1)
        except Exception:
            print('发生异常:' + str_begin)
            time.sleep(60)
            begin = add_date(str_begin, -2)


def add_date(date_str, add_count=1):
    date_list = time.strptime(date_str, "%Y-%m-%d")
    y, m, d = date_list[:3]
    delta = timedelta(days=add_count)
    date_result = datetime(y, m, d) + delta
    # date_result = date_result.strftime("%Y-%m-%d")
    return date_result


if __name__ == '__main__':
    sync_all_daily_data()
