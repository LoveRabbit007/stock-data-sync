import time
from datetime import datetime
from datetime import timedelta
import tuapi.tu_share_api as tu_share_api


def sync_all_daily_data():
    begin = add_date('1992-12-05', 1)
    now_date = datetime.today().date()
    while now_date > datetime.date(begin):
        try:
            str_begin = begin.strftime('%Y-%m-%d')
            tu_share_api.save_daily(str_begin, 'daily')
            print(str_begin)
            begin = add_date(str_begin, 1)
        except Exception:
            print('发生异常:' + str_begin)
            time.sleep(60)
            tu_share_api.save_daily(str_begin, 'daily')


def add_date(date_str, add_count=1):
    date_list = time.strptime(date_str, "%Y-%m-%d")
    y, m, d = date_list[:3]
    delta = timedelta(days=add_count)
    date_result = datetime(y, m, d) + delta
    # date_result = date_result.strftime("%Y-%m-%d")
    return date_result


if __name__ == '__main__':
    sync_all_daily_data()
