import tuapi.tu_share_api as tu_share_api
from datetime import datetime


# 获取所有新股发行
def sync_all_new_share():
    now_date = datetime.today().date()
    str_begin = now_date.strftime('%Y-%m-%d').replace('-', '')
    tu_share_api.save_all_new_share('newShare', str_begin)


if __name__ == '__main__':
    sync_all_new_share()
