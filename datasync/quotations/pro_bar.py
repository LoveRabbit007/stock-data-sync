import time

import tuapi.tu_share_api as tu_share_api
import mongo.mongodb_util as mongodb_util


def sync_pro_bar():
    tu_share_api.save_pro_bar()


if __name__ == '__main__':
    # tushareapi.get_balancesheet('300085.SZ', None, None, None, 'balancesheet')
    sync_pro_bar()
