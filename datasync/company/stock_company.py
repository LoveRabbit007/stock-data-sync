import tuapi.tu_share_api as tu_share_api


# 获取所有上市公司基础信息
def sync_all_basic_stocks():
    tu_share_api.save_all_stocks_company('stockCompany')


if __name__ == '__main__':
    sync_all_basic_stocks()
