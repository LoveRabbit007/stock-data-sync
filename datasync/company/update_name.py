import mongo.mongodb_util as mongodb_util


# 更新所有上市公司名称信息
def update_company_name():
    dfs = mongodb_util.find_all_data('stockBasic')
    for _id, name in zip(dfs['_id'], dfs['name']):

        try:
            stock_company = mongodb_util.find_one('stockCompany', _id)
            if len(name) == 0 or name == None:
                continue
            stock_company['name'] = name
            mongodb_util.update_one(stock_company, 'stockCompany')
        except BaseException:
            print('发生异常:' + _id + name)


if __name__ == '__main__':
    update_company_name()
