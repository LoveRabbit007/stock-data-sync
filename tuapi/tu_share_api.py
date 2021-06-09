import tushare as ts
import time
import mongo.mongodb_util as mongodb_util

pro = ts.pro_api('54e21a9b96874ed98800eb92abb0367e2c9eba29745e77403783db22')


def get_basic(database):
    df = pro.daily(trade_date='20200123')
    mongodb_util.insert_mongo(df, database)


def save_balance_sheet(ts_code=None, ann_date=None, start_date=None, end_date=None, collection_name=None):
    tag = 1
    while tag > 0:
        df = get_balance_sheet(ts_code, ann_date, start_date, end_date, collection_name)
        if len(df) == 100:
            slice_data = df['end_date'].copy()
            end_date = min(slice_data)
            time.sleep(2)
        else:
            tag = 0
        mongodb_util.insert_mongo(df, collection_name)


def get_balance_sheet(ts_code=None, ann_date=None, start_date=None, end_date=None, database=None):
    df = pro.balancesheet(ts_code=ts_code, ann_date=ann_date,
                          start_date=start_date, end_date=end_date,
                          fields='ts_code,	ann_date,	f_ann_date,	end_date,	report_type,	comp_type,	'
                                 'total_share,	cap_rese,	undistr_porfit,	surplus_rese,	special_rese,	'
                                 'money_cap,	trad_asset,	notes_receiv,	accounts_receiv,	oth_receiv,	'
                                 'prepayment,	div_receiv,	int_receiv,	inventories,	amor_exp,	nca_within_1y,	'
                                 'sett_rsrv,	loanto_oth_bank_fi,	premium_receiv,	reinsur_receiv,	'
                                 'reinsur_res_receiv,	pur_resale_fa,	oth_cur_assets,	total_cur_assets,	'
                                 'fa_avail_for_sale,	htm_invest,	lt_eqt_invest,	invest_real_estate,	'
                                 'time_deposits,	oth_assets,	lt_rec,	fix_assets,	cip,	const_materials,	'
                                 'fixed_assets_disp,	produc_bio_assets,	oil_and_gas_assets,	intan_assets,	'
                                 'r_and_d,	goodwill,	lt_amor_exp,	defer_tax_assets,	decr_in_disbur,	'
                                 'oth_nca,	total_nca,	cash_reser_cb,	depos_in_oth_bfi,	prec_metals,	'
                                 'deriv_assets,	rr_reins_une_prem,	rr_reins_outstd_cla,	rr_reins_lins_liab,	'
                                 'rr_reins_lthins_liab,	refund_depos,	ph_pledge_loans,	refund_cap_depos,	'
                                 'indep_acct_assets,	client_depos,	client_prov,	transac_seat_fee,	'
                                 'invest_as_receiv,	total_assets,	lt_borr,	st_borr,	cb_borr,	'
                                 'depos_ib_deposits,	loan_oth_bank,	trading_fl,	notes_payable,	acct_payable,	'
                                 'adv_receipts,	sold_for_repur_fa,	comm_payable,	payroll_payable,	'
                                 'taxes_payable,	int_payable,	div_payable,	oth_payable,	acc_exp,	'
                                 'deferred_inc,	st_bonds_payable,	payable_to_reinsurer,	rsrv_insur_cont,	'
                                 'acting_trading_sec,	acting_uw_sec,	non_cur_liab_due_1y,	oth_cur_liab,	'
                                 'total_cur_liab,	bond_payable,	lt_payable,	specific_payables,	estimated_liab,	'
                                 'defer_tax_liab,	defer_inc_non_cur_liab,	oth_ncl,	total_ncl,	depos_oth_bfi,	'
                                 'deriv_liab,	depos,	agency_bus_liab,	oth_liab,	prem_receiv_adva,	'
                                 'depos_received,	ph_invest,	reser_une_prem,	reser_outstd_claims,	'
                                 'reser_lins_liab,	reser_lthins_liab,	indept_acc_liab,	pledge_borr,	'
                                 'indem_payable,	policy_div_payable,	total_liab,	treasury_share,	'
                                 'ordin_risk_reser,	forex_differ,	invest_loss_unconf,	minority_int,	'
                                 'total_hldr_eqy_exc_min_int,	total_hldr_eqy_inc_min_int,	total_liab_hldr_eqy,	'
                                 'lt_payroll_payable,	oth_comp_income,	oth_eqt_tools,	oth_eqt_tools_p_shr,	'
                                 'lending_funds,	acc_receivable,	st_fin_payable,	payables,hfs_assets,	hfs_sales,'
                                 'update_flag')
    df['_id'] = df['ts_code'] + '-' + df['ann_date'] + '-' + df['end_date'] + '-' + df['update_flag']
    return df


def save_cash_flow(ts_code=None, ann_date=None, start_date=None, end_date=None, collection_name=None):
    tag = 1
    while tag > 0:
        df = get_cash_flow(ts_code, ann_date, start_date, end_date, collection_name)
        if len(df) == 100:
            slice_data = df['end_date'].copy()
            end_date = min(slice_data)
            time.sleep(2)
        else:
            tag = 0
        mongodb_util.insert_mongo(df, collection_name)


def get_cash_flow(ts_code=None, ann_date=None, start_date=None, end_date=None, database=None):
    df = pro.cashflow(ts_code=ts_code, ann_date=ann_date,
                      start_date=start_date, end_date=end_date,
                      fields='amort_intang_assets, ann_date, beg_bal_cash, beg_bal_cash_equ, c_cash_equ_beg_period, '
                             'c_cash_equ_end_period, c_disp_withdrwl_invest, c_fr_oth_operate_a, c_fr_sale_sg, '
                             'c_inf_fr_operate_a, c_paid_for_taxes, c_paid_goods_s, c_paid_invest, '
                             'c_paid_to_for_empl, c_pay_acq_const_fiolta, c_pay_claims_orig_inco, '
                             'c_pay_dist_dpcp_int_exp, c_prepay_amt_borr, c_recp_borrow, c_recp_cap_contrib, '
                             'c_recp_return_invest, comp_type, conv_copbonds_due_within_1y, conv_debt_into_cap, '
                             'decr_def_inc_tax_assets, decr_deferred_exp, decr_inventories, decr_oper_payable, '
                             'depr_fa_coga_dpba, eff_fx_flu_cash, end_bal_cash, end_bal_cash_equ, end_date, '
                             'f_ann_date, fa_fnc_leases, finan_exp, free_cashflow, ifc_cash_incr, im_n_incr_cash_equ, '
                             'im_net_cashflow_oper_act, incl_cash_rec_saims, incl_dvd_profit_paid_sc_ms, '
                             'incr_acc_exp, incr_def_inc_tax_liab, incr_oper_payable, invest_loss, loss_disp_fiolta, '
                             'loss_fv_chg, loss_scr_fa, lt_amort_deferred_exp, n_cap_incr_repur, '
                             'n_cash_flows_fnc_act, n_cashflow_act, n_cashflow_inv_act, n_depos_incr_fi, '
                             'n_disp_subs_oth_biz, n_inc_borr_oth_fi, n_incr_cash_cash_equ, n_incr_clt_loan_adv, '
                             'n_incr_dep_cbob, n_incr_disp_faas, n_incr_disp_tfa, n_incr_insured_dep, '
                             'n_incr_loans_cb, n_incr_loans_oth_bank, n_incr_pledge_loan, n_recp_disp_fiolta, '
                             'n_recp_disp_sobu, n_reinsur_prem, net_profit, oth_cash_pay_oper_act, '
                             'oth_cash_recp_ral_fnc_act, oth_cashpay_ral_fnc_act, oth_pay_ral_inv_act, '
                             'oth_recp_ral_inv_act, others, pay_comm_insur_plcy, pay_handling_chrg, '
                             'prem_fr_orig_contr, proc_issue_bonds, prov_depr_assets, recp_tax_rends, report_type, '
                             'st_cash_out_act, stot_cash_in_fnc_act, stot_cashout_fnc_act, stot_inflows_inv_act, '
                             'stot_out_inv_act, ts_code, uncon_invest_loss,update_flag')
    df['_id'] = df['ts_code'] + '-' + df['ann_date'] + '-' + df['end_date'] + '-' + df['update_flag']
    return df


def save_income(ts_code=None, ann_date=None, start_date=None, end_date=None, collection_name=None):
    tag = 1
    while tag > 0:
        df = get_income(ts_code, ann_date, start_date, end_date)
        if len(df) == 100:
            slice_data = df['end_date'].copy()
            end_date = min(slice_data)
            time.sleep(2)
        else:
            tag = 0
        mongodb_util.insert_mongo(df, collection_name)


def get_income(ts_code=None, ann_date=None, start_date=None, end_date=None):
    df = pro.income(ts_code=ts_code, ann_date=ann_date,
                    start_date=start_date, end_date=end_date, fields='admin_exp,	ann_date,	ass_invest_income,	'
                                                                     'assets_impair_loss,	basic_eps,	'
                                                                     'biz_tax_surchg,	comm_exp,	comm_income,	'
                                                                     'comp_type,	compens_payout,	'
                                                                     'compens_payout_refu,	compr_inc_attr_m_s,	'
                                                                     'compr_inc_attr_p,	diluted_eps,	'
                                                                     'distable_profit,	div_payt,	ebit,	ebitda,	'
                                                                     'end_date,	f_ann_date,	fin_exp,	forex_gain,	'
                                                                     'fv_value_chg_gain,	income_tax,	'
                                                                     'insur_reser_refu,	insurance_exp,	int_exp,	'
                                                                     'int_income,	invest_income,	minority_gain,	'
                                                                     'n_asset_mg_income,	n_commis_income,	'
                                                                     'n_income,	n_income_attr_p,	n_oth_b_income,	'
                                                                     'n_oth_income,	n_sec_tb_income,	'
                                                                     'n_sec_uw_income,	nca_disploss,	'
                                                                     'non_oper_exp,	non_oper_income,	oper_cost,	'
                                                                     'oper_exp,	operate_profit,	oth_b_income,	'
                                                                     'oth_compr_income,	other_bus_cost,	out_prem,	'
                                                                     'prem_earned,	prem_income,	prem_refund,	'
                                                                     'reins_cost_refund,	reins_exp,	reins_income,	'
                                                                     'report_type,	reser_insur_liab,	revenue,	'
                                                                     'sell_exp,	t_compr_income,	total_cogs,	'
                                                                     'total_profit,	total_revenue,	ts_code,	'
                                                                     'undist_profit,	une_prem_reser,update_flag')
    df['_id'] = df['ts_code'] + '-' + df['ann_date'] + '-' + df['end_date'] + '-' + df['update_flag']
    return df


def save_income(ts_code=None, ann_date=None, start_date=None, end_date=None, collection_name=None):
    tag = 1
    while tag > 0:
        df = get_income(ts_code, ann_date, start_date, end_date)
        if len(df) == 100:
            slice_data = df['end_date'].copy()
            end_date = min(slice_data)
            time.sleep(2)
        else:
            tag = 0
        mongodb_util.insert_mongo(df, collection_name)


def get_income(ts_code=None, ann_date=None, start_date=None, end_date=None):
    df = pro.income(ts_code=ts_code, ann_date=ann_date,
                    start_date=start_date, end_date=end_date, fields='admin_exp,	ann_date,	ass_invest_income,	'
                                                                     'assets_impair_loss,	basic_eps,	'
                                                                     'biz_tax_surchg,	comm_exp,	comm_income,	'
                                                                     'comp_type,	compens_payout,	'
                                                                     'compens_payout_refu,	compr_inc_attr_m_s,	'
                                                                     'compr_inc_attr_p,	diluted_eps,	'
                                                                     'distable_profit,	div_payt,	ebit,	ebitda,	'
                                                                     'end_date,	f_ann_date,	fin_exp,	forex_gain,	'
                                                                     'fv_value_chg_gain,	income_tax,	'
                                                                     'insur_reser_refu,	insurance_exp,	int_exp,	'
                                                                     'int_income,	invest_income,	minority_gain,	'
                                                                     'n_asset_mg_income,	n_commis_income,	'
                                                                     'n_income,	n_income_attr_p,	n_oth_b_income,	'
                                                                     'n_oth_income,	n_sec_tb_income,	'
                                                                     'n_sec_uw_income,	nca_disploss,	'
                                                                     'non_oper_exp,	non_oper_income,	oper_cost,	'
                                                                     'oper_exp,	operate_profit,	oth_b_income,	'
                                                                     'oth_compr_income,	other_bus_cost,	out_prem,	'
                                                                     'prem_earned,	prem_income,	prem_refund,	'
                                                                     'reins_cost_refund,	reins_exp,	reins_income,	'
                                                                     'report_type,	reser_insur_liab,	revenue,	'
                                                                     'sell_exp,	t_compr_income,	total_cogs,	'
                                                                     'total_profit,	total_revenue,	ts_code,	'
                                                                     'undist_profit,	une_prem_reser,update_flag')
    df['_id'] = df['ts_code'] + '-' + df['ann_date'] + '-' + df['end_date'] + '-' + df['update_flag']
    return df


def save_stock_dividend(ts_code=None, ann_date=None, collection_name=None):
    tag = 1
    while tag > 0:
        df = get_stock_dividend(ts_code, ann_date)
        if len(df) == 100:
            slice_data = df['end_date'].copy()
            ann_date = min(slice_data)
            time.sleep(2)
        else:
            tag = 0
    mongodb_util.insert_mongo(df, collection_name)


def get_stock_dividend(ts_code=None, ann_date=None):
    df = pro.dividend(ts_code=ts_code, ann_date=ann_date)
    df['_id'] = df['ts_code'] + '-' + df['end_date'] + '-' + df['div_proc']
    return df


def save_daily(trade_date=None, collection_name=None):
    trade_date = trade_date.replace('-', '')
    df = pro.daily(trade_date=trade_date)
    df['_id'] = df['ts_code'] + '-' + df['trade_date']
    mongodb_util.insert_mongo(df, collection_name)
    print(len(df))
    return df


def save_all_stocks(collection_name=None):
    df = pro.stock_basic(
        fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,'
               'list_date,delist_date,is_hs')
    df['_id'] = df['ts_code']
    mongodb_util.insert_mongo(df, collection_name)


def save_all_stocks_company(collection_name=None):
    df = pro.stock_company(
        fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,'
               'introduction,website,email,'
               'office,employees,main_business,business_scope')
    df['_id'] = df['ts_code']
    mongodb_util.insert_mongo(df, collection_name)


def save_all_new_share(collection_name=None, start_date=None):
    df = pro.new_share(start_date=start_date,
                       fields='ts_code,sub_code,name,ipo_date,issue_date,'
                              'amount,amount,price,pe,limit_amount,funds,ballot'
                       )
    df['_id'] = df['ts_code']
    mongodb_util.insert_mongo(df, collection_name)


# 您每天最多访问该接口5次
def save_pro_bar():
    df = ts.pro_bar(ts_code='300085.SZ', freq='1MIN', adj='qfq', asset='E', start_date='20180816', end_date='20180817')
    print(df)
