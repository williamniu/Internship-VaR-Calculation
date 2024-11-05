# -*- coding: utf-8 -*-
"""
Created on Sun Jul  14 12:08:49 2024

@author: WilliamNiu
"""

from WindPy import w
w.start()
import pandas as pd
import numpy as np
import datetime
def update_fund_return_data(old_filename, fundlist_filename):
    print("正在更新基金收益率数据...")
    now_string = "20240930"
    # now_string = datetime.datetime.now().strftime("%Y%m%d")
    r_data = pd.read_pickle(f"{old_filename}")
    stock_list = pd.read_excel(f"{fundlist_filename}",index_col=0).index
    # Data for the last trading day is excluded in case the data for the day of the update is not yet available.
    trade_date_full = w.tdays("2020-01-01", "", "").Data[0][:-1]
    to_add_stock_list = [i for i in stock_list if not i in r_data.columns]
    to_update_stock_list = [i for i in stock_list if i in r_data.columns]
    to_update_date_list = [i for i in trade_date_full if i not in r_data.index]
    # Construct the target structure
    r_data_target = r_data.reindex(index=trade_date_full, columns=set(r_data.columns) | set(stock_list))
    if to_update_date_list:
        update_data_WindData1 = w.wsd(",".join(to_update_stock_list[:4000]), "NAV_adj_return1", \
                            to_update_date_list[0].strftime("%Y-%m-%d"), \
                            to_update_date_list[-1].strftime("%Y-%m-%d"), "")
        update_data_df1 = pd.DataFrame(index=to_update_stock_list[:4000], columns=to_update_date_list, \
                                      data=update_data_WindData1.Data).T / 100
        update_data_WindData2 = w.wsd(",".join(to_update_stock_list[4000:]), "NAV_adj_return1", \
                            to_update_date_list[0].strftime("%Y-%m-%d"), \
                            to_update_date_list[-1].strftime("%Y-%m-%d"), "")
        update_data_df2 = pd.DataFrame(index=to_update_stock_list[4000:], columns=to_update_date_list, \
                                      data=update_data_WindData2.Data).T / 100
        update_data_df = pd.concat([update_data_df1,update_data_df2],axis=1)
        r_data_target.loc[to_update_date_list, to_update_stock_list] = update_data_df
    if to_add_stock_list:
        added_data_WindData = w.wsd(",".join(to_add_stock_list), "NAV_adj_return1", \
                            trade_date_full[0], \
                            trade_date_full[-1].strftime("%Y-%m-%d"), "")
        added_data_df = pd.DataFrame(index=to_add_stock_list, columns=trade_date_full, \
                                      data=added_data_WindData.Data).T / 100
    # if to_add_stock_list:  #166
    #     added_data_df = pd.DataFrame()
    #     for fund in to_add_stock_list:
            
    #         sub_added_data_WindData = w.wsd(fund, "NAV_adj_return1", \
    #                         trade_date_full[0], \
    #                         trade_date_full[-1].strftime("%Y-%m-%d"), "")
    #         sub_added_data_df = pd.DataFrame(index=[fund], columns=trade_date_full, \
    #                                   data=sub_added_data_WindData.Data).T / 100
    #         print(fund)
    #         added_data_df = pd.concat([added_data_df,sub_added_data_df],axis=1)
        r_data_target.loc[trade_date_full, to_add_stock_list] = added_data_df
    r_data_target = r_data_target.sort_index(axis=1).fillna(0)
    r_data_target.to_excel(f"E:/Data/基金收益率/收益率合并数据20100101_{now_string}.xlsx")
    r_data_target.to_pickle(f"E:/Data/基金收益率/收益率合并数据20100101_{now_string}.data")
    print("基金收益率数据更新完毕!")
    return r_data_target

# Updated fund return data
if __name__ == "__main__":
    OLD_FILENMAE = 'E:/Data/基金收益率/收益率合并数据20100101_20240705.data'
    FUNDLIST_FILENMAE =  'E:/Data/基金收益率/基金清单.xlsx'
    update_fund_return_data(OLD_FILENMAE, FUNDLIST_FILENMAE)