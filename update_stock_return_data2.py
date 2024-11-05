# -*- coding: utf-8 -*-
"""
Created on Sun Jul  14 17:46:32 2024

@author: WilliamNiu
"""

from WindPy import w
w.start()
import pandas as pd
import datetime, pdb

# old_filename = OLD_FILENMAE
# stocklist_filename = FUNDLIST_FILENMAE
def update_stock_return_data(old_filename, stocklist_filename):
	print("正在读取原始数据...")
	now_string = datetime.datetime.now().strftime("%Y-%m-%d")
# 	r_data = pd.read_excel(old_filename,index_col=0)
	r_data = pd.read_pickle(old_filename)
	print("正在下载股票收益率数据...")
	stock_list_web_A = w.wset("sectorconstituent",f"date={now_string};sectorid=a001010100000000;field=wind_code").Data[0]
	stock_list_web_H = w.wset("sectorconstituent",f"date={now_string};sectorid=a002010100000000;field=wind_code").Data[0]
	stock_list_local = pd.read_pickle("E:\\Data\\股票数据库\\股票清单_更新至2024-07-14.data").index
	stock_list = set(stock_list_web_A) | set(stock_list_local) | set(stock_list_web_H)
	# Data for the last trading day is excluded in case the data for the day of the update is not yet available.
	trade_date_full = w.tdays("2010-01-01", "", "").Data[0][:-1]
	to_add_stock_list = [i for i in stock_list if not i in r_data.columns]
	to_update_stock_list = [i for i in stock_list if i in r_data.columns]
	to_update_date_list = [i for i in trade_date_full if i not in r_data.index]
	# Construct the target structure
	r_data_target = r_data.reindex(index=trade_date_full, columns=set(r_data.columns) | set(stock_list))
	if to_update_date_list:
		update_data_WindData1 = w.wsd(",".join(to_update_stock_list[:4000]), "pct_chg", \
							to_update_date_list[0].strftime("%Y-%m-%d"), \
							to_update_date_list[-1].strftime("%Y-%m-%d"), "PriceAdj=F")
		update_data_df1 = pd.DataFrame(index=to_update_stock_list[:4000], columns=to_update_date_list, \
								  data=update_data_WindData1.Data).T / 100
		update_data_WindData2 = w.wsd(",".join(to_update_stock_list[4000:]), "pct_chg", \
							to_update_date_list[0].strftime("%Y-%m-%d"), \
							to_update_date_list[-1].strftime("%Y-%m-%d"), "PriceAdj=F")
		update_data_df2 = pd.DataFrame(index=to_update_stock_list[4000:], columns=to_update_date_list, \
								  data=update_data_WindData2.Data).T / 100
		update_data_df = pd.concat([update_data_df1,update_data_df2],axis=1)
		r_data_target.loc[to_update_date_list, to_update_stock_list] = update_data_df
#		pdb.set_trace()
# 	if to_add_stock_list:
# 		added_data_WindData = w.wsd(",".join(to_add_stock_list), "pct_chg", \
# 							trade_date_full[0], \
#  							trade_date_full[-1].strftime("%Y-%m-%d"), "PriceAdj=F")
# 		added_data_df = pd.DataFrame(index=to_add_stock_list, columns=trade_date_full, \
#  									  data=added_data_WindData.Data).T / 100
	if to_add_stock_list:
		added_data_df = pd.DataFrame()
		for stock in to_add_stock_list:
			sub_added_data_WindData = w.wsd(stock, "pct_chg", \
							trade_date_full[0], \
							trade_date_full[-1].strftime("%Y-%m-%d"), "PriceAdj=F")
			sub_added_data_df = pd.DataFrame(index=[stock], columns=trade_date_full, \
									  data=sub_added_data_WindData.Data).T / 100
			added_data_df = pd.concat([added_data_df,sub_added_data_df],axis=1)
# 		print(len(added_data_df.columns))
		r_data_target.loc[trade_date_full, to_add_stock_list] = added_data_df
	print("数据下载完毕，正在存储")
	r_data_target = r_data_target.sort_index(axis=1).fillna(0)
	r_data_target.to_excel(f"E:\Data\股票收益率\股票收益率_更新至{now_string}.xlsx")
	r_data_target.to_pickle(f"E:\Data\股票收益率\股票收益率_更新至{now_string}.data")
	print("股票收益率数据更新完毕!")
	return r_data_target
if __name__ == "__main__":
	old_filename = "E:\Data\股票收益率\股票收益率_更新至2024-10-13.data"
	fundlist_filename = "E:\Data\股票数据库\股票清单_更新至2024-07-14.data"
	update_stock_return_data(old_filename, fundlist_filename)