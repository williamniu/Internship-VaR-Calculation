# -*- coding: utf-8 -*-
"""
Created on Sun Jul  14 15:05:36 2024

@author: WilliamNiu
"""

#VAR_GROUP_LIST=["境内权益-基金","境内权益-股票","境外权益","境外权益-港股通","债券型基金"]

from WindPy import w
w.start()
import pandas as pd
import sys
sys.path.append("E:\\NZH\\VAR计算")
sys.path.append("E:\\Data\\股票收益率")
from update_stock_return_data2 import update_stock_return_data

def standlized_stock_code(code):
 	if len(code) == 5:
         return code[1:] + ".HK"
 	elif len(code) == 6:
         if code[0] == "6":
             return code + ".SH"
         else:
             return code + ".SZ"
def standlized_stock_code(code):
	if len(code) == 5:
 	 	return code[1:] + ".HK"
	elif len(code) == 6:
		if (code[:2] == "82") or(code[:2] == "83") or(code[:2] == "87")or(code[:2] == "88")or(code[:2] == "43") :
 			return code + ".BJ"
		elif code[0] == "6":
 			return code + ".SH"
		else:
 			return code + ".SZ"		
return_data_filename = "E:\\Data\\股票收益率\\股票收益率_更新至2024-10-13.data"

#df = pd.read_excel('L:/后台业务数据/151_人寿投资管理部业务数据/DataWarehouse/股票收益率/股票收益率_更新至2023-01-16.xlsx',index_col=0)
# df.to_pickle("L:/后台业务数据/151_人寿投资管理部业务数据/DataWarehouse/股票收益率/股票收益率_更新至2023-01-16.data")
#弘实基金的资产内码/市场代码目前需要手动修改
whole_data = pd.read_excel("./20240930_ALM分类_Python输出.xlsx",index_col=0)
#L:\部门文件\人寿投资管理部\国内投管人选聘\07-净值数据
# r_data = pd.read_excel(f"L:/部门文件/人寿投资管理部/组合管理与量化/DataWarehouse/股票收益率/{return_data_filename}",index_col=0)
r_data = pd.read_pickle(f"{return_data_filename}")

start_time = "20210930"
end_time = "20240930"
trade_date = w.tdays(start_time, end_time, "").Data[0]
stock_holding_data = whole_data.loc[whole_data["VaR分类"].isin(["境内权益-股票", "境外权益-港股通"]), :].copy()
stock_holding_data.loc[:, "万得代码"] = stock_holding_data.loc[:, "市场资产代码"].apply(lambda x: standlized_stock_code(x))
stock_norepeat_set = set(stock_holding_data["万得代码"])
stock_information_df = pd.read_pickle(r"E:\Data\股票数据库\股票清单_更新至2024-10-13.data")

def add_stock_information(stock_information_df, stock_list):
#	stock_information_df = pd.read_excel("L:/部门文件/人寿投资管理部/组合管理与量化/DataWarehouse/股票收益率/股票清单.xlsx", index_col=0)
 	stock_list_added = [i for i in stock_list if not i in stock_information_df.index]
 	if stock_list_added:
         w_data = w.wss(",".join(stock_list_added), "sec_name,ipo_date", "industryType=1")
         w_data_df = pd.DataFrame(index=["股票名称", "上市日期"], columns=w_data.Codes, data=w_data.Data).T
 	else:
         w_data_df = pd.DataFrame()
 	concated_df = pd.concat([stock_information_df, w_data_df], verify_integrity=True, sort=False)
 	w_data_SHSC = w.wss(",".join(concated_df.index), "SHSC,SHSC2,industry_sw_2021", f"tradeDate={end_time};industryType=1")
 	w_data_df_SHSC = pd.DataFrame(index=["是否沪港通", "是否深港通", "补充分类"], columns=w_data_SHSC.Codes, data=w_data_SHSC.Data).T
 	concated_df.loc[:, "上市日期"] = pd.to_datetime(stock_information_df["上市日期"])
 	concated_df.loc[:, "上市日期"] = concated_df.loc[:, "上市日期"].apply(lambda x: max(x, pd.Timestamp("19000110")))
 	concated_df.loc[:, ["是否沪港通", "是否深港通", "补充分类"]] = w_data_df_SHSC.loc[:, ["是否沪港通", "是否深港通", "补充分类"]]
 	concated_df.to_pickle(r"E:\Data\股票数据库\股票清单_更新至2024-10-13.data")
 	print("股票基本信息增加完毕!")

# Determine if all stock return series are in the database
if not (all([i in r_data.columns for i in stock_norepeat_set]) and all([i in r_data.columns for i in stock_information_df.index])):
# if not (all([i in r_data.columns for i in stock_norepeat_set]) and all([i in r_data.columns for i in stock_information_df.columns])):
 	print("部分股票收益率序列未下载，正在添加股票基本信息...")
 	to_add_stock_list = [i for i in stock_norepeat_set if not i in r_data.columns]
 	if to_add_stock_list:
         add_stock_information(stock_information_df, to_add_stock_list)
 	to_add_stock_list = [i for i in stock_norepeat_set if not i in stock_information_df.columns]
 	if to_add_stock_list:
         add_stock_information(stock_information_df, to_add_stock_list)
 	OLD_FILENMAE = f"{return_data_filename}"
 	FUNDLIST_FILENMAE = (r"E:\Data\股票数据库\股票清单_更新至2024-10-13.data")
 	r_data = update_stock_return_data(OLD_FILENMAE, FUNDLIST_FILENMAE)

ashare_list = set(stock_holding_data.loc[stock_holding_data["VaR分类"]=="境内权益-股票", "万得代码"])
SHSC_list = set(stock_holding_data.loc[stock_holding_data["VaR分类"]=="境外权益-港股通", "资产内码"])
mapping_dict = dict(zip(stock_holding_data['资产内码'], stock_holding_data['万得代码']))
mapping_dict_re = dict(zip(stock_holding_data['万得代码'], stock_holding_data['资产内码']))
# Drawdown of bond fund rate of return
r_data_subset_bf = r_data.reindex(index=trade_date, columns=ashare_list)
r_data_subset_bf.columns = map(lambda x: mapping_dict_re[x], r_data_subset_bf.columns)
r_data_subset_bf.to_excel("./【4】境内权益资产-A股-收益率序列.xlsx")
# Drawdown of equity fund rate of return
r_data_subset_ef = r_data.reindex(index=trade_date, columns=SHSC_list)
for f_stock_code in SHSC_list:
    r_data_subset_ef.loc[:, f_stock_code] = r_data.loc[:, mapping_dict[f_stock_code]]
r_data_subset_ef.to_excel("./【4】境外权益资产-港股通-收益率序列.xlsx")

whole_data.loc[whole_data["VaR分类"].isin(["境外权益-QDII",'境外债券型基金']), "资产内码"].drop_duplicates().to_excel("【4】需要去彭博导数据的境外资产.xlsx")



