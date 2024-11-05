# -*- coding: utf-8 -*-
"""
Created on Sun Jul  14 11:44:22 2024

@author: WilliamNiu
"""

#VAR_GROUP_LIST=["境内权益-基金","境内权益-股票","境外权益","境外权益-港股通","债券型基金"]

from WindPy import w
w.start()
import pandas as pd
import sys
import numpy as np
sys.path.append("E:\\NZH\\VaR计算")
sys.path.append("E:\\Data\\基金收益率")
from netvalue_download3 import update_fund_return_data


#Hong Kong Fund's intra-asset code/market code currently needs to be changed manually
whole_data = pd.read_excel("E:\\NZH\\VAR计算\\20240930_ALM分类_Python输出.xlsx",index_col=0)
OLD_FILENMAE = "E:\\Data\\基金收益率\\收益率合并数据20100101_20240705.data"
FUNDLIST_FILENMAE = "E:\\Data\\基金收益率\\基金清单.xlsx"
r_data = update_fund_return_data(OLD_FILENMAE, FUNDLIST_FILENMAE)
#L:\部门文件\人寿投资管理部\国内投管人选聘\07-净值数据
# r_data = pd.read_excel("E:\\Data\\基金收益率\\收益率合并数据20100101_20240705.xlsx",index_col=0)
# r_data = pd.read_pickle(f"L:/部门文件/人寿投资管理部/国内投管人选聘/07-净值数据/{return_data_filename}")

start_time = "2021-09-30"
end_time = "2024-09-30"
trade_date = w.tdays(start_time, end_time, "").Data[0]
fund_holding_data = whole_data.loc[whole_data["VaR分类"].isin(["境内权益-基金", "境内债券型基金", "境外权益-港股通基金"]), :].copy()
fund_holding_data.loc[:, "万得代码"] = fund_holding_data.loc[:, "市场资产代码"]+".OF"
fund_norepeat_set = set(fund_holding_data["万得代码"])

def add_fund_information(fund_list):
 	fund_information_df = pd.read_excel("E:\\Data\\基金收益率\\基金清单.xlsx", index_col=0)
 	fund_list_added = [i for i in fund_list if not i in fund_information_df.index]
 	if fund_list_added:
          w_data = w.wss(",".join(fund_list_added), "sec_name,fund_setupdate")
          w_data_df = pd.DataFrame(index=["基金简称", "基金成立日"], columns=w_data.Codes, data=w_data.Data).T
          concated_df = pd.concat([fund_information_df, w_data_df], verify_integrity=True)
          concated_df.to_excel('E:\\Data\\基金收益率\\基金清单.xlsx', index_label='基金代码')
 	print("基金基本信息增加完毕!")
# Determine if all of the fund's yield series are in the database.
if not all([i in r_data.columns for i in fund_norepeat_set]):
 	print("部分基金收益率序列未下载，正在添加基金基本信息...")
 	to_add_fund_list = [i for i in fund_norepeat_set if not i in r_data.columns]
 	add_fund_information(to_add_fund_list)



# df2=pd.read_excel("E:\\Data\\基金收益率\基金清单.xlsx")
# r_data['Unnamed: 0'] = pd.to_datetime(r_data['Unnamed: 0'])
# r_data.set_index('Unnamed: 0', inplace=True)

# df2['Unnamed: 0'] = pd.to_datetime(df2['Unnamed: 0'])
# df2.set_index('Unnamed: 0', inplace=True)

# new_columns = [col for col in df2.columns if col not in r_data.columns]

# # 仅选取 df2 中的新的列
# df2_new_columns = df2[new_columns]

# # 合并数据，添加新的列到 r_data
# r_data = pd.concat([r_data, df2_new_columns], axis=1)

# r_data = r_data[~r_data.index.duplicated(keep='first')]
# df2 = df2[~df2.index.duplicated(keep='first')]
output_path = r"./【2】基金-收益率序列.xlsx"
mapping_dict = dict(zip(fund_holding_data['资产内码'], fund_holding_data['万得代码']))
with pd.ExcelWriter(output_path) as writer:
    for fund_type in ["境内权益-基金", "境内债券型基金", "境外权益-港股通基金"]:
        sub_fund_holding_data = fund_holding_data.loc[fund_holding_data["VaR分类"].isin([fund_type]), :]
        sub_mapping_dict = dict(zip(sub_fund_holding_data['资产内码'], sub_fund_holding_data['万得代码']))
        sub_r_data_subset_ef = r_data.reindex(index=trade_date, columns=sub_mapping_dict.keys())
        
        # Filled with data
        for inner_code, wind_code in sub_mapping_dict.items():
            if wind_code in r_data.columns:
                sub_r_data_subset_ef[inner_code] = r_data[wind_code]
            else:
                print(f"Warning: {wind_code} not found in the columns of `r_data`")

        
        # Export data to a different Excel sheet
        sub_r_data_subset_ef.to_excel(writer, sheet_name=fund_type)

    # Write r_data_subset_ef to 'All Funds' worksheet
    r_data_subset_ef = r_data.reindex(index=trade_date, columns=mapping_dict.keys())
    for inner_code, wind_code in mapping_dict.items():
        if wind_code in r_data.columns:
            r_data_subset_ef[inner_code] = r_data[wind_code]
        else:
            print(f"Warning: {wind_code} not found in the columns of `r_data`")

    r_data_subset_ef.to_excel(writer, sheet_name='全部基金')

print("基金数据导出完成")
# fund_data = whole_data.loc[whole_data["VaR分类"].isin([ "境内债券型基金"]), :].copy()
# fund_value = fund_data.groupby('资产内码')['全价账面价值'].sum().sort_values()