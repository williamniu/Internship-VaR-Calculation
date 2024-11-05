# -*- coding: utf-8 -*-
"""
Created on Wes Jul  17 10:53:47 2024

@author: WilliamNiu
"""

import pandas as pd
import numpy as np
DEMOSTIC_STOCK_DF = pd.read_excel("E:\\NZH\\VAR计算\\data\\【7】境内权益资产-A股-收益率序列（已填充）.xlsx", index_col=0)
FUND_DF = pd.read_excel("E:\\NZH\\VAR计算\\data\\【2】基金-收益率序列.xlsx", sheet_name='全部基金',index_col=0)
SHSC_STOCK_DF = pd.read_excel("E:\\NZH\\VAR计算\\data\\【7】境外权益资产-港股通-收益率序列（已填充）.xlsx", index_col=0)
FOREIGN_EQUITY_DF = pd.read_excel("E:\\NZH\\VAR计算\\data\\【9】境外权益QDII数据整理.xlsx", sheet_name="收益率", index_col=0)
HOLDING_DF = pd.read_excel("E:\\NZH\\VAR计算\\20240930_ALM分类_Python输出.xlsx", index_col=0)
VaR_ASSET = HOLDING_DF.loc[~HOLDING_DF["VaR分类"].isnull(), :]
merged_df = pd.concat([DEMOSTIC_STOCK_DF, SHSC_STOCK_DF, FOREIGN_EQUITY_DF, FUND_DF], axis=1)
merged_df.to_excel("E:\\NZH\\VAR计算\\【11】收益率合并.xlsx")
empty_asset = VaR_ASSET.loc[~VaR_ASSET["资产内码"].isin(merged_df.columns), :]
if empty_asset.empty:  
    print("收益率数据准备通过验证，开始计算VaR...")
else:
    print("存在无收益率的资产，请补全后重新运行程序!")
    empty_asset.to_excel("./【10.5】无收益率资产.xlsx")
    print(empty_asset)


ACCOUNT_LIST = ["一般账户", "传统", "红", "万"]
ASSET_DICT = {"境内债券型基金":["境内债券型基金"], "境内权益资产":["境内权益-基金", "境内权益-股票"], \
              "境外权益":["境外权益-QDII", "境外权益-港股通", "境外权益-港股通基金"]}
writer = pd.ExcelWriter("./【12】VaR计算结果.xlsx")
one_year_ans = pd.DataFrame(index=ACCOUNT_LIST, columns=ASSET_DICT.keys())
three_years_ans = pd.DataFrame(index=ACCOUNT_LIST, columns=ASSET_DICT.keys())
holding_ans = pd.DataFrame(index=ACCOUNT_LIST, columns=ASSET_DICT.keys())
ONE_YEAR_START = "2023-09-30"
THREE_YEARS_START = "2021-09-30"

merged_df = merged_df.loc[merged_df.index>pd.Timestamp(THREE_YEARS_START)]
with pd.ExcelWriter("./【12】VaR计算结果.xlsx") as writer:
    one_year_ans = pd.DataFrame(index=ACCOUNT_LIST, columns=ASSET_DICT.keys())
    three_years_ans = pd.DataFrame(index=ACCOUNT_LIST, columns=ASSET_DICT.keys())
    holding_ans = pd.DataFrame(index=ACCOUNT_LIST, columns=ASSET_DICT.keys())

    for account in ACCOUNT_LIST:
        if account == "一般账户":
            sub_holding_df = HOLDING_DF.copy()
        else:
            sub_holding_df = HOLDING_DF.loc[HOLDING_DF['业务层名称'].apply(lambda x: account in x), :]

        for asset_name, asset_range in ASSET_DICT.items():
            var_sub_holding_df = sub_holding_df.loc[sub_holding_df["VaR分类"].isin(asset_range), :]
            if var_sub_holding_df.empty:
                continue
            grouped_df = var_sub_holding_df.groupby("资产内码")['全价账面价值'].sum() / var_sub_holding_df['全价账面价值'].sum()
            
            return_panel = merged_df * grouped_df
            return_panel['合计'] = return_panel.sum(axis=1)
            return_panel['合计'] = (1 + return_panel['合计']).rolling(window=10).apply(lambda x: np.prod(x), raw=False) - 1
            return_panel = return_panel.dropna(subset=['合计'])

            one_year_data = return_panel.loc[return_panel.index > ONE_YEAR_START, '合计'].iloc[9:]
            three_years_data = return_panel.loc[return_panel.index > THREE_YEARS_START, '合计']

            if not one_year_data.empty:
                one_year_ans.loc[account, asset_name] = -np.percentile(one_year_data, 1)
            if not three_years_data.empty:
                three_years_ans.loc[account, asset_name] = -np.percentile(three_years_data, 1)

            holding_ans.loc[account, asset_name] = var_sub_holding_df['全价账面价值'].sum()

    one_year_ans.to_excel(writer, sheet_name="一年VaR")
    (one_year_ans * holding_ans).to_excel(writer, sheet_name="一年VaR_金额")
    three_years_ans.to_excel(writer, sheet_name="三年VaR")
    (three_years_ans * holding_ans).to_excel(writer, sheet_name="三年VaR_金额")
    holding_ans.to_excel(writer, sheet_name="全价账面价值")

print("VaR计算完成并保存到文件中。")

# fund = pd.read_excel("./【2】基金-收益率序列.xlsx", sheet_name='境内债券型基金',index_col=0)
# fund_list = list(fund_value1.index)
# fund = fund.reindex(columns=fund_list)
# fund_rolling = (1+fund).rolling(window=10).apply(lambda x: np.prod(x), raw=False) - 1
# fund_rolling = fund_rolling.dropna()
# fund_var = pd.DataFrame(columns=fund_list)
# for i in fund_var.columns:
#     fund_var[i] = [-np.percentile(fund_rolling.loc[:,i].dropna(), 1) ]
# fund_var.mean(axis=1)
# (fund_var*fund_value1).sum(axis=1)
