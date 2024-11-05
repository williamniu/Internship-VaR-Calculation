# -*- coding: utf-8 -*-
"""
Created on Mon Jul  15 18:01:46 2024

@author: WilliamNiu
"""

import pandas as pd
# Used to populate data for late-listed stocks

SW_RETURN = pd.read_excel("./【5】申万行业指数收益率.xlsx", sheet_name='申万行业收益率', index_col="日期")
SW_CODE = pd.read_excel("./【5】申万行业指数收益率.xlsx", sheet_name='申万代码对应')
SHSC_RETURN = pd.read_excel("./【5】港股通指数收益率.xlsx", sheet_name='替代指数', index_col=0)
start_date = pd.Timestamp("20210930")
STOCK_RETURN_DEMOSTIC = pd.read_excel("./【4】境内权益资产-A股-收益率序列.xlsx", index_col=0)
STOCK_RETURN_SHSC = pd.read_excel("./【4】境外权益资产-港股通-收益率序列.xlsx", index_col=0)
# STOCK_DETAIL = pd.read_excel("L:/后台业务数据/151_人寿投资管理部业务数据/DataWarehouse/股票收益率/股票清单.xlsx")
STOCK_DETAIL = pd.read_pickle("E:\\Data\\股票数据库\\股票清单_更新至2024-10-13.data")
STOCK_DETAIL.set_index('股票代码', inplace=True)
# STOCK_DETAIL[:, "上市日期"] = STOCK_DETAIL[:, "上市日期"].apply(lambda x: max(x, pd.Timestamp("19000110")))
# STOCK_DETAIL = STOCK_DETAIL.dropna() #已手动检查持仓中不含剩余数据  #已手动检查持仓中含剩余数据
SW_DICT = dict(zip(SW_CODE['指数简称'],SW_CODE['代码']))

empty_list = []
for stock_code in set(STOCK_RETURN_DEMOSTIC.columns):
    wind_code = stock_code[:6] + "." + stock_code[-2:]
    
    if pd.isna(STOCK_DETAIL.loc[STOCK_DETAIL.index == wind_code, ["上市日期"]].values[0][0]):
        print(stock_code)
        continue
    
    issue_date = STOCK_DETAIL.loc[STOCK_DETAIL.index == wind_code, ["上市日期"]].values[0][0]
    issue_date = pd.Timestamp(issue_date)
    
    if issue_date <= start_date:
        continue
    
    stock_return = STOCK_RETURN_DEMOSTIC.loc[:, stock_code]
    sw_industry = STOCK_DETAIL.loc[STOCK_DETAIL.index == wind_code, "申万一级行业（2021）"].values[0]

    mask = SW_RETURN.index < issue_date
    # Resampling date index
    stock_return = stock_return.reindex(SW_RETURN.index)
    stock_return.loc[mask] = SW_RETURN.loc[mask, SW_DICT[sw_industry]]
    
    STOCK_RETURN_DEMOSTIC.loc[:, stock_code] = stock_return

STOCK_RETURN_DEMOSTIC.to_excel("【7】境内权益资产-A股-收益率序列（已填充）.xlsx")

for stock_code in set(STOCK_RETURN_SHSC.columns):
    wind_code = stock_code[1:5] + ".HK"
    issue_date = STOCK_DETAIL.loc[STOCK_DETAIL.index == wind_code, ["上市日期"]].values[0][0]
    issue_date = pd.Timestamp(issue_date)
    
    if issue_date <= start_date:
        continue
    
    stock_return = STOCK_RETURN_SHSC.loc[:, stock_code]
    
    if stock_code.endswith("SK"):
        # Shanghai-Hong Kong Link
        replace_code = "H50069.CSI"
    elif stock_code.endswith("ZK"):
        # Shenzhen-Hong Kong Link
        replace_code = "983005.CNI"
        
    mask = SHSC_RETURN.index < issue_date
    # Reindex stock_return to match the index of SHSC_RETURN
    stock_return = stock_return.reindex(SHSC_RETURN.index)
    stock_return.loc[mask] = SHSC_RETURN.loc[mask, replace_code]
    
    STOCK_RETURN_SHSC.loc[:, stock_code] = stock_return

STOCK_RETURN_SHSC.to_excel("【7】境外权益资产-港股通-收益率序列（已填充）.xlsx")