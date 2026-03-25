# -*- coding: utf-8 -*-
"""
Created on 2021-12-31 09:32:16
---------
@summary:  
---------
@author: yangyx01
"""
import tushare as ts
import math

def func_UpdateTradeData_RT_Mem(StockCode_Str, lst_df_ProcDataOri):
    # 抓取实时数据
    # 初始化tushare
    # 设置 Tushare 令牌
    TOKEN = '5b775e827d13d9c259f77dceff4210489e4edb7e25334ea13fd88b05'
    ts.set_token(TOKEN)

    # 初始化 pro 接口
    pro = ts.pro_api()
    t_s_s = t.time()
    print('开始抓取实时数据')
    LoopTime = math.ceil(len(StockCode_Str) / 50)  # 向上取整,通过tushare的新浪接口抓取，一次只能抓50个股票
    RealTimeData = pd.DataFrame()

    for i in range(0, LoopTime):
        Lower = i * 50
        Upper = (i + 1) * 50
        if (i + 1) == LoopTime:
            # ercode , wsd_data = w.wsq(StockCode_Str[Lower:], "rt_open,rt_high,rt_low,rt_latest,rt_vol,rt_amt",usedf=True)

            code_str_tmp = ','.join(StockCode_Str[Lower:])  # tushare的实时抓取函数只接受字符串列，因此改成字符串
            tushare_data = ts.realtime_quote(ts_code=code_str_tmp)  # tushare抓取实时数据
            # 将抓取的实时数据整理为Wind类型
            tushare_data.set_index('TS_CODE', inplace=True)  # 索引变更为代码
            tushare_data = tushare_data.loc[:, ['OPEN', 'HIGH', 'LOW', 'PRICE', 'VOLUME', 'AMOUNT']]
            new_columns = ['RT_OPEN', 'RT_HIGH', 'RT_LOW', 'RT_LATEST', 'RT_VOL', 'RT_AMT']
            tushare_data.rename(columns=dict(zip(tushare_data.columns, new_columns)), inplace=True)

        else:
            # ercode , wsd_data = w.wsq(StockCode_Str[Lower:Upper], "rt_open,rt_high,rt_low,rt_latest,rt_vol,rt_amt",usedf=True)

            code_str_tmp = ','.join(StockCode_Str[Lower:Upper])  # tushare的实时抓取函数只接受字符串列，因此改成字符串
            tushare_data = ts.realtime_quote(ts_code=code_str_tmp)  # tushare抓取实时数据
            # 将抓取的实时数据整理为Wind类型
            tushare_data.set_index('TS_CODE', inplace=True)  # 索引变更为代码
            tushare_data = tushare_data.loc[:, ['OPEN', 'HIGH', 'LOW', 'PRICE', 'VOLUME', 'AMOUNT']]
            new_columns = ['RT_OPEN', 'RT_HIGH', 'RT_LOW', 'RT_LATEST', 'RT_VOL', 'RT_AMT']
            tushare_data.rename(columns=dict(zip(tushare_data.columns, new_columns)), inplace=True)

        if tushare_data.shape[0] > 1:
            RealTimeData = pd.concat([RealTimeData, tushare_data])
        else:
            print('抓取失败')
            # print(wsd_data)
            break
