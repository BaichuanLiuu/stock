"""
  作 者：lidy
  时 间：2023/8/22 22:00
  项目名：tsdpsdk
"""

import tushare

def test_query():
    api = tushare.pro_api('75e70c1ef4bd1a14e2301cbf20ec35e1045971c104ab02853e375284')
    df = api.query('stock_basic', fields='ts_code,symbol')
    print(df)


if __name__ == '__main__':
    test_query()