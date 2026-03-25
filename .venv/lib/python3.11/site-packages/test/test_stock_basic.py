"""
  作 者：lidy
  时 间：2023/8/22 22:00
  项目名：tsdpsdk
"""

import tushare

def test_stock_basic():
    api = tushare.pro_api('75e70c1ef4bd1a14e2301cbf20ec35e1045971c104ab02853e375284')
    df = api.stock_basic()
    print(df)


if __name__ == '__main__':
    test_stock_basic()