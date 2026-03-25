"""
  作 者：lidy
  时 间：2023/12/30 17:36
  项目名：tsdpsdk
"""
import tushare

if __name__ == '__main__':
    api = tushare.set_token('75e70c1ef4bd1a14e2301cbf20ec35e1045971c104ab02853e375284')
    dd = tushare.realtime_quote(
        ts_code="688553.SH", src="sina"
    )
    print(dd)