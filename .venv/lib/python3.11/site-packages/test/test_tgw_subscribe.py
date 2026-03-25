"""
# 作 者：84028
# 时 间：2024/2/29 9:41
# tsdpsdk
"""
from typing import Union

from tushare import set_token
from tushare.subs.model.tick import TsTick, TsTickIdx
from tushare.subs.tgw_subs.subscribe import TgwSubscribe

tushare_token = '75e70c1ef4bd1a14e2301cbf20ec35e1045971c104ab02853e375284'
username = "10100135021"
password = "1010013502120240219"
host = "120.86.124.106"
port = 8600


set_token(tushare_token)
sub = TgwSubscribe(username=username, password=password, host=host, port=port)


@sub.register(ts_codes=['600000.SH', '000300.SH'])
def just_print_index(data: Union[TsTick, TsTickIdx]):
    print(data)


sub.run()
