"""
# 作 者：84028
# 时 间：2023/12/12 22:33
# tsdpsdk
"""
import json

import websocket


def on_open(wsapp):
    print(f'on-open')
    req_data = {
        "action": "listening",
        "token": "75e70c1ef4bd1a14e2301cbf20ec35e1045971c104ab02853e375284",
        "data": {"HQ_FUT_MIN": ["1MIN:*"]}
    }
    aa = wsapp.send(json.dumps(req_data))
    print('application starting... ', aa)


def on_close(wsapp):
    print(f'on-close')


def on_error(wsapp, error):
    print(f'on-error {error}')


def on_message(wsapp, message: str):
    print(f'on-message {message} {type(message)}')


def on_ping(*args, **kwargs):
    print(f'on-ping {args} {kwargs}')


def on_pong(wsapp: websocket.WebSocketApp, message: bytes):
    print(f'on-pong {message}')


websocket = websocket.WebSocketApp(
    "wss://ws.tushare.pro/listening",
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_open=on_open,
    on_ping=on_ping,
    on_pong=on_pong
)

websocket.run_forever(ping_interval=3, ping_timeout=2)
