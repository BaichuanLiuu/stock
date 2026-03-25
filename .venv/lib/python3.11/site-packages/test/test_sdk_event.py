"""
# 作 者：84028
# 时 间：2024/1/9 13:29
# tsdpsdk
"""
import time

import requests

if __name__ == '__main__':
    for i in range(200):
        start = time.time()
        resp = requests.post(
            'http://127.0.0.1:8081/dataapi/sdk-event',
            json={
                "user_token": "af4c3a76ff14972b3467d8950dd1587bb3d4b6b2dc9cd94093910795",
                "event_name": "test",
                "event_detail": "测试"
             }
        )
        print(time.time()-start, resp)