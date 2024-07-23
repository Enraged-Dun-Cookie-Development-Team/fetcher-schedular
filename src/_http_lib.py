
import time
import os
import sys
import traceback
from collections import defaultdict
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from src.instance_utils import get_new_instance_name
from src._log_lib import logger
from src.db import HandleMysql, HandleRedis
from src.strategy import *
from src._conf_lib import CONFIG


import requests
from concurrent.futures import ThreadPoolExecutor
import queue


class PostManager:
    def __init__(self, max_workers=1):
        self.queue = queue.Queue()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.future_to_data = {}

    def post(self, url, data):
        future = self.executor.submit(self._post, url, data)
        self.future_to_data[future] = data

    def _post(self, url, data):
        try:
            response = requests.post(url, json=data)
            response.raise_for_status()
        except Exception as e:
            logger

    def add_data(self, url, data):
        self.post(url, data)

    def shutdown(self):
        self.executor.shutdown(wait=True)


if __name__ == '__main__':
    # 使用示例
    pm = PostManager(max_workers=1) # =1 表示同步.
    for i in range(10):
        data = {'key': f'value{i}'}
        pm.add_data('http://example.com/api', data)

    pm.shutdown()