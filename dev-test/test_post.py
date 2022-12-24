import requests
import socket  # 导入 socket 模块
import time
s = socket.socket()  # 创建 socket 对象
host = socket.gethostname()  # 获取本地主机名
port = 12345  # 设置端口

# 注册蹲饼器测试
url = 'http://0.0.0.0:{}/register'.format(port)
input_data = {'instance_id': 'SilverAsh'}
res = requests.post(url, json=input_data)
print(res.content)

