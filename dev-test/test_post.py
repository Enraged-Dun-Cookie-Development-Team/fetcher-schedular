import requests
import socket  # 导入 socket 模块
import time
s = socket.socket()  # 创建 socket 对象
host = socket.gethostname()  # 获取本地主机名
port = 12345  # 设置端口

# 1. 注册蹲饼器测试
# url = 'http://0.0.0.0:{}/register'.format(port)
# input_data = {'instance_id': 'SilverAsh'}
# res = requests.post(url, json=input_data)
# print(res.content)

# 2. 蹲饼器要求更新配置测试
# url = 'http://0.0.0.0:{}/config-update'.format(port)
# input_data = {'platform': 'weibo'}
# res = requests.post(url, json=input_data)
# print(res.content)


# 3. 蹲饼器心跳测试
url = 'http://0.0.0.0:{}/'.format(port)
input_data = {'instance_id': 'SilverAsh', 'failed_platform': []}
res = requests.post(url, json=input_data)
print(res.content)