import requests
import socket  # 导入 socket 模块
import time
import json
import pprint


s = socket.socket()  # 创建 socket 对象
host = socket.gethostname()  # 获取本地主机名
port = 12345  # 设置端口

# # 0. 蹲饼器心跳测试
# instance_id = 'lwt-01'
# url = 'http://0.0.0.0:{}/heartbeat?instance_id={}'.format(port, instance_id)
# res = requests.get(url)
# print(res.content)

# # 1. 后端要求更新配置测试
# url = 'http://0.0.0.0:{}/schedular-update-config'.format(port)
# input_data = {'platform': 'weibo'}
# res = requests.post(url, json=input_data)
# print(res.content)


# 2. 蹲饼器心跳测试

headers = {'instance_id': 'lwt-01'}
url = 'http://0.0.0.0:{}/heartbeat'.format(port)
res = requests.get(url, headers=headers)
print(res.content)
#
# headers = {'instance_id': 'lwt-02'}
# url = 'http://0.0.0.0:{}/heartbeat'.format(port)
# res = requests.get(url, headers=headers)
# print(res.content)
#
headers = {'instance_id': 'lwt-02'}
url = 'http://0.0.0.0:{}/heartbeat'.format(port)
res = requests.get(url, headers=headers)
print(res.content)

headers = {'instance_id': 'lwt-03'}
url = 'http://0.0.0.0:{}/heartbeat'.format(port)
res = requests.get(url, headers=headers)
print(res.content)

headers = {'instance_id': 'lwt-04'}
url = 'http://0.0.0.0:{}/heartbeat'.format(port)
res = requests.get(url, headers=headers)
print(res.content)

time.sleep(3)

# 3. 报告蹲饼器对某平台异常测试
headers = {'instance_id': 'lwt-02'}
url = 'http://0.0.0.0:{}/report'.format(port)
input_data = {'type': 'unavailable_platform', 'value': 'weibo'}

res = requests.post(url, json=input_data, headers=headers)
print(res.content)

# 4. 蹲饼器获取配置测试

headers = {'instance_id': 'lwt-02'}
url = 'http://0.0.0.0:{}/fetcher-get-config'.format(port)
res = requests.get(url, headers=headers)
pprint.pprint(json.loads(res.content))

# 5. mook蹲饼器获取配置测试
print('测试standalone蹲饼器获取配置：')

url = 'http://0.0.0.0:{}/standalone-fetcher-get-config'.format(port)
input_data = {'datasource_id_list': [28, 25]}
res = requests.post(url, json=input_data)
pprint.pprint(json.loads(res.content))