import requests
import socket  # 导入 socket 模块
import time
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
instance_id = 'lwt-01'
url = 'http://0.0.0.0:{}/heartbeat?instance_id={}'.format(port, instance_id)
res = requests.get(url)
print(res.content)

instance_id = 'lwt-02'
url = 'http://0.0.0.0:{}/heartbeat?instance_id={}'.format(port, instance_id)
res = requests.get(url)
print(res.content)

time.sleep(8)

# 3. 报告蹲饼器对某平台异常测试
instance_id = 'lwt-01'
url = 'http://0.0.0.0:{}/report?instance_id={}'.format(port, instance_id)
input_data = {'type': 'unavailable_platform', 'value': 'weibo'}

res = requests.post(url, json=input_data)
print(res.content)

# 4. 蹲饼器获取配置测试

instance_id = 'lwt-02'
url = 'http://0.0.0.0:{}/fetcher-get-config?instance_id={}'.format(port, instance_id)
res = requests.get(url)
print(res.content)
