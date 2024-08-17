import os
import json
import sys
import traceback

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import time
import grpc
import Ceobe_Proto.pb.log_pb2 as pb2
import Ceobe_Proto.pb.log_pb2_grpc as pb2_grpc
from concurrent import futures # grpc线程包

'''
创建服务端
'''

# 定义服务类
class Loggg(pb2_grpc.LogServicer):
    def PushLog(self, requst, context):

        server = requst.server # enum
        level = requst.level # enum
        manual = requst.manual # bool
        info = requst.info # str
        extra = requst.extra # str

        result = 'server:{}, message: {}'.format(level, info)
        print(result)

        return pb2.LogResponse(success=1)

# 启动服务
def run():
    grpc_service = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )
    # 注册服务到grpc里
    pb2_grpc.add_LogServicer_to_server(Loggg(), grpc_service)
    # 绑定ip和端口号
    grpc_service.add_insecure_port('127.0.0.1:5000')
    print('service will start')
    grpc_service.start()

    try:
        while 1:
            time.sleep(600)     # python里start一下就结束了，所以需要这样让他一直开启，知道鼠标或者键盘事件结束
    except KeyboardInterrupt:
        grpc_service.stop(0)


if __name__ == '__main__':
    run()
