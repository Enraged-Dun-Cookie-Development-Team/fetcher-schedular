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


class MessagerGRPC(object):

    def __init__(self):
        # 定义频道
        conn = grpc.insecure_channel('127.0.0.1:5000')
        self.client = pb2_grpc.LogStub(channel=conn)

    def send_to_bot(self, info_dict):

        server = 3
        level = info_dict.get('level', 2)
        manual = False
        info = info_dict.get('info', 'nothing')
        extra = ''

        # 生成客户端
        response = self.client.PushLog(pb2.LogRequest(
            server=server,
            level=level,
            manual=manual,
            info=info,
            extra=extra
        ))
        return response


messager = MessagerGRPC()
if __name__ == '__main__':
    messager.send_to_bot({'info': 'test-info,haha'})