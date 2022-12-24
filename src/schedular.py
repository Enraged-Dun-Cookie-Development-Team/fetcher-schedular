import tornado
from tornado import web, ioloop
import os
import sys
import time
import humanize
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# print(sys.path)
from src._data_lib import maintainer, fetcher_config_pool
from src._log_lib import logger


class RegisterHandler(web.RequestHandler):
    '''
    注册新蹲饼器的接口实现
    '''
    def get(self):
        pass

    def post(self, *args, **kwargs):
        '''
        处理蹲饼器心跳传入
        :param args:
        :param kwargs:
        :return:
        '''
        input_data = tornado.escape.json_decode(self.request.body)
        logger.info(input_data)
        instance_id = input_data.get('instance_id', '')

        new_name = maintainer.update_instance_status(instance_id)

        self.write(tornado.escape.json_encode({'instance_id': new_name}))


class MainSchedular(web.RequestHandler):
    '''
    已有蹲饼器维持心跳并保证获取最新配置.
    如果无需变化则不更新.(200) 需要更新则更新(202)

    更新包括两种情况:
    1. 来自蹲饼器状态的变化，例如新增一个蹲饼器或某个蹲饼器心跳无响应，带来的有效蹲饼器数量的增加/减少。
    2. 来自fetcher config的变化，fetcher config如果有更新，会强制要求所有(?)蹲饼器更新自己对应的config.

    '''
    def get(self):
        pass

    def post(self, *args, **kwargs):
        '''
        处理蹲饼器心跳传入
        :param args:
        :param kwargs:
        :return:
        '''
        input_data = tornado.escape.json_decode(self.request.body)
        logger.info(input_data)
        instance_id = input_data.get('instance_id', '')

        # 先增加一步判断: 是否需要给蹲饼器更新配置.
        new_name = maintainer.update_instance_status(instance_id)

        # 如需更新config，则为该instance_id的蹲饼器分配一个新的config
        self.write(tornado.escape.json_encode({'instance_id': new_name}))
        # 如果不需要更新


class ConfigUpdateHandler(web.RequestHandler):
    '''
    后台更新config的接口实现
    '''
    def get(self):
        try:
            fetcher_config_pool.update()
            self.write({'status': 'success', 'code': 200})
        except:
            self.write({'status': 'fail', 'code': 20100})

    def post(self, *args, **kwargs):
        pass


class HealthMonitor(object):
    """
    定时任务以确定健康的蹲饼器数量，从而调整config.
    """
    def __init__(self):
        # 记录上一次扫描结束时，存活的蹲饼器数量.
        self.alive_fetcher_count = 0

    def health_scan(self):
        '''
        1. 监控是否有蹲饼器挂掉了，进行log warning.
        2. 根据状态调整蹲饼策略.
        :return:
        '''
        now = time.time()

        # 定期扫描检测各个蹲饼器健康状态
        # 开始扫描
        logger.info('[Start a new scan]: {}'.format(
            humanize.time.naturaltime(now))
        )


        for instance_name in maintainer._last_updated_time:

            # 获取心跳最晚允许时间
            deadline = maintainer.MAX_TIMEOUT + maintainer._last_updated_time[instance_name]
            logger.info('[DEADLINE] {}:{}'.format(
                humanize.time.naturaltime(instance_name),
                humanize.time.naturaltime(deadline))
            )

            if now > deadline:
                logger.warning('[NO HEART BEAT] {}'.format(
                    humanize.time.naturaltime(instance_name))
                )
                # 挂了要更新蹲饼策略:
                '''
                1. 重新计数现在处于ALIVE状态的蹲饼器数量与id.
                2. 给每个ALIVE的id分配一个新的config到
                '''

            else:
                logger.info('[ALIVE]: {}'.format(
                    humanize.time.naturaltime(instance_name))
                )


health_monitor = HealthMonitor()

if __name__ == '__main__':
    application = web.Application([
        (r'/', MainSchedular),
        (r'/register', RegisterHandler),
        (r'/config_update', ConfigUpdateHandler),
    ])
    application.listen(12345)
    ioloop.PeriodicCallback(health_monitor.health_scan, 5000).start()  # start scheduler 每隔2s执行一次f2s
    ioloop.IOLoop.instance().start()