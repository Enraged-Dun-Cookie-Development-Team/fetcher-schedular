import tornado
from tornado import web, ioloop
import os
import sys
import time
import humanize
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

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

        self.write(tornado.escape.json_encode({'code': 200, 'instance_id': new_name}))


class MainSchedular(web.RequestHandler):
    '''
    已有蹲饼器维持心跳并保证获取最新配置.
    如果无需变化则不更新.(200) 需要更新则更新(202)

    更新包括三种情况:
    1. 来自蹲饼器健康数量的变化，例如新增一个蹲饼器或某个蹲饼器心跳无响应，带来的有效蹲饼器数量的增加/减少。
    2. 来自蹲饼器健康状态的变化. 例如一个蹲饼器突然无法蹲微博了，则微博平台对应使用的配置会变成live_number - 1 的新配置.
    3. 来自fetcher config的变化，fetcher config如果有更新，会强制要求所有(?)蹲饼器更新自己对应的config.

    因为有2这种情况，所以每个蹲饼器拿到的config需要独立维护. 

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
        # 记录没有被成功蹲饼的平台列表.
        failed_platform_list = input_data.get('failed_platform', [])

        # 不涉及数据库操作，只对内存里已有的活跃蹲饼器进行梳理和记录，扫描时再进行配置的更新.
        new_name = maintainer.update_instance_status(instance_id, failed_platform_list)

        # 判断: 是否需要给蹲饼器更新配置.
        need_return_config = False
        new_config = {}
        # 如果需要更新配置:
        if maintainer.need_update[instance_id]:
            new_config = maintainer.get_latest_fetcher_config(instance_id)  # 是在心跳扫描时计算的。这里只是取出结果.
            need_return_config = True

        output_dict = {'instance_id': new_name}

        if need_return_config:
            # 返回新配置
            output_dict['code'] = 202
            output_dict['config'] = new_config
        else:
            # 无需返回新配置.
            output_dict['code'] = 200

        self.write(tornado.escape.json_encode(output_dict))


class SchedularConfigHandler(web.RequestHandler):
    '''
    后台更新config的接口实现
    '''
    def get(self):
        pass

    def post(self, *args, **kwargs):
        """
        更新某个平台 platform 的config.
        :param args:
        :param kwargs:
        :return:
        """
        try:
            input_data = tornado.escape.json_decode(self.request.body)
            logger.info(input_data)
            platform_to_update = input_data.get('platform', '')

            if not platform_to_update:
                self.write({'status': 'no platform selected to update', 'code': 500})

            else:
                fetcher_config_pool.update(platform_to_update)
                self.write({'status': 'success', 'code': 200})
        except:
            self.write({'status': 'fail', 'code': 500})


class HealthMonitor(object):
    """
    定时任务以确定健康的蹲饼器数量，从而调整config.
    """
    def __init__(self):
        # 记录上一次扫描结束时，存活的蹲饼器
        self.last_alive_fetcher_list = []
        # 记录上一次 蹲饼器_平台 级别的失败列表
        self.last_failed_flat_list = []

        self.UPDATE_CONFIG_FLAG = False

    def health_scan(self):
        '''
        1. 状态监控：监控是否有蹲饼器挂掉了，进行log warning.
        2. 配置更新：健康状态是以平台为单位的，某个蹲饼器蹲不同平台的健康状态不同。根据状态调整蹲饼策略.
        3. 实例管理：如果有蹲饼器超过一定时长还没有响应，认为它已经重启了。可以从maintainer当中删除了.
        :return:
        '''
        now = time.time()

        # 定期扫描检测各个蹲饼器健康状态
        # 开始扫描
        logger.info('[Start a new scan]: {}'.format(
            humanize.time.naturaltime(now))
        )

        # 通过对 上一次scan时 活跃的蹲饼器 和 这一次scan活跃的蹲饼器 进行对比，判断蹲饼器级别是否需要更新
        last_updated_instance_list = list(maintainer._last_updated_time.keys())
        # 通过对上一次scan时平台级别被ban情况和当前被ban情况的对比，判断是否需要更新config.
        failed_flat_list = maintainer.get_flat_failed_platform_instance_list()

        # 更新活动的蹲饼器
        cur_alive_list = []
        for instance_id in last_updated_instance_list:

            # 告警心跳ddl
            warning_ddl = maintainer.WARNING_TIMEOUT + maintainer._last_updated_time[instance_id]

            # 彻底移除心跳ddl
            remove_ddl = maintainer.REMOVE_TIMEOUT + maintainer._last_updated_time[instance_id]
            # logger.info('[DEADLINE] {}:{}'.format(
            #     humanize.time.naturaltime(instance_id),
            #     humanize.time.naturaltime(deadline))
            # )

            # 超过最后期限，直接移除.
            if now > remove_ddl:
                maintainer.delete_instance(instance_id)
                logger.warning('[REMOVE INSTANCE PERMANENTLY] {}, {}'.format(
                    humanize.time.naturaltime(now), instance_id)
                )

            # 短期无响应，只是warning.
            elif now > warning_ddl:
                logger.warning('[NO HEART BEAT] {}, {}'.format(
                    humanize.time.naturaltime(now), instance_id)
                )

            else:
                cur_alive_list.append(instance_id)
                logger.info('[ALIVE]: {}'.format(
                    humanize.time.naturaltime(instance_id))
                )
                maintainer.alive_instance_id_list = cur_alive_list

        # 活动的蹲饼器list发生了变化
        if set(cur_alive_list) != set(self.last_alive_fetcher_list):
            self.UPDATE_CONFIG_FLAG = True
            # 下次心跳请求时，给每个蹲饼器传回新的config.
            # 如果一个蹲饼器挂了之后又好了，它的id会变化. 所以不冲突.
            for instance_id in cur_alive_list:
                maintainer.need_update[instance_id] = True
            self.last_alive_fetcher_list = cur_alive_list

            # 更新理论存活上限. need_update无论True还是False，都认为未来可能存活；被删除了则认为不会存活了。
            maintainer.redis.set('cookie:fetcher:config:live:number', len(maintainer.need_update))

        elif set(failed_flat_list) != set(self.last_failed_flat_list):
            self.UPDATE_CONFIG_FLAG = True

            for instance_id in cur_alive_list:
                maintainer.need_update[instance_id] = True
            self.last_failed_flat_list = failed_flat_list

        if self.UPDATE_CONFIG_FLAG:
            # 如需更新，则执行对全部配置的更新.
            fetcher_config_pool.fetcher_config_update(maintainer)
            self.UPDATE_CONFIG_FLAG = False  # 复位


health_monitor = HealthMonitor()

if __name__ == '__main__':
    application = web.Application([
        (r'/heartbeat', MainSchedular),
        (r'/register', RegisterHandler),
        (r'/schedular-update-config', SchedularConfigHandler),
        # 接口对齐
        # (r'/report', ReportHandler),
        # (r'/fetcher-get-config', FetcherConfigHandler),

    ])
    application.listen(12345)
    ioloop.PeriodicCallback(health_monitor.health_scan, 5000).start()  # start scheduler 每隔2s执行一次f2s
    ioloop.IOLoop.instance().start()