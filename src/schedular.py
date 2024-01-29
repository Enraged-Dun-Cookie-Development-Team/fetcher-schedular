import tornado
from tornado import web, ioloop
import os
import sys
import time
import json
import humanize
import traceback
import copy
import logging

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
humanize.i18n.activate("zh_CN")
# print(sys.path)
from src._data_lib import maintainer, fetcher_config_pool, NpEncoder
from src._log_lib import logger
from src._conf_lib import CONFIG

MAX_INT = 16777216


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


class HeartBeatSchedular(web.RequestHandler):
    '''
    已有蹲饼器维持心跳.

    # 更新包括三种情况:
    # 1. 来自蹲饼器健康数量的变化，例如新增一个蹲饼器或某个蹲饼器心跳无响应，带来的有效蹲饼器数量的增加/减少。
    # 2. 来自蹲饼器健康状态的变化. 例如一个蹲饼器突然无法蹲微博了，则微博平台对应使用的配置会变成live_number - 1 的新配置.
    # 3. 来自fetcher config的变化，fetcher config如果有更新，会强制要求所有(?)蹲饼器更新自己对应的config.

    # 因为有2这种情况，所以每个蹲饼器拿到的config需要独立维护. 

    '''
    def get(self):
        """
        header: instance_id 蹲饼器id.
        return: 是否需要更新.
        """
        head = self.request.headers
        instance_id = head.get('instance_id', '')

        # 不涉及数据库操作，只对内存里已有的活跃蹲饼器进行梳理和记录，扫描时再进行配置的更新.
        new_name = maintainer.update_instance_status(instance_id)

        # 是否需要给蹲饼器更新配置.
        need_return_config = maintainer.need_update[instance_id]

        output_dict = dict()
        output_dict['code'] = 0
        output_dict['require_update'] = need_return_config

        self.write(tornado.escape.json_encode(output_dict))

    def post(self, *args, **kwargs):
        '''
        处理蹲饼器心跳传入
        :param args:
        :param kwargs:
        :return:
        '''
        pass
        """

        input_data = tornado.escape.json_decode(self.request.body)
        logger.info(input_data)
        instance_id = input_data.get('instance_id', '')
        
        # 2023-04-14 10:44 分拆成两个接口了.
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
        """


class FetcherConfigHandler(web.RequestHandler):
    '''
    蹲饼器获取最新配置.
    施工中.
    '''
    def get(self):
        """
        header: instance_id 蹲饼器id.
        return: latest config. (一定在有必要更新时，才会调用此接口)
        """
        head = self.request.headers
        instance_id = head.get('instance_id', None)
        logger.info('蹲饼器获取配置:{}'.format(instance_id))
        
        output_dict = dict()
        output_dict['code'] = 0
        latest_config = maintainer.get_latest_fetcher_config(instance_id)  # 是在心跳扫描时计算的。这里只是取出结果.
        latest_config = copy.deepcopy(latest_config)
        
        if latest_config:
            for cur_group in latest_config['groups']:
                cur_group.pop('datasource_id')

        output_dict['config'] = latest_config
        # 更新不需要获得新config.
        maintainer.need_update[instance_id] = False
        logger.info('蹲饼器id:{}的配置: '.format(instance_id) + str(latest_config))
        self.write(json.dumps(output_dict, cls=NpEncoder))

    def post(self, *args, **kwargs):
        '''
        处理蹲饼器心跳传入
        :param args:
        :param kwargs:
        :return:
        '''
        pass
        """

        input_data = tornado.escape.json_decode(self.request.body)
        logger.info(input_data)
        instance_id = input_data.get('instance_id', '')
        
        # 2023-04-14 10:44 分拆成两个接口了.
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
        """


class MookFetcherConfigHandler(web.RequestHandler):
    '''
    standalone蹲饼器获取最新配置.
    '''
    def get(self):
        pass

    def post(self, *args, **kwargs):
        """
        header: instance_id 蹲饼器id.
        return: latest config. (一定在有必要更新时，才会调用此接口)
        """
        try:
            # 默认mook fetcher instance id = 'MOOK'
            input_data = tornado.escape.json_decode(self.request.body)
            
            datasource_id_list = input_data.get('datasource_id_list', [])
            datasource_id_list = [int(i) for i in datasource_id_list]
            logger.info('MOOK蹲饼器获取配置, 数据源列表:{}'.format(str(datasource_id_list)))

            output_dict = dict()
            output_dict['code'] = 0
            latest_config = maintainer.get_latest_fetcher_config('MOOK')  # 是在心跳扫描时计算的。这里只是取出结果.

            latest_config = copy.deepcopy(latest_config)
            # 用 datasource_id_list 筛选所需datasource_id的config.
            latest_config['groups'] = list([g for g in latest_config['groups'] if g['datasource_id'] in datasource_id_list])

            for cur_group in latest_config['groups']:
                cur_group.pop('datasource_id')

            output_dict['config'] = latest_config
            logger.info('返回给MOOK蹲饼器配置:{}'.format(latest_config))

            self.write(json.dumps(output_dict, cls=NpEncoder))
        except:
            output_dict = dict()
            output_dict['code'] = 500
            output_dict['config'] = {}
            self.write(json.dumps(output_dict, cls=NpEncoder))


class ReportHandler(web.RequestHandler):
    '''
    记录蹲饼器传入的异常平台信息.
    '''
    def get(self):
        pass

    def post(self, *args, **kwargs):
        '''
        蹲饼器新增异常平台信息。一定伴随着当前instance_id的need_update = True
        :param args:
        :param kwargs:
        :return:
        '''
        # instance_id 在 header里
        head = self.request.headers
        instance_id = head.get('instance_id', None)

        # 其余信息在body里.
        input_data = tornado.escape.json_decode(self.request.body)
        
        logger.info('异常平台信息, 蹲饼器: {}'.format(instance_id))
        logger.info(input_data)
        
        # 更新异常平台信息.
        maintainer.set_abnormal_platform(instance_id, input_data)
        maintainer.need_update[instance_id] = True

        self.write(tornado.escape.json_encode({'code': 0}))


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
            platform_to_update = self.get_argument('platform', '')

            if not platform_to_update:
                self.write({'status': 'no platform selected to update', 'code': 500})

            else:
                # 初版为全量更新.
                # TODO: 精细化更新，按照platform.
                fetcher_config_pool.fetcher_config_update(maintainer)
                self.write({'status': 'success', 'code': 200})
        except:
            traceback.print_exc()
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

        if maintainer.is_init:
            fetcher_config_pool.fetcher_config_update(maintainer)
            print('maintainer初始化:', maintainer.has_valid_config)
            maintainer.is_init = False

        now = time.time()
        # 定期扫描检测各个蹲饼器健康状态
        # 开始扫描；没有状态变化则不记录。
        # logger.info('[Start a new scan]')

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
                logger.warning('[REMOVE INSTANCE PERMANENTLY] {}'.format(instance_id))

            # 短期无响应，只是warning.
            elif now > warning_ddl:
                logger.warning('[NO HEART BEAT] {}'.format(instance_id))

            else:
                cur_alive_list.append(instance_id)
                # logger.info('[ALIVE]: {}'.format(instance_id))
                maintainer.alive_instance_id_list = cur_alive_list

        # 活动的蹲饼器list发生了变化
        if set(cur_alive_list) != set(self.last_alive_fetcher_list):
            if maintainer.has_valid_config:
                self.UPDATE_CONFIG_FLAG = True
                # 下次心跳请求时，给每个蹲饼器传回新的config.
                # 如果一个蹲饼器挂了之后又好了，它的id会变化. 所以不冲突.
                for instance_id in cur_alive_list:
                    maintainer.need_update[instance_id] = True
                self.last_alive_fetcher_list = cur_alive_list

            # 更新理论存活上限. need_update无论True还是False，都认为未来可能存活；被删除了则认为不会存活了。
            max_live_number = maintainer.redis.get('cookie:fetcher:config:live:number', 0)
            if len(maintainer.need_update) > max_live_number:
                redis_update_status = maintainer.redis.set('cookie:fetcher:config:live:number', len(maintainer.need_update))
                logger.warning('[REDIS UPDATE] cookie:fetcher:config:live:number {}, {}'.format(redis_update_status, len(maintainer.need_update)))

        # 蹲饼器蹲失败的平台发生了变化.
        elif set(failed_flat_list) != set(self.last_failed_flat_list) and maintainer.has_valid_config:
            self.UPDATE_CONFIG_FLAG = True

            for instance_id in cur_alive_list:
                maintainer.need_update[instance_id] = True
            self.last_failed_flat_list = failed_flat_list

        # maintainer更新最新的config的操作在这里
        if self.UPDATE_CONFIG_FLAG:
            # 如需更新，则执行对全部配置的更新.
            # print(1)
            fetcher_config_pool.fetcher_config_update(maintainer)
            self.UPDATE_CONFIG_FLAG = False  # 复位

        # 蹲饼器蹲失败的平台情况check:
        # 遍历所有失败的蹲饼器 * 平台, 进行倒计时更新。倒计时小于0则将它从失败平台列表里剔除。
        for instance_id in maintainer.failed_platform_by_instance_countdown:
            for cur_failed_platform in maintainer.failed_platform_by_instance_countdown[instance_id]:
                maintainer.failed_platform_by_instance_countdown[instance_id][cur_failed_platform] -= 5
                if maintainer.failed_platform_by_instance_countdown[instance_id][cur_failed_platform] < 0:
                    maintainer._failed_platform_by_instance[instance_id].remove(cur_failed_platform)
                    maintainer.failed_platform_by_instance_countdown[instance_id][cur_failed_platform] = MAX_INT
        # 已恢复的就从 _failed_platform_by_instance当中去除.

        remove_list = []
        for instance_id in maintainer._failed_platform_by_instance:
            if not maintainer._failed_platform_by_instance[instance_id]:
                remove_list.append(instance_id)

        for instance_id in remove_list:
            maintainer._failed_platform_by_instance.pop(instance_id)

        # logger.info('曾经失败的蹲饼器恢复倒计时状态:大于600代表正常;小于等于600代表等待恢复中')
        # logger.info(str(maintainer.failed_platform_by_instance_countdown))
        # logger.info(str(maintainer._failed_platform_by_instance))


health_monitor = HealthMonitor()

if __name__ == '__main__':
    application = web.Application([
        # 心跳监控
        (r'/heartbeat', HeartBeatSchedular),
        # (r'/register', RegisterHandler),
        # 蹲饼器向调度器报告无效平台
        (r'/report', ReportHandler),
        # 蹲饼器获取最新config
        (r'/fetcher-get-config', FetcherConfigHandler),
        # 后台通知蹲饼器更新config.
        (r'/schedular-update-config', SchedularConfigHandler),
        # 为mook(standalone)蹲饼器提供最新config.
        (r'/standalone-fetcher-get-config', MookFetcherConfigHandler)
    ])
    # 禁用默认log.
    # logging.getLogger('tornado.application').setLevel(logging.CRITICAL)
    logging.getLogger('tornado.access').setLevel(logging.CRITICAL)

    application.listen(CONFIG['SCHEDULAR']['PORT'], address=CONFIG['SCHEDULAR']['HOST'])

    ioloop.PeriodicCallback(health_monitor.health_scan, 5000).start()  # start scheduler 每隔2s执行一次f2s
    ioloop.IOLoop.instance().start()