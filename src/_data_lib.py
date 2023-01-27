
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


class Maintainer(object):
    '''
    多个蹲饼器的信息管理器。
    用于创建与更新蹲饼器的蹲饼策略.
    '''
    def __init__(self,
                 conf: dict = dict()
                 ):
        """
        :param conf: 设置TTL相关参数
        """
        # 需要告警的蹲饼器无心跳的时间(单位：秒)
        self.WARNING_TIMEOUT = conf.get('WARNING_TIMEOUT', 15)
        # 移除蹲饼器的时间(单位: 秒)
        self.REMOVE_TIMEOUT = conf.get('REMOVE_TIMEOUT', 30)

        # instance-level heart beat 记录
        self._last_updated_time = dict()
        # instance-platform-level heart beat 记录。

        # 字典形式的失败列表，可以用 get_flat_failed_platform_instance_list 方法得到展开的失败记录.
        self._failed_platform_by_instance = defaultdict(list)

        # if need_update[instance_id], 更新instance_id对应的蹲饼器的config.
        self.need_update = dict()

        self.alive_instance_id_list = []

        self._init_conn_redis(conf)

        logger.info('调度器Maintainer初始化完成')

    def _init_conn_redis(self, conf):
        self.redis = HandleRedis(conf)
        try:
            self.redis.get('test')
            logger.info('[REDIS conn] success.')
        except:
            logger.error('[REDIS conn] failed: ' + str(traceback.format_exc()))

    def delete_instance(self, instance_id):
        '''
        永久删除蹲饼器.
        :param instance_id: 蹲饼器id
        :return:
        '''
        if instance_id in self.alive_instance_id_list:
            self.alive_instance_id_list.remove(instance_id)

        if instance_id in self.need_update:
            self.need_update.pop(instance_id)

        if instance_id in self._last_updated_time:
            self._last_updated_time.pop(instance_id)

    def update_instance_status(self,
                         instance_id: str = '',
                         failed_platform_list: list = [],
                         ):
        """
        更新蹲饼器实例(instance_id)级别的健康状态.
        此处同时考虑平台级别的健康状态.

        :param instance_id: 蹲饼器id
        :return:
        """
        if not instance_id:
            new_name = get_new_instance_name(self._last_updated_time)
            # 1. 新注册：默认不需要立即返回新config，等health_monitor发现之后会更新need_update状态，下一次心跳时返回新config.
            self.need_update[new_name] = False
            self._last_updated_time[new_name] = time.time()
            return new_name

        else:
            new_name = instance_id
            # 更新一下平台级别的失败记录.
            self._failed_platform_by_instance[new_name] = failed_platform_list
            self.need_update[new_name] = False
            self._last_updated_time[new_name] = time.time()
            return new_name

    def get_flat_failed_platform_instance_list(self):
        """
        获取蹲饼器 * 平台的失败情况，返回 蹲饼器_平台 的列表.
        :return:
        """
        res = []
        for instance in self._failed_platform_by_instance:
            for p in self._failed_platform_by_instance[instance]:
                res.append('{}_{}'.format(instance, p))
        return res

    def get_latest_fetcher_config(self, instance_id):
        """
        根据instance_id, 获取最新的蹲饼器配置.
        :param instance_id 要进行配置的蹲饼器id.
        :return: 新config.
        """
        cur_config = fetcher_config_pool[instance_id]

        return cur_config


class FetcherConfigPool(object):
    """
    维护每个蹲饼器的蹲饼策略.
    v1.0: 来自后台的配置.
    config_pool的 data schema:
        230101更新:
        key: string. fetcher的instance_id.
        value: dict, 该蹲饼器对应的配置.
    """
    def __init__(self, conf):
        self.mysql_handler = HandleMysql(conf)
        logger.info('[MYSQL conn] success.')
        # 直接分配给蹲饼器的config.
        self.config_pool = dict()

        # 平台列表
        self.platform_list = []

        # 二层字典，第一层key为platform，第二层key为str(live_number)
        self.platform_config_by_live_number = defaultdict(dict)

        # 初始化全部平台的全部live_number的配置.
        self.startup_init()

    def startup_init(self):
        """
        调度器启动时，初始获取全部配置
        :return:
        """
        # 先获取全部平台
        self.get_latest_platform_list()
        # 然后对每个平台都使用update.
        for platform in self.platform_list:
            self.update(platform)

    def update(self, platform_to_update):
        """
        响应来自后端的更新请求，更新：
        1. 对应 platform_to_update 的所有live_number的config.
        2. platform_list
        :return:
        """
        # TODO.

        # 更新平台列表
        self.get_latest_platform_list()

        # 更新platform的配置.
        # mock data
        self.platform_config_by_live_number['bilibili'] = {"1": {'data_source': [{'uid': '1265652806'}]}}
        pass

    def get_latest_platform_list(self):
        """
        获取当前蹲饼器蹲的所有平台.
        :return:
        """
        # TODO.

        pass

    def fetcher_config_update(self, maintainer: Maintainer):
        """
        根据必要的数据信息(存活蹲饼器数量；被ban平台情况；当前各平台基于live_number的config，group信息)，分配蹲饼器所需的config
        TODO.
        :return:
        """

    def __getitem__(self, fetcher_instance_id):
        return self.config_pool[fetcher_instance_id]


fetcher_config_pool = FetcherConfigPool(conf=dict())
maintainer = Maintainer(conf=dict())
