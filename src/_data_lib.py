
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
from src._conf_lib import CONFIG


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
        self.WARNING_TIMEOUT = int(conf.get('WARNING_TIMEOUT', 150))
        # 移除蹲饼器的时间(单位: 秒)
        self.REMOVE_TIMEOUT = int(conf.get('REMOVE_TIMEOUT', 300))

        # instance-level heart beat 记录
        self._last_updated_time = dict()
        # instance-platform-level heart beat 记录。

        # 字典形式的失败列表，可以用 get_flat_failed_platform_instance_list 方法得到展开的失败记录.
        self._failed_platform_by_instance = defaultdict(list)

        # 蹲饼器被平台ban掉的倒计时记录.
        self.failed_platform_by_instance_countdown = defaultdict(dict)

        # if need_update[instance_id], 更新instance_id对应的蹲饼器的config.
        self.need_update = dict()

        self.alive_instance_id_list = []

        self._init_conn_redis(conf)

        # 当前是否产出了有效的配置。没有的话就禁止蹲饼器更新.
        self.has_valid_config = False
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

    def get_failed_platform_list(self, instance_id):
        """
        对某蹲饼器instance获取当前最新的失败平台列表.
        """
        return self._failed_platform_by_instance[instance_id]

    def set_abnormal_platform(self, instance_id, abnormal_info):
        """
        对某蹲饼器instance，获取当前最新的异常平台列表.
        目前 type == unavailable_platform 代表无法蹲该平台的饼.
        {
            "type": "string",
            "value": "string"
        }
        """

        cur_abnormal_type = abnormal_info.get('type', '')
        # 仅处理 unavailable_platform
        if cur_abnormal_type != 'unavailable_platform':
            return self._failed_platform_by_instance[instance_id]

        cur_failed_platform = abnormal_info['value']
        self._failed_platform_by_instance[instance_id].append(cur_failed_platform)
        self._failed_platform_by_instance[instance_id] = list(set(
                                        self._failed_platform_by_instance[instance_id]))

        # 加入失败蹲饼器的倒计时
        self.failed_platform_by_instance_countdown[instance_id][cur_failed_platform] = 600

        return self._failed_platform_by_instance[instance_id]

    def update_instance_status(self,
                         instance_id: str = '',
                         # failed_platform_list: list = [],
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
            # 接口拆分了，更新failed_platform_list与蹲饼器心跳刷新解耦.
            
            # 登录新的fetcher. 如果已经登录了，则不改变当前fetcher的 self.need_update的状态.
            if new_name not in self.need_update:
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

    def get_mook_fetcher_config(self, instance_id):
        """
        根据instance_id, 获取最新的蹲饼器配置.
        :param instance_id 要进行配置的蹲饼器id.
        :return: 新config.
        """
        
        cur_config = fetcher_config_pool['MOOK']

        return cur_config

class FetcherConfigPool(object):
    """
    维护每个蹲饼器的蹲饼策略.
    v1.0: 来自后台的配置.
    config_pool的 data schema:
        230101更新:
        key: String[instance_id]
        value: Dict[该蹲饼器对应的配置]
    
    更新FetcherConfigPool存储的蹲饼策略时，依赖 strategy 和 maintainer.
    """
    def __init__(self, conf):
        self.mysql_handler = HandleMysql(conf)
        logger.info('[MYSQL conn] success.')
        # 直接分配给蹲饼器的config.
        self.config_pool = dict()

    # FetcherConfigPool 初始化时，不直接获取config. 
    # 等待maintainer启动后，给出更新调度器config的指令，此时更新 FetcherConfigPool.

    #     # 初始化全部平台的全部live_number的配置.
    #     self.startup_init()

    # def startup_init(self, maintainer: Maintainer):
    #     """
    #     调度器启动时，初始获取全部配置
    #     :return:
    #     """
    #     self.fetcher_config_update(maintainer)

    def fetcher_config_update(self, maintainer: Maintainer):
        """
        根据必要的数据信息(存活蹲饼器数量；被ban平台情况；当前各平台基于live_number的config，group信息)，分配蹲饼器所需的config
        默认调用该函数时，全部蹲饼器的config都会更新.

        :return: (无需返回值). 最新蹲饼器配置
        """
        is_valid, cur_config_pool = manual_strategy.update(maintainer)
        
        # 仅当当前配置认为有效时, 更新配置。
        if is_valid:

            self.config_pool = cur_config_pool
            # 告知蹲饼器需要更新.
            for instance_id in maintainer.need_update:
                maintainer.need_update[instance_id] = True
        else:
            # 如果当前配置无效，则不更新，仍然使用老配置或不配置。
            for instance_id in maintainer.need_update:
                maintainer.need_update[instance_id] = False
        # return latest_config

    def __getitem__(self, fetcher_instance_id):
        return self.config_pool.get(fetcher_instance_id, {})


import json
import numpy as np


class NpEncoder(json.JSONEncoder):
    """
    numpy 和 pandas 库的数据都是numpy对象，不能直接json序列化. 转化成python原生数据格式.

    """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


fetcher_config_pool = FetcherConfigPool(conf=CONFIG)

maintainer = Maintainer(conf=CONFIG)
