
import time
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from src.instance_utils import get_new_instance_name
from src._log_lib import logger
from src.db import HandleMysql


class FetcherConfigPool(object):
    """
    维护每个蹲饼器的蹲饼策略.
    v1.0: 来自后台的配置.
    config_pool的 data schema:
        key: string，通过key可以取出对应的config.
        value: List(dict), 列表长度代表蹲饼器数量；每个dict为对应一个蹲饼器的config.
    """
    def __init__(self, conf):
        self.mysql_handler = HandleMysql(conf)
        self.config_pool = dict()

    def update(self):
        """
        响应来自后端的更新请求，更新所有情况的config.
        :return:
        """
        pass

    def __getitem__(self, item):
        return self.config_pool[item]


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
        # 最长容忍蹲饼器无心跳的时间(单位：秒)
        self.MAX_TIMEOUT = conf.get('MAX_TIMEOUT', 15)
        self._last_updated_time = dict()
        logger.info('调度器初始化完成')

        # if need_update[instance_id], 更新instance_id对应的蹲饼器的config.
        self.need_update = dict()

    def update_instance_status(self,
                         instance_id: str = ''
                         ):
        if not instance_id:
            new_name = get_new_instance_name(self._last_updated_time)

        else:
            new_name = instance_id

        self._last_updated_time[new_name] = time.time()

        return new_name

    def get_latest_fetcher_config(self, config_name, instance_id_list, instance_id):
        """
        根据instance_id, 获取最新的蹲饼器配置.
        :param config_name: 选取哪个config. 当前版本的config_name为 str(# of alive fetcher)
        :param instance_id: 蹲饼器id
        :return: 新config.
        """
        cur_config = fetcher_config_pool[config_name]

        assert instance_id in instance_id_list

        i_idx = -1
        for i_idx, cur_instance_id in enumerate(instance_id_list):
            if cur_instance_id == instance_id:
                break

        # 取出对应位置的config作为当前蹲饼器的config.
        return cur_config[i_idx]


fetcher_config_pool = FetcherConfigPool(conf=dict())
maintainer = Maintainer(conf=dict())
