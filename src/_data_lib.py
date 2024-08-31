import time
import os
import sys
import traceback
import datetime
import json
import numpy as np

from collections import defaultdict
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from src.instance_utils import get_new_instance_name
from src._log_lib import logger
from src.db import HandleMysql, HandleRedis
from src.strategy import *
from src._conf_lib import CONFIG, AUTO_SCHE_CONFIG
from src._http_lib import PostManager
from src.auto_sche.model_loader import MODEL_DICT
from src.auto_sche.model_events import feat_processer

# 打日志
from src._grpc_lib import messager

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

        self.fetcher_url_dict = dict()

        # 当前是否产出了有效的配置。没有的话就禁止蹲饼器更新.
        self.has_valid_config = True

        self.is_init = True
        logger.info('调度器Maintainer初始化完成')

    def _init_conn_redis(self, conf):
        self.redis = HandleRedis(conf)
        try:
            self.redis.get('test')
            self.redis.set('cookie:fetcher:config:live:number', 0)
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


class AutoMaintainer(object):
    '''
    多个蹲饼器的信息管理器。
    用于创建与更新蹲饼器的蹲饼策略.
    '''
    def __init__(self):
        self.pm = PostManager(max_workers=1)  # max=1 即为同步
        self.model = MODEL_DICT['decision_tree_model']


        # 用[数据库里的datasource_id] 查询对应的[config].
        self.datasource_id_to_config_mapping = dict()

        # [不同蹲饼器数量下的]，[数据库里的datasource_id] 查询对应的[蹲饼器编号]。
        self.live_number_to_datasource_id_to_fetcher_count_mapping = dict()

        # 放在 init 阶段执行.
        # self.set_config_mappings()

        # 存储每天模型预测的结果
        self._model_predicted_result_pool = None
        
        # 后面改成配置
        self.interval_seconds = 1
        # 后面改成配置
        self.datasource_num = 24

    def activate_send_request(self, maintainer:Maintainer):
        """
        1. 获取当前时间所需蹲饼的平台 + 获取当前所需蹲饼平台对应的蹲饼器
        2. 发送请求.
        TODO: 这里仍然要传入maintainer. 因为需要获取对应的http地址。
        """

        pending_datasources_id_list = self.get_pending_datasources()
        # print('####', maintainer)
        post_data_list = self.get_post_data_list(pending_datasources_id_list, maintainer)

        self._send_request(post_data_list)

    def daily_model_predict(self):
        """
        每日更新模型全量预测结果
        """
        X_list = feat_processer.feature_combine()

        predicted_result = self.model.predict(X_list)

        self._set_model_predicted_result_pool(X_list, predicted_result)

    def get_post_data_list(self, pending_datasources_id_list, maintainer:Maintainer):
        """

        :param pending_datasources_id_list: 需要蹲饼的datasource_id
        """
        post_data_list = []

        # 当前可用的蹲饼器，给个序号. fetcher_config表里的蹲饼器编号是1开始的。
        fetcher_dict = {idx + 1: m for idx, m in enumerate(maintainer.alive_instance_id_list)}
        # 获取每个蹲饼器对应的url.
        fetcher_url_dict = maintainer.fetcher_url_dict
        alive_fetcher_num = len(fetcher_dict)

        if alive_fetcher_num == 0:
            logger.info('当前蹲饼器全部失效，不主动发送请求')
            return []

        # 当前数量下的所有fetcher的配置。
        fetcher_config_pool_of_live_num = self.live_number_to_datasource_id_to_fetcher_count_mapping[alive_fetcher_num]

        for cur_datasource_id in pending_datasources_id_list:

            # 蹲饼器序号
            cur_fetcher_count = fetcher_config_pool_of_live_num[cur_datasource_id]
            # 当前蹲饼器的id, 用于索引url.
            cur_fetcher_id = fetcher_dict[cur_fetcher_count % alive_fetcher_num + 1]
            # 要发送的蹲饼器的url.
            cur_url = fetcher_url_dict[cur_fetcher_id]
            # 要发送的配置
            cur_config = self.datasource_id_to_config_mapping.get(cur_datasource_id, -1)

            # TODO: 可以放入其他辅助字段，例如后续需要持续的蹲饼时间等.

            if not isinstance(cur_config, int):
                post_data_list.append({
                    'url': cur_url,
                    'config': cur_config,
                })

        return post_data_list

    def set_config_mappings(self):

        # pending_datasources  # 这里还要实现：让哪个蹲饼器来蹲.
        # 用 fetcher_config 来配
        """
        self.live_number_to_datasource_id_to_fetcher_count_mapping = {
            "$live_number$ = 1":{
                "$datasource_id$ = 14": "$fetcher_count$ = 3"
            }
        }

        self.datasource_id_to_config_mapping = {
            "$datasource_id$ = 14": "$config$ = {'xx':'yy'}"
        }
        """
        # 取出和索引datasource、索引蹲饼器及其配置相关的两个数据表。
        fetcher_datasource_config_df = manual_strategy.data_pool.fetcher_datasource_config_df
        fetcher_config_df = manual_strategy.data_pool.fetcher_config_df

        # fetcher_datasource_config_df -> self.datasource_id_to_config_mapping
        for idx in range(fetcher_datasource_config_df.shape[0]):
            cur_id = fetcher_datasource_config_df.iloc[idx]['id']
            cur_config = fetcher_datasource_config_df.iloc[idx]['config']
            if isinstance(cur_config, str):
                cur_config = json.loads(cur_config)

            self.datasource_id_to_config_mapping[cur_id] = cur_config

        # fetcher_config_df -> self.live_number_to_datasource_id_to_fetcher_count_mapping
        # 取出每个存活蹲饼器的数量列表
        alive_fetcher_num_list = list(set(fetcher_config_df['live_number'].tolist()))
        for cur_alive_fetcher_num in alive_fetcher_num_list:
            df_tmp = fetcher_config_df[fetcher_config_df['live_number'] == cur_alive_fetcher_num].copy().reset_index(drop=True)
            self.live_number_to_datasource_id_to_fetcher_count_mapping[cur_alive_fetcher_num] = dict()
            for idx in range(df_tmp.shape[0]):
                line = df_tmp.iloc[idx]
                self.live_number_to_datasource_id_to_fetcher_count_mapping[cur_alive_fetcher_num
                        ][line['datasource_id']] = line['fetcher_count']

    def _set_model_predicted_result_pool(self, X_list, predicted_result):
        """
        把预测结果和原始输入，整合成方便查找蹲饼时间和对应数据源的形式。
        """
        X_list['predicted_y'] = predicted_result

        X_list.columns = ['datasource', '1', '2', '3', '4', 'year', 'month', 'day', 'hour', 'minute', 'second', '11', 'predicted_y']
        
        X_list['datetime'] = pd.to_datetime(X_list[['year', 'month', 'day', 'hour', 'minute', 'second']])

        # 使用.dt.strftime()将日期时间对象格式化为字符串
        X_list['datetime_str'] = X_list['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

        self._model_predicted_result_pool = X_list

    def get_pending_datasources(self,  end_time=None, time_window_seconds=None):
        
        # 设置需要判断的时间段的右端点
        if not end_time:
            end_time = datetime.datetime.now()
        else:
            date_format = '%Y-%m-%d %H:%M:%S'  
      
            end_time = datetime.datetime.strptime(end_time, date_format)
        
        # 从4点过去，经过了多少个小时
        cur_hour_offset = max((end_time.hour + 24 - 4) % 24, 1)

        # 初始化，没有开始预测的时候：
        # print('?' * 20, self._model_predicted_result_pool)
        if self._model_predicted_result_pool is None:
            pending_datasource_id_list = list(self.datasource_id_to_config_mapping.keys())
            return pending_datasource_id_list

        X_list_filtered = self._model_predicted_result_pool.iloc[(cur_hour_offset - 1) * \
                                                                 self.interval_seconds * \
                                                                 self.datasource_num * 3600:
                    (cur_hour_offset + 1) * self.interval_seconds * self.datasource_num * 3600
                    ].reset_index(drop=True)

        # 设置窗口长度
        if not time_window_seconds:
            time_window_seconds = 5

        start_time = end_time - datetime.timedelta(seconds=time_window_seconds)

        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # print(start_time)

        target_df = X_list_filtered[
            np.logical_and(X_list_filtered['datetime_str'] >= start_time,
                          X_list_filtered['datetime_str'] <= end_time)]
        
        pending_datasource_stats_df = target_df.groupby('datasource')['predicted_y'].sum()
        pending_datasource_stats_df = pending_datasource_stats_df[pending_datasource_stats_df > 0].reset_index()
        
        pending_datasource_id_list = MODEL_DICT['datasource_encoder'].inverse_transform(pending_datasource_stats_df['datasource'].tolist())
        pending_datasource_id_list = list(pending_datasource_id_list)

        # 输出的每个元素是 datasource_id，int类型的。可以直接和数据库里的datasource_id对应)。

        return pending_datasource_id_list
        # return [22, 26]

    def _send_request(self, data):

        # logger.info("debug发送的内容:")
        # print(data)

        for d in data:

            cur_url = d['url']
            d.pop('url')
            messager.send_to_bot(info_dict={'info': '{} '.format(datetime.datetime.now()) + str({'url': cur_url, 'data': d})})
            self.pm.add_data(cur_url, d)


fetcher_config_pool = FetcherConfigPool(conf=CONFIG)

maintainer = Maintainer(conf=CONFIG)
auto_maintainer = AutoMaintainer()