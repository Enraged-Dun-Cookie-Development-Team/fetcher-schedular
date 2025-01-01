import time
import os
import sys
import traceback
import datetime
import json
import numpy as np
import gc

from collections import defaultdict
import traceback
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from src.instance_utils import get_new_instance_name
from src._log_lib import logger, get_memory_usage
from src.db import HandleMysql, HandleRedis
from src.strategy import *
from src._conf_lib import CONFIG, AUTO_SCHE_CONFIG
from src._http_lib import PostManager
from src.auto_sche.model_loader import MODEL_DICT
from src.auto_sche.model_events import feat_processer

# 打日志
from src._grpc_lib import messager

import tracemalloc

tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()


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
        self.model = None


        # 用[数据库里的datasource_id] 查询对应的[config].
        self.datasource_id_to_config_mapping = dict()
        self.datasource_id_to_name_mapping = dict()

        # key: 两层，[不同蹲饼器数量下的]，[数据库里的datasource_id]
        # value: 查询对应的[蹲饼器编号]
        self.live_number_to_datasource_id_to_fetcher_count_mapping = dict()

        # 放在 init 阶段执行.
        # self.set_config_mappings()

        # 原：存储每天模型预测的结果
        # 新：存储关注的2小时的预测结果。动态更新
        self._model_predicted_result_pool = None
        # 记录 self._model_predicted_result_pool 的截止时间。和当前时间进行对比以确定是否对其进行更新。
        # 初始化用now.
        self._model_predicted_result_end_time = datetime.datetime.now()

        # 存储redis的前缀
        """
        两个槽位确定小时级别的redis key.
        第一位：YYYYMMDD;
        第二位：h
        """
        self.redis_name_prefix = "cookie:autosche:{}:hour:{}"
        # 后面改成配置
        self.interval_seconds = 1
        # 后面改成配置
        self.datasource_num = 24

        try:
            # 准备就绪，bot发送指令
            messager.send_to_bot(
                info_dict={'info': '{}: '.format(datetime.datetime.now()) + 'auto sche started'})
        except:
            print('grpc 没发出去')

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
        messager.send_to_bot_shortcut('每日更新模型全量预测结果 开始内存：{}'.format(get_memory_usage()))
        # 拆成24个小时的数据运行

        # 加载模型
        MODEL_DICT.load_model('decision_tree_model_v2', path_prefix='./')
        self.model = MODEL_DICT['decision_tree_model']

        # 旧代码：本地list存储结果。
        # self._model_predicted_result_pool = []
        for j in range(24):
            try:
                # debug
                messager.send_to_bot_shortcut('预测第{}个小时的结果'.format(j + 1))

                messager.send_to_bot_shortcut('开始整理输入特征')

                # 现实世界的小时。是AUTO_SCHE_CONFIG['DAILY_PREPROCESS_TIME']['HOUR']作为起点，j作为偏移量的时间。
                hour_index = AUTO_SCHE_CONFIG['DAILY_PREPROCESS_TIME']['HOUR'] + j

                X_list = feat_processer.feature_combine(hour_index)
                messager.send_to_bot_shortcut('输入特征整理完成')

                import psutil
                import time

                def limit_cpu(interval):
                    p = psutil.Process()
                    while True:
                        cpu_usage = p.cpu_percent(interval=interval)

                        # # 发送cpu 使用率监控 到bot.
                        # messager.send_to_bot(
                        #     info_dict={'info': '{} '.format(datetime.datetime.now()) +
                        #                        str({'cpu使用率：': cpu_usage})})

                        if cpu_usage > 20:  # 如果CPU使用率超过40%
                            time.sleep(interval)  # 暂停一小段时间
                            del cpu_usage
                        else:
                            del cpu_usage
                            break

                start_time = time.time()
                # 在预测过程中定期调用此函数
                predictions = []
                batch_size = 1000
                interval = 0.005
                messager.send_to_bot_shortcut('开始预测')

                for i in range(0, len(X_list), batch_size):
                    time.sleep(0.05)
                    batch = X_list[i:i + batch_size]
                    batch_predictions = self.model.predict(batch)
                    if i % 100000 == 0:
                        # gc.collect()
                        messager.send_to_bot_shortcut('预测中，批次{} 内存：{}'.format(i, get_memory_usage()))
                        limit_cpu(interval)
                    predictions.extend(batch_predictions)
                    if i == 0:
                        messager.send_to_bot_shortcut('预测结果第一批样例形状：')
                        messager.send_to_bot_shortcut(batch_predictions.shape)
                    
                    del batch_predictions    

                del interval
                del batch_size

                stop_time = time.time()

                messager.send_to_bot(
                    info_dict={'info': '{} '.format(datetime.datetime.now()) +
                                       str({'模型预测消耗时间：': stop_time - start_time})})
                del start_time
                del stop_time

                # predicted_result = self.model.predict_proba(X_list)[:, 1]

                # 当前时间，用于计算key.
                cur_date = datetime.datetime.now() # .strftime("%Y%m%d")

                time_info = {'cur_date': cur_date, 'hour_index': j}
                self._set_model_predicted_result_pool(X_list, predictions, maintainer, time_info=time_info)

                del predictions
                del X_list
            except Exception as e:
                # 打印报错
                messager.send_to_bot_shortcut('出现报错，详细信息为:')
                messager.send_to_bot_shortcut(str(e))
        # del self._model_predicted_result_pool

        # 删除模型。
        MODEL_DICT.model_dict.pop('decision_tree_model')
        del self.model
        self.model = None

        gc.collect(2)
        messager.send_to_bot_shortcut('最终内存：{}'.format(get_memory_usage()))

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

        # 当前活跃蹲饼起数量下，每个fetcher的配置。
        fetcher_config_pool_of_live_num = self.live_number_to_datasource_id_to_fetcher_count_mapping[alive_fetcher_num]

        # print('当前活跃数量的蹲饼器的配置表:', fetcher_config_pool_of_live_num)

        for cur_datasource_id in pending_datasources_id_list:

            # 蹲饼器序号
            if cur_datasource_id not in fetcher_config_pool_of_live_num:
                continue
            cur_fetcher_count = fetcher_config_pool_of_live_num[cur_datasource_id]
            # 当前蹲饼器的id, 用于索引url.
            cur_fetcher_id = fetcher_dict[cur_fetcher_count % alive_fetcher_num + 1]
            # 要发送的蹲饼器的url.
            cur_url = fetcher_url_dict[cur_fetcher_id]
            # 要发送的配置
            cur_config = self.datasource_id_to_config_mapping.get(cur_datasource_id, -1)
            cur_datasource_name = self.datasource_id_to_name_mapping.get(cur_datasource_id, -1)
            # TODO: 可以放入其他辅助字段，例如后续需要持续的蹲饼时间等.

            if not isinstance(cur_config, int):
                post_data_list.append({
                    'url': cur_url,
                    'config': cur_config,
                    'datasource_name': cur_datasource_name
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

        # id找名字
        for idx in range(fetcher_datasource_config_df.shape[0]):
            cur_id = fetcher_datasource_config_df.iloc[idx]['id']
            cur_name = fetcher_datasource_config_df.iloc[idx]['nickname']

            self.datasource_id_to_name_mapping[cur_id] = cur_name

        # fetcher_config_df -> self.live_number_to_datasource_id_to_fetcher_count_mapping

        # 取出配置中已经配置过的，蹲饼器的数量范围。防止数量超过上限。
        alive_fetcher_num_list = list(set(fetcher_config_df['live_number'].tolist()))
        for cur_alive_fetcher_num in alive_fetcher_num_list:

            # 在当前存活蹲饼器的数量下，为每个datasource_id分配具体的蹲饼器。
            # fetcher_count 是蹲饼器编号。
            df_tmp = fetcher_config_df[fetcher_config_df['live_number'] == cur_alive_fetcher_num].copy().reset_index(drop=True)
            self.live_number_to_datasource_id_to_fetcher_count_mapping[cur_alive_fetcher_num] = dict()
            for idx in range(df_tmp.shape[0]):
                line = df_tmp.iloc[idx]
                self.live_number_to_datasource_id_to_fetcher_count_mapping[cur_alive_fetcher_num
                        ][line['datasource_id']] = line['fetcher_count']

    def _set_model_predicted_result_pool(self, X_list, predicted_result, maintainer:Maintainer, time_info):
        """
        把预测结果和原始输入，整合成方便查找蹲饼时间和对应数据源的形式。
        :param hour_index: 当前小时（例如晚上20时）的结果
        :time_info: 时间相关字段
        """

        cur_date = time_info['cur_date']
        hour_index = int(time_info['hour_index'])

        # 日期 + 1
        if hour_index <= int(AUTO_SCHE_CONFIG['DAILY_PREPROCESS_TIME']['HOUR']):
            cur_date = cur_date + datetime.timedelta(days=1)

        messager.send_to_bot_shortcut('开始后处理，内存：{}'.format(get_memory_usage()))

        X_list.columns = ['datasource', '1', '2', '3', '4', 'year', 'month', 'day', 'hour', 'minute', 'second', '11']

        # 去掉所有无关数据
        # X_list = X_list[['datasource', 'year', 'month', 'day', 'hour', 'minute', 'second']]
        del X_list['1'], X_list['2'], X_list['3'], X_list['4'], X_list['11']

        # for c in ['year', 'month', 'day', 'hour', 'minute', 'second']:
        #     X_list[c] = X_list[c].astype('int8')

        X_list['datetime'] = pd.to_datetime(
          X_list.loc[:, 'year': 'second'])# .astype('datetime64[ns]')

        # X_list.to_csv('./tmp.csv', index=False)
        # del X_list
        # gc.collect()

        # snapshot2 = tracemalloc.take_snapshot()
        # top_stats = snapshot2.compare_to(snapshot1, 'lineno') # statistics('lineno')

        # print("[ Top 10 ]")
        # for stat in top_stats[:30]:
        #     print(stat)

        messager.send_to_bot_shortcut('整体删除X_list 内存：{}'.format(get_memory_usage()))

        # 去掉所有无关数据
        # del X_list['year'], X_list['month'], X_list['day'], X_list['hour'], X_list['minute'], X_list['second']
        X_list.drop(columns=['year', 'month', 'day', 'hour', 'minute', 'second'], inplace=True)

        # X_list = X_list[['datasource', 'datetime']]
        # gc.collect()
        messager.send_to_bot_shortcut('完成时间戳转换')
        messager.send_to_bot_shortcut('完成时间戳转换 内存：{}'.format(get_memory_usage()))
        print(X_list.info(memory_usage='deep'))
        X_list['predicted_y'] = np.array(predicted_result) > 0.99999

        # 只保留需要蹲饼的时间.
        X_list = X_list[X_list['predicted_y'] == True].reset_index(drop=True)

        del predicted_result
        # gc.collect()

        messager.send_to_bot_shortcut('将预测结果与特征完成拼接，完整形状为：')
        messager.send_to_bot_shortcut(X_list.shape)
        messager.send_to_bot_shortcut('预测结果与输入完成拼接 内存：{}'.format(get_memory_usage()))

        # 放置一个空 datetime_str 进来
        X_list['datetime_str'] = ''
        # 计算每个批次的大小
        batch_size = len(X_list) // 1000

        # 处理每个批次
        for i in range(1000):

            if i % 10 == 0:
                # gc.collect()
                messager.send_to_bot_shortcut('时间戳转换字符串批次{} 内存：{}'.format(i, get_memory_usage()))

            start_index = i * batch_size
            # 确保最后一个批次包含所有剩余的行
            if i == 999:
                end_index = len(X_list)
            else:
                end_index = (i + 1) * batch_size

            X_list.loc[start_index:end_index, 'datetime_str'] = X_list.loc[start_index:end_index,
                                                                'datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            X_list.loc[start_index:end_index, 'datetime'] = None
            del start_index
            del end_index
        del batch_size
        # # 使用.dt.strftime()将日期时间对象格式化为字符串
        # X_list['datetime_str'] = X_list['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

        messager.send_to_bot_shortcut('完成时间戳字符串化')
        messager.send_to_bot_shortcut('完成时间戳字符串化 内存：{}'.format(get_memory_usage()))

        # 再去掉所有无关数据
        del X_list['datetime']

        messager.send_to_bot_shortcut('把无关列精简掉 内存：{}'.format(get_memory_usage()))

        X_list = X_list[X_list['datasource'] < 33].reset_index(drop=True)
        messager.send_to_bot_shortcut('完成datasource筛选')

        # gc.collect()

        # debug
        print('未来一天的预测结果')

        a = len(set(X_list[X_list['predicted_y'] == 1]['datetime_str'].tolist()))
        # print(a)
        messager.send_to_bot(
            info_dict={'info': '{} '.format(datetime.datetime.now()) + str({'启动时预测当天可能有饼的时间点数量': a})})

        # 旧：一次性存储一天所有数据
        # self._model_predicted_result_pool = X_list
        # 新：每次存储1小时的数据
        # self._model_predicted_result_pool.append(X_list)

        # 先压缩
        tmp_compressed_X_list = maintainer.redis.compress_data(X_list)
        # 然后存入redis，ttl 26小时

        # 获取正式存储的key.
        # 年月日；小时.
        cur_key = self.get_redis_key_by_hour_level_key(cur_date.strftime("%Y%m%d"), int(hour_index))
        save_redis_status = maintainer.redis.set_with_ttl(cur_key, tmp_compressed_X_list, 26 * 3600)

        messager.send_to_bot_shortcut('第{}小时数据存储状态：{}'.format(cur_key, save_redis_status))

        del X_list
        del tmp_compressed_X_list
        gc.collect(2)

        messager.send_to_bot_shortcut('启动时预测当天可能有饼的时间点数量 内存：{}'.format(get_memory_usage()))

    def update_model_predicted_result_pool(self):
        """
        对当前时间，判断 self._model_predicted_result_pool 还够不够用。不够了的话取出新一个小时的结果。
        :return:
        """
        cur_time = datetime.datetime.now()
        time_difference = self._model_predicted_result_end_time - cur_time
        # 剩余秒数
        seconds_difference = time_difference.total_seconds()

        # 日期
        cur_time_day = cur_time.day
        # 还剩300s时更新。
        if seconds_difference < 300:
            # 以下两个结果均为pd.Dataframe

            # 当前小时的结果
            hour_now_res = maintainer.redis.extract_data(
                maintainer.redis.get(self.get_redis_key_by_hour_level_key(cur_time_day, cur_time.hour)))

            # 下一小时的结果
            # 跨日时特殊处理.
            cur_time_hour = cur_time.hour

            if cur_time_hour == 23:
                next_time_day = (cur_time + datetime.timedelta(days=1)).hour
                next_time_hour = 0
            else:
                next_time_day = cur_time_day
                next_time_hour = cur_time_hour

            hour_next_res = maintainer.redis.extract_data(
                maintainer.redis.get(self.get_redis_key_by_hour_level_key(next_time_day, next_time_hour)))

            self._model_predicted_result_pool = pd.concat([hour_now_res, hour_next_res], axis=0)

            # 时间点替换为整点
            self._model_predicted_result_end_time = cur_time.replace(minute=0, second=0) + datetime.timedelta(hours=2)

    def get_redis_key_by_hour_level_key(self, date, hour):
        """
        对给定的时间信息，获取与redis交互（存、取）的key。
        :param date: 年月日
        :param hour: int，小时
        :return redis_key: key名称。
        """
        redis_key = self.redis_name_prefix.format(date, hour)
        return redis_key

    def get_pending_datasources(self,  end_time=None, time_window_seconds=None):
        """
        :param end_time: 要蹲饼时间的右端点。一般是当前时间。
        :param time_window_seconds:
        :return:
        """
        # 设置需要判断的时间段的右端点.
        if not end_time:
            end_time = datetime.datetime.now()
        else:
            date_format = '%Y-%m-%d %H:%M:%S'  
      
            end_time = datetime.datetime.strptime(end_time, date_format)

        # 初始化，没有开始预测的时候：
        # print('?' * 20, self._model_predicted_result_pool)
        if self._model_predicted_result_pool is None:
            pending_datasource_id_list = list(self.datasource_id_to_config_mapping.keys())

            # 调试阶段调整
            return []
            # return pending_datasource_id_list

        # TODO: 按小时存储后的取数逻辑
        # 用 end_time.hour 确定哪些需要取哪些数据
        """
        取数总体逻辑：
        1. 获取当前时间 ✓
        2.1 根据当前时间，判断是否需要取出新一小时的预测结果 ✓
        2.2 如果需要更新，则更新
        3. 用当前时间段的结果（self._model_predicted_result_pool）根据起、止时间筛选需要蹲饼的数据源。
        """
        # 判断是否需要更新
        self.update_model_predicted_result_pool()

        # 旧：从24h的结果中取出对应时间段
        # X_list_filtered = self._model_predicted_result_pool.iloc[(cur_hour_offset - 1) * \
        #                                                          self.interval_seconds * \
        #                                                          self.datasource_num * 3600:
        #             (cur_hour_offset + 1) * self.interval_seconds * self.datasource_num * 3600
        #             ].reset_index(drop=True)

        # 新：直接使用当前段
        X_list_filtered = self._model_predicted_result_pool
        # print('^^^^^' * 4 + ' X_list_filtered')
        # print(X_list_filtered)

        # 设置窗口长度
        if not time_window_seconds:
            time_window_seconds = 5

        # 确定需要索引的开始时间和结束时间。
        start_time = end_time - datetime.timedelta(seconds=time_window_seconds)

        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # print(start_time)

        target_df = X_list_filtered[
            np.logical_and(X_list_filtered['datetime_str'] >= start_time,
                          X_list_filtered['datetime_str'] <= end_time)]

        # print('*****' * 4 + ' target_df')
        # print(target_df)

        pending_datasource_stats_df = target_df.groupby('datasource')['predicted_y'].sum()
        pending_datasource_stats_df = pending_datasource_stats_df[pending_datasource_stats_df > 0].reset_index()

        # print('%%%%%' * 4 + ' pending_datasource_stats_df')

        # print(pending_datasource_stats_df)
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

    def pass_redis_data_verify(self, maintainer:Maintainer):
        """
        检查redis key是否存储好了所有的结果.
        基于当前时间。到第二天 AUTO_SCHE_CONFIG['DAILY_PREPROCESS_TIME']['HOUR'].
        :return:
        """
        # 生成需要校验的key.
        verify_redis_key_list = []

        cur_date = datetime.datetime.now()

        cur_hour = cur_date.hour

        for i in range(cur_hour, 24):

            cur_key = self.get_redis_key_by_hour_level_key(cur_date.strftime("%Y%m%d"), int(i))
            verify_redis_key_list.append(cur_key)

        cur_date = cur_date + datetime.timedelta(days=1)

        # 第二天.
        for i in range(0, AUTO_SCHE_CONFIG['DAILY_PREPROCESS_TIME']['HOUR']):
            cur_key = self.get_redis_key_by_hour_level_key(cur_date.strftime("%Y%m%d"), int(i))
            verify_redis_key_list.append(cur_key)

        for k in verify_redis_key_list:
            # 如果没找到这个key.
            if not maintainer.redis.get(k):
                return False

        return True


fetcher_config_pool = FetcherConfigPool(conf=CONFIG)

maintainer = Maintainer(conf=CONFIG)
auto_maintainer = AutoMaintainer()