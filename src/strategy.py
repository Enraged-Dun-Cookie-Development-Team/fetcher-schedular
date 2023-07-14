import numpy as np
import sys
import time
import humanize
import os
import pandas as pd
from src._log_lib import logger
import json


sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# print(sys.path)
from src.db import (select_fetcher_datasource_config, select_fetcher_config,
                    select_fetcher_global_config, select_fetcher_platform_config)


class DataPool:
    def __init__(self):
        self.get_latest_data()

    def get_latest_data(self):

        self.fetcher_datasource_config_df = select_fetcher_datasource_config(platform='')
        self.fetcher_config_df = select_fetcher_config()
        self.fetcher_global_config_df = select_fetcher_global_config()
        self.fetcher_platform_config_df = select_fetcher_platform_config()



class BasicStrategy:
    """
    策略基类
    """
    def __init__(self):
        # self.get_latest_data()
        self.status_matrix = None

    def get_latest_data(self):
        self.data_pool = DataPool()

    def get_platform_identifiers(self):
        """
        获取平台列表.
        """
        return self.data_pool.fetcher_platform_config_df.type_id.tolist()

    def initial_matrix(self, rows, columns, default_value=1):
        """
        使用状态矩阵来管理蹲饼器 * 平台级别活动状态。
        """
        matrix_columns = ['platform'] + columns

        matrix = pd.DataFrame([
                [default_value] * (len(columns) + 1)
            ] * len(rows),
        )
        matrix.columns = matrix_columns
        matrix.platform = rows
        matrix.index = matrix.platform

        # 只作为index，从矩阵当中剔除
        matrix.pop('platform')
        return matrix


class ManualStrategy(BasicStrategy):
    """
    手动控制蹲饼逻辑 23.01.19

    """
    def __init__(self):
        super(ManualStrategy, self).__init__()

    def update(self, maintainer=None):
        '''
        更新操作分为2步:
        1. 更新真实蹲饼器的数据.
        2. 更新 MOOK 蹲饼器的数据.
        '''
        latest_config_pool = self._update(maintainer)
        mook_config_pool = self._update(maintainer=maintainer, is_mook=True)
        latest_config_pool['MOOK'] = mook_config_pool['MOOK']
        return latest_config_pool

    def _update(self, maintainer=None, is_mook=False):
        """
        实时状态从 maintainer 中获取.

        1. 初始化一个状态矩阵
        2. 使用被ban信息修正状态矩阵中的状态
        3. 对于每个平台，根据当前alive数量，取出对应的config.
        4. 根据group，分配config.
        5. 最后加入全局信息.

        # 返回值：以instance_id为key的各个蹲饼器配置dict.
        """

        #################### data update ##############################

        # 先取最新数据.
        self.get_latest_data()

        fetcher_config_df = self.data_pool.fetcher_config_df
        fetcher_datasource_config_df = self.data_pool.fetcher_datasource_config_df
        fetcher_global_config_df = self.data_pool.fetcher_global_config_df
        fetcher_platform_config_df = self.data_pool.fetcher_platform_config_df

        #################### status matrix update ##############################

        # 
        # fetcher_name_list = ['SilverAsh', 'Saria']
        fetcher_name_list = maintainer.alive_instance_id_list

        # 模拟平台只有一个蹲饼器.
        if is_mook:
            fetcher_name_list = ['MOOK']

        platform_identifiers = self.get_platform_identifiers()

        # 初始化状态矩阵
        self.status_matrix = self.initial_matrix(rows=platform_identifiers, columns=fetcher_name_list, default_value=1)

        # 平台被ban信息
        # ban_info = {'SilverAsh': [], 'Saria': ['weibo', 'netease-cloud-music']}
        ban_info = maintainer._failed_platform_by_instance
        
        # mook蹲饼器无ban信息.
        if is_mook:
            ban_info = {'MOOK': []}


        # 使用平台被ban信息更新状态矩阵
        self._update_matrix_with_ban_info(ban_info)

        ##################### construct config ############################
        """
        1. 以平台为单位，从 fetcher_config_df 当中依次获取各个datasource的config以及其他辅助信息。
        # 所需信息为: name, type(platform), datasource的列表, interval(可能为空)
        name: 'B站-明日方舟', type: 'bilibili', datasource: [ { uid: '161775300'}], interval: 30000
        """

        global df_tmp, matrix_datasource

        # 首先构建一个存储datasource级别config的matrix
        matrix_datasource = self.initial_matrix(rows=platform_identifiers, columns=fetcher_name_list, default_value=[])
        # print(matrix_datasource)
        undefined_platform_list = []

        # 首先计算某个平台现在可用的蹲饼器数量.
        for p in self.status_matrix.index:
            live_num_of_fetcher_p = int(self.status_matrix.loc[p].sum())   # 例如 = 2. 如果不足1, 直接报警.
            # print(live_num_of_fetcher_p)
            if live_num_of_fetcher_p < 1:
                logger.warning('# of available fetchers on platform: {} is smaller than 1.'.format(p))
                # 防止报错程序崩溃.
                # live_num_of_fetcher_p = 1

            # 然后去 fetcher_config_df 找 live_number 和 platform都能对上的
            # 如果设置的最大蹲饼器数量比实际蹲饼器数量还少，那只使用已设置了的蹲饼器数量的最大值。
            
            # 取出当前p对应的平台所有可能的蹲饼配置。
            df_tmp_cur_fetcher = fetcher_config_df[fetcher_config_df.platform == p]
            
            if df_tmp_cur_fetcher.shape[0]:

                max_set_live_number_of_platform = int(
                    df_tmp_cur_fetcher['live_number'].max()
                    )
            # 如果尚未配置这个蹲饼器的具体配置，则把它从 matrix_datasource 当中删掉。
            else:
                undefined_platform_list.append(p)
                continue

            df_tmp = fetcher_config_df[
                np.logical_and(fetcher_config_df.live_number == min(max_set_live_number_of_platform,
                                                                    live_num_of_fetcher_p),
                               fetcher_config_df.platform == p)
            ].copy()  # 注意copy出来，避免修改原始数据.
            print('#' * 20)
            print(df_tmp)
            # print('#' * 30)
            # print(p)
            # print(df_tmp)
            matrix_datasource = set_config_in_matrix_datasource(df_tmp,
                                                                fetcher_datasource_config_df,
                                                                live_num_of_fetcher_p,
                                                                p,
                                                                self.status_matrix,
                                                                matrix_datasource
                                                                )
            print('^' * 20)
            print('matrix_datasource 处理后:')
            print(matrix_datasource)

        # 把当前未配置的平台从matrix_datasource当中去掉。
        matrix_datasource = matrix_datasource[~matrix_datasource.index.isin(undefined_platform_list)]
        
        latest_config_pool = self.construct_config(matrix_datasource)
        # fetcher_config_pool.config_pool = latest_config_pool
        # print('&' * 20)
        # print(latest_config_pool)
        return latest_config_pool

    def _update_matrix_with_ban_info(self, ban_info):
        """
        维护 status_matrix 里各平台的ban状态.
        """
        for instance_id in ban_info:
            for platform_identifier in ban_info[instance_id]:
                self.status_matrix.loc[platform_identifier, instance_id] = 0

    def construct_config(self, cur_matrix_datasource):
        """
        组装最新的config. doing
        """
        config_pool = dict()

        # 给哪些蹲饼器生成配置
        fetcher_names = cur_matrix_datasource.columns.tolist()

        # 生成配置按平台取
        platforms = cur_matrix_datasource.index.tolist()

        # 取出全局配置.用dict存储
        global_config_df = self.data_pool.fetcher_global_config_df
        global_config_dict = dict()
        for idx in range(global_config_df.shape[0]):
            tmp_val = global_config_df.iloc[idx]['value']
            if tmp_val.isdigit():
                tmp_val = int(tmp_val)
            global_config_dict[global_config_df.iloc[idx]['key']] = tmp_val

        del global_config_df

        platform_config_df = self.data_pool.fetcher_platform_config_df
        platform_config_dict = dict()

        for idx in range(platform_config_df.shape[0]):
            platform_config_dict[platform_config_df.iloc[idx]['type_id']] = dict()

            platform_config_dict[
                platform_config_df.iloc[idx]['type_id']
            ]['min_request_interval'] = platform_config_df.iloc[idx]['min_request_interval']

        # print(platform_config_dict)

        del platform_config_df

        for cur_fetcher in fetcher_names:

            # 对每个fetcher构建一个config.
            tmp_config = dict()
            # 放入全局config:
            for k in global_config_dict:
                tmp_config[k] = global_config_dict[k]

            tmp_config['groups'] = []

            # groups放入这个蹲饼器独立的config.

            for cur_platform in platforms:

                # print(cur_platform, cur_fetcher)
                # cur_group_config = cur_matrix_datasource.loc[cur_platform, cur_fetcher]
                # if cur_group_config:
                #     tmp_config['groups'].append(cur_group_config)

                # 对一个platform 允许有多个group. 因此cur_matrix_datasource当中取出来的是多组分别的config.
                cur_groups_config_list = cur_matrix_datasource.loc[cur_platform, cur_fetcher]
                if cur_groups_config_list:
                    tmp_config['groups'].extend(cur_groups_config_list)

            # platform字段专门放最小间隔.
            tmp_config['platform'] = platform_config_dict

            config_pool[cur_fetcher] = tmp_config

        return config_pool

    def __repr__(self):
        return str(self.status_matrix)


def set_config_in_matrix_datasource(df_given_live_number,
                                    fetcher_datasource_config_df,
                                    live_num_of_fetcher_p,
                                    platform_identifier,
                                    status_matrix,
                                    matrix_datasource,
                                    ):
    """
    df_given_live_number: DataFrame, 给定live_number 和 platform 的 fetcher_config_df 片段.
    live_num_of_fetcher_p: 当前平台的存活数量.
    platform_identifier: 当前平台的名称
    status_matrix: 蹲饼器状态矩阵.
    matrix_datasource: DataFrame, 准备存入datasource_config的部分.
    """

    # fetcher_config_df = self.data_pool.fetcher_config_df
    # fetcher_datasource_config_df = self.data_pool.fetcher_datasource_config_df
    # fetcher_global_config_df = self.data_pool.fetcher_global_config_df
    # fetcher_platform_config_df = self.data_pool.fetcher_platform_config_df

    '''
    fetcher_idx: 需要配置 live_num_of_fetcher_p 个蹲饼器，当前配置的是第 fetcher_idx 个。
    physical_fetcher_idx: 一共有n个蹲饼器，其中一部分被ban了。当前配置的是第几个蹲饼器.

    '''

    physical_fetcher_idx = 0

    print('debug:存活蹲饼器数量大于配置数量时')
    print(live_num_of_fetcher_p)

    # print(status_matrix)

    if live_num_of_fetcher_p < 1:
        logger.warning('skip platform:{} config updating due to unavailable fetchers'.format(platform_identifier))
        return matrix_datasource
    for fetcher_idx in range(1, live_num_of_fetcher_p + 1):

        # 找到第一个有效的蹲饼器
        while not status_matrix.loc[platform_identifier][physical_fetcher_idx]:
            physical_fetcher_idx += 1

        df_given_live_number_fetcher_idx = df_given_live_number[
            df_given_live_number.fetcher_count == fetcher_idx
            ].copy()

        # 用 -1 填充未定义的部分，方便后续取值
        df_given_live_number_fetcher_idx.fillna(-1, inplace=True)
        # 如果不存在则跳过
        if df_given_live_number_fetcher_idx.shape[0] < 1: continue

        # print('########### debug:')
        # print(df_given_live_number_fetcher_idx)

        # table join 取出匹配的datasource configs
        df_tmp = fetcher_datasource_config_df.merge(df_given_live_number_fetcher_idx,
                                                    right_on='datasource_id',
                                                    left_on='id'
                                                    )
        # 同一个group_name的放在一起.
        df_tmp.sort_values('group_name', inplace=True)

        cur_datasource_config_list_by_group = []
        # 填表！
        # TODO: 一个平台，并非只有一个group，需要按平台聚合然后填入.
        # TODO: 填入 interval_by_time_range.

        for idx in range(df_tmp.shape[0]):
            group_name = df_tmp.iloc[idx]['group_name']
            platform = df_tmp.iloc[idx]['datasource']
            interval = df_tmp.iloc[idx]['interval']
            interval_by_time_range = df_tmp.iloc[idx]['interval_by_time_range']
            cur_config = df_tmp.iloc[idx]['config']

            # 换组：
            if idx == 0 or (idx != 0 and group_name != cur_datasource_config_list_by_group[-1]['name']):

                cur_datasource_config = {
                    'name': group_name,
                    'type': platform,
                    'datasource': [json.loads(cur_config)]
                }

                if interval > 0:
                    cur_datasource_config['interval'] = int(interval)

                if not str(interval_by_time_range) == '-1':
                    # print('debug:()')
                    # print(interval_by_time_range)
                    cur_datasource_config['interval_by_time_range'] = json.loads(interval_by_time_range)

                cur_datasource_config_list_by_group.append(cur_datasource_config)

            # 不换组
            else:
                cur_datasource_config['datasource'].extend([json.loads(cur_config)])


        matrix_datasource.loc[platform_identifier][physical_fetcher_idx] = cur_datasource_config_list_by_group
        # 看下一个蹲饼器
        physical_fetcher_idx += 1
    return matrix_datasource


# 用 df_tmp 和 各个df来构建config.
# live_num_of_fetcher_p = 2
# platform_identifier = 'arknights-website'
#
# set_config_in_matrix_datasource(df_tmp,
#                                 live_num_of_fetcher_p,
#                                 platform_identifier,
#                                 manual_strategy.status_matrix,
#                                 matrix_datasource
#                                )

manual_strategy = ManualStrategy()
# manual_strategy.update(maintainer=None)


