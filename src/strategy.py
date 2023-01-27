"""
生成config的策略实现.
"""

from src.db import *

import numpy as np


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
        self.data_pool = DataPool()
        self.matrix = None

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

        self.matrix = pd.DataFrame([
                                       [default_value] * (len(columns) + 1)
                                   ] * len(rows),
                                   )
        self.matrix.columns = matrix_columns
        self.matrix.platform = rows
        self.matrix.index = self.matrix.platform

        # 多余加这列
        self.matrix.pop('platform')


class ManualStrategy(BasicStrategy):
    """
    手动控制蹲饼逻辑 23.01.19

    """

    def __init__(self):
        super(ManualStrategy, self).__init__()

        # 初始化时执行一次

    def update(self, maintainer):
        """
        实时状态从 maintainer 中获取.
        1. 初始化一个状态矩阵
        2. 使用被ban信息修正状态矩阵中的状态
        """

        fetcher_name_list = ['SilverAsh', 'Saria']
        # fetcher_name_list = maintainer.alive_instance_id_list

        platform_identifiers = self.get_platform_identifiers()

        # 初始化状态矩阵
        self.initial_matrix(rows=platform_identifiers, columns=fetcher_name_list, default_value=1)

        # 平台被ban信息
        ban_info = {'SilverAsh': [], 'Saria': ['weibo', 'netease-cloud-music']}
        # failed_platform_by_instance = maintainer._failed_platform_by_instance
        self._update_matrix_with_ban_info(ban_info)
        # 使用平台被ban信息更新状态矩阵

    def _update_matrix_with_ban_info(self, ban_info):
        for instance_id in ban_info:
            for platform_identifier in ban_info[instance_id]:
                self.matrix.loc[platform_identifier, instance_id] = 0

    def __repr__(self):
        return str(self.matrix)


dp = DataPool()
manual_strategy = ManualStrategy()
manual_strategy.update(maintainer=None)



