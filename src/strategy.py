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
        self.get_latest_data()
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

    def update(self, maintainer=None, fetcher_config_pool=None):
        """
        实时状态从 maintainer 中获取.
        结果存入 fetcher_config_pool.config_pool # key in [fetcher_instance_ids]

        1. 初始化一个状态矩阵
        2. 使用被ban信息修正状态矩阵中的状态
        3. 对于每个平台，根据当前alive数量，取出对应的config.
        4. 根据group，分配config.
        5. 最后加入全局信息.
        """

        #################### data update ##############################

        # 先取最新数据.
        self.get_latest_data()

        fetcher_config_df = self.data_pool.fetcher_config_df
        fetcher_datasource_config_df = self.data_pool.fetcher_datasource_config_df
        fetcher_global_config_df = self.data_pool.fetcher_global_config_df
        fetcher_platform_config_df = self.data_pool.fetcher_platform_config_df


        #################### status matrix update ##############################

        fetcher_name_list = ['SilverAsh', 'Saria']
        # fetcher_name_list = maintainer.alive_instance_id_list

        platform_identifiers = self.get_platform_identifiers()

        # 初始化状态矩阵
        self.status_matrix = self.initial_matrix(rows=platform_identifiers, columns=fetcher_name_list, default_value=1)

        # 平台被ban信息
        ban_info = {'SilverAsh': [], 'Saria': ['weibo', 'netease-cloud-music']}
        # ban_info = maintainer._failed_platform_by_instance

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

        # 首先计算某个平台现在可用的蹲饼器数量.
        for p in self.status_matrix.index:
            live_num_of_fetcher_p = self.status_matrix.loc[p].sum() # 例如 = 2
            # 然后去 fetcher_config_df 找 live_number 和 platform都能对上的
            df_tmp = fetcher_config_df[
                np.logical_and(fetcher_config_df.live_number == live_num_of_fetcher_p,
                              fetcher_config_df.platform == p)
            ].copy() # 注意copy出来，避免修改原始数据.

            print('#' * 30)
            print(p)
            print(df_tmp)
            matrix_datasource = set_config_in_matrix_datasource(df_tmp,
                                            live_num_of_fetcher_p,
                                            p,
                                            self.status_matrix,
                                            matrix_datasource
                                           )
            print('^' * 20)
            print('matrix_datasource 处理后:')
            print(matrix_datasource)
        return matrix_datasource

    def _update_matrix_with_ban_info(self, ban_info):
        """
        维护 status_matrix 里各平台的ban状态.
        """
        for instance_id in ban_info:
            for platform_identifier in ban_info[instance_id]:
                self.status_matrix.loc[platform_identifier, instance_id] = 0

    def __repr__(self):
        return str(self.status_matrix)

manual_strategy = ManualStrategy()
manual_strategy.update(maintainer=None)


