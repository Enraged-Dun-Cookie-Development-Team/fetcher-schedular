import os
import sys
import time
import json
import traceback
import copy
import logging
import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.auto_sche.model_loader import MODEL_DICT
from src._conf_lib import AUTO_SCHE_CONFIG
import numpy as np
import pandas as pd
from tqdm import tqdm

from src._grpc_lib import messager


class FeatureProcesser:

    def __init__(self):
        pass

    def feature_combine(self):
        # 需要处理：12个feature
        X_list = []
        feature_num = 12
        datasource_num = AUTO_SCHE_CONFIG['DATASOURCE_POSSIBLE_NUMS'] # 域内实际出现过的蹲饼器编号数量.
        messager.send_to_bot_shortcut('适配的的数据源数量：{}'.format(datasource_num))
        messager.send_to_bot_shortcut('开始梳理时间相关的特征')

        time_points = self.feature_of_time()
        messager.send_to_bot_shortcut('梳理时间相关的特征完成')

        time_points_nums = len(time_points)

        for t_idx, t in tqdm(enumerate(time_points)):

            # 打印10次中间过程。
            if t_idx % (time_points_nums // 10) == 0:
                messager.send_to_bot_shortcut('合成最终特征中，进度{}/{}'.format(t_idx, time_points_nums))

            cur_feature = np.zeros([datasource_num, feature_num], dtype=int)

            # datasource_encoded
            cur_feature[:, 0] = np.arange(datasource_num)
            # time
            cur_feature[:, 5:11] = np.array(t, dtype=int)
            # weekday encoded
            cur_feature[:, 11] = MODEL_DICT['weekday_encoder'].transform([self._convert_date(*t[:3])])

            X_list.append(cur_feature)
        
        # 组织成dataframe用于模型输入.
        X_list = pd.DataFrame(np.concatenate(X_list))
        
        X_list.columns = ['datasource_encoded',
                          'is_top',
                          'is_retweeted',
                          'category_encoded',
                          'source_type_encoded',
                          'year',
                          'month',
                          'day',
                          'hour',
                          'minute',
                          'second',
                          'weekday_encoded']

        messager.send_to_bot_shortcut('完整的特征形状')
        messager.send_to_bot_shortcut(X_list.shape)

        return X_list

    def feature_of_time(self):
        scheduled_time = datetime.datetime.now().replace(hour=AUTO_SCHE_CONFIG['DAILY_PREPROCESS_TIME']['HOUR'],
                                                         minute=AUTO_SCHE_CONFIG['DAILY_PREPROCESS_TIME']['MINUTE'],
                                                         second=AUTO_SCHE_CONFIG['DAILY_PREPROCESS_TIME']['SECOND'],
                                                         microsecond=0)

        # 改为部署时预测一次.
        # scheduled_time = datetime.datetime.now()
        # # 如果当前时间已经过了今天的凌晨4点，那么将定时任务时间设置为明天的凌晨4点
        # if datetime.datetime.now() > scheduled_time:
        #     scheduled_time += datetime.timedelta(days=1)

        # 生成未来24小时的时间点，从凌晨4点开始，到凌晨4点结束（不包括）
        end_time = scheduled_time + datetime.timedelta(hours=2)
        time_points = self._generate_time_points(scheduled_time, end_time)

        return time_points

    def _generate_time_points(self, start_time, end_time, interval=1):
        """
        生成指定时间范围内的时间点列表。

        :param start_time: 起始时间
        :param end_time: 结束时间
        :param interval: 时间间隔（秒）
        :return: 时间点列表
        """
        time_points = []
        current_time = start_time
        while current_time < end_time:
            time_points.append((current_time.year,
                                current_time.month,
                                current_time.day,
                                current_time.hour,
                                current_time.minute,
                                current_time.second))

            current_time += datetime.timedelta(seconds=interval)

        return time_points

    @staticmethod
    def _convert_date(year, month, day):
        '''
        获取日期对应的星期几
        '''
        date = datetime.date(int(year), int(month), int(day))
        weekday = date.strftime("%A")  # %A表示完整的星期名称（如Monday）

        return weekday


# 以及用模型进行预测
# 记录开始执行的时间
start_time = datetime.datetime.now()


def model_predict():
    global predictions, start_time
    print("Model prediction triggered at {}".format(datetime.datetime.now()))

    # 先按每秒1个点写demo.
    predictions = [False] * 86400
    predictions[10] = True
    predictions[20] = True
    # 重新记录程序开始执行的时间
    start_time = datetime.datetime.now()
    

# 用于每日形成待预测的特征
feat_processer = FeatureProcesser()
