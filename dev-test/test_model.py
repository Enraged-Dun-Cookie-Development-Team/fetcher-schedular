# 测试模型预测结果是否符合预期
import os
import sys
import time
import json
import traceback

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.auto_sche.model_loader import MODEL_DICT

import pandas as pd


""" 当前feature列表
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
"""

test_data = pd.DataFrame(
    [
        {'datasource_encoded': 12,
                          'is_top': 0,
                          'is_retweeted': 0,
                          'category_encoded': 0,
                          'source_type_encoded': 0,
                          'year': 2025,
                          'month': 1,
                          'day': 1,
                          'hour': 15,
                          'minute': 0,
                          'second': 5,
                          'weekday_encoded': 2},
    ]
)

# 端到端输出结果
print(MODEL_DICT['decision_tree_model'].predict(test_data))

# 看 datasource_encoder 输出结果
print(MODEL_DICT['datasource_encoder'].inverse_transform(12))


