import os
import sys
import time
import json
import traceback
import copy
import logging

# 调整路径，确保项目根目录在Python路径中
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)
sys.path.append(grandparent_dir)
sys.path.append(parent_dir)
sys.path.append(current_dir)  # 确保当前目录也在路径中

from joblib import dump, load
# 明确导入所需的类，确保在反序列化时能被找到
from .encoder_kit import OrderedLabelEncoder
import src.auto_sche.encoder_kit as encoder_kit  # 使用绝对导入


class ModelLoader:
    def __init__(self):
        self.model_dict = dict()
        self.load_all_model()

    def load_all_model(self):
        for name in [
            # 'decision_tree_model_v2',
            'weekday_encoder_v3',
            'datasource_encoder_v3'
        ]:
            self.load_model(name)

    def load_model(self, name, suffix='.joblib', path_prefix=''):
        # 加载模型、编码器等等
        loaded_model = load('{}./ml_model/{}{}'.format(path_prefix, name, suffix))
        self.model_dict[name.split('_v')[0]] = loaded_model

    def __getitem__(self, key):
        return self.model_dict.get(key)


MODEL_DICT = ModelLoader()
