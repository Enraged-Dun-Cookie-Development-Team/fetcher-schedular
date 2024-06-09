import os
import sys
import time
import json
import traceback
import copy
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from joblib import dump, load


class ModelLoader:
    def __init__(self):
        self.model_dict = dict()
        self.load_all_model()

    def load_all_model(self):
        for name in [
            'decision_tree_model',
            'weekday_encoder',
            'datasource_encoder'
        ]:
            self.load_model(name)

    def load_model(self, name, suffix='.joblib'):
        # 加载模型、编码器等等
        loaded_model = load('{}{}'.format(name, suffix))
        self.model_dict[name] = loaded_model

    def __getitem__(self, key):
        return self.model_dict.get(key)


MODEL_DICT = ModelLoader()