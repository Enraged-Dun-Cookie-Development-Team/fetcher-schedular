import os
import sys
import time
import json
import traceback
import copy
import logging
import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.auto_sche.encoder_kit import OrderedLabelEncoder 


from joblib import load
import pandas as pd

class EncoderManager:
    """编码器管理工具类"""
    def __init__(self, encoder_path):
        self.encoders = load(encoder_path)
        self._validate_encoders()
    
    def _validate_encoders(self):
        """验证加载的编码器有效性"""
        for name, encoder in self.encoders.items():
            if not hasattr(encoder, 'classes_'):
                raise ValueError(f"编码器 {name} 未正确初始化")
    
    def get_encoder(self, name: str) -> OrderedLabelEncoder:
        """获取指定编码器"""
        encoder = self.encoders.get(name)
        if not encoder:
            raise KeyError(f"找不到编码器: {name}")
        return encoder
    
    def encode_dataframe(self, df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
        """
        批量编码DataFrame
        :param mapping: 列名到编码器名称的映射，例如
            {'weekday_col': 'weekday_encoding'}
        """
        encoded_df = df.copy()
        for col, encoder_name in mapping.items():
            encoder = self.get_encoder(encoder_name)
            encoded_df[col] = encoder.transform(df[col])
        return encoded_df

# 使用示例
if __name__ == "__main__":
    # 初始化管理器
    manager = EncoderManager("production_encoders.joblib")
    
    # 原始数据
    data = pd.DataFrame({
        'weekday': ['周一', '周三', '周八', '周二'],
        'test_month': ['一月', '三月', '八月', '闰二月'],

    })
    
    # 定义编码映射关系
    encoding_mapping = {
        'weekday': 'weekday_encoding',
        'test_month': 'test_month_encoding'
    }
    
    # 执行编码
    encoded_data = manager.encode_dataframe(data, encoding_mapping)
    
    print("\n编码结果：")
    print(encoded_data)
    
    # 解码演示
    decoder = manager.get_encoder('weekday_encoding')
    print("\n解码示例：")
    print(decoder.inverse_transform([0, 1, 6, -1]))