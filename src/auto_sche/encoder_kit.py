import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.utils.validation import column_or_1d
from joblib import dump, load

class OrderedLabelEncoder(LabelEncoder):
    """
    老版LabelEncoder是根据原始数据value在样本中出现的先后顺序来依次编码的。
    不直观，难以管理 & debug。
    更新一版，支持预定义顺序和未知类别处理。
    继承LabelEncoder，支持所有原始的method.
    """
    def __init__(self, classes=None, default_label=-1, unknown_value="unknown"):
        super().__init__()
        self.predefined_classes = classes
        self.default_label = default_label
        self.unknown_value = unknown_value
    
    def fit(self, y):
        # 强制使用预定义类别
        if self.predefined_classes is not None:
            self.classes_ = np.array(self.predefined_classes)
        else:
            super().fit(y)
        return self
    
    def transform(self, y):
        y = column_or_1d(y, warn=True)
        y = np.asarray(y)
        
        # 创建布尔掩码标识已知类别
        known_mask = np.isin(y, self.classes_)
        
        # 初始化结果数组
        transformed = np.full_like(y, self.default_label, dtype=int)
        
        # 处理已知类别
        known_values = y[known_mask]
        if known_values.size > 0:
            transformed[known_mask] = super().transform(known_values)
        
        return transformed
    
    def inverse_transform(self, y):
        y = np.asarray(y)
        
        # 初始化结果数组
        inversed = np.full_like(y, self.unknown_value, dtype=object)
        
        # 创建有效值掩码
        valid_mask = (y != self.default_label) & (y >= 0) & (y < len(self.classes_))
        
        # 处理有效值
        valid_indices = y[valid_mask]
        if valid_indices.size > 0:
            inversed[valid_mask] = self.classes_[valid_indices]
        
        return inversed

# 使用示例
if __name__ == "__main__":

    # deepseek帮写的test case.
    
    # 1. 初始化编码器
    encoder = OrderedLabelEncoder(
        classes=['周一', '周二', '周三', '周四', '周五', '周六', '周日'],
        default_label=-1,
        unknown_value="UNK"
    )
    encoder.fit([])  # 空fit，触发预定义类别

    # 2. 编码测试
    test_data = ['周一', '周三', '周八', '周二']
    encoded = encoder.transform(test_data)
    print("Encoded:", encoded)  # 输出: [0 2 -1 1]

    # 3. 解码测试
    decoded = encoder.inverse_transform([0, 2, -1, 100])
    print("Decoded:", decoded)  # 输出: ['周一' '周三' 'UNK' 'UNK']

    # 4. 保存加载测试
    dump(encoder, 'enhanced_encoder.joblib')
    loaded_encoder = load('enhanced_encoder.joblib')
    
    print("\nLoaded encoder test:")
    print(loaded_encoder.transform(['周日', '周九']))  # 输出: [6 -1]
    print(loaded_encoder.inverse_transform([5, -1])) # 输出: ['周六' 'UNK']