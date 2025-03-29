import json
from pathlib import Path
from joblib import dump
from encoder_kit import OrderedLabelEncoder  # 自定义编码器类

def load_encoder_config(config_path):
    """加载并验证编码器配置文件"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except Exception as e:
        raise ValueError(f"配置文件读取失败: {str(e)}")

    # 配置验证
    required_keys = ['classes']
    for encoder_name, params in config.items():
        if not isinstance(params, dict):
            raise ValueError(f"{encoder_name} 配置项必须为字典类型")
        if 'classes' not in params:
            raise ValueError(f"{encoder_name} 缺少必须的classes配置")
        if not isinstance(params['classes'], list):
            raise ValueError(f"{encoder_name}.classes 必须为列表类型")
    
    return config

def init_encoders(config_path, output_path="encoders.joblib"):
    """
    初始化并保存编码器集合
    发现可以保存到同一个joblib里。不需要分开保存.
    """
    config = load_encoder_config(config_path)
    
    encoders = {}
    for encoder_name, params in config.items():
        # 初始化编码器
        encoder = OrderedLabelEncoder(
            classes=params['classes'],
            default_label=params.get('default_label', -1),
            unknown_value=params.get('unknown_value', 'unknown')
        )
        encoder.fit([])  # 触发预定义类别加载
        
        encoders[encoder_name] = encoder
    
    # 保存编码器集合
    dump(encoders, output_path)
    print(f"成功初始化 {len(encoders)} 个编码器，已保存至 {output_path}")

if __name__ == "__main__":
    # 使用示例
    init_encoders(
        config_path="encoder_config.json",
        output_path="production_encoders.joblib"
    )