"""
实际使用前，通过校验不同渠道的数据特征是否匹配，double check机器学习模型生产的数据的有效性。
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np

# from scipy import stats # 后续的高阶校验

class ValidationResult:
    def __init__(self, is_valid: bool, errors: Dict[str, Any] = None):
        self.is_valid = is_valid
        self.errors = errors or {}
        
    def __bool__(self):
        return self.is_valid

class Validator(ABC):
    @abstractmethod
    def validate(self, data: pd.DataFrame, reference_data: pd.DataFrame = None) -> ValidationResult:
        pass

# 具体校验器实现
class FeaturePresenceValidator(Validator):
    def __init__(self, required_features: List[str]):
        self.required_features = set(required_features)
        
    def validate(self, data: pd.DataFrame, **kwargs) -> ValidationResult:
        missing = self.required_features - set(data.columns)
        return ValidationResult(
            is_valid=len(missing) == 0,
            errors={"missing_features": list(missing)} if missing else {}
        )
 
class DataTypeValidator(Validator):
    def __init__(self, expected_dtypes: Dict[str, np.dtype]):
        self.expected_dtypes = expected_dtypes
        
    def validate(self, data: pd.DataFrame, **kwargs) -> ValidationResult:
        errors = {}
        for feature, dtype in self.expected_dtypes.items():
            if feature not in data.columns:
                continue
            if data[feature].dtype != dtype:
                errors[feature] = f"Expected {dtype}, got {data[feature].dtype}"
        return ValidationResult(len(errors) == 0, errors)

class NumberEqualityValidator(Validator):
	"""
	数字是否相等校验。
	典型使用场景是按小时产出的模型预测结果是否与数据源（from数据库）所需的结果数量相匹配。
	"""
    def __init__(self, 
                 value1: float, 
                 value2: float, 
                 rel_tol: float = 1e-9,
                 abs_tol: float = 0.0):
        """
        :param value1: 第一个数值
        :param value2: 第二个数值
        :param rel_tol: 相对误差容忍度（默认1e-9）
        :param abs_tol: 绝对误差容忍度（默认0）
        """
        self.value1 = value1
        self.value2 = value2
        self.rel_tol = rel_tol
        self.abs_tol = abs_tol
        
    def validate(self, **kwargs) -> ValidationResult:
        is_close = np.isclose(
            self.value1, 
            self.value2,
            rtol=self.rel_tol,
            atol=self.abs_tol
        )
        
        if is_close:
            return ValidationResult(True)
            
        return ValidationResult(
            False,
            errors={
                "value1": self.value1,
                "value2": self.value2,
                "tolerance": {
                    "relative": self.rel_tol,
                    "absolute": self.abs_tol
                }
            }
        )


class RangeValidator(Validator):
    def __init__(self, feature_ranges: Dict[str, tuple]):
        self.feature_ranges = feature_ranges
        
    def validate(self, data: pd.DataFrame, **kwargs) -> ValidationResult:
        errors = {}
        for feature, (min_val, max_val) in self.feature_ranges.items():
            if feature not in data.columns:
                continue
                
            invalid_count = ((data[feature] < min_val) | (data[feature] > max_val)).sum()
            if invalid_count > 0:
                errors[feature] = {
                    'violations': invalid_count,
                    'valid_range': [min_val, max_val]
                }
        return ValidationResult(len(errors) == 0, errors)

# 组合校验器：
class CompositeValidator(Validator):
    def __init__(self, validators: List[Validator]):
        self.validators = validators
        
    def validate(self, data: pd.DataFrame, **kwargs) -> ValidationResult:
        all_errors = {}
        is_valid = True
        
        for validator in self.validators:
            result = validator.validate(data, **kwargs)
            if not result.is_valid:
                is_valid = False
                all_errors.update({
                    type(validator).__name__: result.errors
                })
                
        return ValidationResult(is_valid, all_errors)


if __name__ == "__main__":
    
    # 先用简单的示例数据测试上述validator的正确性
    train_data = pd.DataFrame({
        'age': np.random.normal(40, 5, 1000),
        'income': np.random.lognormal(3, 0.3, 1000)
    })
    
    prod_data = pd.DataFrame({
        'age': np.random.normal(38, 6, 500),
        'income': np.random.lognormal(3.2, 0.4, 500),
        'new_feature': np.random.rand(500)
    })
    
    # 构建验证器
    validators = CompositeValidator([
        FeaturePresenceValidator(['age', 'income']),
        DataTypeValidator({'age': np.float64, 'income': np.float64}),
        RangeValidator({'age': (0, 120), 'income': (0, 1000000)}),

    ])
    
    # 执行验证
    result = validators.validate(prod_data, reference_data=train_data)
    
    if result:
        print("数据验证通过")
    else:
        print("数据验证失败，错误详情:")
        for validator_name, errors in result.errors.items():
            print(f"[{validator_name}]:")
            for feature, info in errors.items():
                print(f"  {feature}: {info}")

    # 关于数值相等性的验证：
    # 数值简单相等验证
    validator = NumberEqualityValidator(5.0, 5.0)
    print(validator.validate())  # 验证通过
    
    # 数值带容差的浮点数验证
    validator = NumberEqualityValidator(
        value1=1.0000001,
        value2=1.0000002,
        rel_tol=1e-6
    )
    print(validator.validate())  # 验证通过
    
    # 数值验证失败案例
    validator = NumberEqualityValidator(3.14, 3.1416)
    result = validator.validate()
    if not result:
        print(f"验证失败，差异详情：{result.errors}")