# 总体思路
1. 蹲饼器可能出现被ban、重启等情况，老方案已明确
    * 在出现上述情况时，如何进行调整，即更新配置
    * 轮询确保蹲饼器健康

2. 新方案则明确了蹲饼器在时间上的策略。减小蹲饼次数

3. 最小改动：
    * 新增一个蹲饼post接口，通知蹲饼器需要蹲饼了
    * 一些串联函数，通过数据源，索引得到当前时刻需要蹲饼的蹲饼器。思路：
        - 从最新配置拿到每个蹲饼器蹲哪些饼
        - 发送post请求.

    * 需要使用 encoder.classes_ 索引出模型输入的 feature 实际对应哪个 datasource。
        - 额外加第n + 1类对应的default配置。
    * MODEL_DICT 的初始化，升级成 manager.

feature process + model prediction + post rules

* feature process(离线部分)

新旧代码替换对模型效果无影响。但可以保证处理后的feature仍有高可读性，例如周一就是0，周日就是6. 
方便后序蹲饼策略使用，降低开发阶段的认知成本。

```python3
## 旧
le1 = LabelEncoder()  
df_2['weekday_encoded'] = le1.fit_transform(df_2['weekday'])  # 出现序


## 新
from src.auto_sche.encoder_kit import OrderedLabelEncoder # 实现的自排序编码器

encoder = OrderedLabelEncoder(
    classes=['Monday', 'Truesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
    default_label=-1,
    unknown_value="UNK"
)
encoder.fit([])  # 空fit，触发预定义类别

df_2['weekday_encoded'] = le1.transform(df_2['weekday'])  

# 离线训练的配置
classes=[i for i in range(AUTO_SCHE_CONFIG['DATASOURCE_POSSIBLE_NUMS'])]

```

