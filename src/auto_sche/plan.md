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