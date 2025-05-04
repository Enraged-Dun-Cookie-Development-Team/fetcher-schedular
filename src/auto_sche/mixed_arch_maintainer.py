"""
以ml model + rules 混合模式控制请求。
"""

class RuleEngine:
    def __init__(self, prediction_service, rule_loader):
        self.static_rules = []  # 预计算规则
        self.dynamic_rules = []  # 实时规则
        self.rule_loader = rule_loader
        self.prediction_service = prediction_service
        self._load_initial_rules()

    def _load_initial_rules(self):
        """初始化加载规则"""
        pass

    def precompute_static_actions(self):
        """预计算静态规则结果"""
        actions = []
        pass

    def evaluate_realtime(self, user_id, current_ts):
        """实时规则评估"""
        results = []
        pass


class StaticRule:
    def evaluate(self, user):
        """预计算评估（先实现了时间窗口规则）"""
        triggers = []
        for target_time in self.params['target_times']:
            window_start = calc_time(target_time, -self.params['pre_sec'])
            window_end = calc_time(target_time, self.params['post_sec'])
            
            # 检查预测数据中是否存在1
            predictions = self.prediction_service.get_range(
                user.id, window_start, window_end)
            if any(p.value == 1 for p in predictions):
                triggers.append(TriggerAction(
                    time=target_time,
                    rule=self,
                    user=user
                ))
        return triggers


class DynamicRule:
    def evaluate(self, ctx):
        """动态评估（以频率控制为例。例如晚上10点之后减慢频率）"""
        # 获取过去N小时的触发记录
        history = get_trigger_history(ctx.data_source_id, ctx.current_ts - 24*3600)
        
        # 应用滑动窗口算法
        window_start = ctx.current_ts - self.params['window_sec']
        recent_triggers = [t for t in history if t.timestamp >= window_start]
        
        # 检查频率限制
        return len(recent_triggers) < self.params['max_calls']


# 需要有一套coodinator来执行与协调所有的规则。
class DecisionCoordinator:
    def __init__(self, rule_engine):
        self.rule_engine = rule_engine
        self.precomputed = load_precomputed_actions()


    def get_decision(self, user_id, current_ts):
        # 1. 检查预计算结果
        if pre := self._check_precomputed(user_id, current_ts):
            return pre

        # 2. 实时计算动态规则
        dynamic_result = self.rule_engine.evaluate_realtime(user_id, current_ts)
        
        # 3. 冲突解决（静态规则优先）
        return self._resolve_conflicts(pre, dynamic_result)

    # TODO: 具体实现。
