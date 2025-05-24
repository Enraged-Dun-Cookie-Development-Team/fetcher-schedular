from datetime import datetime, timedelta
import bisect
from collections import defaultdict

class RuleEngine:
    def __init__(self, prediction_service, rule_loader):
        self.static_rules = []  # 预计算规则
        self.dynamic_rules = []  # 实时规则
        self.rule_loader = rule_loader
        self.prediction_service = prediction_service
        self._load_initial_rules()
        
        # 动态规则所需的映射关系：source_id -> 相关的target_ids列表
        self.activation_mapping = defaultdict(list)

    def _load_initial_rules(self):
        """初始化加载规则"""
        # 加载静态规则：检查过去10秒内是否被激活
        static_rule = StaticRule({
            'type': 'recent_activation',
            'window_size': 10,  # 10秒窗口
            'threshold': 1     # 至少1次激活
        }, self.prediction_service)
        self.static_rules.append(static_rule)
        
        # 加载动态规则：激活传播规则
        dynamic_rule = DynamicRule({
            'type': 'activation_propagation',
            'window_size': 300,  # 5分钟传播窗口
            'mapping': self.activation_mapping
        })
        self.dynamic_rules.append(dynamic_rule)
        
        # 从规则加载器加载配置
        self.rule_loader.load_rules(self)

    def precompute_static_actions(self, current_ts):
        """预计算静态规则结果"""
        actions = []
        for rule in self.static_rules:
            # 对所有已知数据源执行静态规则评估
            for datasource_id in self.prediction_service.get_known_datasources():
                triggers = rule.evaluate(datasource_id, current_ts)
                actions.extend(triggers)
        return actions

    def evaluate_realtime(self, datasource_id, current_ts):
        """实时规则评估"""
        results = []
        for rule in self.dynamic_rules:
            ctx = EvaluationContext(datasource_id, current_ts, self.prediction_service)
            if rule.evaluate(ctx):
                results.append(TriggerAction(
                    time=current_ts,
                    rule=rule,
                    datasource_id=datasource_id,
                    action_type='realtime'
                ))
        return results

    def add_activation_mapping(self, source_id, target_ids):
        """添加激活传播映射关系"""
        self.activation_mapping[source_id].extend(target_ids)


class StaticRule:
    def __init__(self, params, prediction_service):
        self.params = params
        self.prediction_service = prediction_service

    def evaluate(self, datasource_id, current_ts):
        """预计算评估：检查过去10秒内是否被激活"""
        triggers = []
        
        if self.params['type'] == 'recent_activation':
            window_start = current_ts - self.params['window_size']
            window_end = current_ts
            
            # 获取预测数据
            predictions = self.prediction_service.get_range(
                datasource_id, window_start, window_end)
            
            # 检查是否满足阈值
            activation_count = sum(1 for p in predictions if p.value == 1)
            if activation_count >= self.params['threshold']:
                triggers.append(TriggerAction(
                    time=current_ts,
                    rule=self,
                    datasource_id=datasource_id,
                    action_type='static'
                ))
        
        return triggers


class DynamicRule:
    def __init__(self, params):
        self.params = params

    def evaluate(self, ctx):
        """动态评估：激活传播规则"""
        if self.params['type'] == 'activation_propagation':
            # 检查当前数据源是否被激活
            current_prediction = ctx.get_prediction(ctx.datasource_id, ctx.current_ts)
            if current_prediction and current_prediction.value == 1:
                # 获取映射的目标数据源ID
                target_ids = self.params['mapping'].get(ctx.datasource_id, [])
                for target_id in target_ids:
                    # 为每个目标数据源创建未来触发动作
                    ctx.add_future_action(
                        target_id, 
                        ctx.current_ts + self.params['window_size'],
                        'propagation'
                    )
                return True
        return False


class DecisionCoordinator:
    def __init__(self, rule_engine):
        self.rule_engine = rule_engine
        self.precomputed = {}  # 存储预计算结果
        self.future_actions = defaultdict(list)  # 存储未来动作

    def precompute(self, current_ts):
        """执行预计算并存储结果"""
        actions = self.rule_engine.precompute_static_actions(current_ts)
        self.precomputed = {(action.datasource_id, action.time): action for action in actions}
        
        # 清理过期的未来动作
        now = current_ts
        self.future_actions = defaultdict(
            list, 
            {k: [a for a in v if a['time'] > now] 
             for k, v in self.future_actions.items()}
        )

    def get_decision(self, datasource_id, current_ts):
        """获取决策结果"""
        # 1. 检查预计算结果
        if (datasource_id, current_ts) in self.precomputed:
            return self.precomputed[(datasource_id, current_ts)]

        # 2. 检查是否有传播的未来动作
        future_actions = self.future_actions.get(datasource_id, [])
        for action in future_actions:
            if action['time'] == current_ts:
                return TriggerAction(
                    time=current_ts,
                    rule=None,  # 传播动作没有特定规则
                    datasource_id=datasource_id,
                    action_type='propagation'
                )

        # 3. 实时计算动态规则
        dynamic_results = self.rule_engine.evaluate_realtime(datasource_id, current_ts)
        if dynamic_results:
            # 假设只返回第一个结果，实际可能需要更复杂的冲突解决
            return dynamic_results[0]

        return None  # 没有触发任何规则


class EvaluationContext:
    """评估上下文，提供评估过程中所需的数据和功能"""
    def __init__(self, datasource_id, current_ts, prediction_service):
        self.datasource_id = datasource_id
        self.current_ts = current_ts
        self.prediction_service = prediction_service
        self.future_actions = []  # 存储需要创建的未来动作

    def get_prediction(self, datasource_id, timestamp):
        """获取特定时间的预测结果"""
        predictions = self.prediction_service.get_range(datasource_id, timestamp, timestamp)
        return predictions[0] if predictions else None

    def add_future_action(self, datasource_id, timestamp, action_type):
        """添加未来动作"""
        self.future_actions.append({
            'datasource_id': datasource_id,
            'time': timestamp,
            'type': action_type
        })


class TriggerAction:
    """触发动作，表示需要对数据源执行的操作"""
    def __init__(self, time, rule, datasource_id, action_type):
        self.time = time
        self.rule = rule
        self.datasource_id = datasource_id
        self.action_type = action_type  # 'static', 'realtime', 'propagation'

    def __repr__(self):
        return f"TriggerAction(time={self.time}, datasource_id={self.datasource_id}, type={self.action_type})"


def main():
    # 创建预测服务实例
    prediction_service = PredictionService()
    
    # 创建规则加载器
    rule_loader = RuleLoader()
    
    # 创建规则引擎
    rule_engine = RuleEngine(prediction_service, rule_loader)
    
    # 添加激活传播映射关系
    rule_engine.add_activation_mapping(1, [2, 3])  # 数据源1激活会触发数据源2和3
    rule_engine.add_activation_mapping(4, [5])    # 数据源4激活会触发数据源5
    
    # 创建决策协调器
    coordinator = DecisionCoordinator(rule_engine)
    
    # 当前时间
    current_ts = int(datetime.now().timestamp())
    
    # 预计算
    coordinator.precompute(current_ts)
    
    # 对数据源1获取决策
    decision = coordinator.get_decision(1, current_ts)
    if decision:
        print(f"对数据源 {decision.datasource_id} 在时间 {decision.time} 触发操作 (类型: {decision.action_type})")
    else:
        print(f"未对数据源1触发任何操作")


# 预测服务(是否要替换成 预测结果 的数据class，而非做出预测的这个动作的class)
class PredictionService:
    """预测服务接口，提供对数据源激活状态的预测"""
    def get_range(self, datasource_id, start_ts, end_ts):
        return []
    
    def get_known_datasources(self):
        """获取所有已知数据源ID"""
        return []


# 规则加载器实现
class RuleLoader:
    """规则加载器，从配置文件或其他来源加载规则"""
    def load_rules(self, rule_engine):
        """加载规则到规则引擎"""
        pass


if __name__ == "__main__":
    main()