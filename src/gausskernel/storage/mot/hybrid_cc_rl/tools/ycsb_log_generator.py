import pandas as pd
import numpy as np
import random
import time
import os
from tqdm import tqdm

# --- 1. 全局配置 (Configuration) ---
# 你可以在这里调整所有参数来生成不同的模拟数据
CONFIG = {
    # 负载配置
    'workload_type': 'YCSB', # 用于日志中的txn_type
    'num_transactions': 100,  # 总共生成多少条事务日志
    'num_records': 100,        # 数据库中的总记录数
    
    # 🆕 工作负载多样性配置（让 ops_per_txn 变化更大）
    'min_ops_per_txn': 2,      # 最少操作数（小事务）
    'max_ops_per_txn': 20,     # 最多操作数（大事务）
    
    'read_ratio': 0.95,         # 读操作比例 (YCSB-A: 0.5, YCSB-B: 0.95, YCSB-C: 1.0)
    'zipf_alpha': 0.99,         # Zipfian分布参数, 越大表示冲突越集中 (0表示均匀分布) - 增大以产生更多热点
    
    # 🆕 增强的冲突配置（让 contention 变化更大）
    'base_conflict_probability': 0.05,  # 基础冲突概率
    'hot_key_conflict_multiplier': 8,    # 热点key的冲突倍增器

    # 交互式事务模拟配置（扩大范围）
    'min_think_time_ms': 1,    # 最小思考时间(毫秒) - 降低以产生更快的事务
    'max_think_time_ms': 100,   # 最大思考时间(毫秒) - 提高以产生更慢的事务

    # 特征离散化边界 (与你的config/default.yaml配置对应)
    'tiers': {
        'contention': [2, 5],          # 0-2: low contention, 3-5: medium, >5: high
        'workload': [5, 15],           # 0-5: small workload, 6-15: medium, >15: large
        'retry': [1, 3],               # 对应 Tier-0 (0), Tier-1 (1-2), Tier-2 (3+)
        'exec_time': [19.59, 80.6],    # 根据实际数据动态调整（可用analyze_tiers.py分析）
        'query_interval': [3.07, 8.66], # 根据实际数据动态调整（可用analyze_tiers.py分析）
        'priority_score': [1000000000000000, 5000000000000000],  # Low, Medium, High priority
    },

    # 模拟系统状态
    'initial_global_tps': 50000,
    'initial_global_abort_rate': 0.05,
    
    # 🆕 Action 平衡模式（确保各种 action 都有一定比例）
    'enable_action_balancing': True,  # 是否启用action平衡模式
    'target_action_distribution': {   # 目标action分布（比例）
        0: 0.20,  # Stay Optimistic - 20%
        1: 0.15,  # Lock Hot Write Set - 15%
        2: 0.15,  # Lock All Hot Sets - 15%
        3: 0.15,  # Lock All Writes & Hot Reads - 15%
        4: 0.20,  # Lock Entire Access Set - 20%
        5: 0.15,  # Prioritize Transaction Only - 15%
    },

    # 输出文件（相对于项目根目录的input文件夹）
    'output_file': None  # 将在main()中动态设置
}

# --- 2. 模拟数据库核心 (Simplified Database Core) ---
class MockDB:
    def __init__(self, num_records, workload_gen):
        self.records = {f"key_{i}": {'value': f"value_{i}", 'version': 0} for i in range(num_records)}
        self.hot_records = set()
        self.workload_gen = workload_gen
        self.access_count = {}  # 跟踪每个key的访问次数

    def read(self, key):
        # 统计访问次数
        self.access_count[key] = self.access_count.get(key, 0) + 1
        # 如果访问次数超过阈值，标记为热点
        if self.access_count[key] > 5:
            self.hot_records.add(key)
        return self.records[key]

    def write(self, key, new_version):
        self.records[key]['version'] = new_version
        self.access_count[key] = self.access_count.get(key, 0) + 1
        if self.access_count[key] > 5:
            self.hot_records.add(key)
        return True
    
    def is_hot_key(self, key):
        """判断是否为热点key"""
        return key in self.hot_records
    
    def simulate_concurrent_write(self, prefer_hot=False):
        """
        模拟一个后台线程随机完成了一次写操作
        prefer_hot: 是否更倾向于修改热点数据
        """
        if prefer_hot and self.hot_records and random.random() < 0.7:
            # 70%概率选择热点key进行并发写
            key_to_update = random.choice(list(self.hot_records))
        else:
            key_to_update = self.workload_gen.generate_ops(num_ops=1)[0][1]
        
        new_version = int(time.time() * 1000) + random.randint(0, 1000)
        self.write(key_to_update, new_version)

# --- 3. 决策逻辑 (Decision Logic) ---
def enhanced_decision_logic(txn_profile: dict, force_action: int = None) -> int:
    """
    增强的决策逻辑，能够生成更多样化的action分布。
    
    Args:
        txn_profile: 事务特征字典
        force_action: 如果指定，强制返回该action（用于平衡模式）
    
    Returns:
        int: action编号 (0-5)
    """
    if force_action is not None:
        return force_action
    
    retries = txn_profile['retry_count']
    hot_visits = txn_profile['hot_visits']
    workload_size = txn_profile['read_set_size'] + txn_profile['write_set_size']
    priority_score = txn_profile.get('priority_score', 0)
    
    # 引入随机性，避免过于确定性的决策
    randomness = random.random()
    
    # 🎯 策略1: 高重试次数 -> 强锁策略或提升优先级
    if retries >= 3:
        # 重试3次以上：60% Action-4（全锁），30% Action-5（仅优先级），10%其他
        if randomness < 0.6:
            return 4  # Lock Entire Access Set
        elif randomness < 0.9:
            return 5  # Prioritize Transaction Only
        else:
            return 3  # Lock All Writes & Hot Reads
    
    if retries == 2:
        # 重试2次：40% Action-4, 30% Action-3, 20% Action-5, 10%其他
        if randomness < 0.4:
            return 4
        elif randomness < 0.7:
            return 3
        elif randomness < 0.9:
            return 5
        else:
            return 2
    
    if retries == 1:
        # 重试1次：考虑热点和负载
        if hot_visits >= 3:
            return 3  # 热点多 -> 锁写+热读
        elif workload_size > 15:
            return 3  # 负载大 -> 锁写+热读
        else:
            # 30% Action-3, 30% Action-2, 20% Action-5, 20%其他
            if randomness < 0.3:
                return 3
            elif randomness < 0.6:
                return 2
            elif randomness < 0.8:
                return 5
            else:
                return 1
    
    # 🎯 策略2: 基于热点访问的细粒度控制（无重试情况）
    if hot_visits >= 5:
        # 访问很多热点 -> 高风险
        if workload_size > 15:
            return 4  # 负载也大 -> 全锁
        elif workload_size > 10:
            return 3  # 负载中等 -> 锁写+热读
        else:
            return 2  # 负载小 -> 锁热点集
    
    if hot_visits >= 3:
        # 访问中等热点
        if workload_size > 15:
            return 3  # 负载大 -> 锁写+热读
        elif workload_size > 8:
            return 2  # 负载中等 -> 锁热点集
        else:
            # 考虑优先级策略
            if randomness < 0.3:
                return 5  # 30% 仅提升优先级
            else:
                return 1  # 70% 锁热写集
    
    if hot_visits >= 1:
        # 少量热点接触
        if workload_size > 18:
            return 2  # 负载很大 -> 锁热点集
        elif workload_size > 12:
            return 1  # 负载大 -> 锁热写集
        else:
            # 引入更多随机性
            if randomness < 0.2:
                return 5  # 20% 仅优先级
            elif randomness < 0.5:
                return 1  # 30% 锁热写集
            else:
                return 0  # 50% 保持乐观
    
    # 🎯 策略3: 无热点但负载大的情况
    if workload_size > 18:
        # 即使无热点，负载巨大也可能需要保护
        if randomness < 0.3:
            return 2  # 30% 锁热点集（预防性）
        elif randomness < 0.5:
            return 5  # 20% 提升优先级
        else:
            return 0  # 50% 保持乐观
    
    if workload_size > 12:
        # 负载较大
        if randomness < 0.2:
            return 1  # 20% 锁热写集
        elif randomness < 0.3:
            return 5  # 10% 优先级
        else:
            return 0  # 70% 保持乐观
    
    # 🎯 策略4: 低风险场景（默认）
    # 即使是低风险，也给其他action一些机会
    if randomness < 0.75:
        return 0  # 75% 保持乐观
    elif randomness < 0.85:
        return 5  # 10% 仅优先级（尝试优化延迟）
    elif randomness < 0.95:
        return 1  # 10% 锁热写集（保守策略）
    else:
        return 2  # 5% 锁热点集（更保守）

# --- 4. 事务模拟器 (Transaction Simulator) ---
class Transaction:
    def __init__(self, txn_id, db: MockDB, workload_gen, decision_logic_func, force_action=None):
        self.id = txn_id
        self.db = db
        self.workload_gen = workload_gen
        self.decision_logic = decision_logic_func
        self.force_action = force_action  # 🆕 用于action平衡模式
        
        self.start_time = time.time()
        self.ops = []
        self.read_set = {}
        self.write_set = set()
        self.mode = 'optimistic'
        self.status = 'running'
        
        self.txn_type = CONFIG['workload_type']
        self.hot_visits = 0
        self.retry_count = 0
        self.final_action = 0 # 默认是0 (保持乐观), 记录最终决策
        self.query_intervals = []
        self.age = 0  # 事务年龄（从第一次尝试到现在的时间，毫秒）

    def _get_profile(self):
        return {
            'hot_visits': self.hot_visits,
            'read_set_size': len(self.read_set),
            'write_set_size': len(self.write_set),
            'retry_count': self.retry_count,
            'priority_score': self._calculate_priority_score()
        }

    def run(self):
        self.ops = self.workload_gen.generate_ops()
        
        for op, key in self.ops:
            think_time = random.uniform(CONFIG['min_think_time_ms'], CONFIG['max_think_time_ms']) / 1000.0
            time.sleep(think_time)
            self.query_intervals.append(think_time * 1000)

            if self.mode == 'optimistic':
                if op == 'read':
                    # 🆕 增强的冲突模拟：热点key更容易发生冲突
                    is_hot = self.db.is_hot_key(key)
                    conflict_prob = CONFIG['base_conflict_probability']
                    if is_hot:
                        conflict_prob *= CONFIG['hot_key_conflict_multiplier']
                    
                    if random.random() < conflict_prob:
                        self.db.simulate_concurrent_write(prefer_hot=True)
                    
                    data = self.db.read(key)
                    self.read_set[key] = data['version']
                    if is_hot:
                        self.hot_visits += 1
                else: # write
                    self.write_set.add(key)
                    self.db.write(key, int(time.time() * 1000))
            else: # pessimistic
                if op == 'read':
                    data = self.db.read(key)
                    self.read_set[key] = data['version']
                else:
                    self.write_set.add(key)
                    self.db.write(key, int(time.time() * 1000))
            
            if self.mode == 'optimistic':
                # 使用增强的决策逻辑，支持force_action
                action = self.decision_logic(self._get_profile(), force_action=self.force_action)
                
                if action > 0:
                    self.final_action = action
                    
                    validation_ok = self.validate()
                    if validation_ok:
                        self.mode = 'pessimistic'
                    else:
                        self.status = 'aborted'
                        break

        if self.status != 'aborted':
            if self.mode == 'optimistic':
                if self.validate():
                    self.commit()
                else:
                    self.status = 'aborted'
            else: # pessimistic
                self.commit()

        return self._generate_log()

    def validate(self):
        for key, version in self.read_set.items():
            if self.db.read(key)['version'] != version:
                self.db.hot_records.add(key)
                return False
        return True

    def commit(self):
        new_version = int(time.time() * 1000) + random.randint(0, 1000)
        for key in self.write_set:
            self.db.write(key, new_version)
        self.status = 'committed'

    def _calculate_priority_score(self):
        """
        计算事务的优先级分数
        优先级 = retry_count * 权重1 + age * 权重2
        重试次数和等待时间越长，优先级越高
        """
        # 使用较大的权重使得分数范围更大，便于分层
        retry_weight = 1e15  # 每次重试增加的权重
        age_weight = 1e12    # 每毫秒年龄增加的权重
        
        priority = (self.retry_count * retry_weight) + (self.age * age_weight)
        return priority

    def _generate_log(self):
        end_time = time.time()
        exec_time = (end_time - self.start_time) * 1000
        avg_interval = np.mean(self.query_intervals) if self.query_intervals else 0

        # 计算原始特征值
        contention = self.hot_visits
        workload = len(self.read_set) + len(self.write_set)
        priority_score = self._calculate_priority_score()
        
        global_tps = CONFIG['initial_global_tps'] + random.uniform(-500, 500)
        global_abort_rate = max(0, CONFIG['initial_global_abort_rate'] + random.uniform(-0.01, 0.01))

        # 只生成原始数据，tier 将由 FeatureEngineer 动态生成
        return {
            "timestamp": int(self.start_time * 1000),
            "txn_id": self.id,
            "txn_type": self.txn_type,
            # 原始特征值（用于 FeatureEngineer 离散化）
            "contention": contention,
            "workload": workload,
            "retry_count": self.retry_count,
            "exec_time": exec_time,
            "query_interval": avg_interval,
            "priority_score": priority_score,
            # 执行信息
            "action": self.final_action,
            "is_commit": 1 if self.status == 'committed' else 0,
            "latency": exec_time - sum(self.query_intervals) if self.query_intervals else 0,
            "global_tps": global_tps,
            "global_abort_rate": global_abort_rate,
            "read_set": ",".join(self.read_set.keys()),
            "write_set": ",".join(self.write_set)
        }

# --- 5. 负载生成器 (Workload Generator) ---
class WorkloadGenerator:
    def __init__(self, num_records, min_ops, max_ops, read_ratio, zipf_alpha):
        self.num_records = num_records
        self.min_ops = min_ops
        self.max_ops = max_ops
        self.read_ratio = read_ratio
        
        if zipf_alpha > 0:
            raw_probs = [(i+1)**(-zipf_alpha) for i in range(num_records)]
            sum_probs = sum(raw_probs)
            self.key_probs = [p/sum_probs for p in raw_probs]
            self.keys = [f"key_{i}" for i in range(num_records)]
        else:
            self.keys = [f"key_{i}" for i in range(num_records)]
            self.key_probs = None

    def generate_ops(self, num_ops=None):
        # 🆕 如果未指定，随机选择操作数（增加workload的多样性）
        if num_ops is None:
            ops_to_generate = random.randint(self.min_ops, self.max_ops)
        else:
            ops_to_generate = num_ops
            
        ops = []
        # 确保不超过可用key数量
        ops_to_generate = min(ops_to_generate, self.num_records)
        
        if self.key_probs:
            chosen_keys = np.random.choice(self.keys, size=ops_to_generate, p=self.key_probs, replace=False)
        else:
            chosen_keys = np.random.choice(self.keys, size=ops_to_generate, replace=False)
        for key in chosen_keys:
            op = 'read' if random.random() < self.read_ratio else 'write'
            ops.append((op, key))
        return ops

# --- 6. Action 平衡器 (Action Balancer) ---
class ActionBalancer:
    """管理action分布，确保各种action都有足够的样本"""
    def __init__(self, target_distribution, total_transactions):
        self.target_distribution = target_distribution
        self.total_transactions = total_transactions
        self.current_counts = {action: 0 for action in target_distribution.keys()}
        self.target_counts = {
            action: int(total_transactions * ratio) 
            for action, ratio in target_distribution.items()
        }
        self.completed_transactions = 0
    
    def should_force_action(self):
        """决定是否需要强制某个action以保持平衡"""
        if self.completed_transactions >= self.total_transactions * 0.7:
            # 在后30%的事务中，检查是否有action严重不足
            for action, target in self.target_counts.items():
                if self.current_counts[action] < target * 0.5:  # 不足目标的50%
                    return action
        return None
    
    def record_action(self, action):
        """记录一个action的使用"""
        self.current_counts[action] = self.current_counts.get(action, 0) + 1
        self.completed_transactions += 1
    
    def get_statistics(self):
        """获取action分布统计"""
        return {
            'current': self.current_counts,
            'target': self.target_counts,
            'completion': {
                action: (self.current_counts.get(action, 0) / max(target, 1)) * 100
                for action, target in self.target_counts.items()
            }
        }

# --- 7. 主执行逻辑 (Main Execution) ---
def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    input_dir = os.path.join(project_root, 'input')
    os.makedirs(input_dir, exist_ok=True)
    
    if CONFIG['output_file'] is None:
        CONFIG['output_file'] = os.path.join(input_dir, 'simulated_ycsb_logs.csv')
    
    print("--- Starting YCSB Log Generation (V3 - Enhanced & Balanced) ---")
    print(f"Project Root: {project_root}")
    print(f"Output Directory: {input_dir}")
    print(f"Configuration:")
    print(f"  - Transactions: {CONFIG['num_transactions']}")
    print(f"  - Records: {CONFIG['num_records']}")
    print(f"  - Ops per txn: {CONFIG['min_ops_per_txn']}-{CONFIG['max_ops_per_txn']}")
    print(f"  - Zipf alpha: {CONFIG['zipf_alpha']}")
    print(f"  - Action balancing: {CONFIG['enable_action_balancing']}")

    workload_gen = WorkloadGenerator(
        CONFIG['num_records'],
        CONFIG['min_ops_per_txn'],
        CONFIG['max_ops_per_txn'],
        CONFIG['read_ratio'],
        CONFIG['zipf_alpha']
    )
    db = MockDB(CONFIG['num_records'], workload_gen)
    
    # 🆕 初始化action平衡器
    action_balancer = None
    if CONFIG['enable_action_balancing']:
        action_balancer = ActionBalancer(
            CONFIG['target_action_distribution'],
            CONFIG['num_transactions']
        )
        print(f"  - Target action distribution: {CONFIG['target_action_distribution']}")
    
    logs = []
    
    for i in tqdm(range(CONFIG['num_transactions']), desc="Simulating Transactions"):
        max_retries = 5
        current_retry = 0
        first_attempt_time = time.time()  # 记录第一次尝试的时间
        
        # 🆕 检查是否需要强制某个action
        force_action = None
        if action_balancer:
            force_action = action_balancer.should_force_action()
        
        while current_retry < max_retries:
            txn = Transaction(
                f"{i}-{current_retry}", 
                db, 
                workload_gen, 
                enhanced_decision_logic,  # 🆕 使用增强的决策逻辑
                force_action=force_action
            )
            txn.retry_count = current_retry
            # 计算从第一次尝试到现在的年龄（毫秒）
            txn.age = (time.time() - first_attempt_time) * 1000
            
            log = txn.run()
            
            if log['is_commit'] == 1:
                logs.append(log)
                # 🆕 记录使用的action
                if action_balancer:
                    action_balancer.record_action(log['action'])
                break
            else: # Aborted
                current_retry += 1
                if current_retry == max_retries:
                    logs.append(log)
                    if action_balancer:
                        action_balancer.record_action(log['action'])

    if logs:
        log_df = pd.DataFrame(logs)
        # 确保列顺序与数据契约一致（只包含原始数据，不包含tier）
        log_df = log_df[[
            "timestamp", "txn_id", "txn_type", 
            # 原始特征值（tier 将由 FeatureEngineer 动态生成）
            "contention", "workload", "retry_count", "exec_time", "query_interval", "priority_score",
            # 执行信息
            "action", "is_commit", "latency", "global_tps", "global_abort_rate", "read_set", "write_set"
        ]]
        log_df.to_csv(CONFIG['output_file'], index=False)
        
        print(f"\n{'='*70}")
        print(f"[SUCCESS] 成功生成 {len(logs)} 条日志记录")
        print(f"{'='*70}")
        print(f"输出文件: {os.path.abspath(CONFIG['output_file'])}")
        
        print(f"\n{'='*70}")
        print("[STATS] 动作分布 (Action Distribution)")
        print(f"{'='*70}")
        action_counts = log_df['action'].value_counts().sort_index()
        action_names = {
            0: "Stay Optimistic",
            1: "Lock Hot Write Set",
            2: "Lock All Hot Sets",
            3: "Lock All Writes & Hot Reads",
            4: "Lock Entire Access Set",
            5: "Prioritize Transaction Only"
        }
        for action_id in range(6):
            count = action_counts.get(action_id, 0)
            percentage = (count / len(logs)) * 100
            name = action_names.get(action_id, f"Action {action_id}")
            print(f"  Action {action_id} ({name:30s}): {count:4d} ({percentage:5.1f}%)")
        
        # 🆕 如果启用了action平衡，显示目标达成情况
        if action_balancer:
            print(f"\n{'='*70}")
            print("[BALANCING] Action平衡目标达成情况")
            print(f"{'='*70}")
            stats = action_balancer.get_statistics()
            for action_id in range(6):
                current = stats['current'].get(action_id, 0)
                target = stats['target'].get(action_id, 0)
                completion = stats['completion'].get(action_id, 0)
                name = action_names.get(action_id, f"Action {action_id}")
                status = "✓" if completion >= 80 else "⚠" if completion >= 50 else "✗"
                print(f"  {status} Action {action_id} ({name:30s}): {current:4d}/{target:4d} ({completion:5.1f}%)")
        
        print(f"\n{'='*70}")
        print("[STATS] 特征统计摘要")
        print(f"{'='*70}")
        print(log_df[['exec_time', 'query_interval', 'priority_score', 'contention', 'workload', 'retry_count']].describe())
        
        print(f"\n{'='*70}")
        print("[DATA] 生成数据样本（前10条）")
        print(f"{'='*70}")
        print(log_df[['txn_id', 'contention', 'workload', 'retry_count', 'action', 'is_commit']].head(10))
        
        print(f"\n{'='*70}")
        print(f"[INFO] 💡 提示")
        print(f"{'='*70}")
        print("  - Tier 特征将在训练时由 FeatureEngineer 根据 config/default.yaml 动态生成")
        print("  - 如需调整特征分布，可修改 CONFIG 中的参数后重新运行")
        print("  - 建议运行 'python tools/analyze_tiers.py --update' 自动分析最优tier配置")
    else:
        print("\n[ERROR] 未生成任何日志。")
        
    print(f"\n{'='*70}")
    print("--- Log Generation Finished ---")
    print(f"{'='*70}\n")

if __name__ == '__main__':
    main()