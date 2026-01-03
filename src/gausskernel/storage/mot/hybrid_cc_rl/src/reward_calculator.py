import pandas as pd
import numpy as np
from typing import Dict, Any

class RewardCalculator:
    """
    Calculates rewards based on the dual-perspective model,
    including a non-linear, time-dependent abort cost for agentic transactions.
    
    The abort cost follows: C_abort = c1 * (T_base + T_exec) + c2 * (T_base + T_exec)^2
    This reflects that longer-running transactions have exponentially higher costs when aborted.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the RewardCalculator with configuration.

        Args:
            config: A dictionary containing the weights for the reward function.
        """
        self.weights = config['reward_function']
        # 验证新权重是否存在，提供默认值以向后兼容
        self.weights.setdefault('c1', 0.0)
        self.weights.setdefault('c2', 0.0)
        self.weights.setdefault('t_base', 0.0)

    def calculate_rewards(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates local, global, and final rewards for each transaction.
        The local reward incorporates a dynamic abort cost based on execution time.

        Args:
            df: The DataFrame, which must include all necessary columns
                (latency, is_commit, retry_count, global_tps, etc.).

        Returns:
            The input DataFrame with three new columns: 'reward_local', 
            'reward_global', and 'reward_final'.
        """
        if df.empty:
            return df

        reward_df = df.copy()
        w = self.weights

        # --- 1. Calculate Transaction-Local Reward (rt) ---
        
        # 1a. 计算动态中止成本 (Dynamic Abort Cost: C_abort)
        # C_abort = c1 * (T_base + T_exec) + c2 * (T_base + T_exec)^2
        # T_exec 用 latency 近似，因为对于事务而言，其延迟就是执行时间
        t_exec = reward_df['latency']
        
        # 计算基础项 (T_base + T_exec)
        cost_base_term = w['t_base'] + t_exec
        
        # 分别计算线性和二次项成本
        linear_cost = w['c1'] * cost_base_term
        quadratic_cost = w['c2'] * np.power(cost_base_term, 2)
        
        # 总中止成本
        total_abort_cost = linear_cost + quadratic_cost
        
        # 只有当事务中止时 (is_commit == 0)，这个成本才生效
        # 我们乘以 (1 - is_commit) 来实现这一点
        dynamic_abort_penalty = total_abort_cost * (1 - reward_df['is_commit'])

        # 1b. 计算最终的局部奖励
        # rt = β*Commit - α*Latency - ε*Retry - C_abort
        retry_numeric = reward_df['retry_count'].astype(int)
        
        reward_df['reward_local'] = (
            w['beta'] * reward_df['is_commit'] -
            w['alpha'] * reward_df['latency'] -
            w['epsilon'] * retry_numeric -
            dynamic_abort_penalty  # 🆕 使用新的动态中止惩罚替换旧的固定惩罚
        )

        # --- 2. Calculate System-Global Reward (rs) ---
        # 这部分逻辑保持不变
        reward_df = reward_df.sort_values(by='timestamp').reset_index(drop=True)
        
        # Calculate deltas (change from the previous transaction record)
        delta_tps = reward_df['global_tps'].diff().fillna(0)
        delta_latency = reward_df['latency'].diff().fillna(0)
        delta_abort_rate = reward_df['global_abort_rate'].diff().fillna(0)

        # rs = λ1*ΔTPS - λ2*ΔLatency - λ3*ΔAbortRate
        reward_df['reward_global'] = (
            w['lambda1'] * delta_tps -
            w['lambda2'] * delta_latency -
            w['lambda3'] * delta_abort_rate
        )

        # --- 3. Calculate Final Weighted Reward ---
        # r = μ * rt + (1 - μ) * rs
        reward_df['reward_final'] = (
            w['mu'] * reward_df['reward_local'] +
            (1 - w['mu']) * reward_df['reward_global']
        )
        
        print("✅ Reward calculation complete with dynamic abort cost model.")
        print(f"   - Applied non-linear abort penalty: C_abort = {w['c1']}*(T_base+T_exec) + {w['c2']}*(T_base+T_exec)^2")
        return reward_df

if __name__ == '__main__':
    # Test the RewardCalculator with real configuration
    import yaml
    from data_loader import DataLoader
    
    try:
        # 确保使用的是更新后的 config 文件
        with open('config/default.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        data_loader = DataLoader(config)
        raw_df = data_loader.load_data()
        
        if not raw_df.empty:
            reward_calculator = RewardCalculator(config)
            rewarded_df = reward_calculator.calculate_rewards(raw_df)
            
            print(f"\n{'='*70}")
            print(f"成功为 {len(rewarded_df)} 条记录计算奖励")
            print(f"{'='*70}")
            
            # 筛选出成功和失败的事务进行对比
            committed_sample = rewarded_df[rewarded_df['is_commit'] == 1].head(5)
            aborted_sample = rewarded_df[rewarded_df['is_commit'] == 0].head(5)
            
            if not committed_sample.empty:
                print(f"\n--- ✅ 提交事务样本 (Committed Transactions) ---")
                print(committed_sample[['latency', 'retry_count', 'is_commit', 'reward_local', 'reward_final']])
            
            if not aborted_sample.empty:
                print(f"\n--- ❌ 中止事务样本 (Aborted Transactions) ---")
                print(aborted_sample[['latency', 'retry_count', 'is_commit', 'reward_local', 'reward_final']])
                print(f"\n💡 注意：中止事务的奖励更低，且延迟越长惩罚越重（非线性增长）")
            else:
                print(f"\n✓ 数据中没有中止的事务")
                
        else:
            print("没有可用于奖励计算的数据")
            
    except Exception as e:
        print(f"测试 RewardCalculator 时出错: {e}")
        import traceback
        traceback.print_exc()
