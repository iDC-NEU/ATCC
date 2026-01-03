# tools/compare_policy.py
# -*- coding: utf-8 -*-

"""
策略对比脚本 (增强版 v2.1 - 含数据覆盖率统计)
用于对比训练得到的LDT策略与纯OCC、纯PCC策略的优劣。

主要改进:
1. 增加 '数据覆盖率' 统计：显示策略选择的动作有多少比例在历史日志中真实出现过。
   覆盖率越高，评估结果越可信；覆盖率低说明策略在尝试未曾探索过的路径。
2. 包含 OCC 和 PCC 基准对比。
3. 自动生成详细报告。
"""

import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['OMP_NUM_THREADS'] = '1'
import sys
import pandas as pd
import yaml
import json
from collections import Counter
from datetime import datetime

# 将项目根目录添加到sys.path中，以便导入src中的模块
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.data_loader import DataLoader
from src.feature_engineer import FeatureEngineer
from src.reward_calculator import RewardCalculator

def evaluate_policy(policy, stats_lookup, state_counts, global_stats):
    """
    评估单个策略的性能，并统计数据覆盖率。

    Args:
        policy (dict): 要评估的策略 {state_key: action}.
        stats_lookup (dict): 包含各(状态,动作)组合的平均指标.
        state_counts (dict): 各状态在日志中出现的次数.
        global_stats (dict): 全局平均指标，用于备用.

    Returns:
        dict: 包含评估结果的字典.
    """
    total_reward = 0
    expected_commits = 0
    expected_latency_sum = 0
    total_txns = sum(state_counts.values())
    
    # 统计覆盖率
    hit_count = 0  # 命中：历史数据中有该(状态, 动作)的记录
    miss_count = 0 # 未命中：历史数据中无记录，使用了全局平均值
    
    action_usage = Counter()

    for state, count in state_counts.items():
        # 如果策略对某个状态没有定义动作，则使用默认动作 (0 for LDT, or the fixed action for baseline)
        action = policy.get(state, list(policy.values())[0] if policy else 0) 
        action_usage[action] += count # 按事务实际发生次数统计

        stats = stats_lookup.get((state, action))
        
        if stats:
            # ✅ 命中历史数据
            hit_count += count
            total_reward += stats['mean_reward'] * count
            expected_commits += stats['commit_rate'] * count
            expected_latency_sum += stats['mean_latency'] * count
        else:
            # ❌ 未命中（Off-Policy 评估的盲区），回退到全局平均
            miss_count += count
            total_reward += global_stats['mean_reward'] * count
            expected_commits += global_stats['commit_rate'] * count
            expected_latency_sum += global_stats['mean_latency'] * count
            
    return {
        "总预期奖励": total_reward,
        "预期提交数": expected_commits,
        "预期中止数": total_txns - expected_commits,
        "预期提交率": (expected_commits / total_txns) * 100 if total_txns > 0 else 0,
        "预期平均延迟(ms)": expected_latency_sum / total_txns if total_txns > 0 else 0,
        "数据覆盖率(%)": (hit_count / total_txns) * 100 if total_txns > 0 else 0,  # 新增指标
        "动作使用次数": action_usage
    }

def run_comparison():
    """
    运行策略对比的主函数
    """
    print("--- 开始运行策略对比脚本 (v2.1: 含覆盖率统计) ---")
    
    # 1. 加载配置文件
    config_path = os.path.join(project_root, 'config', 'default.yaml')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ 错误: 找不到配置文件 {config_path}")
        return

    # 2. 加载训练好的LDT策略
    ldt_path = os.path.join(project_root, config['data_paths']['output_ldt_path'])
    try:
        with open(ldt_path, 'r', encoding='utf-8') as f:
            ldt = json.load(f)
        trained_policy = ldt['table']
        print(f"✅ 成功加载LDT策略: {ldt_path}")
    except FileNotFoundError:
        print(f"❌ 错误: 找不到LDT文件 {ldt_path}。请先运行 train.py 训练一个策略。")
        return

    # 3. 加载并处理数据
    print("\n--- 正在加载和处理数据... ---")
    data_loader = DataLoader(config)
    raw_df = data_loader.load_data()

    if raw_df.empty:
        print("❌ 错误: 输入目录中没有找到日志数据。")
        return

    reward_calculator = RewardCalculator(config)
    rewarded_df = reward_calculator.calculate_rewards(raw_df)

    feature_engineer = FeatureEngineer(config)
    processed_df = feature_engineer.transform(rewarded_df)
    print("✅ 数据加载和特征工程完成。")

    # 4. 构建增强的奖励/指标统计数据
    stats_agg = processed_df.groupby(['state_key', 'action']).agg(
        mean_reward=('reward_final', 'mean'),
        count=('reward_final', 'count'),
        commit_rate=('is_commit', 'mean'),
        mean_latency=('latency', 'mean')
    ).reset_index()

    stats_lookup = {
        (row['state_key'], int(row['action'])): {
            'mean_reward': float(row['mean_reward']),
            'count': int(row['count']),
            'commit_rate': float(row['commit_rate']),
            'mean_latency': float(row['mean_latency'])
        }
        for _, row in stats_agg.iterrows()
    }
    
    global_stats = {
        'mean_reward': processed_df['reward_final'].mean(),
        'commit_rate': processed_df['is_commit'].mean(),
        'mean_latency': processed_df['latency'].mean()
    }
    
    state_counts = processed_df['state_key'].value_counts().to_dict()
    unique_states = list(state_counts.keys())
    total_transactions = len(processed_df)
    print("✅ 已根据日志构建状态-动作的详细指标统计表。")

    # 5. 定义基准策略
    occ_policy = {state: 0 for state in unique_states}
    # 纯悲观策略，总是选择最保守的动作 (Action 4)
    pcc_policy = {state: 4 for state in unique_states}

    # 6. 评估三种策略
    print("\n--- 开始评估策略... ---")
    ldt_results = evaluate_policy(trained_policy, stats_lookup, state_counts, global_stats)
    occ_results = evaluate_policy(occ_policy, stats_lookup, state_counts, global_stats)
    pcc_results = evaluate_policy(pcc_policy, stats_lookup, state_counts, global_stats)
    print("✅ 策略评估完成。")

    # 7. 生成并保存报告
    report_lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    report_lines.append("="*60)
    report_lines.append(f"策略对比分析报告 ({now})")
    report_lines.append("="*60)
    
    report_lines.append("\n### 1. 基础信息 ###")
    report_lines.append(f"- 评估的事务总数: {total_transactions}")
    report_lines.append(f"- 唯一状态总数: {len(unique_states)}")

    report_lines.append("\n### 2. 性能指标对比 ###")
    report_lines.append("(注: 数据覆盖率越高，说明评估是基于真实历史数据，结果越可信)")
    
    def format_results(name, results):
        lines = []
        lines.append(f"\n--- {name} ---")
        for key, value in results.items():
            if key != "动作使用次数":
                lines.append(f"- {key:<18}: {value:,.2f}")
        return lines

    report_lines.extend(format_results("LDT 策略 (Our System)", ldt_results))
    report_lines.extend(format_results("纯 OCC 策略 (Baseline 1)", occ_results))
    report_lines.extend(format_results("纯 PCC 策略 (Baseline 2)", pcc_results))

    # 添加覆盖率警告
    coverage = ldt_results.get("数据覆盖率(%)", 0)
    if coverage < 80.0:
        report_lines.append(f"\n⚠️ 警告: LDT 策略的数据覆盖率仅为 {coverage:.2f}%。")
        report_lines.append("这意味着策略在大量状态下选择了历史数据中未曾尝试过的动作。")
        report_lines.append("建议增加随机探索比例 (Epsilon) 并收集更多训练数据。")

    report_lines.append("\n### 3. 性能提升分析 (LDT vs 基准) ###")
    
    def print_improvement(metric_name, ldt_val, base_val, higher_is_better=True):
        if base_val == 0:
            improvement = float('inf') if ldt_val > 0 else 0
        else:
            improvement = ((ldt_val - base_val) / abs(base_val)) * 100
        
        if not higher_is_better:
            improvement *= -1
            
        arrow = "↑" if improvement > 0 else "↓"
        # 定义稍微宽松一点的判定，提升大于0就算好
        is_good = improvement > 0
        
        color = "✅" if is_good else "🔻"
        return f"{color} {metric_name}提升: {improvement:+.2f}% {arrow}"

    report_lines.append("\n--- LDT 策略 vs 纯 OCC 策略 ---")
    report_lines.append(print_improvement("总预期奖励", ldt_results['总预期奖励'], occ_results['总预期奖励']))
    report_lines.append(print_improvement("预期提交率", ldt_results['预期提交率'], occ_results['预期提交率']))
    report_lines.append(print_improvement("预期平均延迟", ldt_results['预期平均延迟(ms)'], occ_results['预期平均延迟(ms)'], higher_is_better=False))

    report_lines.append("\n--- LDT 策略 vs 纯 PCC 策略 ---")
    report_lines.append(print_improvement("总预期奖励", ldt_results['总预期奖励'], pcc_results['总预期奖励']))
    report_lines.append(print_improvement("预期提交率", ldt_results['预期提交率'], pcc_results['预期提交率']))
    report_lines.append(print_improvement("预期平均延迟", ldt_results['预期平均延迟(ms)'], pcc_results['预期平均延迟(ms)'], higher_is_better=False))

    report_lines.append("\n### 4. LDT 策略动作分布 ###")
    action_names = ldt.get('actions', [f"动作 {i}" for i in range(10)]) # 范围给大一点防止越界
    for i in range(len(action_names)):
        # 只打印被使用过的动作
        if i in ldt_results['动作使用次数']:
            count = ldt_results['动作使用次数'][i]
            percentage = (count / total_transactions) * 100 if total_transactions > 0 else 0
            report_lines.append(f"- {action_names[i]} (动作 {i}): {count} 次 ({percentage:.2f}%)")

    report_content = "\n".join(report_lines)
    
    # 打印到控制台
    print("\n" + report_content)
    
    # 保存到文件
    output_dir = os.path.join(project_root, 'output')
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, 'comparison_report.txt')
    
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        print(f"\n✅ 详细报告已成功保存到: {report_path}")
    except IOError as e:
        print(f"\n❌ 错误: 无法保存报告文件: {e}")

if __name__ == '__main__':
    run_comparison()