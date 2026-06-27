# tools/analyze_tiers.py
# -*- coding: utf-8 -*-

"""
Tiers (层级) 自动分析脚本

本脚本通过K-Means聚类算法分析日志数据中的连续特征 (如 exec_time, query_interval)，
并自动推荐最优的层级划分阈值。

解决了`config/default.yaml`中tiers配置硬编码的问题，使得层级划分更加数据驱动和科学。

新功能：可以直接将分析结果更新到 config/default.yaml 文件中！
"""

import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['OMP_NUM_THREADS'] = '1'
import sys
import re
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import warnings
import yaml
import argparse

from ..src.data.data_loader import DataLoader

# 忽略KMeans在单核CPU上的内存泄漏警告
os.environ['OMP_NUM_THREADS'] = '1'
warnings.filterwarnings('ignore', category=FutureWarning, module='sklearn.cluster._kmeans')

# 将项目根目录添加到sys.path中，以便导入src中的模块
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 假设 DataLoader 在 src 目录下
try:
    from src.data_loader import DataLoader
except ImportError:
    print("❌ 无法导入 DataLoader。请确保脚本在项目的 'tools' 文件夹下，并且项目根目录结构正确。")
    sys.exit(1)

# --- 配置 ---
# 需要分析的连续特征列
# 注意：priority_score, contention_tier, workload_tier 这些特征需要从原始日志中存在
# CONTINUOUS_FEATURES_TO_ANALYZE = [
#     "exec_time",
#     "query_interval",
#     "priority_score",  # 新增：事务优先级分数（基于retry_cnt + age等计算）
#     "contention",      # 如果日志中有原始contention值（非tier）
#     "workload",        # 如果日志中有原始workload值（非tier）
# ]

CONTINUOUS_FEATURES_TO_ANALYZE = [
    "exec_time",
    "query_interval",
    "priority_score",  # 新增：事务优先级分数（基于retry_cnt + age等计算）
    "contention",      # 如果日志中有原始contention值（非tier）
    "workload",        # 如果日志中有原始workload值（非tier）
    "s_global_abort_rate",      # 新增全局abort, tps
    "s_global_throughput",
]

# 为每个特征设定要划分的簇的数量 (K值)
# K=3 意味着将数据划分为 低/中/高 三个层级
K_CLUSTERS = 3 

# 配置文件路径
CONFIG_FILE_PATH = 'config/default.yaml'

def update_yaml_config(recommended_tiers, config_path=CONFIG_FILE_PATH):
    """
    将推荐的tier配置更新到YAML文件中，保持原有格式和注释。
    
    Args:
        recommended_tiers: 字典，包含每个特征的推荐阈值
        config_path: YAML配置文件的路径
    
    Returns:
        bool: 更新成功返回True，失败返回False
    """
    try:
        # 读取文件内容
        with open(config_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # 逐行处理，只替换tiers中的数字
        updated_lines = []
        in_tiers_section = False
        
        for line in lines:
            updated_line = line
            
            # 检测是否进入tiers部分
            if 'tiers:' in line:
                in_tiers_section = True
                updated_lines.append(line)
                continue
            
            # 如果在tiers部分，尝试匹配并替换
            if in_tiers_section:
                # 检测是否离开tiers部分（遇到同级或更高级的配置项）
                if line.strip() and not line.startswith(' ' * 4) and not line.strip().startswith('#'):
                    in_tiers_section = False
                    updated_lines.append(line)
                    continue
                
                # 尝试匹配每个特征的配置行
                for feature, new_thresholds in recommended_tiers.items():
                    # 匹配格式: "    feature_name: [数字, 数字, ...]"
                    pattern = rf'^(\s*{re.escape(feature)}\s*:\s*)\[([^\]]+)\](.*)$'
                    match = re.match(pattern, line)
                    
                    if match:
                        prefix = match.group(1)  # "    exec_time: "
                        suffix = match.group(3)  # 可能的注释或其他内容
                        
                        # 构建新的数值列表
                        new_values_str = ', '.join([str(v) for v in new_thresholds])
                        updated_line = f"{prefix}[{new_values_str}]{suffix}\n"
                        
                        print(f"  ✓ 更新 {feature}: {new_thresholds}")
                        break
            
            updated_lines.append(updated_line)
        
        # 写回文件
        with open(config_path, 'w', encoding='utf-8') as f:
            f.writelines(updated_lines)
        
        print(f"\n✅ 成功更新配置文件: {config_path}")
        print("   格式和注释已保留")
        return True
        
    except Exception as e:
        print(f"\n❌ 更新配置文件失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def analyze_and_recommend_tiers(auto_update=True, config_path=CONFIG_FILE_PATH):
    """
    主函数：加载数据，运行聚类分析，并打印推荐的YAML配置。
    """
    print("--- 开始运行 Tiers 自动分析脚本 ---")
    
    # 1. 加载数据
    # 使用一个临时的、不依赖于完整配置的DataLoader配置
    # 这样脚本就不会因为config.yaml中的其他部分不完整而失败
    temp_config = {'data_paths': {'raw_log_dir': 'input'}}
    data_loader = DataLoader(temp_config)
    df = data_loader.load_data()

    if df.empty:
        print("\n❌ 在 'input' 目录下没有找到任何数据。无法进行分析。")
        print("--- 脚本结束 ---")
        return

    print(f"✅ 成功加载 {len(df)} 条记录用于分析。")
    print("\n--- 开始为每个特征进行K-Means聚类分析 ---")
    
    recommended_tiers = {}

    for feature in CONTINUOUS_FEATURES_TO_ANALYZE:
        if feature not in df.columns:
            print(f"\n⚠️ 警告: 在数据中找不到特征列 '{feature}'，已跳过。")
            continue
        
        # 准备数据：需要将1D数据转换为2D数组以用于sklearn
        data_to_cluster = df[[feature]].dropna()
        if len(data_to_cluster) < K_CLUSTERS:
            print(f"\n⚠️ 警告: 特征 '{feature}' 的有效数据点 ({len(data_to_cluster)}) 少于K值 ({K_CLUSTERS})，无法聚类。")
            continue

        # 数据标准化：使得聚类对数值尺度不敏感
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(data_to_cluster)

        # 运行K-Means
        kmeans = KMeans(n_clusters=K_CLUSTERS, random_state=42, n_init=10)
        kmeans.fit(scaled_data)

        # 将聚类中心转换回原始数据的尺度
        cluster_centers = scaler.inverse_transform(kmeans.cluster_centers_)
        
        # 对聚类中心进行排序，以便找到边界
        sorted_centers = sorted(cluster_centers.flatten())
        
        print(f"\n✅ 特征 '{feature}' 分析完成:")
        print(f"  - 聚类中心 (原始值): {[f'{c:.2f}' for c in sorted_centers]}")
        
        # 计算划分边界：边界是相邻两个聚类中心的中点
        thresholds = []
        for i in range(len(sorted_centers) - 1):
            threshold = (sorted_centers[i] + sorted_centers[i+1]) / 2
            thresholds.append(threshold)
        
        # 对阈值进行四舍五入
        rounded_thresholds = [round(t, 2) for t in thresholds]
        
        # 检查是否有重复的阈值
        if len(rounded_thresholds) != len(set(rounded_thresholds)):
            print(f"  ⚠️  警告: 特征 '{feature}' 的聚类中心过于接近，导致阈值重复")
            print(f"     原始聚类中心: {[f'{c:.4f}' for c in sorted_centers]}")
            print(f"     计算的阈值: {rounded_thresholds}")
            
            # 尝试使用更高精度或稍微调整阈值
            unique_thresholds = []
            for i, t in enumerate(thresholds):
                # 使用更高精度（4位小数）
                rounded_t = round(t, 4)
                # 如果仍然重复，手动调整
                if rounded_t in unique_thresholds:
                    # 稍微增加一点值以避免重复
                    rounded_t = round(t + 0.01 * (i + 1), 4)
                unique_thresholds.append(rounded_t)
            
            # 再次检查
            if len(unique_thresholds) != len(set(unique_thresholds)):
                print(f"  ❌ 无法为特征 '{feature}' 生成唯一阈值，已跳过")
                print(f"     建议: 检查该特征的数据分布，或考虑减少K值")
                continue
            
            recommended_tiers[feature] = unique_thresholds
            print(f"  ✓ 调整后的划分阈值: {unique_thresholds}")
        else:
            recommended_tiers[feature] = rounded_thresholds
            print(f"  - 推荐的划分阈值: {recommended_tiers[feature]}")

    if not recommended_tiers:
        print("\n❌ 未能为任何特征生成推荐配置。请检查输入数据。")
        print("--- 脚本结束 ---")
        return None

    # 4. 打印最终的YAML配置建议
    print("\n\n--- 推荐的 `tiers` 配置 ---")
    print("feature_engineering:")
    print("  tiers:")
    for feature, thresholds in recommended_tiers.items():
        print(f"    {feature}: {thresholds}")
    
    # 5. 自动更新配置文件（如果启用）
    if auto_update:
        print("\n" + "="*60)
        print("正在自动更新配置文件...")
        print("="*60)
        
        if update_yaml_config(recommended_tiers, config_path):
            print("\n🎉 配置文件已成功更新！你可以直接运行训练脚本了。")
        else:
            print("\n⚠️  自动更新失败，请手动复制上面的配置到 config/default.yaml")
    else:
        print("\n💡 提示：使用 --update 参数可以自动更新配置文件。")
        print("   或者手动将以上配置复制到 config/default.yaml 文件中。")
    
    print("\n--- 脚本结束 ---")
    return recommended_tiers

if __name__ == '__main__':
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description='自动分析日志数据并推荐最优的tier划分阈值',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例：
  python tools/analyze_tiers.py              # 仅显示推荐配置（不更新文件）
  python tools/analyze_tiers.py --update     # 分析并自动更新 config/default.yaml
  python tools/analyze_tiers.py --dry-run    # 仅显示推荐配置（等同于不加参数）
        """
    )
    
    parser.add_argument(
        '--update', 
        action='store_true',
        help='自动将推荐的tier配置更新到 config/default.yaml 文件中'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='仅显示推荐配置，不更新文件（默认行为）'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default=CONFIG_FILE_PATH,
        help=f'指定配置文件路径（默认: {CONFIG_FILE_PATH}）'
    )
    
    args = parser.parse_args()
    
    # 确定是否自动更新
    auto_update = args.update and not args.dry_run
    
    # 运行分析
    analyze_and_recommend_tiers(auto_update=auto_update, config_path=args.config)



