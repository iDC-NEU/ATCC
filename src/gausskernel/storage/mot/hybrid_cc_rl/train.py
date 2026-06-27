#!/usr/bin/env python3
"""
HybridCC RL Training Script
训练脚本用于运行完整的离线训练管道
"""

import sys
import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['OMP_NUM_THREADS'] = '1'
from src.pipeline import run_training_pipeline

def main():
    """主函数：运行训练管道"""
    print("=" * 60)
    print("HybridCC RL 训练脚本")
    print("=" * 60)
    
    # 获取项目根目录（train.py所在目录）
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    # 检查配置文件是否存在
    config_path = os.path.join(project_root, 'config', 'default.yaml')
    if not os.path.exists(config_path):
        print(f"错误：配置文件 {config_path} 不存在！")
        return 1
    
    # 检查数据目录是否存在
    data_dir = os.path.join(project_root, "input")
    if not os.path.exists(data_dir):
        print(f"警告：数据目录 {data_dir} 不存在，将尝试创建...")
        os.makedirs(data_dir, exist_ok=True)
    
    # 检查数据目录中是否有日志文件
    log_files = [f for f in os.listdir(data_dir) if f.endswith(('.csv', '.log'))]
    if not log_files:
        print(f"警告：数据目录 {data_dir} 中没有找到 .csv 或 .log 文件！")
        print("训练将继续，但可能无法加载数据。")
    else:
        print(f"找到 {len(log_files)} 个日志文件: {', '.join(log_files)}")
    
    print(f"使用配置文件: {config_path}")
    print(f"数据目录: {data_dir}")
    print("-" * 60)
    
    try:
        # 运行训练管道（使用相对于项目根目录的路径）
        run_training_pipeline(config_path)
        print("\n" + "=" * 60)
        print("训练完成！")
        print("=" * 60)
        return 0
        
    except Exception as e:
        print(f"\n训练过程中出现错误: {e}")
        print("请检查配置文件和数据文件是否正确。")
        return 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
