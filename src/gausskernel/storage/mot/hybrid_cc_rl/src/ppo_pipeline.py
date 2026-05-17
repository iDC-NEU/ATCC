# src/ppo_pipeline.py
# python -m src.ppo_pipeline
import yaml
import time
import socket
import os

from .data.data_loader import DataLoader
from .data.data_filter import DataFilter
from .feature_engineer import FeatureEngineer
from .ppo_trainer import RewardCalculator
from .ppo_trainer import PPOTrainer
from .ldt_manager import LdtManager


def notify_cpp(version, host="127.0.0.1", port=1556):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        msg = f"READY:{version}"
        s.send(msg.encode())
        s.close()
        print(f"[Notify] Sent policy version {version} to C++")
    except Exception as e:
        print(f"[Notify] Failed: {e}")


def run_training_pipeline(config_path='config/default.yaml'):

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    data_loader = DataLoader(config)
    data_filter = DataFilter(config)
    feature_engineer = FeatureEngineer(config)
    reward_calc = RewardCalculator(config)
    ldt_manager = LdtManager(config)

    min_samples = config.get("training", {}).get("min_samples", 20000)
    interval = config.get("training", {}).get("interval_sec", 6000)

    state_cols = config['features']['state_columns']
    next_state_cols = config['features']['next_state_columns']

    state_dim = len(state_cols)
    num_actions = config['optimizer']['num_actions']

    ppo = PPOTrainer(state_dim, num_actions, config)
    model_path = "models/ppo.pt"
    model_dir = os.path.dirname(model_path)
    os.makedirs(model_dir, exist_ok=True)

    if os.path.exists(model_path):
        ppo.load(model_path)
        print(f"=== Model loaded from : {model_path} ===\n")

    last_train_time = 0
    latest_version = ldt_manager.get_current_version()
    # latest_version = 5

    state_matrix, all_state_keys = all_states()

    print("=== PPO Training Service Started===")
    print(f"=== Current Latest version : {latest_version} =========")

    while True:

        raw_df = data_loader.load_data()
        if raw_df.empty:
            time.sleep(600)
            continue

        filtered_df = data_filter.filter_by_policy_version(raw_df, latest_version)
        if len(filtered_df) < min_samples:
            print("[Pipeline] Not enough data, waiting...")
            time.sleep(600)
            continue

        now = time.time()
        if now - last_train_time < interval:
            time.sleep(60)
            continue

        print("\n=== Start Training ===")

        # ✅ Reward重算
        filtered_df = adapt_columns(filtered_df)
        max_samples = config.get("training", {}).get("max_samples", None)
        if max_samples:
            filtered_df = filtered_df.tail(max_samples)

        rewarded_df = reward_calc.compute(filtered_df)

        # ✅ Feature
        final_df = feature_engineer.transform_traj(rewarded_df)
        final_df = final_df.sort_values(['txn_id', 'start_time'])

        states = final_df[state_cols].values
        actions = final_df['action'].values
        old_probs = final_df['action_prob'].values
        rewards = final_df['reward'].values
        next_states = final_df[next_state_cols].values
        dones = final_df['done'].values.astype(float)

        loss = ppo.train(states, actions, old_probs, rewards, next_states, dones)
        print(f"[PPO] Training done. Loss={loss:.4f}")
        print(final_df['reward'].describe())

        # ✅ 生成LDT
        # prob_policy = {}
        # for _, row in final_df.iterrows():
        #     key = row['state_key']
        #     state_vec = row[state_cols].values
        #     probs = ppo.predict(state_vec)
        #     prob_policy[key] = [round(float(p), 4) for p in probs]
        # unique_states_df = final_df.drop_duplicates(subset=['state_key'])
        # unique_state_keys = unique_states_df['state_key'].values
        # unique_state_vecs = unique_states_df[state_cols].values

        all_probs = ppo.predict_batch(state_matrix)

        prob_policy = {}
        for key, probs in zip(all_state_keys, all_probs):
            prob_policy[key] = [round(float(p), 4) for p in probs]

        version = ldt_manager.save_ldt_prob(prob_policy)
        latest_version = version

        # ✅ 通知 C++
        notify_cpp(version)
        ppo.save(model_path)
        print(f"=== Model saved in : {model_path} ===\n")

        # ✅ 计算本轮数据集的全局基线，并注入到 reward_calc 供下轮使用
        terminal_df = filtered_df[filtered_df['done'] == 1]
        if len(terminal_df) > 0:
            current_avg_tps = terminal_df['tps'].mean()
            current_p99_lat = terminal_df['latency'].quantile(0.99)
            current_avg_abort = terminal_df['abort_rate'].mean()
            current_avg_exec = terminal_df['exec_time'].mean()
        else:
            current_avg_tps = filtered_df['tps'].mean()
            current_p99_lat = filtered_df['latency'].quantile(0.99)
            current_avg_abort = filtered_df['abort_rate'].mean()
            current_avg_exec = filtered_df['exec_time'].mean()
            print("[Warning] No completed trajectories found in this batch!")

        print(f"[Stats] Round completed -> Avg TPS: {current_avg_tps:.2f}, P99 Latency: {current_p99_lat:.2f}, Avg Abort Rate: {current_avg_abort:.4f}")
        # 更新基线
        reward_calc.update_baselines(current_avg_tps, current_p99_lat)

        last_train_time = now

        print("=== Training Round Finished ===\n")


def adapt_columns(df):
    df = df.copy()
    df['commit'] = df['is_commit'].astype(float)
    df['abort'] = 1.0 - df['commit']
    # 全局指标映射
    df['tps'] = df['s_global_throughput'].astype(float)
    df['abort_rate'] = df['s_global_abort_rate'].astype(float)

    df['real_retry_count'] = df['real_retry_count'].astype(float)
    df['real_tps'] = df['real_global_tps'].astype(float)
    df['real_abort_rate'] = df['real_abort_rate'].astype(float)

    # 时延
    df['latency'] = df['exec_time'].astype(float)
    df['s_work'] = 10
    df['s_retry'] = df['retry_tier'].astype(float)
    return df

import itertools
import numpy as np
def all_states() -> tuple[np.ndarray, list]:
    print("[Pipeline] Generating full Cartesian product LDT for all states...")
    # 定义每个特征的 Tier 范围 (0, 1, 2)
    tier_range = [0, 1, 2]
    # 5个维度的全排列，生成 3^5 种组合
    all_combinations = list(itertools.product(
        tier_range, # s_cont
        tier_range, # s_work
        tier_range, # s_retry
        tier_range, # s_global_abort_rate
        tier_range, # s_global_throughput
    ))
    all_state_keys = []
    all_state_vecs = []
    for combo in all_combinations:
        c, w, r, ga, gt = combo
        # 1. 拼接与 C++ 端 ExtractStateKeyTraj 严格一致的 Key
        key = f"c{c}_w{w}_r{r}_ga{ga}_gt{gt}"
        all_state_keys.append(key)
        # 2. 构造输入神经网络的状态向量
        # 这里的列表顺序，必须与 config['features']['state_columns'] 中定义的顺序完全一致！
        # 假设你的 yaml 中 state_columns 顺序依次为:
        # ['s_cont', 's_work', 's_retry', 's_global_abort_rate', 's_global_throughput', 's_lock_queue_length']
        state_vec = [c, w, r, ga, gt]
        all_state_vecs.append(state_vec)
    state_matrix = np.array(all_state_vecs, dtype=np.float32)
    return state_matrix, all_state_keys

if __name__ == "__main__":
    # notify_cpp(7)
    run_training_pipeline('config/default.yaml')