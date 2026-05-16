# src/ppo_pipeline.py

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


def notify_cpp(version, host="127.0.0.1", port=6556):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        msg = f"NEW_POLICY:{version}"
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

    min_samples = config.get("training", {}).get("min_samples", 2000)
    interval = config.get("training", {}).get("interval_sec", 10)

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

    last_train_time = 0
    latest_version = ldt_manager.get_current_version()

    print("=== PPO Training Service Started ===")

    while True:

        raw_df = data_loader.load_data()
        if raw_df.empty:
            time.sleep(2)
            continue

        filtered_df = data_filter.filter_by_policy_version(raw_df, latest_version)
        if len(filtered_df) < min_samples:
            print("[Pipeline] Not enough data, waiting...")
            time.sleep(2)
            continue

        now = time.time()
        if now - last_train_time < interval:
            time.sleep(1)
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
        # TODO: 对应所有state生成对应policy
        # all_states = all_possible_states()  # list of state vectors
        # prob_policy = {}
        # for _, row in final_df.iterrows():
        #     key = row['state_key']
        #     state_vec = row[state_cols].values
        #     probs = ppo.predict(state_vec)
        #     prob_policy[key] = [round(float(p), 4) for p in probs]
        unique_states_df = final_df.drop_duplicates(subset=['state_key'])
        unique_state_keys = unique_states_df['state_key'].values
        unique_state_vecs = unique_states_df[state_cols].values

        all_probs = ppo.predict_batch(unique_state_vecs)

        prob_policy = {}
        for key, probs in zip(unique_state_keys, all_probs):
            prob_policy[key] = [round(float(p), 4) for p in probs]

        version = ldt_manager.save_ldt_prob(prob_policy)
        latest_version = version

        # ✅ 通知 C++
        notify_cpp(version)
        ppo.save(model_path)

        last_train_time = now

        print("=== Training Round Finished ===\n")

def adapt_columns(df):
    df = df.copy()
    df['commit'] = df['is_commit'].astype(float)
    df['abort'] = 1.0 - df['commit']
    # 全局指标映射
    df['tps'] = df['s_global_throughput'].astype(float)
    df['abort_rate'] = df['s_global_abort_rate'].astype(float)
    # 时延
    df['latency'] = df['latency'].astype(float)

    return df


if __name__ == "__main__":
    run_training_pipeline('config/default.yaml')