# src/ppo_trainer.py

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim

class ActorCriticNet(nn.Module):
    def __init__(self, state_dim, action_dim):
        super().__init__()
        # 共享特征提取层
        self.shared = nn.Sequential(
            nn.Linear(state_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 64),
            nn.ReLU()
        )
        # Actor头：输出动作概率
        self.actor = nn.Sequential(
            nn.Linear(64, action_dim),
            nn.Softmax(dim=-1)
        )
        # Critic头：输出状态的标量价值
        self.critic = nn.Linear(64, 1)

        # =========================================================
        # ✅ 专家先验注入 (Expert Initialization)
        # 强制让未经训练的网络初始输出 P(action=4) ≈ 70%
        # =========================================================
        actor_last_layer = self.actor[0] # 获取 nn.Linear(64, action_dim)
        # 1. 把最后一层权重初始化得非常小，让 bias 占据绝对主导
        torch.nn.init.orthogonal_(actor_last_layer.weight, gain=0.01)
        # 2. 初始化 bias 为 0
        torch.nn.init.constant_(actor_last_layer.bias, 0.0)
        # 3. 注入先验偏差：ln(0.70 / 0.075) ≈ 2.2336
        # action_dim=5 的情况下，索引 4 就是动作 4 (全悲观锁)
        actor_last_layer.bias.data[4] = 2.4849
        # actor_last_layer.bias.data[5] = 1.3862
        # =========================================================

    def forward(self, x):
        features = self.shared(x)
        action_probs = self.actor(features)
        state_value = self.critic(features)
        return action_probs, state_value

class PolicyNet(nn.Module):
    def __init__(self, state_dim, action_dim):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(state_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 64),
            nn.ReLU(),
            nn.Linear(64, action_dim),
            nn.Softmax(dim=-1)
        )

    def forward(self, x):
        return self.net(x)

class PPOTrainer:
    def __init__(self, state_dim, action_dim, config):
        self.policy = ActorCriticNet(state_dim, action_dim)
        # self.policy = PolicyNet(state_dim, action_dim)
        self.optimizer = optim.Adam(self.policy.parameters(), lr=1e-3)

        self.clip_eps = config.get("ppo", {}).get("clip_eps", 0.2)
        self.epochs = config.get("ppo", {}).get("epochs", 5)
        self.batch_size = config.get("ppo", {}).get("batch_size", 256)
        self.gamma = config['ppo'].get('gamma', 0.99)
        self.lam = config['ppo'].get('lambda', 0.95)

    # def train_bak(self, states, actions, old_probs, rewards):
    #
    #     states = torch.tensor(states, dtype=torch.float32)
    #     actions = torch.tensor(actions, dtype=torch.long)
    #     old_probs = torch.tensor(old_probs, dtype=torch.float32)
    #     rewards = torch.tensor(rewards, dtype=torch.float32)
    #
    #     dataset_size = states.shape[0]
    #     indices = np.arange(dataset_size)
    #
    #     for epoch in range(self.epochs):
    #         np.random.shuffle(indices)
    #
    #         for start in range(0, dataset_size, self.batch_size):
    #             end = start + self.batch_size
    #             batch_idx = indices[start:end]
    #
    #             s = states[batch_idx]
    #             a = actions[batch_idx]
    #             old_p = old_probs[batch_idx]
    #             r = rewards[batch_idx]
    #
    #             probs = self.policy(s)
    #             new_p = probs.gather(1, a.unsqueeze(1)).squeeze()
    #
    #             ratio = new_p / (old_p + 1e-8)
    #             clipped = torch.clamp(ratio, 1 - self.clip_eps, 1 + self.clip_eps)
    #
    #             loss = -torch.mean(torch.min(ratio * r, clipped * r))
    #
    #             self.optimizer.zero_grad()
    #             loss.backward()
    #             self.optimizer.step()
    #
    #     return loss.item()
    #
    # def predict_bak(self, state):
    #     with torch.no_grad():
    #         s = torch.tensor(state, dtype=torch.float32)
    #         return self.policy(s).numpy()

    def train(
            self,
            states,
            actions,
            old_probs,
            rewards,
            next_states,
            dones
    ):
        """
        PPO + GAE 训练

        参数:
            states: 当前状态
            actions: 执行动作
            old_probs: 旧策略下该动作概率
            rewards: reward
            next_states: 下一状态
            dones: 是否终止

        Returns:
            平均loss
        """

        # =========================
        # 1. numpy -> tensor
        # =========================
        states = torch.tensor(states, dtype=torch.float32)
        next_states = torch.tensor(next_states, dtype=torch.float32)
        actions = torch.tensor(actions, dtype=torch.long)
        old_probs = torch.tensor(old_probs, dtype=torch.float32)
        rewards = torch.tensor(rewards, dtype=torch.float32)
        dones = torch.tensor(dones, dtype=torch.float32)

        dataset_size = states.shape[0]

        indices = np.arange(dataset_size)

        # PPO超参数
        gamma = self.gamma
        lam = self.lam

        # ====================================================
        # 2. 计算 Value(s), Value(s')
        # ====================================================
        with torch.no_grad():
            # 当前状态价值
            _, state_values = self.policy(states)
            state_values = state_values.squeeze(-1)

            # 下一状态价值
            _, next_state_values = self.policy(next_states)
            next_state_values = next_state_values.squeeze(-1)

            # ====================================================
            # 3. TD Target
            #
            # target = r + gamma * V(s')
            # done时不bootstrap
            # ====================================================
            targets = (
                    rewards
                    + gamma * next_state_values * (1 - dones)
            )

            # ====================================================
            # 4. TD Error
            # δ_t
            # ====================================================
            deltas = targets - state_values

            # ====================================================
            # 5. GAE (Generalized Advantage Estimation)
            # ====================================================
            advantages = torch.zeros_like(rewards)
            gae = 0.0

            for t in reversed(range(dataset_size)):
                gae = (deltas[t]+ gamma * lam * (1 - dones[t]) * gae)
                advantages[t] = gae

            # ====================================================
            # 6. Advantage Normalization
            # PPO稳定关键
            # ====================================================
            returns = advantages + state_values
            advantages = (advantages - advantages.mean()) / (advantages.std() + 1e-8)

        # ====================================================
        # 7. PPO Multi-Epoch Update
        # ====================================================
        total_loss = 0.0
        sum_total_loss = 0.0
        sum_actor_loss = 0.0
        sum_critic_loss = 0.0
        sum_entropy = 0.0
        update_steps = 0  # 记录总共执行了多少次 mini-batch 更新

        for epoch in range(self.epochs):
            np.random.shuffle(indices)
            for start in range(0, dataset_size, self.batch_size):
                end = start + self.batch_size
                batch_idx = indices[start:end]

                # ------------------------
                # mini-batch
                # ------------------------
                s = states[batch_idx]
                a = actions[batch_idx]
                old_p = old_probs[batch_idx]
                adv = advantages[batch_idx]
                # tgt = targets[batch_idx]
                ret = returns[batch_idx]

                # ====================================================
                # 8. Forward
                # ====================================================
                probs, values = self.policy(s)
                values = values.squeeze(-1)
                # 当前策略下动作概率
                new_probs = probs.gather(1,a.unsqueeze(1)).squeeze(-1)

                # ====================================================
                # 9. PPO Ratio
                # ====================================================
                ratio = new_probs / (old_p + 1e-8)
                # PPO clip
                clipped_ratio = torch.clamp(
                    ratio,
                    1 - self.clip_eps,
                    1 + self.clip_eps
                )

                # ====================================================
                # 10. Actor Loss
                # ====================================================
                actor_loss = -torch.mean(
                    torch.min(
                        ratio * adv,
                        clipped_ratio * adv
                    )
                )

                # ====================================================
                # 11. Critic Loss
                # ====================================================
                # critic_loss = nn.MSELoss()(values, tgt)
                critic_loss = nn.MSELoss()(values, ret)
                # ====================================================
                # 12. Entropy Bonus
                #
                # 防止策略塌缩
                # ====================================================
                entropy = -torch.mean(
                    torch.sum(
                        probs * torch.log(probs + 1e-8),
                        dim=1
                    )
                )

                # ====================================================
                # 13. Total Loss
                # ====================================================
                loss = (
                        actor_loss
                        + 0.5 * critic_loss
                        - 0.01 * entropy
                )

                # ====================================================
                # 14. Backprop
                # ====================================================
                self.optimizer.zero_grad()

                loss.backward()

                # 防止梯度爆炸
                torch.nn.utils.clip_grad_norm_(
                    self.policy.parameters(),
                    0.5
                )

                self.optimizer.step()
                total_loss += loss.item()
                sum_total_loss += loss.item()
                sum_actor_loss += actor_loss.item()
                sum_critic_loss += critic_loss.item()
                sum_entropy += entropy.item()
                update_steps += 1

        avg_loss = total_loss / (
                self.epochs * (dataset_size // self.batch_size + 1)
        )
        return {
            "total_loss": sum_total_loss / update_steps,
            "actor_loss": sum_actor_loss / update_steps,
            "critic_loss": sum_critic_loss / update_steps,
            "entropy": sum_entropy / update_steps
        }
        # return avg_loss


    def predict(self, state):
        with torch.no_grad():
            s = torch.tensor(state, dtype=torch.float32)
            # 解包 tuple
            action_probs, _ = self.policy(s)
            return action_probs.numpy()

    def predict_batch(self, state_matrix):
        with torch.no_grad():
            s = torch.tensor(state_matrix, dtype=torch.float32)
            action_probs, _ = self.policy(s)
            return action_probs.numpy()

    def save(self, path):
        torch.save(self.policy.state_dict(), path)

    def load(self, path):
        self.policy.load_state_dict(torch.load(path))

class RewardCalculator:
    def __init__(self, config: dict):
        self.config = config

        # 权重（可在yaml调）
        # self.w_commit = config.get("reward", {}).get("w_commit", 1.0)
        # self.w_abort = config.get("reward", {}).get("w_abort", 1.5)
        # self.w_latency = config.get("reward", {}).get("w_latency", 0.8)
        # self.w_tps = config.get("reward", {}).get("w_tps", 0.3)
        # self.w_abort_rate = config.get("reward", {}).get("w_abort_rate", 1.2)

        # 主干权重 (R_succ, R_fail)
        self.r_succ = config.get("reward", {}).get("r_succ", 2.0)
        self.r_fail = config.get("reward", {}).get("r_fail", 1.0)

        # 子项 C(T) 权重 (omega_1, omega_2, omega_3) C(T) = w1*f_interval + w2*f_rs_ws + w3*f_retry
        self.omega1 = config.get("reward", {}).get("omega1", 0.1)
        self.omega2 = config.get("reward", {}).get("omega2", 0.5)
        self.omega3 = config.get("reward", {}).get("omega3", 1.5)

        # 系统权重 (psi, eta, theta)
        self.psi = config.get("reward", {}).get("psi", 2.0)      # 对应 r_wait (用 latency 替代)
        self.eta = config.get("reward", {}).get("eta", 8.0)      # 对应 \Delta TPS
        self.theta = config.get("reward", {}).get("theta", 3.0)  # 对应 \Delta Latency (用 abort_rate 替代)

        self.max_latency_sla = 1000.0      # 10000ms
        self.max_tps_sla = 200000.0      # 20万 TPS
        self.prev_avg_tps = None
        self.prev_p99_latency = None

    def compute(self, df):
        target_abort_rate = 0.005
        # 归一化
        # df['lat_norm'] = df['latency'] / self.max_latency_sla
        df['lat_norm'] = np.clip(df['latency'] / self.max_latency_sla, 0.0, 3.0)
        df['tps_norm'] = df['tps'] / self.max_tps_sla
        df['query_int_norm'] = df['query_interval'] / 10.0
        df['abort_rate_norm'] = df['abort_rate']

        # 2. C(T) = w1*f_interval + w2*f_rs_ws + w3*f_retry
        c_t = (
                self.omega1 * df['query_int_norm'] +   # 操作间隔
                self.omega2 * df['s_work'] +        # 读写集大小
                self.omega3 * df['s_retry']         # 重试次数
        )

        # reward = (
        #         self.w_commit * df['commit']
        #         - self.w_abort * df['abort']
        #         - self.w_latency * df['lat_norm']
        #         + self.w_tps * df['tps_norm']
        #         - self.w_abort_rate * df['abort_rate_norm']
        # )

        abort_excess = np.maximum(
            0,
            df['abort_rate'] - target_abort_rate
        )


        # 计算 \Delta TPS 和 \Delta Latency_p99
        if self.prev_avg_tps is None or self.prev_p99_latency is None:
            # 如果是系统刚启动的第一轮，没有历史对比，\Delta 设为 0 或者用绝对值近似
            delta_tps_norm = 0.0
            delta_lat_norm = 0.0
        else:
            # \Delta：当前事务的物理表现 减去 上一轮全局基线
            delta_tps_norm = (df['tps'] - self.prev_avg_tps) / self.max_tps_sla
            # 同样对 delta 进行截断保护 (-3.0 到 3.0)
            delta_lat_norm = np.clip(
                (df['latency'] - self.prev_p99_latency) / self.max_latency_sla,
                -3.0, 3.0
            )
            # delta_lat_norm = (df['latency'] - self.prev_p99_latency) / self.max_latency_sla

        # 3. R_t = I_commit * R_succ - I_abort * (R_fail + C(T)) - psi*r_wait + eta*TPS - theta*Latency
        terminal_bonus = (
                df['commit'] * self.r_succ
                - df['abort'] * (self.r_fail + c_t)
                - self.psi * df['lat_norm']            # 使用 latency 模拟锁等待
                + self.eta * delta_tps_norm            # \Delta TPS 带来的增益
                - self.theta * delta_lat_norm          # \Delta Latency 带来的系统级恶化
                - 20 * abort_excess
        )

        base_reward = 0.0

        # 使用 numpy.where 进行向量化分支：
        # 如果 df['done'] == 1（最后一步），给它 base_reward + terminal_bonus
        # 如果 df['done'] == 0（中间步骤），只给它 base_reward
        reward = np.where(df['done'] == 1, terminal_bonus, base_reward)
        # 标准化
        reward = (reward - reward.mean()) / (reward.std() + 1e-8)

        df['reward'] = reward
        return df

    def update_baselines(self, avg_tps, p99_latency):
        """在每轮训练结束后被 pipeline 调用，更新全局基线"""
        self.prev_avg_tps = avg_tps
        self.prev_p99_latency = p99_latency