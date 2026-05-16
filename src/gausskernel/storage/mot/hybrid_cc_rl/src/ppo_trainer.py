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

        avg_loss = total_loss / (
                self.epochs * (dataset_size // self.batch_size + 1)
        )

        return avg_loss


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
        self.w_commit = config.get("reward", {}).get("w_commit", 1.0)
        self.w_abort = config.get("reward", {}).get("w_abort", 1.5)
        self.w_latency = config.get("reward", {}).get("w_latency", 0.8)
        self.w_tps = config.get("reward", {}).get("w_tps", 0.3)
        self.w_abort_rate = config.get("reward", {}).get("w_abort_rate", 1.2)
        self.max_latency_sla = 10.0      # 例如 10ms
        self.max_tps_sla = 100000.0      # 例如 10万 TPS

    def compute(self, df):
        # 归一化
        df['lat_norm'] = df['latency'] / self.max_latency_sla
        df['tps_norm'] = df['tps'] / self.max_tps_sla
        df['abort_rate_norm'] = df['abort_rate']

        reward = (
                self.w_commit * df['commit']
                - self.w_abort * df['abort']
                - self.w_latency * df['lat_norm']
                + self.w_tps * df['tps_norm']
                - self.w_abort_rate * df['abort_rate_norm']
        )

        # 标准化（PPO稳定关键）
        reward = (reward - reward.mean()) / (reward.std() + 1e-8)

        df['reward'] = reward
        return df
