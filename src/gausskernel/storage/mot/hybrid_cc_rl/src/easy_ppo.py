import numpy as np

# 假设最大TPS、最大延迟和最大重试次数
MAX_TPS = 10000
MAX_LATENCY = 200  # ms
MAX_ABORT_RATE = 1  # 比如最大回滚率是100%
MAX_RETRY_COUNT = 10

def calculate_reward(commit, abort, tps, latency, abort_rate, retry_count, hotspot_conflict):
    # Step 1: 归一化各个指标
    tps_norm = tps / MAX_TPS
    latency_norm = latency / MAX_LATENCY
    abort_rate_norm = abort_rate / MAX_ABORT_RATE
    retry_count_norm = retry_count / MAX_RETRY_COUNT

    # Step 2: 计算基本奖励
    reward_commit = 1.0 if commit else -1.0  # 提交事务奖励 +1，回滚事务惩罚 -1
    reward_abort = -1.5 * abort_rate_norm  # 回滚率惩罚（权重大于提交奖励）

    # Step 3: 计算系统稳定性和用户体验的惩罚
    reward_tps = 0.3 * tps_norm  # TPS的奖励，低TPS对系统不利
    reward_latency = -0.8 * latency_norm  # 延迟的惩罚，过高的延迟会严重影响性能
    reward_abort_rate = -1.2 * abort_rate_norm  # 回滚率对系统稳定性影响的惩罚

    # Step 4: 重试次数的影响
    reward_retry_count = -0.2 * retry_count_norm  # 重试次数的惩罚，越多越差

    # Step 5: 热点冲突的惩罚
    reward_hotspot = -0.5 * hotspot_conflict  # 热点冲突发生时惩罚

    # Step 6: 计算最终奖励
    total_reward = (reward_commit + reward_abort + reward_tps + reward_latency +
                    reward_abort_rate + reward_retry_count + reward_hotspot)

    return total_reward

# 测试用例
print(calculate_reward(commit=True, abort=False, tps=8000, latency=50,
                       abort_rate=0.05, retry_count=2, hotspot_conflict=1))


import torch
import torch.nn as nn
import torch.optim as optim

class PPOPolicyNetwork(nn.Module):
    def __init__(self, state_dim, action_dim):
        super(PPOPolicyNetwork, self).__init__()
        self.fc1 = nn.Linear(state_dim, 64)
        self.fc2 = nn.Linear(64, 64)
        self.fc3 = nn.Linear(64, action_dim)
        self.softmax = nn.Softmax(dim=-1)

    def forward(self, state):
        x = torch.relu(self.fc1(state))
        x = torch.relu(self.fc2(x))
        action_probs = self.softmax(self.fc3(x))
        return action_probs

# 模拟的训练函数
def train_ppo_policy_network(policy_network, optimizer, states, actions, rewards, gamma=0.99):
    # 转换成PyTorch张量
    states = torch.tensor(states, dtype=torch.float32)
    actions = torch.tensor(actions, dtype=torch.long)
    rewards = torch.tensor(rewards, dtype=torch.float32)

    # 计算折扣奖励（基于gamma折扣因子）
    discounted_rewards = []
    cumulative_reward = 0
    for r in rewards.flip(dims=[0]):  # 反转reward，倒序计算累计奖励
        cumulative_reward = r + gamma * cumulative_reward
        discounted_rewards.insert(0, cumulative_reward)
    discounted_rewards = torch.tensor(discounted_rewards, dtype=torch.float32)

    # 计算策略的损失
    action_probs = policy_network(states)
    chosen_action_probs = action_probs.gather(1, actions.unsqueeze(1)).squeeze(1)
    loss = -torch.mean(torch.log(chosen_action_probs) * discounted_rewards)  # 目标函数：损失最小化

    # 执行反向传播和优化
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

    return loss.item()

# 创建PPO模型
policy_network = PPOPolicyNetwork(state_dim=5, action_dim=3)
optimizer = optim.Adam(policy_network.parameters(), lr=0.001)

# 示例训练数据
states = [[0.2, 0.3, 0.4, 0.1, 0.5]]  # 状态的示例（如事务特征）
actions = [1]  # 选择的动作
rewards = [calculate_reward(commit=True, abort=False, tps=8000, latency=50,
                            abort_rate=0.05, retry_count=2, hotspot_conflict=0)]

# 训练一步
loss = train_ppo_policy_network(policy_network, optimizer, states, actions, rewards)
print(f"Loss: {loss}")

