# mot/hybrid_cc_rl/src/optimizer.py

import pandas as pd
import numpy as np
from typing import Dict, Any, List

class Optimizer:
    """
    Implements an Evolutionary Algorithm (EA) to find the optimal LDT policy.
    基于日志中统计得到的状态-动作平均奖励来评估候选策略的效果，更适合集成在实际系统中的离线学习流程。
    """

    def __init__(self, config: Dict[str, Any], num_actions: int):
        """
        Initializes the EA Optimizer.
        """
        self.params = config.get('optimizer', {})
        self.population_size = self.params.get('population_size', 20)
        self.generations = self.params.get('generations', 50)
        self.mutation_rate = self.params.get('mutation_rate', 0.1)
        self.crossover_rate = self.params.get('crossover_rate', 0.7)
        self.elite_size = int(self.population_size * self.params.get('elite_ratio', 0.1))
        self.num_actions = num_actions
        self.low_support_threshold = self.params.get('low_support_threshold', 5)
        self.low_support_penalty = self.params.get('low_support_penalty', 1.0)
        self.missing_action_penalty = self.params.get('missing_action_penalty', 25.0)
        
        # Ensure at least one elite individual is preserved
        if self.elite_size == 0 and self.population_size > 0:
            self.elite_size = 1

        # 训练时按日志统计得到的支持信息
        self._state_counts: Dict[str, int] = {}
        self._reward_lookup: Dict[tuple, Dict[str, float]] = {}
        self._global_reward_mean: float = 0.0

    def _initialize_population(self, unique_states: List[str]) -> List[Dict[str, int]]:
        """Creates an initial population of random policies."""
        population = []
        for _ in range(self.population_size):
            policy = {state: np.random.randint(0, self.num_actions) for state in unique_states}
            population.append(policy)
        return population

    def _prepare_reward_statistics(self, data: pd.DataFrame):
        """预计算状态-动作奖励表，用于快速评估候选策略。"""
        grouped = (
            data.groupby(['state_key', 'action'])['reward_final']
            .agg(['mean', 'count'])
            .reset_index()
        )

        self._reward_lookup = {
            (row['state_key'], int(row['action'])): {
                'mean': float(row['mean']),
                'count': int(row['count'])
            }
            for _, row in grouped.iterrows()
        }

        self._state_counts = data['state_key'].value_counts().to_dict()
        self._global_reward_mean = float(data['reward_final'].mean()) if not data.empty else 0.0

    def _evaluate_policy_reward(self, policy: Dict[str, int]) -> float:
        """基于预统计的奖励信息评估策略的平均收益。"""
        if not self._state_counts:
            return -float('inf')

        total_weighted_reward = 0.0
        total_weight = 0
        penalty = 0.0

        for state, freq in self._state_counts.items():
            action = policy.get(state, 0)
            stats = self._reward_lookup.get((state, action))

            if stats:
                total_weighted_reward += stats['mean'] * freq
                if stats['count'] < self.low_support_threshold:
                    penalty += (self.low_support_threshold - stats['count']) * self.low_support_penalty
            else:
                # 对于缺乏数据支撑的动作，使用全局均值并额外惩罚
                total_weighted_reward += (self._global_reward_mean - self.missing_action_penalty) * freq
                penalty += self.missing_action_penalty * freq

            total_weight += freq

        if total_weight == 0:
            return -float('inf')

        mean_reward = total_weighted_reward / total_weight
        normalized_penalty = penalty / total_weight
        fitness = mean_reward - normalized_penalty

        return fitness if not pd.isna(fitness) else -float('inf')

    def _selection(self, population: List[Dict[str, int]], fitness_scores: List[float]) -> List[Dict[str, int]]:
        """Selects the best individuals for the next generation (elitism)."""
        sorted_indices = np.argsort(fitness_scores)[::-1] # Sort descending
        elites = [population[i] for i in sorted_indices[:self.elite_size]]
        return elites

    def _crossover(self, parent1: Dict[str, int], parent2: Dict[str, int]) -> Dict[str, int]:
        """[IMPROVED] Performs single-point crossover on the shared state space."""
        child = parent1.copy()
        # Find states present in both parents to avoid introducing purely random actions
        shared_states = list(set(parent1.keys()) & set(parent2.keys()))
        
        if not shared_states:
            return child # No common ground for crossover

        crossover_point = np.random.randint(1, len(shared_states))
        states_to_swap = shared_states[crossover_point:]

        for state in states_to_swap:
            if np.random.rand() < self.crossover_rate:
                child[state] = parent2[state]
        return child

    def _mutation(self, policy: Dict[str, int], generation: int) -> Dict[str, int]:
        """[IMPROVED] Performs mutation with a decaying probability for fine-tuning."""
        mutated_policy = policy.copy()
        # Decay mutation strength over generations for better convergence
        mutation_strength = 1.0 - (generation / self.generations)

        for state in mutated_policy:
            if np.random.rand() < self.mutation_rate:
                if np.random.rand() > mutation_strength:
                    # Fine-tuning later in the training: small step
                    step = np.random.choice([-1, 1])
                    mutated_policy[state] = np.clip(mutated_policy[state] + step, 0, self.num_actions - 1)
                else:
                    # Exploration earlier in the training: big jump
                    mutated_policy[state] = np.random.randint(0, self.num_actions)
        return mutated_policy

    def optimize(self, data: pd.DataFrame) -> Dict[str, int]:
        """
        Runs the evolutionary algorithm to find the best LDT policy.
        """
        if data.empty:
            print("Warning: Optimizer received an empty dataframe. Skipping optimization.")
            return {}
            
        unique_states = data['state_key'].unique().tolist()
        if not unique_states:
             print("Warning: No unique states found in data. Skipping optimization.")
             return {}

        required_columns = {'state_key', 'action', 'reward_final'}
        missing_columns = required_columns - set(data.columns)
        if missing_columns:
            raise ValueError(f"Optimizer requires columns {required_columns}, but missing {missing_columns}.")

        # 使用一份副本，避免意外修改上游数据
        working_df = data[['state_key', 'action', 'reward_final']].copy()

        if not np.issubdtype(working_df['action'].dtype, np.number):
            try:
                working_df['action'] = pd.to_numeric(working_df['action'])
            except ValueError:
                working_df['action'] = working_df['action'].astype('category').cat.codes

        self._prepare_reward_statistics(working_df)

        if not self._reward_lookup:
            print("Warning: 无法根据日志构建状态-动作奖励表，跳过优化。")
            return {}

        population = self._initialize_population(unique_states)
        best_policy_so_far = None
        best_fitness_so_far = -float('inf')

        for gen in range(self.generations):
            fitness_scores = [self._evaluate_policy_reward(p) for p in population]

            current_best_fitness_in_gen = max(fitness_scores)
            if current_best_fitness_in_gen > best_fitness_so_far:
                best_fitness_so_far = current_best_fitness_in_gen
                best_policy_so_far = population[np.argmax(fitness_scores)]

            print(f"Generation {gen+1}/{self.generations}, Best Fitness in Gen: {current_best_fitness_in_gen:.4f}, Overall Best: {best_fitness_so_far:.4f}")

            elites = self._selection(population, fitness_scores)
            next_population = elites[:]  # Start with elites
            
            # Generate the rest of the population through crossover and mutation
            while len(next_population) < self.population_size:
                # Select parents from the entire population (not just elites)
                p1, p2 = np.random.choice(population, 2, replace=True)
                child = self._crossover(p1, p2)
                child = self._mutation(child, gen)
                next_population.append(child)
            
            population = next_population
        
        print("\nOptimization finished.")
        return best_policy_so_far