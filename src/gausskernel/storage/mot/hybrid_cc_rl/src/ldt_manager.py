import json
import os
from typing import Dict, List, Any


class LdtManager:
    """
    Manages the creation, saving, and loading of the Lightweight Decision Table (LDT).
    The LDT is stored in a JSON format that is interpretable by the C++ engine.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the LdtManager with configuration.

        Args:
            config: A dictionary containing the application configuration, 
                    including the output path for the LDT file and tier thresholds.
        """
        output_path = config['data_paths']['output_ldt_path']
        
        # 如果是相对路径，转换为绝对路径（相对于当前工作目录）
        if not os.path.isabs(output_path):
            self.output_path = os.path.abspath(output_path)
        else:
            self.output_path = output_path
        
        # HYBRID_CC: 从配置中提取tier阈值
        # 这些阈值将被嵌入到LDT文件中，确保C++运行时使用与训练时完全一致的离散化标准
        tier_config = config.get('feature_engineering', {}).get('tiers', {})
        
        # 转换配置格式：从default.yaml的格式转换为LDT期望的格式
        # default.yaml中的键名（如 'contention'）需要转换为C++期望的键名（如 'contention_tier'）
        self.tier_thresholds = {}
        
        # 映射关系：default.yaml中的特征名 -> LDT中的特征名
        feature_mapping = {
            'exec_time': 'exec_time',
            'query_interval': 'query_interval',
            'priority_score': 'priority_score',
            'contention': 'contention_tier',      # CSV列名是contention_tier，但yaml配置是contention
            'workload': 'workload_tier',          # CSV列名是workload_tier，但yaml配置是workload
            'retry_count': 'retry_tier'           # CSV列名是retry_tier，但yaml配置是retry_count
        }
        
        for yaml_key, ldt_key in feature_mapping.items():
            if yaml_key in tier_config:
                thresholds = tier_config[yaml_key]
                # 确保阈值是列表格式
                if isinstance(thresholds, list):
                    self.tier_thresholds[ldt_key] = thresholds
                    print(f"  Loaded tier config: {ldt_key} = {thresholds}")
        
        print(f"LdtManager initialized with {len(self.tier_thresholds)} tier configurations")
        self.num_actions = config['optimizer']['num_actions']
        self.config = config
        self.ldt_structure = {
            "state_features": [
                "transaction_type",
                "contention_tier",
                "workload_tier",
                "retry_count_tier",
                "execution_time_tier",
                "query_interval_tier",
                "priority_score_tier"  # 新增：优先级层级特征
            ],
            "actions": [
                "Stay Optimistic",                 # Action 0
                "Lock Hot Write Set",              # Action 1
                "Lock All Hot Sets",               # Action 2
                "Lock All Writes & Hot Reads",     # Action 3
                "Lock Entire Access Set",          # Action 4
                "Prioritize Transaction Only"      # Action 5: 新增动作 - 只提升优先级，不切换模式
            ],
            "tiers": self.tier_thresholds,  # HYBRID_CC: 添加tier阈值配置
            "table": {}
        }

    def save_ldt(self, best_policy: Dict[str, int]):
        """
        Saves the learned policy into the LDT JSON file.
        
        HYBRID_CC: The saved LDT includes:
        - tiers: Threshold configurations for discretizing continuous features
        - table: The trained decision table mapping states to actions

        Args:
            best_policy: A dictionary representing the optimal policy found by the optimizer.
                         Keys are string representations of states, and values are action indices.
        """
        # 创建LDT结构（不包含state_features和actions元数据，只保留C++需要的部分）
        ldt_to_save = {
            "tiers": self.tier_thresholds,  # HYBRID_CC: Tier阈值配置
            "table": {}
        }
        
        # Convert numpy int64 to regular int for JSON serialization
        serializable_policy = {}
        for state, action in best_policy.items():
            serializable_policy[state] = int(action)
        
        ldt_to_save['table'] = serializable_policy

        try:
            # 确保输出目录存在
            output_dir = os.path.dirname(self.output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            with open(self.output_path, 'w') as f:
                json.dump(ldt_to_save, f, indent=2)
            
            print(f"\n{'='*60}")
            print(f"Successfully saved LDT to: {self.output_path}")
            print(f"{'='*60}")
            print(f"LDT Statistics:")
            print(f"  - Tier configurations: {len(self.tier_thresholds)}")
            print(f"  - Decision table entries: {len(serializable_policy)}")
            print(f"  - Unique states covered: {len(serializable_policy)}")
            
            # 显示tier配置摘要
            if self.tier_thresholds:
                print(f"\nTier Thresholds:")
                for feature, thresholds in self.tier_thresholds.items():
                    print(f"  - {feature}: {thresholds}")
            
            # 显示action分布
            action_counts = {}
            for action in serializable_policy.values():
                action_counts[action] = action_counts.get(action, 0) + 1
            
            if action_counts:
                print(f"\nAction Distribution:")
                action_names = self.ldt_structure['actions']
                for action_id in sorted(action_counts.keys()):
                    action_name = action_names[action_id] if action_id < len(action_names) else f"Action {action_id}"
                    count = action_counts[action_id]
                    percentage = (count / len(serializable_policy)) * 100
                    print(f"  - Action {action_id} ({action_name}): {count} ({percentage:.1f}%)")
            
            print(f"{'='*60}\n")
            
        except IOError as e:
            print(f"Error saving LDT file: {e}")

    def load_ldt(self) -> Dict[str, Any]:
        """
        Loads an LDT from a JSON file.

        Returns:
            A dictionary representing the loaded LDT.
        """
        try:
            with open(self.output_path, 'r') as f:
                ldt = json.load(f)
            print(f"Successfully loaded LDT from {self.output_path}")
            return ldt
        except FileNotFoundError:
            print(f"LDT file not found at {self.output_path}. Returning empty structure.")
            return self.ldt_structure
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error loading or parsing LDT file: {e}")
            return self.ldt_structure

    def save_ldt_prob(self, prob_policy: Dict[str, List[float]]) -> int:
        """
        保存 PPO 概率策略 LDT

        Args:
            prob_policy: {state_key: [prob0, prob1, ...]}
        """

        # ✅ version 自动递增
        version = self._get_next_version()

        ldt_to_save = {
            "version": version,
            "num_actions": self.num_actions,
            "tiers": self.tier_thresholds,
            "table": {}
        }

        # ✅ 概率校验 + 修正
        min_prob = self.config.get("ldt", {}).get("min_prob", 0.001)
        validated_policy = {}
        for state, probs in prob_policy.items():

            # 转 float + 防止 numpy 类型
            probs = [float(p) for p in probs]

            # 防御：长度错误
            if len(probs) != self.num_actions:
                print(f"[LDT] WARNING: invalid prob length for {state}, fallback uniform")
                probs = [1.0 / self.num_actions] * self.num_actions

            # 防止概率为0
            probs = [max(p, min_prob) for p in probs]
            # 防御：归一化（防止数值误差）
            s = sum(probs)
            if s <= 0:
                probs = [1.0 / self.num_actions] * self.num_actions
            else:
                probs = [p / s for p in probs]

            # 保留4位小数（减少JSON体积）
            probs = [round(p, 4) for p in probs]

            validated_policy[state] = probs

        ldt_to_save["table"] = validated_policy

        # ✅ 原子写入（防止C++读到半文件）
        tmp_path = self.output_path + ".tmp"

        try:
            output_dir = os.path.dirname(self.output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)

            # 先写 tmp
            with open(tmp_path, 'w') as f:
                json.dump(ldt_to_save, f, indent=2)

            # 原子替换
            os.replace(tmp_path, self.output_path)

            # ===== 打印统计 =====
            print(f"\n{'='*60}")
            print(f"[LDT] Saved PPO LDT → {self.output_path}")
            print(f"{'='*60}")

            print(f"Version: {version}")
            print(f"States: {len(validated_policy)}")
            print(f"Actions: {self.num_actions}")

            # Action 分布（看策略是否塌缩）
            action_usage = [0] * self.num_actions

            for probs in validated_policy.values():
                best_action = int(max(range(len(probs)), key=lambda i: probs[i]))
                action_usage[best_action] += 1

            print("\nAction Distribution (argmax):")
            for i, cnt in enumerate(action_usage):
                pct = cnt / len(validated_policy) * 100 if validated_policy else 0
                print(f"  - Action {i}: {cnt} ({pct:.1f}%)")

            print(f"{'='*60}\n")

        except IOError as e:
            print(f"[LDT] ERROR saving file: {e}")

        return version

    def get_current_version(self) -> int:
        if not os.path.exists(self.output_path):
            print(f"[LDT] ERROR file: {self.output_path} not found")
            return 0

        try:
            with open(self.output_path, 'r') as f:
                print(f"[LDT] Get file: {self.output_path}")
                old = json.load(f)
                return int(old.get("version", 0))
        except:
            print(f"[LDT] Except file: {self.output_path} except")
            return 0
    # ==========================
    # version 管理（关键）
    # ==========================
    def _get_next_version(self) -> int:
        """
        自动递增 version
        """
        if not os.path.exists(self.output_path):
            return 1

        try:
            with open(self.output_path, 'r') as f:
                old = json.load(f)
                return int(old.get("version", 0)) + 1
        except:
            return 1


if __name__ == '__main__':
    # Example usage - demonstrating tier configuration integration
    mock_config = {
        'data_paths': {
            'output_ldt_path': 'output/ldt_example.json'
        },
        'feature_engineering': {
            'tiers': {
                'exec_time': [19.59, 80.6],
                'query_interval': [3.07, 8.66],
                'priority_score': [1e15, 5e15],
                'contention': [2, 5],
                'workload': [5, 15],
                'retry_count': [1, 3]
            }
        }
    }
    
    # Create a manager
    ldt_manager = LdtManager(mock_config)

    # Create a mock policy with correct state key format
    # Format: "YCSB,contention_TIER_X,workload_TIER_X,retry_count_TIER_X,exec_time_TIER_X,query_interval_TIER_X,priority_score_TIER_X"
    mock_policy = {
        "YCSB,contention_TIER_0,workload_TIER_0,retry_count_TIER_0,exec_time_TIER_0,query_interval_TIER_0,priority_score_TIER_0": 0,
        "YCSB,contention_TIER_1,workload_TIER_1,retry_count_TIER_0,exec_time_TIER_2,query_interval_TIER_2,priority_score_TIER_0": 1,
        "YCSB,contention_TIER_2,workload_TIER_2,retry_count_TIER_1,exec_time_TIER_2,query_interval_TIER_2,priority_score_TIER_1": 4,
        "YCSB,contention_TIER_2,workload_TIER_2,retry_count_TIER_2,exec_time_TIER_2,query_interval_TIER_2,priority_score_TIER_2": 5,
    }

    print("\n" + "="*60)
    print("LdtManager Test - Saving LDT with Tier Configuration")
    print("="*60)
    
    # Save the policy
    ldt_manager.save_ldt(mock_policy)

    # Load it back
    loaded_ldt = ldt_manager.load_ldt()
    print("\nLoaded LDT structure:")
    print(json.dumps(loaded_ldt, indent=2))
    
    print("\n" + "="*60)
    print("Test completed successfully!")
    print("="*60)
