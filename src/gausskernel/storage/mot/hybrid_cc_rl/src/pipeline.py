# src/pipeline.py

import yaml
from typing import Dict, Any

from .data_loader import DataLoader
from .feature_engineer import FeatureEngineer
from .reward_calculator import RewardCalculator
from .optimizer import Optimizer
from .ldt_manager import LdtManager

def run_training_pipeline(config_path: str = 'config/default.yaml'):
    """
    Executes the complete offline LDT training pipeline.

    This function serves as the main entry point for an external system (e.g., a database)
    to trigger the training process. It orchestrates the loading of raw data,
    feature engineering, reward calculation, policy optimization, and saving the
    final LDT artifact.

    Args:
        config_path (str): The path to the YAML configuration file.
    """
    print("--- Starting HybridCC Offline Training Pipeline ---")
    
    # 1. Load Configuration
    print(f"Loading configuration from '{config_path}'...")
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(f"FATAL: Error loading configuration from {config_path}: {e}")
        return

    # 2. Load Data
    print("\nStep 1: Loading data...")
    data_loader = DataLoader(config)
    raw_df = data_loader.load_data()

    if raw_df.empty:
        print("\nFATAL: No data found. Exiting pipeline. Please ensure logs exist in the specified directory.")
        print("--- Pipeline Finished ---")
        return

    # 3. Calculate Rewards
    print("\nStep 2: Calculating rewards...")
    reward_calculator = RewardCalculator(config)
    rewarded_df = reward_calculator.calculate_rewards(raw_df)

    # 4. Feature Engineering
    print("\nStep 3: Performing feature engineering...")
    feature_engineer = FeatureEngineer(config)
    final_df = feature_engineer.transform(rewarded_df)

    # 5. Run Optimization
    print("\nStep 4: Optimizing LDT policy...")
    ldt_manager = LdtManager(config)
    # Read number of actions from config to avoid hard-coding
    num_actions = int(config.get('optimizer', {}).get('num_actions', 5))
    
    optimizer = Optimizer(config, num_actions=num_actions)
    best_policy = optimizer.optimize(final_df)

    # 6. Save the Optimal LDT
    if best_policy:
        print("\nStep 5: Saving the best LDT found...")
        ldt_manager.save_ldt(best_policy)
    else:
        print("\nWarning: Optimization did not produce a valid policy. LDT not saved.")

    print("\n--- Pipeline Finished ---")

if __name__ == '__main__':
    # This allows the pipeline to be run directly for testing purposes.
    # In production, the `run_training_pipeline` function would be imported and called.
    run_training_pipeline()

