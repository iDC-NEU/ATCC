# mot/hybrid_cc_rl/src/feature_engineer.py (FINAL VERSION)

import pandas as pd
from typing import Dict, Any

class FeatureEngineer:
    """
    Transforms raw data into features suitable for the learning algorithm.
    This includes:
    1. Discretizing continuous values into tiers based on config.
    2. Creating a composite state_key for the LDT.
    3. Pre-processing read/write sets from string to Python set objects.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('feature_engineering', {})

    def _discretize(self, series: pd.Series, tiers: list, labels: list) -> pd.Series:
        bins = [-float('inf')] + tiers + [float('inf')]
        return pd.cut(series, bins=bins, labels=labels, ordered=False)

    def _preprocess_sets(self, series: pd.Series) -> pd.Series:
        """Helper function to convert comma-separated strings to sets."""
        return series.astype(str).apply(lambda x: set(x.split(',')) if x and x != 'nan' else set())

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        transformed_df = df.copy()

        # --- 1. Discretize features for state definition ---
        # 从原始特征（contention, workload, retry_count, exec_time, query_interval, priority_score）
        # 生成离散化的 tier 特征（contention_tier, workload_tier, retry_count_tier, etc.）
        tier_configs = self.config.get('tiers', {})
        
        # Dynamically apply discretization based on config
        for col_name, tiers in tier_configs.items():
            if col_name in transformed_df.columns:
                # e.g., labels for 'exec_time' will be ['exec_time_TIER_0', 'exec_time_TIER_1', ...]
                labels = [f"{col_name}_TIER_{i}" for i in range(len(tiers) + 1)]
                # The new column name will be 'exec_time_tier'
                new_col_name = f"{col_name}_tier"
                transformed_df[new_col_name] = self._discretize(transformed_df[col_name], tiers, labels)
                print(f"[OK] Discretized '{col_name}' -> '{new_col_name}' with {len(tiers)+1} tiers")
            else:
                print(f"[WARNING] Column '{col_name}' for discretization not found in data.")

        # --- 2. Create the composite state_key ---
        state_columns = self.config.get('state_key_features', [])
        
        # Optimized: Vectorized string concatenation to avoid OOM on large datasets
        if state_columns:
            # Initialize with the first column
            transformed_df['state_key'] = transformed_df[state_columns[0]].astype(str)
            # Iteratively add the rest
            for col in state_columns[1:]:
                transformed_df['state_key'] = transformed_df['state_key'] + ',' + transformed_df[col].astype(str)
        else:
            transformed_df['state_key'] = ''

        # --- 3. Pre-process read/write sets for the optimizer ---
        if 'read_set' in transformed_df.columns:
            transformed_df['read_set'] = self._preprocess_sets(transformed_df['read_set'])
        else:
            print("Warning: 'read_set' column not found. Optimizer simulation might be inaccurate.")
            transformed_df['read_set'] = [set()] * len(transformed_df)

        if 'write_set' in transformed_df.columns:
            transformed_df['write_set'] = self._preprocess_sets(transformed_df['write_set'])
        else:
            print("Warning: 'write_set' column not found. Optimizer simulation might be inaccurate.")
            transformed_df['write_set'] = [set()] * len(transformed_df)
        
        # --- 4. Prepare timestamps for the optimizer ---
        if 'timestamp' in transformed_df.columns:
            transformed_df.rename(columns={'timestamp': 'start_time'}, inplace=True)
            transformed_df['end_time'] = transformed_df['start_time'] + transformed_df['latency']
        else:
            raise ValueError("Missing 'timestamp' column for event-driven simulation.")

        print("Feature engineering complete. Added discretized tiers, 'state_key', and pre-processed sets.")
        return transformed_df


    def transform_traj(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        # =========================
        # 0. 排序（极重要！为GAE准备）
        # =========================
        df = df.sort_values(['txn_id', 'timestamp'])

        # =========================
        # 1. 当前 state tier
        # =========================
        df['c_tier'] = df['contention_tier'].astype(int)
        df['w_tier'] = df['workload_tier'].astype(int)
        df['r_tier'] = df['retry_tier'].astype(int)

        # =========================
        # 2. 当前 global → tier
        # =========================
        abort_bins = self.config['tiers']['abort_rate_bins']
        tps_bins = self.config['tiers']['tps_bins']

        df['ga_tier'] = pd.cut(
            df['s_global_abort_rate'],
            bins=abort_bins,
            labels=False,
            include_lowest=True
        ).fillna(0).astype(int)

        df['gt_tier'] = pd.cut(
            df['s_global_throughput'],
            bins=tps_bins,
            labels=False,
            include_lowest=True
        ).fillna(0).astype(int)

        # =========================
        # 3. next state tier（核心）
        # =========================
        df['ns_c_tier'] = df['next_contention_tier'].fillna(0).astype(int)
        df['ns_w_tier'] = df['next_workload_tier'].fillna(0).astype(int)
        df['ns_r_tier'] = df['next_retry_tier'].fillna(0).astype(int)

        df['ns_ga_tier'] = pd.cut(
            df['ns_global_abort_rate'],
            bins=abort_bins,
            labels=False,
            include_lowest=True
        ).fillna(0).astype(int)

        df['ns_gt_tier'] = pd.cut(
            df['ns_global_throughput'],
            bins=tps_bins,
            labels=False,
            include_lowest=True
        ).fillna(0).astype(int)

        # =========================
        # 4. state_key（当前）
        # =========================
        df['state_key'] = (
                "c" + df['c_tier'].astype(str) +
                "_w" + df['w_tier'].astype(str) +
                "_r" + df['r_tier'].astype(str) +
                "_ga" + df['ga_tier'].astype(str) +
                "_gt" + df['gt_tier'].astype(str)
        )

        # =========================
        # 5. next_state_key（用于debug/可选）
        # =========================
        df['next_state_key'] = (
                "c" + df['ns_c_tier'].astype(str) +
                "_w" + df['ns_w_tier'].astype(str) +
                "_r" + df['ns_r_tier'].astype(str) +
                "_ga" + df['ns_ga_tier'].astype(str) +
                "_gt" + df['ns_gt_tier'].astype(str)
        )

        # =========================
        # 6. read/write set
        # =========================
        if 'read_set' in df.columns:
            df['read_set'] = self._preprocess_sets(df['read_set'])
        else:
            df['read_set'] = [set()] * len(df)

        if 'write_set' in df.columns:
            df['write_set'] = self._preprocess_sets(df['write_set'])
        else:
            df['write_set'] = [set()] * len(df)

        # =========================
        # 7. 时间字段
        # =========================
        if 'timestamp' in df.columns:
            df.rename(columns={'timestamp': 'start_time'}, inplace=True)
            df['end_time'] = df['start_time'] + df['latency']
        else:
            raise ValueError("Missing 'timestamp' column")

        print("✅ Feature engineering complete (PPO-ready with next_state)")

        return df
