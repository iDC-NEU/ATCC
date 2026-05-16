import pandas as pd


class DataFilter:
    def __init__(self, config: dict):
        self.config = config

    def filter_by_policy_version(self, df: pd.DataFrame, latest_version) -> pd.DataFrame:
        print("\n[Step 2] Filtering by policy_version (On-Policy)...")

        if df.empty:
            print("[DataFilter] Empty dataframe, skip filtering.")
            return df

        if "policy_version" not in df.columns:
            print("[DataFilter] ERROR: policy_version not found.")
            return pd.DataFrame()


        filtered_df = df[df["policy_version"] == latest_version].copy()

        print(f"[DataFilter] Latest version: {latest_version}")
        print(f"[DataFilter] Rows after filter: {len(filtered_df)}")

        # 防止数据太少
        min_samples = self.config.get("training", {}).get("min_samples", 1000)

        if len(filtered_df) < min_samples:
            print(
                f"[DataFilter] WARNING: Not enough samples "
                f"({len(filtered_df)} < {min_samples})"
            )

        return filtered_df
