import os
import pandas as pd
from typing import List
from .schema import REQUIRED_COLUMNS


class DataLoader:
    def __init__(self, config: dict):
        self.config = config
        self.data_path = config.get("data", {}).get("path", "logs/")
        self.file_type = config.get("data", {}).get("type", "csv")

    def _load_csv_files(self, files: List[str]) -> pd.DataFrame:
        dfs = []
        for f in files:
            try:
                df = pd.read_csv(f)
                dfs.append(df)
                print(f"[DataLoader] Loaded {f}, rows={len(df)}")
            except Exception as e:
                print(f"[DataLoader] ERROR loading {f}: {e}")

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    def _validate_schema(self, df: pd.DataFrame) -> bool:
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            print(f"[DataLoader] FATAL: Missing columns: {missing}")
            return False
        return True

    def load_data(self) -> pd.DataFrame:
        print("\n[Step 1] Loading data...")

        if not os.path.exists(self.data_path):
            print(f"[DataLoader] Path not found: {self.data_path}")
            return pd.DataFrame()

        files = [
            os.path.join(self.data_path, f)
            for f in os.listdir(self.data_path)
            if f.endswith(".csv")
        ]

        if not files:
            print("[DataLoader] No CSV files found.")
            return pd.DataFrame()

        df = self._load_csv_files(files)

        if df.empty:
            print("[DataLoader] Loaded empty dataframe.")
            return df

        if not self._validate_schema(df):
            return pd.DataFrame()

        print(f"[DataLoader] Total rows: {len(df)}")

        return df
