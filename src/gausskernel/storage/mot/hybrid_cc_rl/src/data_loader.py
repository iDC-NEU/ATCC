import pandas as pd
import os
from typing import Dict, Any, List

class DataLoader:
    """
    Responsible for loading transaction log data from raw CSV files.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the DataLoader with configuration.

        Args:
            config: A dictionary containing the application configuration,
                    including the directory path for raw log files.
        """
        raw_log_dir = config['data_paths']['raw_log_dir']
        
        # 如果是相对路径，转换为绝对路径（相对于当前工作目录）
        if not os.path.isabs(raw_log_dir):
            self.raw_log_dir = os.path.abspath(raw_log_dir)
        else:
            self.raw_log_dir = raw_log_dir
        
        # This defines the "data contract" with the C++ engine.
        # 注意：所有的特征都是原始值，tier 将由 FeatureEngineer 动态生成
        self.expected_columns = [
            "timestamp", "txn_id", "txn_type",
            # 原始特征值（用于离散化）
            "contention", "workload", "retry_count", 
            "exec_time", "query_interval", "priority_score",
            # 执行信息
            "action", "is_commit", "latency",
            "global_tps", "global_abort_rate", 
            "read_set", "write_set"
        ]
        
        # 列名映射：支持不同数据源的列名变体
        # 键是我们期望的标准列名，值是可能的别名列表
        self.column_mappings = {
            "contention": ["contention", "contention_tier"],
            "workload": ["workload", "workload_tier"],
            "retry_count": ["retry_count", "retry_tier", "retry_cnt"],
            "priority_score": ["priority_score", "priority"],
            "global_tps": ["s_global_throughput"],
            "global_abort_rate": ["s_global_abort_rate"],
            "query_interval": ["query_interval"]
        }

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        标准化DataFrame的列名，将各种变体映射到标准列名。
        
        Args:
            df: 原始DataFrame
            
        Returns:
            列名已标准化的DataFrame
        """
        df = df.copy()
        current_columns = list(df.columns)
        rename_dict = {}
        
        # 对每个期望的标准列名，查找并映射可能的别名
        for standard_name, aliases in self.column_mappings.items():
            for alias in aliases:
                if alias in current_columns and alias != standard_name:
                    rename_dict[alias] = standard_name
                    break
        
        if rename_dict:
            df.rename(columns=rename_dict, inplace=True)
            print(f"  ✓ 列名映射: {rename_dict}")
        
        return df
    
    def _validate_and_select_columns(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """
        验证并选择所需的列，填充缺失的列。
        
        Args:
            df: 已标准化列名的DataFrame
            filename: 文件名（用于日志）
            
        Returns:
            包含所有期望列的DataFrame，如果验证失败则返回None
        """
        current_columns = set(df.columns)
        expected_columns = set(self.expected_columns)
        
        # 检查缺失的列
        missing_columns = expected_columns - current_columns
        
        # 如果缺少关键列，尝试用默认值填充
        if missing_columns:
            print(f"  ⚠️  文件 {os.path.basename(filename)} 缺少列: {missing_columns}")
            
            # 为缺失的列填充默认值
            for col in missing_columns:
                if col == "priority_score":
                    # 如果缺少 priority_score，使用默认值 0
                    df[col] = 0
                    print(f"     - 已为 '{col}' 填充默认值: 0")
                elif col in ["read_set", "write_set"]:
                    # 如果缺少 read_set 或 write_set，使用空字符串, 新增txn_type
                    df[col] = ""
                    print(f"     - 已为 '{col}' 填充默认值: ''")
                else:
                    # 其他关键列缺失，无法继续
                    print(f"     ✗ 关键列 '{col}' 缺失且无法填充，跳过该文件")
                    return None
        
        # 删除额外的列（如 pre_csn）并按期望顺序排列
        extra_columns = current_columns - expected_columns
        if extra_columns:
            print(f"  ℹ️  忽略额外的列: {extra_columns}")
        
        # 只选择期望的列，按照期望的顺序
        df = df[self.expected_columns]
        
        return df

    def load_data(self) -> pd.DataFrame:
        """
        Loads all CSV and LOG files from the raw log directory, concatenates them,
        and returns a single pandas DataFrame.

        Returns:
            A pandas DataFrame containing the combined transaction log data.
            Returns an empty DataFrame if no valid CSV or LOG files are found.
        """
        # 支持 .csv 和 .log 文件
        all_files = [os.path.join(self.raw_log_dir, f) 
                     for f in os.listdir(self.raw_log_dir) if f.endswith(('.csv', '.log'))]
        
        if not all_files:
            print(f"Warning: No CSV or LOG files found in {self.raw_log_dir}")
            return pd.DataFrame(columns=self.expected_columns)

        df_list = []
        for file in all_files:
            try:
                print(f"\n📂 正在处理文件: {os.path.basename(file)}")
                df = pd.read_csv(file)
                
                # 标准化列名
                df = self._normalize_columns(df)
                
                # 验证并选择列
                df = self._validate_and_select_columns(df, file)
                
                if df is not None:
                    df_list.append(df)
                    print(f"  ✅ 成功加载 {len(df)} 条记录")
                else:
                    print(f"  ❌ 跳过该文件")
                    
            except Exception as e:
                print(f"  ❌ 读取文件时出错: {e}")
        
        if not df_list:
            print("\n⚠️  警告: 没有成功加载任何有效数据。")
            return pd.DataFrame(columns=self.expected_columns)

        combined_df = pd.concat(df_list, ignore_index=True)
        print(f"\n{'='*60}")
        print(f"✅ 数据加载完成")
        print(f"{'='*60}")
        print(f"  - 成功加载的文件数: {len(df_list)}")
        print(f"  - 总记录数: {len(combined_df)}")
        print(f"  - 数据列: {list(combined_df.columns)}")
        print(f"{'='*60}\n")
        
        return combined_df

if __name__ == '__main__':
    # Test the DataLoader with real configuration
    import yaml
    
    try:
        with open('config/default.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        data_loader = DataLoader(config)
        loaded_df = data_loader.load_data()
        
        print(f"Successfully loaded {len(loaded_df)} records")
        print(f"Columns: {list(loaded_df.columns)}")
        print(f"Sample data:\n{loaded_df.head()}")
        
    except Exception as e:
        print(f"Error testing DataLoader: {e}")
