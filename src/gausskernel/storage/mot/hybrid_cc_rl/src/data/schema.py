# src/data/schema.py

# timestamp,txn_id,policy_version,contention_tier,workload_tier,retry_tier,s_global_abort_rate,s_global_throughput,next_contention_tier,next_workload_tier,next_retry_tier,ns_global_abort_rate,ns_global_throughput,action,action_prob,exec_time,query_interval,priority_score,is_commit,latency,done
REQUIRED_COLUMNS = [
    "timestamp",
    "txn_id",
    "policy_version",
    "contention_tier",
    "workload_tier",
    "retry_tier",
    "s_global_abort_rate",
    "s_global_throughput",
    "next_contention_tier",
    "next_workload_tier",
    "next_retry_tier",
    "ns_global_abort_rate",
    "ns_global_throughput",
    "action",
    "action_prob",
    "exec_time",
    "query_interval",
    "priority_score",
    "is_commit",
    "latency",
    "done"
]

# 可选字段（用于分析 / 扩展）
OPTIONAL_COLUMNS = [
    "timestamp"
]
