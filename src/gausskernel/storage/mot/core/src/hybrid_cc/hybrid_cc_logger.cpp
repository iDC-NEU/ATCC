#include "hybrid_cc_logger.h"
#include "txn_access.h"
#include "row.h"
#include "table.h"
#include "utils/utilities.h"  // For now_to_us()
#include "system/global.h"    // For TxnState enum
#include <sstream>
#include <algorithm>

namespace MOT {

// Note: Header string is emitted in WriteHeader(); no global LOG_HEADER constant is needed.

HybridCcLogger& HybridCcLogger::GetInstance()
{
    static HybridCcLogger instance;
    return instance;
}

bool HybridCcLogger::Init(const std::string& filepath)
{
    if (m_isInitialized) {
        return true;
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_isInitialized) {
        return true;
    }

    m_filepath = filepath;
    m_logFile.open(m_filepath, std::ios_base::app); // Append mode

    if (!m_logFile.is_open()) {
        // In a real engine, we'd use the engine's logging facility, e.g., MOT_LOG_ERROR
        fprintf(stderr, "Failed to open HybridCC log file at %s\n", m_filepath.c_str());
        return false;
    }

    // Write header if the file is new/empty
    if (m_logFile.tellp() == 0) {
        WriteHeader();
    }

    // 初始化 PPO 轨迹日志文件 (自动追加 _traj 后缀)
    m_trajFilepath = filepath + "_" + std::to_string(m_currentVersion.load()) + "_traj";
    m_trajFile.open(m_trajFilepath, std::ios_base::app);
    if (!m_trajFile.is_open()) {
        fprintf(stderr, "Failed to open HybridCC trajectory file at %s\n", m_trajFilepath.c_str());
        return false;
    }
    if (m_trajFile.tellp() == 0) {
        WriteTrajHeader();
    }

    // Initialize global statistics
    m_startTime = std::chrono::steady_clock::now();
    m_lastUpdateTime = m_startTime;
    m_totalTransactions = 0;
    m_committedTransactions = 0;
    m_abortedTransactions = 0;
    m_windowIndex = 0;

    m_isInitialized = true;
    return true;
}

HybridCcLogger::~HybridCcLogger()
{
    if (m_logFile.is_open()) {
        m_logFile.close();
    }
    if (m_trajFile.is_open()) m_trajFile.close();
}

void HybridCcLogger::WriteHeader()
{
    m_logFile << "timestamp,txn_id,txn_type,contention_tier,workload_tier,retry_tier,"
              << "exec_time,query_interval,action,is_commit,latency,"
              << "global_tps,global_abort_rate,priority_score,pre_csn,read_set,write_set\n";
}

// 新增 PPO 轨迹 CSV 的表头写入方法
void HybridCcLogger::WriteTrajHeader()
{

        // start_timestamp,txn_id,policy_version,
        // s_cont,s_work,s_retry,s_global_abort_rate,s_global_throughput,
        // ns_cont,ns_work,ns_retry,ns_global_abort_rate,ns_global_throughput,
        // real_retry_count, real_global_tps, real_abort_rate
        // action,action_prob,exec_time,query_interval,priority_score,is_commit,latency,done
    m_trajFile
        << "timestamp,txn_id,policy_version,"
        << "contention_tier,workload_tier,retry_tier,s_global_abort_rate,s_global_throughput,"
        << "next_contention_tier,next_workload_tier,next_retry_tier,ns_global_abort_rate,ns_global_throughput,"
        << "real_retry_count,real_global_tps,real_abort_rate,"
        << "action,action_prob,"
        << "exec_time,query_interval,priority_score,"
        << "is_commit,latency,done\n";
}

void HybridCcLogger::LogTransaction(TxnManager* txMan)
{
    if (!m_isInitialized || !m_logFile.is_open()) {
        return;
    }

    std::stringstream ss;

    // 1. Extract features from TxnManager
    // Note: Some of these are placeholders and will need to be implemented
    // in the TxnManager or passed to this function.
    uint64_t timestamp = txMan->start_time;
    uint64_t txn_id = txMan->GetInternalTransactionId();
    const char* txn_type = "YCSB";  // Placeholder: Needs real logic, e.g. from a stored procedure id
    int contention_tier = txMan->GetHotCnt();
    int workload_tier = txMan->m_accessMgr->GetOrderedRowSet().size();
    int retry_tier = txMan->retry_cnt;
    
    uint64_t end_time = now_to_us();
    double exec_time = (end_time > timestamp) ? (double)(end_time - timestamp) / 1000.0 : 0.0;
    double query_interval = txMan->GetAvgOperationInterval(); // HYBRID_CC: Now using actual operation interval
    int action = txMan->m_hybridCcAction;
    bool commit = (txMan->GetTxnState() == TxnState::TXN_COMMIT);
    double latency = exec_time;

    // 2. Extract global statistics
    UpdateGlobalStats(commit);
    double global_tps = GetGlobalTPS();
    double global_abort_rate = GetGlobalAbortRate();
    
    // HYBRID_CC: Extract priority information
    uint64_t priority_score = txMan->score_;
    uint64_t pre_csn = txMan->pre_csn;

    // 3. Build read_set and write_set strings
    std::stringstream read_set_ss;
    std::stringstream write_set_ss;
    bool first_read = true;
    bool first_write = true;

    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    for (const auto& raPair : orderedSet) {
        const Access* access = raPair.second;
        if (access->m_localRow == nullptr) continue;

        std::string row_identifier = access->m_localRow->GetTable()->GetLongTableName() + ":" + 
                                     std::to_string(access->m_localRow->GetRowId());

        if (access->m_type == RD) {
            if (!first_read) read_set_ss << ",";
            read_set_ss << row_identifier;
            first_read = false;
        } else { // WR, INS, DEL
            if (!first_write) write_set_ss << ",";
            write_set_ss << row_identifier;
            first_write = false;
        }
    }

    // 4. Assemble the final log line
    ss << timestamp << ","
       << txn_id << ","
       << txn_type << ","
       << contention_tier << ","
       << workload_tier << ","
       << retry_tier << ","
       << exec_time << ","
       << query_interval << ","
       << action << ","
       << (commit ? 1 : 0) << ","
       << latency << ","
       << global_tps << ","
       << global_abort_rate << ","
       << priority_score << ","
       << pre_csn << ","
       << "\"" << read_set_ss.str() << "\","
       << "\"" << write_set_ss.str() << "\"\n";

    // 5. Write to file under a lock
    std::lock_guard<std::mutex> lock(m_mutex);
    m_logFile << ss.str();
    m_logFile.flush(); // Ensure data is written immediately, good for debugging
}

void HybridCcLogger::UpdateGlobalStats(bool isCommit)
{
    m_totalTransactions.fetch_add(1);
    if (isCommit) {
        m_committedTransactions.fetch_add(1);
    } else {
        m_abortedTransactions.fetch_add(1);
    }
    
    // Update rolling window for TPS calculation
    auto now = std::chrono::steady_clock::now();
    size_t currentIndex = m_windowIndex.load();
    m_recentCompletions[currentIndex] = now;
    m_recentCommits[currentIndex] = isCommit;
    m_windowIndex.store((currentIndex + 1) % TPS_WINDOW_SIZE);
    m_lastUpdateTime = now;
}

double HybridCcLogger::GetGlobalTPS() const
{
    return CalculateTPS();
}

double HybridCcLogger::GetGlobalAbortRate() const
{
    return CalculateAbortRate();
}

double HybridCcLogger::CalculateTPS() const
{
    auto now = std::chrono::steady_clock::now();
    auto timeSinceStart = std::chrono::duration_cast<std::chrono::milliseconds>(now - m_startTime);
    
    if (timeSinceStart.count() == 0) {
        return 0.0;
    }
    
    // Calculate TPS based on total transactions since start
    double totalSeconds = timeSinceStart.count() / 1000.0;
    return static_cast<double>(m_totalTransactions.load()) / totalSeconds;
}

double HybridCcLogger::CalculateAbortRate() const
{
    uint64_t total = m_totalTransactions.load();
    if (total == 0) {
        return 0.0;
    }
    
    uint64_t aborted = m_abortedTransactions.load();
    return static_cast<double>(aborted) / static_cast<double>(total);
}

// 将多步轨迹展开为 CSV 行，并写入独立的 PPO 轨迹文件
void HybridCcLogger::LogTransactionTrajectory(std::unique_ptr<MOT::TxnTrajectory> traj) {
    if (m_currentVersion.load() != m_newVersion.load()) {
        RotateTrajFile(m_newVersion.load());
        m_currentVersion.store(m_newVersion.load());
    }
    // 注意这里检查的是 m_trajFile
    if (!m_trajFile.is_open() || !traj || traj->step_count == 0) return;

    // 使用 stringstream 缓存所有的行，避免在循环中一直持锁写磁盘
    std::stringstream ss;

    // start_timestamp,txn_id,policy_version,
    // s_cont,s_work,s_retry,s_global_abort_rate,s_global_throughput,
    // ns_cont,ns_work,ns_retry,ns_global_abort_rate,ns_global_throughput,
    // real_retry_count, real_global_tps, real_abort_rate
    // action,action_prob,exec_time,query_interval,priority_score,is_commit,latency,done
    for (int i = 0; i < traj->step_count; ++i) {
        auto& step = traj->steps[i];

        // 拼接 CSV 行，严格匹配 WriteTrajHeader 的顺序
        ss << traj->start_timestamp << ","
           << traj->txn_id << ","
           << traj->policy_version << ","

           // ===== 状态（必须对齐Python）=====
           << step.s_cont << ","
           << step.s_work << ","
           << step.s_retry << ","
           << step.s_global_abort_rate << ","
           << step.s_global_throughput << ","

           << step.ns_cont << ","
           << step.ns_work << ","
           << step.ns_retry << ","
           << step.ns_global_abort_rate << ","
           << step.ns_global_throughput << ","

           << traj->retry_count << ","
           << traj->global_tps << ","
           << traj->abort_rate << ","

           // ===== 动作 =====
           << step.action << ","
           << step.action_prob << ","

           // ===== 特征信息 =====
           << step.exec_time << ","
           << step.query_interval << ","
           << step.priority_score << ","

           // ===== 结果（用于reward）=====
           << (step.is_commit ? 1 : 0) << ","
           << traj->total_latency << "," // Reward 在python端统一计算
           << (step.done ? 1 : 0) << "\n";
    }

    // 写入独立的 PPO 轨迹文件，并缩短锁临界区
    std::lock_guard<std::mutex> lock(m_mutex);
    m_trajFile << ss.str();
    m_trajFile.flush();
}

void HybridCcLogger::RotateTrajFile(uint64_t new_version)
{
    // 获取轨迹文件的独占锁！这会阻塞所有正在尝试写入 CSV 的工作线程
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_currentVersion.load() == new_version) {
        return; // 版本没变，无需切换
    }

    // 关闭旧文件
    if (m_trajFile.is_open()) {
        m_trajFile.flush(); // 确保旧数据全部落盘
        m_trajFile.close();
    }

    // 组装新文件名：{filepath}_{version}_traj
    m_currentVersion = new_version;
    m_trajFilepath = m_filepath + "_" + std::to_string(new_version) + "_traj";

    // 打开新文件
    m_trajFile.open(m_trajFilepath, std::ios_base::app);
    if (!m_trajFile.is_open()) {
        return;
    }

    // 写入 Header
    if (m_trajFile.tellp() == 0) {
        WriteTrajHeader();
    }
}
} // namespace MOT
