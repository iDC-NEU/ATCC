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
}

void HybridCcLogger::WriteHeader()
{
    m_logFile << "timestamp,txn_id,txn_type,contention_tier,workload_tier,retry_tier,"
              << "exec_time,query_interval,action,is_commit,latency,"
              << "global_tps,global_abort_rate,priority_score,pre_csn,read_set,write_set\n";
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

} // namespace MOT
