
#include <random>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <thread>

#include "system/global.h"
#include "utils/utilities.h"
#include "system/transaction/txn_access.h"
#include <sys/socket.h>

#include "hybrid_cc_manager.h"
#include "hybrid_cc_logger.h"

namespace MOT {

DECLARE_LOGGER(HybridCcManager, System);



HybridCcManager& HybridCcManager::GetInstance()
{
    static HybridCcManager HybridCcManager_instance;

    return HybridCcManager_instance;
}

bool HybridCcManager::Init(const std::string& ldtFilePath)
{
    if (m_isInitialized) {
        return true;
    }

    //std::lock_guard<std::mutex> lock(m_tableMutex);
    if (m_isInitialized) {
        return true;
    }

    m_ldtFilePath = ldtFilePath;

    // Load the initial LDT
    if (!LoadLDT()) {
        // If LDT file doesn't exist, create a default decision table
        // Default behavior: all states map to action 0 (Stay Optimistic)
        MOT_LOG_INFO("LDT file not found, using default optimistic policy");
        m_decisionTable.clear();
    }

    // No explicit OCC manager pointer required; we'll use TxnManager wrapper methods

    m_isInitialized = true;
    MOT_LOG_INFO("HybridCcManager initialized successfully");
    return true;
}

int HybridCcManager::Decide(TxnManager* txMan)
{
    if (!m_isInitialized) {
        return 0; // Default to optimistic
    }

    std::string stateKey = ExtractStateKey(txMan);

    std::lock_guard<std::mutex> lock(m_tableMutex);
    auto it = m_decisionTable.find(stateKey);

    if (it != m_decisionTable.end()) {
        return it->second;
    } else {
        // If state not found in table, default to optimistic
        return 0;
    }
}

int HybridCcManager::DecideTraj(TxnManager* txMan)
{
    if (!m_isInitialized) {
        return 0; // Default to optimistic
    }

    std::string stateKey = ExtractStateKeyTraj(txMan);
    int action = 0;
    double action_prob = 1.0;
    uint32_t policy_version = m_currentVersion;

    std::lock_guard<std::mutex> lock(m_tableMutex);
    auto it = m_decisionTable_prob.find(stateKey);

    if (it != m_decisionTable_prob.end()) {
        const std::vector<double>& probs = it->second;
        // 根据 PPO 提供的概率分布进行随机采样
        // 使用 thread_local 保证多线程高并发下的无锁随机数生成性能
        static thread_local std::mt19937 generator;
        static thread_local bool is_seeded = false;
        if (!is_seeded) {
            uint64_t seed = now_to_us() ^ std::hash<std::thread::id>{}(std::this_thread::get_id());
            generator.seed(seed);
            is_seeded = true;
        }
        std::uniform_real_distribution<double> distribution(0.0, 1.0);
        double r = distribution(generator);

        double cumulative = 0.0;
        bool selected = false;

        for (size_t i = 0; i < probs.size(); ++i) {
            cumulative += probs[i];
            if (r <= cumulative) {
                action = static_cast<int>(i);
                action_prob = probs[i];
                selected = true;
                break;
            }
        }

        // 浮点数精度防错兜底
        if (!selected && !probs.empty()) {
            action = probs.size() - 1;
            action_prob = probs.back();
        }
    } else {
        // Table Miss: 状态未命中，走默认兜底策略
        action = 0;
        action_prob = 1.0;
    }

    // 在做出决定的这一刻，立刻将Tier快照记录下来
    // 并发控制算法不会回退，action只能提升
    if (txMan->m_hybridCcAction <= action) RecordDecisionTraj(txMan, action, action_prob, policy_version);
    return action;
}

RC HybridCcManager::ExecuteAction(TxnManager* txMan, int action, int prev_action)
{
    if (!m_isInitialized) {
        return RC_OK; // Fallback to default behavior
    }
    if (action > prev_action) {
        if (action != 5) txMan->m_hybridCcAction = action;
    } else {
        return RC_OK;
    }

    // HYBRID_CC: Action definitions (see README.md for details)
    // 0: Stay Optimistic (do nothing)
    // 1: Lock Hot Write Set
    // 2: Lock All Hot Sets
    // 3: Lock All Writes & Hot Reads
    // 4: Lock Entire Access Set
    // 5: Boost Priority (increase transaction priority)

    switch (action) {
        case 0:
            // HYBRID_CC: Stay optimistic - no action needed
            return RC_OK;

        case 1:
            // HYBRID_CC: Lock Hot Write Set only
            return txMan->WriteLockForSwitchHotRows_Plor();

        case 2:
            // HYBRID_CC: Lock All Hot Sets (both read and write)
            {
                RC rc = txMan->ReadLockForSwitchHotRows_Plor(); // hot reads
                if (rc != RC_OK) return rc;
                return txMan->WriteLockForSwitchHotRows_Plor(); // hot writes
            }

        case 3:
            // HYBRID_CC: Lock All Writes & Hot Reads
            {
                RC rc = txMan->ReadLockForSwitchHotRows_Plor(); // hot reads
                if (rc != RC_OK) return rc;
                return txMan->WriteLockForSwitch_Plor(); // all writes
            }

        case 4:
            // HYBRID_CC: Lock Entire Access Set (full pessimistic)
            {
                RC rc = txMan->ReadLockForSwitch_Plor(); // all reads
                if (rc != RC_OK) return rc;
                return txMan->WriteLockForSwitch_Plor(); // all writes
            }

        case 5:
            // HYBRID_CC: Boost Priority - increase transaction priority for lock competition
            // This increases retry_cnt by 1, which gives a huge priority boost (~1.44e17)
            txMan->BoostPriority(1);
            MOT_LOG_INFO("HYBRID_CC: Action 5 executed - Priority boosted for txn_id=%llu, new_priority=%llu",
                        txMan->GetInternalTransactionId(), txMan->GetCurrentPriority());
            return RC_OK;

        default:
            MOT_LOG_WARN("HYBRID_CC: Unknown action %d, defaulting to optimistic", action);
            return RC_OK;
    }
}

bool HybridCcManager::ReloadLDT()
{
    if (!m_isInitialized) {
        return false;
    }

    return LoadLDT();
}

// HYBRID_CC: Map raw value to tier index based on thresholds
int HybridCcManager::MapValueToTier(double value, const std::vector<double>& thresholds) const
{
    if (thresholds.empty()) {
        return 0; // Default to TIER_0 if no thresholds
    }

    if (thresholds.size() == 1) {
        // Binary classification: <= threshold is TIER_0, > threshold is TIER_1
        return (value <= thresholds[0]) ? 0 : 1;
    }

    // Standard 3-tier classification
    if (value <= thresholds[0]) {
        return 0; // TIER_0
    } else if (value <= thresholds[1]) {
        return 1; // TIER_1
    } else {
        return 2; // TIER_2
    }
}

std::string HybridCcManager::ExtractStateKey(TxnManager* txMan)
{
    // HYBRID_CC: Build state key by extracting raw values and mapping to tiers
    std::stringstream ss;

    // 1. Transaction Type (simplified for now - could be extended)
    ss << "YCSB,";

    // 2. Contention Tier - based on hot_cnt (number of hot rows accessed)
    int contention_raw = txMan->hot_cnt;
    auto contention_it = m_tierThresholds.find("contention_tier");
    int contention_tier = (contention_it != m_tierThresholds.end())
                         ? MapValueToTier(contention_raw, contention_it->second)
                         : 0;
    ss << "contention_TIER_" << contention_tier << ",";

    // 3. Workload Tier - based on access set size
    int workload_raw = txMan->m_accessMgr->GetOrderedRowSet().size();
    auto workload_it = m_tierThresholds.find("workload_tier");
    int workload_tier = (workload_it != m_tierThresholds.end())
                       ? MapValueToTier(workload_raw, workload_it->second)
                       : 0;
    ss << "workload_TIER_" << workload_tier << ",";

    // 4. Retry Tier - based on retry_cnt
    int retry_raw = txMan->retry_cnt;
    auto retry_it = m_tierThresholds.find("retry_tier");
    int retry_tier = (retry_it != m_tierThresholds.end())
                    ? MapValueToTier(retry_raw, retry_it->second)
                    : 0;
    ss << "retry_count_TIER_" << retry_tier << ",";

    // 5. Execution Time Tier - time since transaction start (in ms)
    uint64_t currentTime = now_to_us();
    double exec_time_raw = (currentTime > txMan->start_time) ?
                           (double)(currentTime - txMan->start_time) / 1000.0 : 0.0;
    auto exec_time_it = m_tierThresholds.find("exec_time");
    int exec_time_tier = (exec_time_it != m_tierThresholds.end())
                        ? MapValueToTier(exec_time_raw, exec_time_it->second)
                        : 0;
    ss << "exec_time_TIER_" << exec_time_tier << ",";

    // 6. Query Interval Tier - average time between operations (in ms)
    double query_interval_raw = txMan->GetAvgOperationInterval();
    auto query_interval_it = m_tierThresholds.find("query_interval");
    int query_interval_tier = (query_interval_it != m_tierThresholds.end())
                             ? MapValueToTier(query_interval_raw, query_interval_it->second)
                             : 0;
    ss << "query_interval_TIER_" << query_interval_tier << ",";

    // 7. Priority Score Tier - transaction priority score
    double priority_raw = (double)txMan->score_;
    auto priority_it = m_tierThresholds.find("priority_score");
    int priority_tier = (priority_it != m_tierThresholds.end())
                       ? MapValueToTier(priority_raw, priority_it->second)
                       : 0;
    ss << "priority_score_TIER_" << priority_tier;

    return ss.str();
}


bool HybridCcManager::LoadLDT()
{
    std::ifstream file(m_ldtFilePath);
    if (!file.is_open()) {
        MOT_LOG_ERROR("Cannot open LDT file: %s", m_ldtFilePath.c_str());
        return false;
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string jsonContent = buffer.str();
    file.close();

    return ParseJSONProb(jsonContent);
}

bool HybridCcManager::ParseJSON(const std::string& jsonContent)
{
    // HYBRID_CC: Parse LDT JSON format with tiers and table sections
    // Expected format: {"tiers": {...}, "table": {...}}

    std::lock_guard<std::mutex> lock(m_tableMutex);
    m_decisionTable.clear();
    m_tierThresholds.clear();

    // ========== Parse "tiers" section ==========
    size_t tiersStart = jsonContent.find("\"tiers\"");
    if (tiersStart != std::string::npos) {
        // Find the opening brace after "tiers"
        tiersStart = jsonContent.find("{", tiersStart);
        if (tiersStart != std::string::npos) {
            // Parse tier thresholds
            size_t pos = tiersStart + 1;
            while (pos < jsonContent.length()) {
                // Skip whitespace
                while (pos < jsonContent.length() && std::isspace(jsonContent[pos])) {
                    pos++;
                }

                if (pos >= jsonContent.length() || jsonContent[pos] == '}') {
                    break; // End of tiers object
                }

                // Find feature name (between quotes)
                if (jsonContent[pos] != '"') {
                    pos++;
                    continue;
                }

                size_t featureStart = pos + 1;
                size_t featureEnd = jsonContent.find('"', featureStart);
                if (featureEnd == std::string::npos) break;

                std::string featureName = jsonContent.substr(featureStart, featureEnd - featureStart);

                // Find colon
                pos = featureEnd + 1;
                while (pos < jsonContent.length() && jsonContent[pos] != ':') {
                    pos++;
                }
                if (pos >= jsonContent.length()) break;
                pos++; // Skip colon

                // Find array opening bracket
                while (pos < jsonContent.length() && std::isspace(jsonContent[pos])) {
                    pos++;
                }
                if (pos >= jsonContent.length() || jsonContent[pos] != '[') {
                    pos++;
                    continue;
                }
                pos++; // Skip '['

                // Parse threshold values in array
                std::vector<double> thresholds;
                while (pos < jsonContent.length()) {
                    // Skip whitespace and commas
                    while (pos < jsonContent.length() &&
                           (std::isspace(jsonContent[pos]) || jsonContent[pos] == ',')) {
                        pos++;
                    }

                    if (pos >= jsonContent.length() || jsonContent[pos] == ']') {
                        pos++; // Skip ']'
                        break;
                    }

                    // Parse number (could be int or double)
                    size_t numStart = pos;
                    while (pos < jsonContent.length()) {
                        char c = jsonContent[pos];
                        // Allow digits, '.', '-', 'e', 'E', and '+' for exponents
                        if (std::isdigit(c) || c == '.' || c == '-' || c == 'e' || c == 'E' || c == '+') {
                            pos++;
                        } else {
                            // Stop if we hit a character that cannot be part of a number, like comma or bracket
                            break;
                        }
                    }
                    // while (pos < jsonContent.length() &&
                    //        (std::isdigit(jsonContent[pos]) || jsonContent[pos] == '.' ||
                    //         jsonContent[pos] == '-' || jsonContent[pos] == 'e' || jsonContent[pos] == 'E')) {
                    //     pos++;
                    // }

                    if (numStart < pos) {
                        std::string numStr = jsonContent.substr(numStart, pos - numStart);
                        // Use strtod instead of stod to avoid exceptions
                        char* endPtr = nullptr;
                        double value = strtod(numStr.c_str(), &endPtr);
                        // Check if conversion was successful
                        if (endPtr != numStr.c_str() && *endPtr == '\0') {
                            thresholds.push_back(value);
                        }
                        // Otherwise skip invalid numbers
                    }
                }

                if (!thresholds.empty()) {
                    m_tierThresholds[featureName] = thresholds;
                }

                // Skip to next entry
                while (pos < jsonContent.length() &&
                       (std::isspace(jsonContent[pos]) || jsonContent[pos] == ',')) {
                    pos++;
                }
            }

            MOT_LOG_INFO("HYBRID_CC: Loaded %zu tier configurations", m_tierThresholds.size());
        }
    } else {
        MOT_LOG_WARN("HYBRID_CC: No 'tiers' section found in LDT, will use default tier mapping");
    }

    // ========== Parse "table" section ==========
    size_t tableStart = jsonContent.find("\"table\"");
    if (tableStart == std::string::npos) {
        MOT_LOG_ERROR("LDT JSON format error: 'table' section not found");
        return false;
    }

    // Find the opening brace after "table"
    tableStart = jsonContent.find("{", tableStart);
    if (tableStart == std::string::npos) {
        MOT_LOG_ERROR("LDT JSON format error: table object not found");
        return false;
    }

    // Simple parsing of key-value pairs
    size_t pos = tableStart + 1;
    while (pos < jsonContent.length()) {
        // Skip whitespace
        while (pos < jsonContent.length() && std::isspace(jsonContent[pos])) {
            pos++;
        }

        if (pos >= jsonContent.length() || jsonContent[pos] == '}') {
            break; // End of table object
        }

        // Find key (between quotes)
        if (jsonContent[pos] != '"') {
            pos++;
            continue;
        }

        size_t keyStart = pos + 1;
        size_t keyEnd = jsonContent.find('"', keyStart);
        if (keyEnd == std::string::npos) break;

        std::string key = jsonContent.substr(keyStart, keyEnd - keyStart);

        // Find colon
        pos = keyEnd + 1;
        while (pos < jsonContent.length() && jsonContent[pos] != ':') {
            pos++;
        }
        if (pos >= jsonContent.length()) break;
        pos++; // Skip colon

        // Find value (integer)
        while (pos < jsonContent.length() && std::isspace(jsonContent[pos])) {
            pos++;
        }

        size_t valueStart = pos;
        while (pos < jsonContent.length() &&
               (std::isdigit(jsonContent[pos]) || jsonContent[pos] == '-')) {
            pos++;
        }

        if (valueStart < pos) {
            std::string valueStr = jsonContent.substr(valueStart, pos - valueStart);
            // Use strtol instead of stoi to avoid exceptions
            char* endPtr = nullptr;
            long actionLong = strtol(valueStr.c_str(), &endPtr, 10);
            // Check if conversion was successful
            if (endPtr != valueStr.c_str() && *endPtr == '\0') {
                int action = static_cast<int>(actionLong);
                m_decisionTable[key] = action;
            }
        }

        // Skip comma if present
        while (pos < jsonContent.length() &&
               (std::isspace(jsonContent[pos]) || jsonContent[pos] == ',')) {
            pos++;
        }
    }

    MOT_LOG_INFO("Loaded %zu decision table entries from LDT", m_decisionTable.size());
    return true;
}

bool HybridCcManager::ParseJSONProb(const std::string& jsonContent)
{
    std::lock_guard<std::mutex> lock(m_tableMutex);
    m_decisionTable_prob.clear();
    m_tierThresholds.clear();

    // ========== 0. Parse "version" section ==========
    size_t versionStart = jsonContent.find("\"version\"");
    if (versionStart != std::string::npos) {
        size_t colonPos = jsonContent.find(':', versionStart);
        if (colonPos != std::string::npos) {
            size_t valStart = colonPos + 1;
            while (valStart < jsonContent.length() && std::isspace(jsonContent[valStart])) valStart++;
            size_t valEnd = valStart;
            while (valEnd < jsonContent.length() && std::isdigit(jsonContent[valEnd])) valEnd++;
            if (valEnd > valStart) {
                m_currentVersion = std::stoull(jsonContent.substr(valStart, valEnd - valStart));
            }
        }
    }

    // ========== 1. Parse "tiers" section ==========
    size_t tiersStart = jsonContent.find("\"tiers\"");
    if (tiersStart != std::string::npos) {
        tiersStart = jsonContent.find("{", tiersStart);
        if (tiersStart != std::string::npos) {
            size_t pos = tiersStart + 1;
            while (pos < jsonContent.length()) {
                while (pos < jsonContent.length() && std::isspace(jsonContent[pos])) pos++;
                if (pos >= jsonContent.length() || jsonContent[pos] == '}') break;

                if (jsonContent[pos] != '"') { pos++; continue; }
                size_t featureStart = pos + 1;
                size_t featureEnd = jsonContent.find('"', featureStart);
                if (featureEnd == std::string::npos) break;
                std::string featureName = jsonContent.substr(featureStart, featureEnd - featureStart);

                pos = featureEnd + 1;
                while (pos < jsonContent.length() && jsonContent[pos] != ':') pos++;
                if (pos >= jsonContent.length()) break;
                pos++;

                while (pos < jsonContent.length() && std::isspace(jsonContent[pos])) pos++;
                if (pos >= jsonContent.length() || jsonContent[pos] != '[') { pos++; continue; }
                pos++;

                std::vector<double> thresholds;
                while (pos < jsonContent.length()) {
                    while (pos < jsonContent.length() && (std::isspace(jsonContent[pos]) || jsonContent[pos] == ',')) pos++;
                    if (pos >= jsonContent.length() || jsonContent[pos] == ']') {
                        pos++; break;
                    }
                    size_t numStart = pos;
                    while (pos < jsonContent.length()) {
                        char c = jsonContent[pos];
                        if (std::isdigit(c) || c == '.' || c == '-' || c == 'e' || c == 'E' || c == '+') {
                            pos++;
                        } else break;
                    }
                    if (numStart < pos) {
                        std::string numStr = jsonContent.substr(numStart, pos - numStart);
                        char* endPtr = nullptr;
                        double value = strtod(numStr.c_str(), &endPtr);
                        if (endPtr != numStr.c_str() && *endPtr == '\0') {
                            thresholds.push_back(value);
                        }
                    }
                }
                if (!thresholds.empty()) {
                    m_tierThresholds[featureName] = thresholds;
                }
                while (pos < jsonContent.length() && (std::isspace(jsonContent[pos]) || jsonContent[pos] == ',')) pos++;
            }
        }
    }

    // ========== 2. Parse "table" section (支持数组概率解析) ==========
    size_t tableStart = jsonContent.find("\"table\"");
    if (tableStart != std::string::npos) {
        tableStart = jsonContent.find("{", tableStart);
        if (tableStart != std::string::npos) {
            size_t pos = tableStart + 1;
            while (pos < jsonContent.length()) {
                while (pos < jsonContent.length() && std::isspace(jsonContent[pos])) pos++;
                if (pos >= jsonContent.length() || jsonContent[pos] == '}') break;

                if (jsonContent[pos] != '"') { pos++; continue; }
                size_t keyStart = pos + 1;
                size_t keyEnd = jsonContent.find('"', keyStart);
                if (keyEnd == std::string::npos) break;
                std::string key = jsonContent.substr(keyStart, keyEnd - keyStart);

                pos = keyEnd + 1;
                while (pos < jsonContent.length() && jsonContent[pos] != ':') pos++;
                if (pos >= jsonContent.length()) break;
                pos++;

                // 寻找概率数组的左括号 '['
                while (pos < jsonContent.length() && std::isspace(jsonContent[pos])) pos++;
                if (pos < jsonContent.length() && jsonContent[pos] == '[') {
                    pos++; // Skip '['
                    std::vector<double> probs;
                    while (pos < jsonContent.length()) {
                        while (pos < jsonContent.length() && (std::isspace(jsonContent[pos]) || jsonContent[pos] == ',')) pos++;
                        if (pos >= jsonContent.length() || jsonContent[pos] == ']') {
                            pos++; break; // Skip ']'
                        }

                        size_t numStart = pos;
                        while (pos < jsonContent.length()) {
                            char c = jsonContent[pos];
                            if (std::isdigit(c) || c == '.' || c == '-' || c == 'e' || c == 'E' || c == '+') {
                                pos++;
                            } else break;
                        }

                        if (numStart < pos) {
                            std::string numStr = jsonContent.substr(numStart, pos - numStart);
                            char* endPtr = nullptr;
                            double value = strtod(numStr.c_str(), &endPtr);
                            if (endPtr != numStr.c_str() && *endPtr == '\0') {
                                probs.push_back(value);
                            }
                        }
                    }
                    if (!probs.empty()) {
                        m_decisionTable_prob[key] = probs; // 将概率数组存入表
                    }
                }
                while (pos < jsonContent.length() && (std::isspace(jsonContent[pos]) || jsonContent[pos] == ',')) pos++;
            }
        }
    }

    MOT_LOG_INFO("HYBRID_CC: Loaded %zu decision table entries from LDT", m_decisionTable_prob.size());
    return true;
}

void HybridCcManager::ListenerLoop(int port) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == 0) {
        MOT_LOG_ERROR("Socket creation failed");
        return;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        MOT_LOG_ERROR("Socket bind failed");
        return;
    }

    listen(server_fd, 3);

    // 设置为非阻塞模式，以便定期检查 m_stopListener
    struct timeval tv;
    tv.tv_sec = 1; // 1秒超时
    tv.tv_usec = 0;
    setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
    bool m_stopListener = false;
    while (!m_stopListener) {
        int addrlen = sizeof(address);
        int new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);

        if (new_socket < 0) continue; // 超时，继续检查 m_stopListener

        char buffer[1024] = {0};
        read(new_socket, buffer, 1024);
        std::string msg(buffer);

        if (ParseUpdateMessage(msg)) {
            MOT_LOG_INFO("Received LDT update trigger. Reloading...");
            if (ReloadLDT()) {
                int new_version = GetCurrentVersion();
                std::string ack = "ACK\n";
                send(new_socket, ack.c_str(), ack.length(), 0);
                HybridCcLogger::GetInstance().SetNewVersion(new_version);
            }
        }
        close(new_socket);
    }
    close(server_fd);
}

bool HybridCcManager::ParseUpdateMessage(const std::string& msg) {
    // 期望格式 "READY:5"
    if (msg.find("READY:") == 0) {
        uint64_t newVersion = std::stoull(msg.substr(6));
        if (newVersion > m_currentVersion) {
            m_currentVersion = newVersion;
            return true;
        }
    }
    return false;
}

int HybridCcManager::GetFeatureTier(const std::string& featureName, double rawValue) {
    auto it = m_tierThresholds.find(featureName);
    if (it != m_tierThresholds.end()) {
        return MapValueToTier(rawValue, it->second);
    }
    return 0; // 找不到配置默认返回 Tier 0
}

void HybridCcManager::RecordDecisionTraj(TxnManager* txMan, int action, double action_prob, uint32_t policy_version)
{
    if (!txMan->m_trajectoryBuffer) return;

    // 1. 获取本地特征的 Tier
    int c_tier = GetFeatureTier("s_cont", (double)txMan->hot_cnt);
    int w_tier = GetFeatureTier("s_work", (double)txMan->m_accessMgr->GetOrderedRowSet().size());
    int r_tier = GetFeatureTier("s_retry", (double)txMan->retry_cnt);

    // 2. 获取全局特征的 Tier
    int ga_tier = GetFeatureTier("s_global_abort_rate", txMan->m_recent_abort_rate);
    int gt_tier = GetFeatureTier("s_global_throughput", txMan->m_recent_global_tps);
    int lq_tier = GetFeatureTier("s_lock_queue_length", 0.0); // 请确认 TxnManager 有此方法

    // 3. 原始标量特征
    uint64_t currentTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    uint64_t exec_time = (currentTime > txMan->start_time) ? (currentTime - txMan->start_time) : 0;
    uint64_t query_interval = txMan->GetAvgOperationInterval() * 1000.0;

    // 写入当前事务的轨迹缓存
    txMan->m_trajectoryBuffer->policy_version = policy_version; // 版本号通常整个事务一致
    txMan->m_trajectoryBuffer->AddStep(
        action, action_prob,
        c_tier, w_tier, r_tier,
        ga_tier, gt_tier, lq_tier, // 将全局 Tier 也传进去
        exec_time, query_interval, txMan->score_
    );
}


std::string HybridCcManager::ExtractStateKeyTraj(TxnManager* txMan)
{
    std::stringstream ss;

    int c_tier = GetFeatureTier("s_cont", (double)txMan->hot_cnt);
    int w_tier = GetFeatureTier("s_work", (double)txMan->m_accessMgr->GetOrderedRowSet().size());
    int r_tier = GetFeatureTier("s_retry", (double)txMan->retry_cnt);
    int ga_tier = GetFeatureTier("s_global_abort_rate", txMan->m_recent_abort_rate);
    int gt_tier = GetFeatureTier("s_global_throughput", txMan->m_recent_global_tps);
//    int lq_tier = GetFeatureTier("s_lock_queue_length", txMan->GetLockQueueLength());

    // Python 侧的 LDT key 必须与这里生成的字符串对齐！
    ss << "c" << c_tier
       << "_w" << w_tier
       << "_r" << r_tier
       << "_ga" << ga_tier
       << "_gt" << gt_tier;

    return ss.str();
}

} // namespace MOT

