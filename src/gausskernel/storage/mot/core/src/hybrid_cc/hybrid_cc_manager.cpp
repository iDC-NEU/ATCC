

#include <fstream>
#include <sstream>
#include <algorithm>

#include "system/global.h"
#include "utils/utilities.h"
#include "system/transaction/txn_access.h"

#include "hybrid_cc_manager.h"

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

RC HybridCcManager::ExecuteAction(TxnManager* txMan, int action)
{
    if (!m_isInitialized) {
        return RC_OK; // Fallback to default behavior
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

    return ParseJSON(jsonContent);
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

} // namespace MOT

