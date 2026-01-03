#ifndef HYBRID_CC_MANAGER_H
#define HYBRID_CC_MANAGER_H

#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <memory>
#include "system/transaction/txn.h" // For TxnManager

namespace MOT {

/**
 * @class HybridCcManager
 * @brief Manages the hybrid concurrency control decision-making process.
 * 
 * This class is responsible for:
 * 1. Loading and managing the LDT (Lightweight Decision Table) from JSON files
 * 2. Making real-time concurrency control decisions based on transaction features
 * 3. Coordinating with OccTransactionManager to execute PCC strategies
 */
class HybridCcManager {
public:
    /**
     * @brief Get the singleton instance of the manager.
     * @return Reference to the singleton manager instance.
     */
    static HybridCcManager& GetInstance();

    /**
     * @brief Initialize the manager with LDT file path.
     * @param ldtFilePath Path to the LDT JSON file.
     * @return True if initialization is successful, false otherwise.
     */
    bool Init(const std::string& ldtFilePath);

    /**
     * @brief Make a concurrency control decision for a transaction.
     * @param txMan The transaction manager containing transaction context.
     * @return The recommended action (0-4, as defined in README.md).
     */
    int Decide(TxnManager* txMan);

    /**
     * @brief Execute the recommended action by coordinating with OccTransactionManager.
     * @param txMan The transaction manager.
     * @param action The action to execute (0-5):
     *               0: Stay Optimistic (no locking)
     *               1: Lock Hot Write Set
     *               2: Lock All Hot Sets (read + write)
     *               3: Lock All Writes & Hot Reads
     *               4: Lock Entire Access Set (full pessimistic)
     *               5: Boost Priority (HYBRID_CC: increase transaction priority)
     * @return Return code indicating success or failure.
     */
    RC ExecuteAction(TxnManager* txMan, int action);

    /**
     * @brief Reload the LDT from file (for hot-updating).
     * @return True if reload is successful, false otherwise.
     */
    bool ReloadLDT();

    // Delete copy constructor and assignment operator for singleton pattern
    HybridCcManager(const HybridCcManager&) = delete;
    void operator=(const HybridCcManager&) = delete;

private:
    /** @brief Private constructor for singleton pattern. */
    HybridCcManager() = default;

    /** @brief Private destructor. */
    ~HybridCcManager() = default;

    /**
     * @brief Extract transaction features to build a state key.
     * @param txMan The transaction manager.
     * @return String representation of the transaction state.
     */
    std::string ExtractStateKey(TxnManager* txMan);

    /**
     * @brief Load LDT from JSON file.
     * @return True if loading is successful, false otherwise.
     */
    bool LoadLDT();

    /**
     * @brief Parse JSON file (simple implementation without external dependencies).
     * @param jsonContent The JSON content as string.
     * @return True if parsing is successful, false otherwise.
     */
    bool ParseJSON(const std::string& jsonContent);

    // HYBRID_CC: Map a raw value to a tier index based on thresholds
    /**
     * @brief Map a raw value to a tier index (0, 1, 2) based on thresholds.
     * @param value The raw value to map.
     * @param thresholds Vector of thresholds. For 3-tier system: [low_to_mid, mid_to_high].
     *                   - value <= thresholds[0] -> TIER_0
     *                   - thresholds[0] < value <= thresholds[1] -> TIER_1
     *                   - value > thresholds[1] -> TIER_2
     * @return Tier index (0, 1, or 2).
     */
    int MapValueToTier(double value, const std::vector<double>& thresholds) const;

    // Configuration
    std::string m_ldtFilePath;
    bool m_isInitialized = false;

    // LDT data structure
    std::unordered_map<std::string, int> m_decisionTable;
    std::mutex m_tableMutex;

    // HYBRID_CC: Tier thresholds configuration
    // Each feature maps to a vector of thresholds [low_to_mid, mid_to_high]
    std::unordered_map<std::string, std::vector<double>> m_tierThresholds;

    // No direct dependency on OccTransactionManager; execute via TxnManager wrappers
};

} // namespace MOT

#endif // HYBRID_CC_MANAGER_H

