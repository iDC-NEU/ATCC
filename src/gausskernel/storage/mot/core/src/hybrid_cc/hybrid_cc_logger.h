#ifndef HYBRID_CC_LOGGER_H
#define HYBRID_CC_LOGGER_H

#include <fstream>
#include <string>
#include <mutex>
#include <atomic>
#include <chrono>
#include "system/transaction/txn.h" // For TxnManager

namespace MOT {

/**
 * @class HybridCcLogger
 * @brief A singleton logger to record transaction details for offline analysis and training.
 * 
 * This class is responsible for collecting runtime information about each transaction
 * and writing it to a CSV file. The format of this CSV file is the "data contract"
 * for the offline Python-based learning model.
 */
class HybridCcLogger {
public:
    /**
     * @brief Get the singleton instance of the logger.
     * @return Reference to the singleton logger instance.
     */
    static HybridCcLogger& GetInstance();

    /**
     * @brief Initializes the logger. Must be called once at startup.
     * @param filepath The path to the output CSV log file.
     * @return True if initialization is successful, false otherwise.
     */
    bool Init(const std::string& filepath);

    /**
     * @brief Logs the details of a completed transaction.
     * @param txMan The transaction manager instance containing all details of the transaction.
     */
    void LogTransaction(TxnManager* txMan);

    /**
     * @brief Gets the current global TPS (Transactions Per Second).
     * @return Current TPS value.
     */
    double GetGlobalTPS() const;

    /**
     * @brief Gets the current global abort rate.
     * @return Current abort rate as a percentage (0.0 to 1.0).
     */
    double GetGlobalAbortRate() const;

    // Delete copy constructor and assignment operator for singleton pattern
    HybridCcLogger(const HybridCcLogger&) = delete;
    void operator=(const HybridCcLogger&) = delete;

private:
    /** @brief Private constructor for singleton pattern. */
    HybridCcLogger() = default;

    /** @brief Private destructor. */
    ~HybridCcLogger();

    /** @brief Helper function to write the CSV header if the file is new. */
    void WriteHeader();

    /** @brief Updates global statistics with transaction completion. */
    void UpdateGlobalStats(bool isCommit);

    /** @brief Calculates TPS based on recent transaction completions. */
    double CalculateTPS() const;

    /** @brief Calculates abort rate based on recent transaction completions. */
    double CalculateAbortRate() const;

    std::ofstream m_logFile;
    std::string m_filepath;
    std::mutex m_mutex;
    bool m_isInitialized = false;

    // Global statistics tracking
    std::atomic<uint64_t> m_totalTransactions{0};
    std::atomic<uint64_t> m_committedTransactions{0};
    std::atomic<uint64_t> m_abortedTransactions{0};
    std::chrono::steady_clock::time_point m_startTime;
    std::chrono::steady_clock::time_point m_lastUpdateTime;
    
    // Rolling window for TPS calculation (last 10 seconds)
    static constexpr size_t TPS_WINDOW_SIZE = 10;
    std::array<std::chrono::steady_clock::time_point, TPS_WINDOW_SIZE> m_recentCompletions;
    std::array<bool, TPS_WINDOW_SIZE> m_recentCommits;
    std::atomic<size_t> m_windowIndex{0};
};

} // namespace MOT

#endif // HYBRID_CC_LOGGER_H
