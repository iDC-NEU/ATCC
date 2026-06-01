/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * row_header.h
 *    Row header implementation in OCC
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/concurrency_control/row_header.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ROW_HEADER_H
#define ROW_HEADER_H

#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <functional>
#include "global.h"
#include "mot_atomic_ops.h"
#include "debug_utils.h"
#include <algorithm>

namespace MOT {
// forward declarations
class Catalog;
class Index;
class Row;
class TxnManager;
class TxnAccess;
class CheckpointWorkerPool;
class RecoveryOps;
// forward declarations
enum RC : uint32_t;
enum AccessType : uint8_t;

// forward declaration
class OccTransactionManager;
class Access;

/** @define masks for CSN word   */
#define CSN_BITS 0x1FFFFFFFFFFFFFFFUL

#define STATUS_BITS 0xE000000000000000UL

/** @define Bit position designating locked state. */
#define LOCK_BIT (1UL << 63)

/** @define Bit position designation version. */
#define LATEST_VER_BIT (1UL << 62)

/** @define Bit position designating absent row. */
#define ABSENT_BIT (1UL << 61)

/**
 * @class RowHeader
 * @brief Helper class for managing a single row in Optimistic concurrency control.
 */

class __attribute__((__packed__)) RowHeader {
public:
    /**
     * @brief Constructor.
     * @param row The row manage.
     */
    explicit inline RowHeader() : m_csnWord(0), stable_csnWord(0)
    {}

    inline RowHeader(const RowHeader& src)
    {
        stable_csnWord = m_csnWord = src.m_csnWord;

        // wzy: 拷贝构造，但不添加version 和active list
        commitEpoch = src.commitEpoch;
        startEpoch = src.startEpoch;
        server_id = src.server_id;
        csn = src.csn;
        stable_commitEpoch = src.stable_commitEpoch;
        stable_startEpoch = src.stable_startEpoch;
        stable_server_id = src.stable_server_id;

        t_xmin_csn = src.t_xmin_csn;
        t_xmax_csn = src.t_xmax_csn;
    }

    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    RowHeader(RowHeader&&) = delete;

    RowHeader& operator=(const RowHeader&) = delete;

    RowHeader& operator=(RowHeader&&) = delete;
    /** @endcond */

private:
    /**
     * @brief Gets a consistent snapshot of a managed row.
     * @param txn The transaction access object.
     * @param type The access type.
     * @param[out] localRow Receives the row contents except during new row
     * insertion.
     * @return Return code denoting success or failure.
     */
    RC GetLocalCopy(TxnAccess* txn, AccessType type, Row* localRow, const Row* origRow, TransactionId& lastTid) const;

    /**
     * @brief Validates the row was not changed by a concurrent transaction
     * @param tid The transaction identifier.
     * @return Boolean value denoting whether the row header is valid.
     */
    bool ValidateWrite(TransactionId tid) const;

    /**
     * @brief Validates the row was not changed or locked by a concurrent transaction
     * @param tid The transaction identifier.
     * @return Boolean value denoting whether the row header is valid.
     */
    bool ValidateRead(TransactionId tid) const;

    /**
     * @brief Apply changes to the public row
     * @param access Container for the row
     * @param csn Commit Serial Number
     */
    void WriteChangesToRow(const Access* access, uint64_t csn, uint64_t server_id);

    // wzy:
    void WriteChangesToRow(const Access* access, TxnManager* txMan, TxnAccess* tx, uint64_t csn, uint64_t server_id);

public:
    /** @brief Locks the row. */
    void Lock();

    /** @brief Unlocks the row. */
    void Release()
    {
        // COMPILER_BARRIER       // wzy: 测试屏障
//        asm volatile("mfence" ::: "memory");
        MOT_ASSERT(m_csnWord & LOCK_BIT);
#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
        m_csnWord = m_csnWord & (~LOCK_BIT);
#else
        uint64_t v = m_csnWord;
        while (!__sync_bool_compare_and_swap(&m_csnWord, v, (v & ~LOCK_BIT))) {
            PAUSE
            v = m_csnWord;
        }
#endif
    }

    /**
     * @brief Attempts to lock the row.
     * @return True if the row was locked.
     */
    bool TryLock()
    {
        uint64_t v = m_csnWord;
        if (v & LOCK_BIT) {  // already locked
            return false;
        }
        return __sync_bool_compare_and_swap(&m_csnWord, v, (v | LOCK_BIT));
    }

    /** @var Assert row is locked. */
    void AssertLock() const
    {
        MOT_ASSERT(m_csnWord & LOCK_BIT);
    }

    /**
     * @brief Retrieves the transaction identifier stripped off all
     * meta-data.
     * @return The bare transaction identifier.
     */
    uint64_t GetCSN() const
    {
        return (m_csnWord & CSN_BITS);
    }

    /** Set the CSN of the row */
    void SetCSN(uint64_t csn)
    {
        m_csnWord = (m_csnWord & STATUS_BITS) | (csn & CSN_BITS);
    }

    /**
     * @brief Queries whether the absent bit is set for the row.
     * @return True if the absent bit is set in the row.
     */
    bool IsAbsent() const
    {
        return (m_csnWord & ABSENT_BIT) == ABSENT_BIT;
    }

    /**
     * @brief Queries whether the lock bit is set in the row.
     * @return True if the lock bit is set in the row.
     */
    bool IsLocked() const
    {
        return (m_csnWord & LOCK_BIT) == LOCK_BIT;
    }

    /**
     * @brief Queries whether the row is deleted.
     * @return True if the absent bit and the latet vsersion are set for the row.
     */
    bool IsRowDeleted() const
    {

        return (m_csnWord & (ABSENT_BIT | LATEST_VER_BIT)) == (ABSENT_BIT | LATEST_VER_BIT);
    }

    /** @brief Sets the absent bit in the row. */
    void SetAbsentBit()
    {
        m_csnWord |= ABSENT_BIT;
    }

    /** @brief Sets the absent and locked bits in the row. */
    void SetAbsentLockedBit()
    {
        m_csnWord |= (ABSENT_BIT | LOCK_BIT);
    }

    /** @brief Set the deletion bits in the row  */
    void SetDeleted()
    {
        m_csnWord |= (ABSENT_BIT | LATEST_VER_BIT);
    }

    /** @brief Clears the absent bit in the row. */
    void UnsetAbsentBit()
    {
        m_csnWord &= ~ABSENT_BIT;
    }

    /** @brief Usnet latest version bit   */
    void UnsetLatestVersionBit()
    {
        m_csnWord &= ~LATEST_VER_BIT;
    }

public:
    /** @var Transaction identifier and meta data for locking and deleting a row. */
    volatile uint64_t m_csnWord;

    // Allows privileged access
    friend Row;
    friend OccTransactionManager;
    friend CheckpointWorkerPool;
    friend RecoveryOps;
    friend Index;


public:
    ///ADDBY NEU
    bool ValidateAndSetWriteForCommit(uint64_t m_csn, uint64_t start_epoch, uint64_t commit_epoch, uint32_t server_id);
    bool InteractiveSetWriteForCommit(uint64_t m_csn, uint64_t start_epoch, uint64_t commit_epoch, uint32_t server_id);
    bool ValidateAndSetWriteForCommitInteractive(uint64_t m_csn, uint64_t start_epoch, uint64_t commit_epoch, uint32_t server_id, bool interactive);
    bool SetWriteForCommitPCC(uint64_t m_csn, uint64_t start_epoch, uint64_t commit_epoch, uint32_t server_id, bool interactive);


    // ============ wzy: MVCC相关
    enum VisibleType : uint32_t { RowIsVisible = 0, RowIsDeleted = 1, RowNotExist = 2 };

    bool RowSatisfiesPack(TransactionId tid, uint64_t csn, uint32_t server_id);

    // wzy: 判断当前事务对行是否可见
    VisibleType RowSatisfiesNow(TransactionId tid, uint64_t csn, uint32_t server_id);

    // wzy: 向版本链首中添加version
    void AddNewVersion(TxnAccess* tx, Row* newVersion, uint64_t xmin, uint64_t xmax);  // -1 为无效标识位

    // wzy: 找到符合的row，不符合则返回空
    Row* FindVisibleVersion(TransactionId tid, uint64_t csn, uint32_t server_id,  RowHeader*& resRowHeader);

    // wzy: 添加到事务链表
    void AddActiveTxnList(TransactionId tid, uint32_t server_id)
    {
        std::lock_guard<std::mutex> lock(active_tid_mutex);
        for (auto temp : active_tid_) {
            if (temp == tid)
                return;
        }
        active_tid_.push_back(tid);
    }

    // wzy: 从正在访问的事务链表中删除
    void RemoveActiveTxn(TransactionId tid, uint32_t server_id);

    // wzy: 将txnlist从b移动到a
    void MoveActiveTxnList(RowHeader* a, RowHeader* b) {
        std::lock_guard<std::mutex> lock(b->active_tid_mutex);
        a->active_tid_ = b->active_tid_;
        b->active_tid_.clear();
    }

    bool isInteractive() {return is_interactive;}
    void setInteractive(bool interactive) { is_interactive = interactive;}

    void setPCCWriteSet(bool pcc) { is_pcc_write_set = pcc;}

    // wzy: 清除多余版本，当前Row并不清除
    void CleanupVersions(TransactionId min_active_txn_id);

    ///////////////////////////////////////////////////

    bool IsStableLocked() const
    {
        return (stable_csnWord & LOCK_BIT) == LOCK_BIT;
    }

    uint64_t GetStableCsnWord() {
        return stable_csnWord;
    }

    void LockStable();

    void ReleaseStable(){
        // COMPILER_BARRIER       // wzy: 测试屏障
//        asm volatile("mfence" ::: "memory");
        MOT_ASSERT(stable_csnWord & LOCK_BIT);
#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
        stable_csnWord = stable_csnWord & (~LOCK_BIT);
#else
        uint64_t v = stable_csnWord;
        while (!__sync_bool_compare_and_swap(&stable_csnWord, v, (v & ~LOCK_BIT))) {
            PAUSE
            v = stable_csnWord;
        }
#endif
    }

    uint64_t GetStableCSN() const
    {
        return (stable_csnWord & CSN_BITS);
    }

    /** Set th CSN of the row   */
    void SetStableCSN(uint64_t csn)
    {
        stable_csnWord = (stable_csnWord & STATUS_BITS) | (csn & CSN_BITS);
    }
    uint64_t GetStableCommitEpoch() const
    {
        return stable_commitEpoch;
    }

    /** Set th stable   commit epoch of the row   */
    void SetStableCommitEpoch(uint64_t commit_epoch)
    {
        stable_commitEpoch = commit_epoch;
    }

    uint64_t GetStableStartEpoch() const {
        return stable_startEpoch;
    }

    void SetStableStartEpoch(uint64_t start_epoch) {
        stable_startEpoch = start_epoch;
    }

    uint64_t GetCommitEpoch() const
    {
        return commitEpoch;
    }

    /** Set th last motify commit epoch of the row   */
    void SetCommitEpoch(uint64_t commit_epoch)
    {
        commitEpoch = commit_epoch;
    }

    uint64_t GetStartEpoch() const {
        return startEpoch;
    }

    void SetStartEpoch(uint64_t start_epoch) {
        startEpoch = start_epoch;
    }

    void RecoverToStable(){
        Lock();
        startEpoch = stable_startEpoch;
        commitEpoch = stable_commitEpoch;
        m_csnWord = stable_csnWord;
        server_id = stable_server_id;
        Release();
    }

    void KeepStable(){
        stable_startEpoch = startEpoch;
        stable_commitEpoch = commitEpoch;
        stable_csnWord = m_csnWord;
        stable_server_id = server_id;
        // TODO: 需要设置interactive吗?
        interactive_flag = false;
        is_pcc_write_set = false;
        in_commit_phase_ = false;
    }

    void SetServerId(uint32_t id){
        server_id = id;
    }

    uint32_t GetServerId() const{
        return server_id;
    }

    void SetStableServerId(uint32_t id){
        stable_server_id = id;
    }

    uint32_t GetStableServerId() const{
        return stable_server_id;
    }

    uint64_t GetCSN_1() const
    {
        return (m_csnWord & CSN_BITS);
    }

    void Setcsn(uint64_t v) {
        csn = v;
    }

    uint64_t Getcsn() {
        return csn;
    }

    void SetInteractiveFlag(bool interactive) {
        interactive_flag = interactive;
    }

    void SetPCCFlag(bool pcc) {
        is_pcc_write_set = pcc;
    }

    bool ValidateReadI(TransactionId tid, uint32_t server_id) const;
    bool ValidateReadForSnap(TransactionId tid, uint64_t start_epoch, uint32_t server_id) const;
    bool ValidateReadI_Switching(TransactionId tid, uint32_t server_id) const;
    bool ValidateReadForSnap_Switching(TransactionId tid, uint64_t start_epoch, uint32_t server_id) const;
    bool ValidateWriteForMVCC(TransactionId tid, uint64_t start_epoch, uint32_t server_id) const;
    bool ValidateWriteI(TransactionId tid, uint32_t server_id) const;

private:
    volatile uint64_t commitEpoch = 0;//ADDBY NEU
    volatile uint64_t startEpoch = 0;
    volatile uint32_t server_id = 0;
    volatile bool interactive_flag = false;     // 是否需要设置为false? 是否需要设置为stable?
    volatile bool is_pcc_write_set = false;
    volatile bool in_commit_phase_ = false;

    volatile uint64_t stable_csnWord = 0;
    volatile uint64_t csn = 0;
    volatile uint64_t stable_commitEpoch = 0;
    volatile uint64_t stable_startEpoch = 0;
    volatile uint32_t stable_server_id = 0;

    friend TxnAccess;//ADDBY NEU

    // wzy: 判断可见性，根据可见性返回scan的结果，一个版本可见范围[xmin, xmax]
     volatile uint64_t t_xmin_csn;  // csn insert this row
     volatile uint64_t t_xmax_csn;  // csn delete this row， -1是无效值

     // 版本链，指向上一个版本，update操作会产生新版本
     std::list<Row*> version_;
     std::mutex version_mutex;

     std::list<TransactionId> active_tid_;  // 正在访问row的活跃事务id，垃圾回收
     std::mutex active_tid_mutex;

     bool is_interactive;
};
}  // namespace MOT

#endif /* ROW_HEADER_H */
