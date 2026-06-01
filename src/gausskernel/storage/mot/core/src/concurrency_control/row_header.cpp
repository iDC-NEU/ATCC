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
 * row_header.cpp
 *    Row header implementation in OCC
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/concurrency_control/row_header.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "row_header.h"
#include "global.h"
#include "mot_atomic_ops.h"
#include "row.h"
#include "txn_access.h"
#include "utilities.h"
#include "cycles.h"
#include "debug_utils.h"
#include "mot_engine.h"
#include "../../../fdw_adapter/src/mot_internal.h"

namespace MOT {
DECLARE_LOGGER(RowHeader, ConcurrenyControl);

///ADDBY NEU
RC RowHeader::GetLocalCopy(
    TxnAccess* txn, AccessType type, Row* localRow, const Row* origRow, TransactionId& lastTid) const
{
    uint64_t sleepTime = 1;
    uint64_t v = 0;
    uint64_t v2 = 1;

    // concurrent update/delete after delete is not allowed - abort current transaction
    // if ((m_csnWord & ABSENT_BIT) && type != AccessType::INS) {
    if ((stable_csnWord & ABSENT_BIT) && type != AccessType::INS) {
        return RC_ABORT;
    }
    uint64_t retryCnt = 0;       // wzy:
    while (v2 != v) {
        // contend for exclusive access
        v = stable_csnWord;
        while (v & LOCK_BIT) {
            if (sleepTime > LOCK_TIME_OUT) {
                sleepTime = LOCK_TIME_OUT;
                struct timespec ts = {0, 5000};
                retryCnt++;
                if (retryCnt % 10000 == 0) {
                    MOT_LOG_INFO("Get local copy retry, stable_csnWord = %llu, row id = %llu", v, origRow->GetRowId());
                }

                (void)nanosleep(&ts, NULL);
            } else {
                CpuCyclesLevelTime::Sleep(1);
                sleepTime = sleepTime << 1;
            }
            v = stable_csnWord;
        }
        // No need to copy new-row.
        if (type != AccessType::INS) {  // get current row contents (not required during insertion of new row)
            localRow->Copy(origRow);
        }
        COMPILER_BARRIER
        v2 = stable_csnWord;
    }
    if ((v & ABSENT_BIT) && (v & LATEST_VER_BIT)) {
        return RC_ABORT;
    }
    lastTid = v & (~LOCK_BIT);

    if (type == AccessType::INS) {
        // ROW ALREADY COMMITED
        if ((stable_csnWord & (ABSENT_BIT)) == 0) {
            return RC_ABORT;
        }
        lastTid &= (~ABSENT_BIT);
    }

    return RC_OK;
}

bool RowHeader::ValidateWrite(TransactionId tid) const
{
    return (tid == GetCSN());
}

bool RowHeader::ValidateRead(TransactionId tid) const
{
    if (IsLocked() or (tid != GetCSN())) {
        return false;
    }

    return true;
}

//ADDBY NEU
bool RowHeader::ValidateReadI(TransactionId tid, uint32_t server_id) const
{
    if (IsStableLocked() or (tid != GetStableCSN()) or (server_id != GetStableServerId())) {
        return false;
    }
    return true;
}

bool RowHeader::ValidateWriteI(TransactionId tid, uint32_t server_id) const
{
    return (tid == GetStableCSN() && (server_id == GetStableServerId()));
}

bool RowHeader::ValidateReadForSnap(TransactionId tid, uint64_t start_epoch, uint32_t server_id) const
{
    if (GetStableCommitEpoch() == 0 || GetStableCSN() == 0) return true;
    if (IsStableLocked() or (start_epoch <= GetStableCommitEpoch()) or (tid != GetStableCSN()) or (server_id != GetStableServerId())) {
        return false;
    }
    return true;
}

// wzy: switching validation
bool RowHeader::ValidateReadI_Switching(TransactionId tid, uint32_t server_id) const
{
    if (IsStableLocked() or (tid != GetStableCSN()) or (server_id != GetStableServerId()) or in_commit_phase_) {
        return false;
    }
    return true;
}

bool RowHeader::ValidateReadForSnap_Switching(TransactionId tid, uint64_t start_epoch, uint32_t server_id) const
{
    if (cc_mode == 2) {
        if (GetStableCommitEpoch() == 0 || GetStableCSN() == 0) return true;
        if (IsStableLocked() or (start_epoch <= GetStableCommitEpoch()) or (tid != GetStableCSN()) or (server_id != GetStableServerId()) or in_commit_phase_) {
            return false;
        }
    }
    if (cc_mode == 4) {
        // wzy: Silo 不记录stable Commit epoch
        if (GetStableCSN() == 0) return true;
        if (IsStableLocked() or (tid != GetStableCSN()) or (server_id != GetStableServerId()) or in_commit_phase_) {
            return false;
        }
    }
    return true;
}

// wzy MVCC写验证
bool RowHeader::ValidateWriteForMVCC(TransactionId tid, uint64_t start_epoch, uint32_t server_id) const
{
    if (IsStableLocked() or (start_epoch <= GetStableCommitEpoch()) or (tid != GetStableCSN()) or (server_id != GetStableServerId())) {
        return false;
    }
    return true;
}

void RowHeader::WriteChangesToRow(const Access* access, uint64_t csn, uint64_t server_id)
{
    Row* row = access->GetRowFromHeader();
    AccessType type = access->m_type;

    if (type == RD) {
        return;
    }
#ifdef MOT_DEBUG
    if (access->m_params.IsPrimarySentinel()) {
        uint64_t v = m_csnWord;
        if (!MOTEngine::GetInstance()->IsRecovering()) {
            if (!(csn > GetCSN() && (v & LOCK_BIT))) {
                MOT_LOG_ERROR(
                    "csn=%ld, v & LOCK_BIT=%ld, v & (~LOCK_BIT)=%ld\n", csn, (v & LOCK_BIT), (v & (~LOCK_BIT)));
                MOT_ASSERT(false);
            }
        }
    }
#endif
    switch (type) {
        case WR:
            MOT_ASSERT(access->m_params.IsPrimarySentinel() == true);
            if(is_full_async_exec) {
                if(row->GetRowHeader()->GetCSN() == csn && row->GetRowHeader()->GetServerId() == server_id) {
                    row->GetRowHeader()->Lock();
                    row->GetRowHeader()->LockStable();
                    row->Copy(access->m_localRow);
                    m_csnWord = (csn | LOCK_BIT);
                }
            }
            else {
                row->Copy(access->m_localRow);
                m_csnWord = (csn | LOCK_BIT);
                if (is_debug_print_enable) MOT_LOG_INFO("Write changes to row csn = %llu  -> m_csnWord = %llu ", csn, m_csnWord);

            }
            // m_csnWord = csn;
            //ADDBY NEU
            break;
        case DEL:
            MOT_ASSERT(access->m_origSentinel->IsCommited() == true);
            if (access->m_params.IsPrimarySentinel()) {
                // m_csnWord = (csn | ABSENT_BIT | LATEST_VER_BIT);
                //ADDBY NEU
                m_csnWord = (csn | LOCK_BIT | ABSENT_BIT | LATEST_VER_BIT);
                // and allow reuse of the original row
            }
            // Invalidate sentinel  - row is still locked!
            access->m_origSentinel->SetDirty();
            break;
        case INS:
            if (access->m_params.IsPrimarySentinel()) {
                // At this case we have the new-row and the old row
                if (access->m_params.IsUpgradeInsert()) {
                    // We set the global-row to be locked and deleted
                    m_csnWord = (csn | LOCK_BIT | LATEST_VER_BIT);
                    // The new row is locked and absent!
                    access->m_auxRow->UnsetAbsentRow();
                    access->m_auxRow->SetCommitSequenceNumber(csn);
                } else {
                    //ADDBY NEU
                    m_csnWord = (csn | LOCK_BIT);
                }
            }
            break;
        default:
            break;
    }
    KeepStable();
}

// wzy: 通过txnAccess创建多版本
void RowHeader::WriteChangesToRow(const Access* access, TxnManager* txMan, TxnAccess* tx, uint64_t csn, uint64_t server_id)
{
    Row* row = access->GetRowFromHeader();
    AccessType type = access->m_type;

    auto currRow = access->m_localRow;
    auto ts = now_to_us();

//    if (type != INS) {  // wzy: INS操作没有现存的row id
//        std::string tmp_rowid = currRow->GetTable()->GetTableName() + ":" + to_string(currRow->GetRowId());
//        MOTAdaptor::dynamic_hot_rows.visit_row(tmp_rowid);      // wzy: 添加统计
//    }

    if (type == RD && txMan->IsInteractive()) {
//        RemoveActiveTxn(txMan->GetInternalTransactionId(), 0);
        return;
    }
#ifdef MOT_DEBUG
    if (access->m_params.IsPrimarySentinel()) {
        uint64_t v = m_csnWord;
        if (!MOTEngine::GetInstance()->IsRecovering()) {
            if (!(csn > GetCSN() && (v & LOCK_BIT))) {
                MOT_LOG_ERROR(
                    "csn=%ld, v & LOCK_BIT=%ld, v & (~LOCK_BIT)=%ld\n", csn, (v & LOCK_BIT), (v & (~LOCK_BIT)));
                MOT_ASSERT(false);
            }
        }
    }
#endif
    switch (type) {
        case WR:
            MOT_ASSERT(access->m_params.IsPrimarySentinel() == true);
            if(is_full_async_exec) {
                if(row->GetRowHeader()->GetCSN() == csn && row->GetRowHeader()->GetServerId() == server_id) {
                    // wzy: 拷贝当前版本，插入version中，修改x_max和当前 x_min
                    // 当前版本xmin更新
                    // AddNewVersion(tx, row, t_xmin_csn, csn);
                    if (isMVCC_Active) {
                        AddNewVersion(tx, row, t_xmin_csn, ts);
                        row->Copy(access->m_localRow);

                        row->GetRowHeader()->Lock();
                        row->GetRowHeader()->LockStable();
                        row->Copy(access->m_localRow);
                        m_csnWord = (csn | LOCK_BIT);

                        // t_xmin_csn = (m_csnWord & CSN_BITS);
                        t_xmin_csn = ts;
                        t_xmax_csn = -1;
                        MOTAdaptor::AddCleanRow(row);
                        if (is_debug_print_enable) MOT_LOG_INFO("Write changes to row csn = %llu  -> m_csnWord = %llu ", csn, m_csnWord);
                    } else {
                        row->GetRowHeader()->Lock();
                        row->GetRowHeader()->LockStable();
                        row->Copy(access->m_localRow);
                        m_csnWord = (csn | LOCK_BIT);
                        if (is_debug_print_enable) MOT_LOG_INFO("Write changes to row csn = %llu  -> m_csnWord = %llu ", csn, m_csnWord);
                    }
                }
            }
            else {
                // wzy: 拷贝当前版本，插入version中，修改x_max和当前 x_min
                // 当前版本xmin更新
                if (isMVCC_Active) {
                    AddNewVersion(tx, row, t_xmin_csn, ts);
                    row->Copy(access->m_localRow);
                    m_csnWord = (csn | LOCK_BIT);
                    t_xmin_csn = ts;
                    t_xmax_csn = -1;
                    MOTAdaptor::AddCleanRow(row);
                    if (is_debug_print_enable) MOT_LOG_INFO("Write changes to row csn = %llu  -> m_csnWord = %llu ", csn, m_csnWord);
                } else {
                    row->Copy(access->m_localRow);
                    m_csnWord = (csn | LOCK_BIT);
                    if (is_debug_print_enable) MOT_LOG_INFO("Write changes to row csn = %llu  -> m_csnWord = %llu ", csn, m_csnWord);
                }
            }
            // m_csnWord = csn;
            //ADDBY NEU
            break;
        case DEL:
            MOT_ASSERT(access->m_origSentinel->IsCommited() == true);
            if (access->m_params.IsPrimarySentinel()) {
                // m_csnWord = (csn | ABSENT_BIT | LATEST_VER_BIT);
                //ADDBY NEUP
                //                m_csnWord = (csn | LOCK_BIT | ABSENT_BIT | LATEST_VER_BIT);
                // and allow reuse of the original row
                // wzy: MVCC 即使是该row被删除也依旧能够访问
                m_csnWord = (csn | LOCK_BIT | LATEST_VER_BIT);          // wzy: 测试
                t_xmax_csn = ts;
            }
            // Invalidate sentinel  - row is still locked!
            // access->m_origSentinel->SetDirty();       // wzy: 暂时删除
            break;
        case INS:
            if (access->m_params.IsPrimarySentinel()) {
                // At this case we have the new-row and the old row
                if (access->m_params.IsUpgradeInsert()) {
                    // We set the global-row to be locked and deleted
                    m_csnWord = (csn | LOCK_BIT | LATEST_VER_BIT);
                    // The new row is locked and absent!
                    access->m_auxRow->UnsetAbsentRow();
                    access->m_auxRow->SetCommitSequenceNumber(csn);

                    // wzy: MVCC insert操作更新可见性位
                    t_xmin_csn = ts;
                    t_xmax_csn = -1;
                } else {
                    //ADDBY NEU
                    m_csnWord = (csn | LOCK_BIT);
                    // m_csnWord = csn;
                    // wzy: MVCC insert操作更新可见性位
                    t_xmin_csn = ts;
                    t_xmax_csn = -1;
                }
            }
            break;
        default:
            break;
    }
    KeepStable();
}


void RowHeader::Lock()
{
    uint64_t v = m_csnWord;
    while ((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&m_csnWord, v, v | LOCK_BIT)) {
        PAUSE
        v = m_csnWord;
    }
    // COMPILER_BARRIER       // wzy: 测试屏障
//    asm volatile("mfence" ::: "memory");
}

void RowHeader::LockStable()
{
    uint64_t v = stable_csnWord;
    while ((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&stable_csnWord, v, v | LOCK_BIT)) {
        PAUSE
        v = stable_csnWord;
    }
    // COMPILER_BARRIER       // wzy: 测试屏障
//    asm volatile("mfence" ::: "memory");
}

// wzy:
bool RowHeader::InteractiveSetWriteForCommit(uint64_t m_csn, uint64_t start_epoch, uint64_t commit_epoch, uint32_t server_id) {
    /// get the lock first
    if(is_full_async_exec) {
        Lock();

        SetCSN(m_csn);
        SetServerId(server_id);

        Release();
        return true;
    }
    else {
        Lock();
        std::string str = std::to_string(GetCSN()) + ":" + std::to_string(GetServerId());
        std::string csn_tmp = std::to_string(m_csn) + ":" + std::to_string(server_id);
        if (str != csn_tmp) {
            SetCSN(m_csn);
            SetStartEpoch(start_epoch);
            SetCommitEpoch(commit_epoch);
            SetServerId(server_id);
            MOTAdaptor::abort_transcation_csn_set.insert(str, str);
        }
        /// release the lock
        Release();
        return true;
    }
}

// wzy: CRDT + PCC
bool RowHeader::ValidateAndSetWriteForCommitInteractive(uint64_t m_csn, uint64_t start_epoch, uint64_t commit_epoch, uint32_t server_id, bool interactive) {
    /// get the lock first
    if(is_full_async_exec) {
        Lock();
        in_commit_phase_ = true;
        // 非交互型 和交互型
        if (is_pcc_write_set) {
            Release();
            return false;
        }
        if (!interactive_flag && interactive) {
            SetCSN(m_csn);
            SetServerId(server_id);
            SetInteractiveFlag(interactive);
        } else if (interactive_flag && !interactive) {
            // 交互型和非交互型
            // do nothing
        } else {
            // 交互型和交互型
            // 非交互型和非交互型
            if(GetCSN() < m_csn) {
                SetCSN(m_csn);
                SetServerId(server_id);
            }
            else if(GetCSN() == m_csn && server_id < GetStableServerId()) {
                SetServerId(server_id);
            }
            else {

            }
        }
        Release();
        return true;
    }
    else {
        Lock();
        bool result = true;
        in_commit_phase_ = true;
        if (is_pcc_write_set) {
            Release();
            return false;
        }
        if (!interactive_flag && interactive) {
            // 非交互型 交互型
            if (commit_epoch == GetCommitEpoch()) {
                std::string str = std::to_string(GetCSN()) + ":" + std::to_string(GetServerId());
                MOTAdaptor::abort_transcation_csn_set.insert(str, str);
            }
            SetCSN(m_csn);          // 1769008171513358
            SetStartEpoch(start_epoch);
            SetCommitEpoch(commit_epoch);
            SetServerId(server_id);
            SetInteractiveFlag(interactive);
        } else if (interactive_flag && !interactive) {
            // 交互型 非交互型
            result = false;
        } else {
            // 同类型
            if (commit_epoch > GetCommitEpoch()) {  // the first transaction in current epoch, direct write is ok
                SetCSN(m_csn);
                SetStartEpoch(start_epoch);
                SetCommitEpoch(commit_epoch);
                SetServerId(server_id);
            } else if (commit_epoch == GetCommitEpoch()) {
                if (GetStartEpoch() < start_epoch) {  // current transaction is the shorter transaction, win
                    std::string str = std::to_string(GetCSN()) + ":" + std::to_string(GetServerId());
                    MOTAdaptor::abort_transcation_csn_set.insert(str, str);
                    SetCSN(m_csn);
                    SetStartEpoch(start_epoch);
                    SetCommitEpoch(commit_epoch);
                    SetServerId(server_id);
                } else if (GetStartEpoch() > start_epoch) {  // current transaction is the longer transaction, failed
                    result = false;
                } else {
                    if (GetCSN() < m_csn) {  // current transaction commit later, abort
                        result = false;
                    } else if (GetCSN() > m_csn) {  // current transaction commit earlier, commit
                        std::string str = std::to_string(GetCSN()) + ":" + std::to_string(GetServerId());
                        MOTAdaptor::abort_transcation_csn_set.insert(str, str);
                        SetCSN(m_csn);
                        SetStartEpoch(start_epoch);
                        SetCommitEpoch(commit_epoch);
                        SetServerId(server_id);
                    } else {  // csn equals, startEpoch equal, endEpoch equal,
                        if (server_id > GetServerId()) {
                            result = false;
                        } else if (server_id == GetServerId()) {
                            // do nothing
                        } else {
                            std::string str = std::to_string(GetCSN()) + ":" + std::to_string(GetServerId());
                            MOTAdaptor::abort_transcation_csn_set.insert(str, str);
                            SetCSN(m_csn);
                            SetStartEpoch(start_epoch);
                            SetCommitEpoch(commit_epoch);
                            SetServerId(server_id);
                        }
                    }
                }
            } else
                result = false;
        }
        /// release the lock
        Release();
        return result;
    }
}

bool RowHeader::SetWriteForCommitPCC(uint64_t m_csn, uint64_t start_epoch, uint64_t commit_epoch, uint32_t server_id, bool interactive) {
    Lock();

    SetCSN(m_csn);
    SetStartEpoch(start_epoch);
    SetCommitEpoch(commit_epoch);
    SetServerId(server_id);
    SetInteractiveFlag(interactive);
    SetPCCFlag(true);

    Release();
    return true;
}


bool RowHeader::ValidateAndSetWriteForCommit(uint64_t m_csn, uint64_t start_epoch, uint64_t commit_epoch, uint32_t server_id) {
    /// get the lock first
    if(is_full_async_exec) {
        Lock();
        if(GetCSN() < m_csn) {
            SetCSN(m_csn);
            SetServerId(server_id);
        }
        else if(GetCSN() == m_csn && server_id < GetStableServerId()) {
            SetServerId(server_id);
        }
        else {

        }
        Release();
        return true;
    }
    else {
        Lock();
        bool result = true;
        if(commit_epoch > GetCommitEpoch()) { // the first transaction in current epoch, direct write is ok
            SetCSN(m_csn);
            SetStartEpoch(start_epoch);
            SetCommitEpoch(commit_epoch);
            SetServerId(server_id);
        } 
        else if(commit_epoch == GetCommitEpoch()){
            if(GetStartEpoch() < start_epoch) { // current transaction is the shorter transaction, win
                std::string str = std::to_string(GetCSN()) + ":" + std::to_string(GetServerId());
                MOTAdaptor::abort_transcation_csn_set.insert(str, str);
                SetCSN(m_csn);
                SetStartEpoch(start_epoch);
                SetCommitEpoch(commit_epoch);
                SetServerId(server_id);
            } else if(GetStartEpoch() > start_epoch){ // current transaction is the longer transaction, failed
                result = false;
            } else {
                if(GetCSN() < m_csn) { // current transaction commit later, abort
                    result = false;
                } else if(GetCSN() > m_csn) { // current transaction commit earlier, commit
                    std::string str = std::to_string(GetCSN()) + ":" + std::to_string(GetServerId());
                    MOTAdaptor::abort_transcation_csn_set.insert(str, str);
                    SetCSN(m_csn);
                    SetStartEpoch(start_epoch);
                    SetCommitEpoch(commit_epoch);
                    SetServerId(server_id);
                } else {// csn equals, startEpoch equal, endEpoch equal, 
                    if(server_id > GetServerId()){
                        result = false;
                    }
                    else if(server_id == GetServerId() ){
                        //do nothing
                    }
                    else{
                        std::string str = std::to_string(GetCSN()) + ":" + std::to_string(GetServerId());
                        MOTAdaptor::abort_transcation_csn_set.insert(str, str);
                        SetCSN(m_csn);
                        SetStartEpoch(start_epoch);
                        SetCommitEpoch(commit_epoch);
                        SetServerId(server_id);
                    }
                }
            }
        }
        else result = false;
        /// release the lock
        Release();
        return result;
    }
}

bool RowHeader::RowSatisfiesPack(TransactionId tid, uint64_t csn, uint32_t server_id) {
    if (this->t_xmax_csn == 0 && this->t_xmin_csn == 0) return true;
    if (this->t_xmin_csn > csn) {
        return false;  // 当前版本没有（因为被删了），去下一个版本
    }
    if (this->t_xmax_csn != -1 && this->t_xmax_csn < csn) {
        return false;    // 不存在
    }
    return true;
}

// wzy: 未上锁
RowHeader::VisibleType RowHeader::RowSatisfiesNow(TransactionId tid, uint64_t csn, uint32_t server_id)
{
    if (this->t_xmin_csn > csn) {
        return VisibleType::RowIsDeleted;  // 当前版本没有（因为被删了），去下一个版本
    }
    if (this->t_xmax_csn != -1 && this->t_xmax_csn < csn) {
        return VisibleType::RowNotExist;    // 不存在
    }
    return VisibleType::RowIsVisible;
}

void RowHeader::AddNewVersion(TxnAccess* tx, Row* newVersion, uint64_t xmin, uint64_t xmax)
{
    std::lock_guard<std::mutex> lock(version_mutex);
    if (!tx) return;
    // 深拷贝当前version，修改xmin, xmax，插入版本链
//    Row* oldVersion = tx->GetDummyTable()->CreateMaxRow();      // 不从dummy table创建
    Row* oldVersion = newVersion->GetTable()->CreateNewRow();
    oldVersion->Copy(newVersion); // 复制data

    // 继承active tid，newVersion的active tid置空
    MoveActiveTxnList(oldVersion->GetRowHeader(), newVersion->GetRowHeader());

    newVersion->GetRowHeader()->version_.push_front(oldVersion);
    oldVersion->GetRowHeader()->t_xmin_csn = xmin;
    oldVersion->GetRowHeader()->t_xmax_csn = xmax;
    oldVersion->GetRowHeader()->version_.clear();   // 清空list
}

Row* RowHeader::FindVisibleVersion(TransactionId tid, uint64_t csn, uint32_t server_id, RowHeader*& resRowHeader)
{
    std::lock_guard<std::mutex> lock(version_mutex);
    for (auto iter = version_.begin(); iter != version_.end(); iter++){
        auto row = *iter;
        auto header = row->GetRowHeader();
        auto res = header->RowSatisfiesNow(tid, csn, server_id);
        if (res == VisibleType::RowIsVisible) {
            resRowHeader = header;
            return row;
        }
        else if (res == VisibleType::RowIsDeleted){
            continue;
        }
        else if (res == VisibleType::RowNotExist) {
            return nullptr;
        }
    }
    return nullptr;
}

// wzy: 从正在访问的事务链表中删除           // delete
void RowHeader::RemoveActiveTxn(TransactionId tid, uint32_t server_id)
{
    std::lock_guard<std::mutex> lock(active_tid_mutex);
    active_tid_.remove(tid);
    for (auto iter = version_.begin(); iter != version_.end(); iter++){
        auto row = *iter;
        auto header = row->GetRowHeader();
        std::unique_lock<std::mutex> active_tid_lock(header->active_tid_mutex);
        header->active_tid_.remove(tid);
        active_tid_lock.unlock();
    }
}

// wzy: 清除多余版本，当前Row并不清除
void RowHeader::CleanupVersions(TransactionId min_active_txn_id) {
    std::lock_guard<std::mutex> lock(version_mutex);
    auto iter = version_.begin();
    while(iter != version_.end()) {
        auto row = *iter;
        auto header = row->GetRowHeader();
        TransactionId version_max_txn_id = *(std::max_element(header->active_tid_.begin(), header->active_tid_.end()));
        if (version_max_txn_id < min_active_txn_id){
            iter = version_.erase(iter);
            // TODO: 释放row和rowheader
            row->GetTable()->DestroyRow(row);
        } else {
            iter++;
        }
    }
}


///

///

}  // namespace MOT
