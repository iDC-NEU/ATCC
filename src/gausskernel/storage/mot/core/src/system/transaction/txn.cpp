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
 * txn.cpp
 *    Transaction manager used to manage the life cycle of a single transaction.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/transaction/txn.cpp
 *
 * -------------------------------------------------------------------------
 */

 #include <stdlib.h>
 #include <stdio.h>
 #include <algorithm>
 #include <unordered_map>
 
 #include "table.h"
 #include "mot_engine.h"
 #include "redo_log_writer.h"
 #include "sentinel.h"
 #include "txn.h"
 #include "txn_access.h"
 #include "txn_insert_action.h"
 #include "db_session_statistics.h"
 #include "utilities.h"
 #include "mm_api.h"
 #include "../../../../fdw_adapter/src/mot_internal.h"//ADDBY NEU
 // #include "../../../../fdw_adapter/src/mot_internal.cpp"//ADDBY NEU
 #include "postmaster/postmaster.h"
 #include <cpuid.h>
 #include <sstream>
 #include "utils/timestamp.h"
 #include <random>
 #include <stdlib.h>
 #include "hybrid_cc/hybrid_cc_manager.h" // HYBRID_CC: Include HybridCC manager
 #include <cstdlib>
 #include <unistd.h>
 #include <ctime>
 
 namespace MOT {
 DECLARE_LOGGER(TxnManager, System);
 
 
 void TxnManager::RemoveTableFromStat(Table* t)
 {
     m_accessMgr->RemoveTableFromStat(t);
 }
 
 void TxnManager::UpdateRow(Row* row, const int attr_id, double attr_value)
 {
     row->SetValue(attr_id, attr_value);
     UpdateLastRowState(AccessType::WR);
 }
 
 void TxnManager::UpdateRow(Row* row, const int attr_id, uint64_t attr_value)
 {
     row->SetValue(attr_id, attr_value);
     UpdateLastRowState(AccessType::WR);
 }
 
 InsItem* TxnManager::GetNextInsertItem(Index* index)
 {
     return m_accessMgr->GetInsertMgr()->GetInsertItem(index);
 }
 
 Key* TxnManager::GetTxnKey(MOT::Index* index)
 {
     int size = index->GetAlignedKeyLength() + sizeof(Key);
     void* buf = MemSessionAlloc(size);
     if (buf == nullptr) {
         return nullptr;
     }
     return new (buf) Key(index->GetAlignedKeyLength());
 }
 Key* TxnManager::GetTxnKey(MOT::Index* index, void* buf)
 {
     int size = index->GetAlignedKeyLength() + sizeof(Key);
     buf = MemSessionAlloc(size);
     // buf = new(size);
     if (buf == nullptr) {
         return nullptr;
     }
     return new (buf) Key(index->GetAlignedKeyLength());
 }
 
RC TxnManager::InsertRow(Row* row)
{
    GcSessionStart();
    
    // HYBRID_CC: Record operation for interval tracking
    RecordOperation();
    
    RC result = m_accessMgr->GetInsertMgr()->ExecuteOptimisticInsert(row);
    if (result == RC_OK) {
        MOT::DbSessionStatisticsProvider::GetInstance().AddInsertRow();
    }
    return result;
}
 
 Row* TxnManager::RowLookup(const AccessType type, Sentinel* const& originalSentinel, RC& rc)
 {
     // wzy: 应对后置设置交互性事务
     uint32_t s_id = u_sess->mot_cxt.session_id;
     if (u_sess->storage_cxt.interactiveTxn > 0 && !IsInteractive()) {
         InitInteractiveTxn();
         if (is_debug_print_enable) {
             MOT_LOG_INFO("[First time] TxnManager interactive RowLookup thrd_interactiveTxn : %d, session ID : %d", IsInteractive(), s_id);
             MOT_LOG_INFO("[First time] TxnManager thrd_interactiveTxn : %d, retry_cnt : %d, csn : %llu, score : %%lu", IsInteractive(), retry_cnt, pre_csn, score_);
         }
     }

     if (is_debug_print_enable) {
         MOT_LOG_INFO("TxnManager interactive RowLookup thrd_interactiveTxn : %d, session ID : %d, m_tid : %llu", IsInteractive(), s_id, this->m_internalTransactionId);
         MOT_LOG_INFO("TxnManager thrd_interactiveTxn : %d, retry_cnt : %d, csn : %llu", IsInteractive(), retry_cnt, pre_csn);
     }
 
     auto csn_temp = std::to_string(pre_csn) + ":" + std::to_string(local_ip_index);
     TryRecordTimestamp(1, startExec);//ADDBY NEU HW
 
     // wzy: 检验是否切换
     uint64_t cur_time = now_to_us();
     if (kInteractive_Active && type == AccessType::RD && IsInteractive() && is_CC_Switch_enable && ValidateTxnPessimistic(cur_time)) {
         if (abort_) return nullptr;
         if (first_time_pessimistic) {
             rc = SwitchToPCC();
             if (rc != MOT::RC_OK)
                 return nullptr;
         }
     }
 
     rc = RC_OK;
     // Look for the Sentinel in the cache
     Row* local_row = nullptr;
     // error handlingDeleteLastRow
     if (unlikely(originalSentinel == nullptr)) {
         return nullptr;
     }
     // if txn not started, tag as started and take global epoch
     GcSessionStart();
 
    // wzy: 添加统计
    AddReadCnt();
    
    // HYBRID_CC: Record operation for interval tracking
    RecordOperation();

    // wzy: 先查找cache
     if(isMVCC_Active && type == AccessType::RD && read_cache.GetReadCache(originalSentinel, local_row)){
         MOTAdaptor::AddActiveTxnRow(GetInternalTransactionId(), local_row);
         return local_row;
     }
     RC res = RC_OK;
 
     if (!isMVCC_Active) {
         res = AccessLookup(type, originalSentinel, local_row);
     } else {
         res = AccessLookupMVCC(type, originalSentinel, local_row);
     }
 
     // wzy: 插入全局活跃事务表
     Row* r_local_row = nullptr;
     if (type == AccessType::RD && local_row) MOTAdaptor::AddActiveTxnRow(GetInternalTransactionId(), local_row);
 
     switch (res) {
         case RC::RC_LOCAL_ROW_DELETED:
             // wzy: 插入全局活跃事务表
             // wzy: 获取到的row找到合适发版本
             if (type == AccessType::RD && local_row && isMVCC_Active) {
                 uint64_t csn = start_time;       // wzy : 测试
                 auto curr_header = local_row->GetRowHeader();
                 if (curr_header->RowSatisfiesPack(GetInternalTransactionId(), csn, 0)) {
                     curr_header->AddActiveTxnList(GetInternalTransactionId(), 0);
                 } else {
                     auto temp_header = curr_header;
                     r_local_row = curr_header->FindVisibleVersion(GetInternalTransactionId(), csn, 0, temp_header);
                     if (r_local_row) local_row = r_local_row;
                     temp_header->AddActiveTxnList(GetInternalTransactionId(), 0);
                 }
                 read_cache.AddReadCache(originalSentinel, local_row);
                 return local_row;
             }
             return nullptr;
         case RC::RC_LOCAL_ROW_FOUND: {
             // wzy: 插入全局活跃事务表
             // wzy: 获取到的row找到合适发版本，仅在多版本下支持
             if (type == AccessType::RD && local_row && isMVCC_Active) {
                 uint64_t csn = start_time;       // wzy : 测试
                 auto curr_header = local_row->GetRowHeader();
                 if (curr_header->RowSatisfiesPack(GetInternalTransactionId(), csn, 0)) {
                     curr_header->AddActiveTxnList(GetInternalTransactionId(), 0);
                 }
                 else {
                     auto temp_header = curr_header;
                     r_local_row = curr_header->FindVisibleVersion(GetInternalTransactionId(), csn, 0, temp_header);
                     if (r_local_row) local_row = r_local_row;
                     temp_header->AddActiveTxnList(GetInternalTransactionId(), 0);
                 }
                 read_cache.AddReadCache(originalSentinel, local_row);
             }
 
             if (local_row) {
                 std::string tmp_rowid = local_row->GetTable()->GetLongTableName() + ":" + to_string(local_row->GetRowId());
                 if (MOTAdaptor::dynamic_hot_rows.isHotRows(tmp_rowid)) {  // wzy: INS操作没有现存的row id
                     AddHotRowCnt();      // wzy: 添加统计
                     // 设置row header是hot，用于仅对热数据加锁
                     if(kHotRow_Active) hot_rowid_records.emplace(local_row->GetRowId());
                 }
             }
 
             if (local_row && !isMVCC_Active && type == AccessType::RD && IsInteractive() && pessimistic_flag && !is_hybrid_cc_enable) {
                 if (cc_mode == 2) rc = GetReadLock_Plor(local_row);          // Plor上读锁
                 else if (cc_mode == 3) rc = GetReadLock_WoundWait(local_row);
                 else if (cc_mode == 4) rc = GetReadLock_Plor(local_row);
                 else if (cc_mode == 5) rc = GetReadLock_DL(local_row);
             }

             if (local_row && !isMVCC_Active && type == AccessType::RD && IsInteractive() && is_hybrid_cc_enable) {
                 if (cc_mode == 4 && ShouldLock(false, local_row->GetRowId())) rc = GetReadLock_Plor(local_row);
             }
 
             return local_row;
         }
         case RC::RC_LOCAL_ROW_NOT_FOUND:
             if (likely(originalSentinel->IsCommited() == true)) {
                 // For Read-Only Txn return the Commited row
                 // if (GetTxnIsoLevel() == READ_COMMITED and type == AccessType::RD) {      // original with no RD set
                 if (GetTxnIsoLevel() == READ_COMMITED and type == AccessType::RD and false) {
                     if (!isMVCC_Active) {
                         local_row = m_accessMgr->GetReadCommitedRow(originalSentinel);
                     }
                     else {
                         local_row = m_accessMgr->GetReadCommitedRowMVCC(type, originalSentinel, true);
                         // wzy: 插入全局活跃事务表 获取到的row找到合适发版本
                         if (type == AccessType::RD && local_row) {
                             uint64_t csn = start_time;       // wzy : 测试
                             auto curr_header = local_row->GetRowHeader();
                             if (curr_header->RowSatisfiesPack(GetInternalTransactionId(), csn, 0)) {
                                 curr_header->AddActiveTxnList(GetInternalTransactionId(), 0);
                             } else {
                                 auto temp_header = curr_header;
                                 r_local_row = curr_header->FindVisibleVersion(GetInternalTransactionId(), csn, 0, temp_header);
                                 if (r_local_row) local_row = r_local_row;
                                 temp_header->AddActiveTxnList(GetInternalTransactionId(), 0);
                             }
                             read_cache.AddReadCache(originalSentinel, local_row);
                         }
                     }
                     if (local_row) {
                         std::string tmp_rowid =
                             local_row->GetTable()->GetLongTableName() + ":" + to_string(local_row->GetRowId());
                         if (MOTAdaptor::dynamic_hot_rows.isHotRows(tmp_rowid)) {  // wzy: INS操作没有现存的row id
                             AddHotRowCnt();                                       // wzy: 添加统计
                             if(kHotRow_Active) hot_rowid_records.emplace(local_row->GetRowId());
                         }
                     }
                     if (local_row && !isMVCC_Active && type == AccessType::RD && IsInteractive() && pessimistic_flag && !is_hybrid_cc_enable) {
                         if (cc_mode == 2) rc = GetReadLock_Plor(local_row);          // Plor上读锁
                         else if (cc_mode == 3) rc = GetReadLock_WoundWait(local_row);
                         else if (cc_mode == 4) rc = GetReadLock_Plor(local_row);
                         else if (cc_mode == 5) rc = GetReadLock_DL(local_row);
                     }
                     if (local_row && !isMVCC_Active && type == AccessType::RD && IsInteractive() && is_hybrid_cc_enable) {
                         if (cc_mode == 4 && ShouldLock(false, local_row->GetRowId())) rc = GetReadLock_Plor(local_row);
                     }
                     return local_row;
                 }
                 else {
                     // Row is not in the cache,map it and return the local row
                     AccessType rd_type = (type != RD_FOR_UPDATE) ? RD : RD_FOR_UPDATE;
                     if (!isMVCC_Active) {
                         local_row = m_accessMgr->MapRowtoLocalTable(rd_type, originalSentinel, rc);
                     } else {
                         // wzy：update这里type为RD
                         local_row = m_accessMgr->MapRowtoLocalTableMVCC(type, rd_type, originalSentinel, rc, true);
 
                         // wzy: 插入全局活跃事务表 获取到的row找到合适发版本
                         if (type == AccessType::RD && local_row) {
                             uint64_t csn = start_time;       // wzy : 测试
                             auto curr_header = local_row->GetRowHeader();
                             if (curr_header->RowSatisfiesPack(GetInternalTransactionId(), csn, 0)) {
                                 curr_header->AddActiveTxnList(GetInternalTransactionId(), 0);
                             } else {
                                 auto temp_header = curr_header;
                                 r_local_row = curr_header->FindVisibleVersion(GetInternalTransactionId(), csn, 0, temp_header);
                                 if (r_local_row) local_row = r_local_row;
                                 temp_header->AddActiveTxnList(GetInternalTransactionId(), 0);
                             }
                             read_cache.AddReadCache(originalSentinel, local_row);
                         }
                     }
                     if (local_row) {
                         std::string tmp_rowid =
                             local_row->GetTable()->GetLongTableName() + ":" + to_string(local_row->GetRowId());
                         if (MOTAdaptor::dynamic_hot_rows.isHotRows(tmp_rowid)) {  // wzy: INS操作没有现存的row id
                             AddHotRowCnt();                                       // wzy: 添加统计
                             if(kHotRow_Active) hot_rowid_records.emplace(local_row->GetRowId());
                         }
                     }
                     if (local_row && !isMVCC_Active && type == AccessType::RD && IsInteractive() && pessimistic_flag && !is_hybrid_cc_enable) {
                         if (cc_mode == 2) rc = GetReadLock_Plor(local_row);          // Plor上读锁
                         else if (cc_mode == 3) rc = GetReadLock_WoundWait(local_row);
                         else if (cc_mode == 4) rc = GetReadLock_Plor(local_row);
                         else if (cc_mode == 5) rc = GetReadLock_DL(local_row);
                     }

                     if (local_row && !isMVCC_Active && type == AccessType::RD && IsInteractive() && is_hybrid_cc_enable) {
                         if (cc_mode == 4 && ShouldLock(false, local_row->GetRowId())) rc = GetReadLock_Plor(local_row);
                     }
 
                     return local_row;
                 }
             } else
                 return nullptr;
         case RC::RC_MEMORY_ALLOCATION_ERROR:
             rc = RC_MEMORY_ALLOCATION_ERROR;
             return nullptr;
         default:
             return nullptr;
     }
 }
 
 RC TxnManager::AccessLookup(const AccessType type, Sentinel* const& originalSentinel, Row*& localRow)
 {
     return m_accessMgr->AccessLookup(type, originalSentinel, localRow);
 }
 
 RC TxnManager::AccessLookupMVCC(const AccessType type, Sentinel* const& originalSentinel, Row*& localRow)
 {
     return m_accessMgr->AccessLookupMVCC(type, originalSentinel, localRow);
 }
 
 RC TxnManager::DeleteLastRow()
 {
     RC rc;
     Access* access = m_accessMgr->GetLastAccess();
     if (access == nullptr)
         return RC_ERROR;
 
     rc = m_accessMgr->UpdateRowState(AccessType::DEL, access);
     if (rc != RC_OK)
         return rc;
     access->m_stmtCount = GetStmtCount();
     return rc;
 }
 
 RC TxnManager::UpdateLastRowState(AccessType state)
 {
     return m_accessMgr->UpdateRowState(state, m_accessMgr->GetLastAccess());
 }
 
 bool TxnManager::IsUpdatedInCurrStmt()
 {
     Access* access = m_accessMgr->GetLastAccess();
     if (access == nullptr) {
         return false;
     }
     if (m_internalStmtCount == m_accessMgr->GetLastAccess()->m_stmtCount) {
         return true;
     }
     return false;
 }
 
 uint64_t GetThreadID(){
     return (uint64_t) std::hash<std::thread::id>{}(std::this_thread::get_id());
 }
 
 RC TxnManager::StartTransaction(uint64_t transactionId, int isolationLevel)
 {
     m_transactionId = transactionId;
     m_isolationLevel = isolationLevel;
     m_state = TxnState::TXN_START;
     GcSessionStart();
 
     if(GetStartEpoch() == 0) {
         SetIndexPack(GetThreadID() % kPackageNum);
         add_num:
         SetStartEpoch(MOTAdaptor::GetPhysicalEpoch());//ADDBY NEU
         // MOT_LOG_INFO("Start Transaction 1 %llu %llu", GetStartEpoch(), MOTAdaptor::GetLogicalEpoch());
         if(is_sync_exec) {
             if(!MOTAdaptor::TryIncLocalChangeSetNum(GetStartEpoch(), GetIndexPack(), 1)) goto add_num;
             // MOT_LOG_INFO("==Start Transaction 1 %llu %llu", GetStartEpoch(), MOTAdaptor::GetLogicalEpoch());
             // uint64_t cnt = 0;
             while(GetStartEpoch() > MOTAdaptor::GetLogicalEpoch() || !MOTAdaptor::IsRecordCommitted()) {
                 usleep(200);
                 // cnt ++;
                 // if(cnt % 100 == 0) {
                 //     MOT_LOG_INFO("Start Transaction 1 %llu %llu", GetStartEpoch(), MOTAdaptor::GetLogicalEpoch());
                 // }
             }
         }
         SetCommitEpoch(GetStartEpoch());
         SetStartLogicalEpoch(MOTAdaptor::GetLogicalEpoch());
     }
     return RC_OK;
 }
 
 void TxnManager::InitInteractiveTxn() {
     SetInteractive(true);
     retry_cnt = u_sess->storage_cxt.retryCnt;
     SetCommitSequenceNumber(start_time);
     session_id = u_sess->mot_cxt.session_id;
     pre_csn = ((start_time & HIGH_MASK) << 16) | (session_id & 0xFFFF);
     GetScore();
     MOTAdaptor::start_interactive_txn_num.fetch_add(1);
     hot_rowid_records.clear();
     read_lock_rowid_records.clear();
     write_lock_rowid_records.clear();
     abort_ = false;
 //    traj = std::make_unique<std::vector<std::unique_ptr<MOT::StateAction>>>();
     traj.reset(new std::vector<std::unique_ptr<StateAction>>());
     traj->resize(10);
     traj_index = 0;
 }
 
 RC TxnManager::StartTransactionInteractive(uint64_t transactionId, int isolationLevel, bool interactive_)
 {
     m_transactionId = transactionId;
     m_isolationLevel = isolationLevel;
     m_state = TxnState::TXN_START;
     GcSessionStart();
 
     if(GetStartEpoch() == 0) {
         SetIndexPack(GetThreadID() % kPackageNum);
     add_num:
         SetStartEpoch(MOTAdaptor::GetPhysicalEpoch());//ADDBY NEU
         // MOT_LOG_INFO("Start Transaction 1 %llu %llu", GetStartEpoch(), MOTAdaptor::GetLogicalEpoch());
         if(is_sync_exec) {
             if(!MOTAdaptor::TryIncLocalChangeSetNum(GetStartEpoch(), GetIndexPack(), 1)) goto add_num;
             // MOT_LOG_INFO("==Start Transaction 1 %llu %llu", GetStartEpoch(), MOTAdaptor::GetLogicalEpoch());
             // uint64_t cnt = 0;
             while(GetStartEpoch() > MOTAdaptor::GetLogicalEpoch() || !MOTAdaptor::IsRecordCommitted()) {
                 usleep(200);
             }
         }
         SetCommitEpoch(GetStartEpoch());
         SetStartLogicalEpoch(MOTAdaptor::GetLogicalEpoch());
     }
 
     // wzy: 记为交互型事务
     SetInteractive(interactive_);
     MOTAdaptor::start_num_start_txn.fetch_add(1);
     start_time = now_to_us();       // wzy: 访问多版本用
     txnId = MOTAdaptor::start_txn_num.fetch_add(1);
     write_cnt = read_cnt = 0;
     retry_cnt = 0;
     hot_cnt = 0;
     // wzy: 记录访问过的热点数据row id
     hot_rowid_records.clear();
     read_lock_rowid_records.clear();
     write_lock_rowid_records.clear();
     abort_ = false;
 
     if (u_sess->storage_cxt.interactiveTxn > 0) {
         InitInteractiveTxn();
         MOT_LOG_INFO("TxnManager interactive StartTransactionInteractive thrd_interactiveTxn : %d", u_sess->storage_cxt.interactiveTxn);
     } else {
         MOT_LOG_INFO("TxnManager stored process StartTransactionInteractive");
     }
 
     int state = 0;
     MOTAdaptor::txn_state_map_plor_.insert(start_time, state);
     MOTAdaptor::AddActiveTxn(m_internalTransactionId);
     return RC_OK;
 }
 
 
 void TxnManager::LiteRollback()
 {
     if (m_txnDdlAccess->Size() > 0) {
         RollbackDDLs();
         Cleanup();
     }
     MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackTxn();
 }
 
 void TxnManager::LiteRollbackPrepared()
 {
     if (m_txnDdlAccess->Size() > 0) {
         m_redoLog.RollbackPrepared();
         RollbackDDLs();
         Cleanup();
     }
     MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackPreparedTxn();
 }
 
 void TxnManager::RollbackInternal(bool isPrepared)
 {
     if (isPrepared) {
         if (GetGlobalConfiguration().m_enableCheckpoint) {
             GetCheckpointManager()->FreePreAllocStableRows(this);
         }
 
         m_occManager.ReleaseHeaders(this);
         m_redoLog.RollbackPrepared();
     } else {
         m_occManager.ReleaseLocks(this);
     }
 
     // We have to undo changes to secondary indexes and ddls
     m_occManager.RollbackInserts(this);
     RollbackDDLs();
     Cleanup();
     if (isPrepared) {
         MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackPreparedTxn();
     } else {
         MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackTxn();
     }
 }
 
 void TxnManager::CleanTxn()
 {
     Cleanup();
 }
 
 RC TxnManager::Prepare()
 {
     // Run only first validation phase
     RC rc = m_occManager.ValidateOcc(this);
     if (rc == RC_OK) {
         m_redoLog.Prepare();
     }
     return rc;
 }
 
 void TxnManager::LitePrepare()
 {
     if (m_txnDdlAccess->Size() == 0) {
         return;
     }
 
     m_redoLog.Prepare();
 }
 
 void TxnManager::CommitInternal()
 {
     if (m_csn == CSNManager::INVALID_CSN) {
         SetCommitSequenceNumber(GetCSNManager().GetNextCSN());
     }
 
     // first write to redo log, then write changes
     m_redoLog.Commit();
     m_occManager.WriteChanges(this, 0);
 
     if (GetGlobalConfiguration().m_enableCheckpoint) {
         GetCheckpointManager()->EndCommit(this);
     }
 
     // 释放锁
     if (!GetGlobalConfiguration().m_enableRedoLog) {
         m_occManager.ReleaseLocks(this);
     }
 }
 
 void TxnManager::CommitInternalPlor(uint64_t& pre_csn)
 {
     if (m_csn == CSNManager::INVALID_CSN) {
         SetCommitSequenceNumber(GetCSNManager().GetNextCSN());
     }
 
     // first write to redo log, then write changes
     m_redoLog.Commit();
     m_occManager.WriteChanges(this, 0);
 
     if (GetGlobalConfiguration().m_enableCheckpoint) {
         GetCheckpointManager()->EndCommit(this);
     }
 
     // 释放锁
     if (!GetGlobalConfiguration().m_enableRedoLog) {
         m_occManager.ReleaseLocks(this);        // header 和 sentinel 解锁
     }
 }
 
 
 RC TxnManager::ValidateCommit()
 {
     return m_occManager.ValidateOcc(this);
 }
 
 void TxnManager::RecordCommit()
 {
     //ADDBY NEU
     if(cc_mode == 3) CommitInternal();            // Silo record commit
     else if(cc_mode == 4) CommitInternal();            // Silo record commit
     else CommitInternalII();// 原来为CommitInternal
     MOT::DbSessionStatisticsProvider::GetInstance().AddCommitTxn();
 }
 
 void TxnManager::RecordCommit(uint64_t &pre_csn)
 {
     //ADDBY NEU
     if(cc_mode == 3) CommitInternalPlor(pre_csn);
     else if(cc_mode == 4) CommitInternalPlor(pre_csn);            // Silo record commit
     else CommitInternalII();        //原来为CommitInternal
     MOT::DbSessionStatisticsProvider::GetInstance().AddCommitTxn();
 }
 
 //ADDBY NEU
 // RC TxnManager::Commit()
 // {
 //     // Validate concurrency control
 //     RC rc = ValidateCommit();
 //     if (rc == RC_OK) {
 //         RecordCommit();
 //     }
 //     return rc;
 // }
 
 void TxnManager::LiteCommit()
 {
     if (m_txnDdlAccess->Size() > 0) {
         // write to redo log
         m_redoLog.Commit();
         CleanDDLChanges();
         Cleanup();
     }
     MOT::DbSessionStatisticsProvider::GetInstance().AddCommitTxn();
 }
 
 void TxnManager::CommitPrepared()
 {
     if (m_csn == CSNManager::INVALID_CSN) {
         SetCommitSequenceNumber(GetCSNManager().GetNextCSN());
     }
 
     // first write to redo log, then write changes
     m_redoLog.CommitPrepared();
     m_occManager.WriteChanges(this, 0);
 
     if (GetGlobalConfiguration().m_enableCheckpoint) {
         GetCheckpointManager()->EndCommit(this);
     }
 
     if (!GetGlobalConfiguration().m_enableRedoLog) {
         m_occManager.ReleaseLocks(this);
     }
     MOT::DbSessionStatisticsProvider::GetInstance().AddCommitPreparedTxn();
 }
 
 void TxnManager::LiteCommitPrepared()
 {
     if (m_txnDdlAccess->Size() > 0) {
         // first write to redo log, then write changes
         m_redoLog.CommitPrepared();
         CleanDDLChanges();
         Cleanup();
     }
     MOT::DbSessionStatisticsProvider::GetInstance().AddCommitPreparedTxn();
 }
 
 void TxnManager::EndTransaction()
 {
     if (GetGlobalConfiguration().m_enableRedoLog) {
         // m_occManager.ReleaseLocks(this);            // header 和 sentinel 解锁?
         // wzy: 只对sentinel解锁
         if (cc_mode == 4) {
             m_occManager.ReleaseLocksSentinel(this);
             if (pessimistic_flag) m_occManager.UnlockReadWriteLockPlor(this, local_ip_index, pre_csn, false);
         } else if (cc_mode == 3) {
             m_occManager.ReleaseLocksSentinel(this);
             if (pessimistic_flag) m_occManager.UnlockReadWriteLockWoundWait(this, local_ip_index, pre_csn, false);
         } else {
             m_occManager.ReleaseLocks(this);
         }
     } else {
         if (cc_mode == 4) {
             m_occManager.ReleaseLocksSentinel(this);
             if (pessimistic_flag) m_occManager.UnlockReadWriteLockPlor(this, local_ip_index, pre_csn, false);
         } else if (cc_mode == 3) {
             m_occManager.ReleaseLocksSentinel(this);
             if (pessimistic_flag) m_occManager.UnlockReadWriteLockWoundWait(this, local_ip_index, pre_csn, false);
         } else {
             m_occManager.ReleaseLocks(this);
         }
     }
     CleanDDLChanges();
     Cleanup();
 }
 
 void TxnManager::RedoWriteAction(bool isCommit)
 {
     m_redoLog.SetForceWrite();
     if (isCommit)
         m_redoLog.Commit();
     else
         m_redoLog.Rollback();
 }
 
 void TxnManager::Cleanup()
 {
     if (m_isLightSession == false) {
         m_accessMgr->ClearSet();
     }
     m_txnDdlAccess->Reset();
     m_checkpointPhase = CheckpointPhase::NONE;
     m_csn = CSNManager::INVALID_CSN;
     m_occManager.CleanUp();
     m_err = RC_OK;
     m_errIx = nullptr;
     m_flushDone = false;
     m_internalTransactionId++;
     m_internalStmtCount = 0;
     m_redoLog.Reset();
     GcSessionEnd();
     ClearErrorStack();
     m_accessMgr->ClearTableCache();
     m_queryState.clear();
     read_cache.ClearState();        // wzy: 清空cache
 }
 
 void TxnManager::UndoInserts()
 {
     uint32_t rollbackCounter = 0;
     TxnOrderedSet_t& OrderedSet = m_accessMgr->GetOrderedRowSet();
     for (const auto& ra_pair : OrderedSet) {
         Access* ac = ra_pair.second;
         if (ac->m_type != AccessType::INS) {
             continue;
         }
 
         rollbackCounter++;
         RollbackInsert(ac);
         m_accessMgr->IncreaseTableStat(ac->GetTxnRow()->GetTable());
     }
 
     if (rollbackCounter == 0) {
         return;
     }
 
     // Release local rows!
     for (const auto& ra_pair : OrderedSet) {
         Access* ac = ra_pair.second;
         if (ac->m_type == AccessType::INS) {
             MOT::Index* index_ = ac->GetSentinel()->GetIndex();
             // Row is local and was not inserted in the commit
             if (index_->GetIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
                 // Release local row to the GC!!!!!
                 ac->GetTxnRow()->GetTable()->DestroyRow(ac->GetTxnRow());
             }
             rollbackCounter--;
             if (rollbackCounter == 0) {
                 break;
             }
         }
     }
 }
 
 RC TxnManager::RollbackInsert(Access* ac)
 {
     Sentinel* outputSen = nullptr;
     RC rc;
     Sentinel* sentinel = ac->GetSentinel();
     MOT::Index* index_ = sentinel->GetIndex();
 
     MOT_ASSERT(sentinel != nullptr);
     rc = sentinel->RefCountUpdate(DEC, GetThdId());
     MOT_ASSERT(rc != RC::RC_INDEX_RETRY_INSERT);
     if (rc == RC::RC_INDEX_DELETE) {
         MaxKey m_key;
         // Memory reclamation need to release the key from the primary sentinel back to the pool
         m_key.InitKey(index_->GetKeyLength());
         index_->BuildKey(ac->GetTxnRow()->GetTable(), ac->GetTxnRow(), &m_key);
         MOT_ASSERT(sentinel->GetCounter() == 0);
 #ifdef MOT_DEBUG
         Sentinel* curr_sentinel = index_->IndexReadHeader(&m_key, GetThdId());
         MOT_ASSERT(curr_sentinel == sentinel);
 #endif
         outputSen = index_->IndexRemove(&m_key, GetThdId());
         MOT_ASSERT(outputSen != nullptr);
         GcSessionRecordRcu(index_->GetIndexId(), outputSen, nullptr, Index::SentinelDtor, SENTINEL_SIZE(index_));
         // If we are the owner of the key and insert on top of a deleted row,
         // lets check if we can reclaim the deleted row
         if (ac->m_params.IsUpgradeInsert() and index_->IsPrimaryKey()) {
             MOT_ASSERT(sentinel->GetData() != nullptr);
             GcSessionRecordRcu(index_->GetIndexId(),
                 sentinel->GetData(),
                 nullptr,
                 Row::RowDtor,
                 ROW_SIZE_FROM_POOL(ac->GetTxnRow()->GetTable()));
         }
     }
     return rc;
 }
 
 void TxnManager::RollbackSecondaryIndexInsert(Index* index)
 {
     if (m_isLightSession)
         return;
 
     TxnOrderedSet_t& access_row_set = m_accessMgr->GetOrderedRowSet();
     TxnOrderedSet_t::iterator it = access_row_set.begin();
     while (it != access_row_set.end()) {
         Access* ac = it->second;
         if (ac->m_type == INS && ac->GetSentinel()->GetIndex() == index) {
             RollbackInsert(ac);
             it = access_row_set.erase(it);
             // need to perform index clean-up!
             m_accessMgr->PubReleaseAccess(ac);
         } else {
             it++;
         }
     }
 }
 
 void TxnManager::RollbackDDLs()
 {
     // early exit
     if (m_txnDdlAccess->Size() == 0)
         return;
 
     // rollback DDLs in reverse order (avoid rolling back parent object before rolling back child)
     for (int i = m_txnDdlAccess->Size() - 1; i >= 0; i--) {
         MOT::Index* index = nullptr;
         MOTIndexArr* indexArr = nullptr;
         Table* table = nullptr;
         TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->Get(i);
         switch (ddl_access->GetDDLAccessType()) {
             case DDL_ACCESS_CREATE_TABLE:
                 table = (Table*)ddl_access->GetEntry();
                 MOT_LOG_INFO("Rollback of create table %s", table->GetLongTableName().c_str());
                 table->DropImpl();
                 RemoveTableFromStat(table);
                 if (table != nullptr)
                     delete table;
                 break;
             case DDL_ACCESS_DROP_TABLE:
                 table = (Table*)ddl_access->GetEntry();
                 MOT_LOG_INFO("Rollback of drop table %s", table->GetLongTableName().c_str());
                 break;
             case DDL_ACCESS_TRUNCATE_TABLE:
                 indexArr = (MOTIndexArr*)ddl_access->GetEntry();
                 table = indexArr->GetTable();
                 table->WrLock();
                 if (indexArr->GetNumIndexes() > 0) {
                     MOT_ASSERT(indexArr->GetNumIndexes() == table->GetNumIndexes());
                     MOT_LOG_INFO("Rollback of truncate table %s", table->GetLongTableName().c_str());
                     for (int idx = 0; idx < indexArr->GetNumIndexes(); idx++) {
                         uint16_t oldIx = indexArr->GetIndexIx(idx);
                         MOT::Index* oldIndex = indexArr->GetIndex(idx);
                         index = table->m_indexes[oldIx];
                         table->m_indexes[oldIx] = oldIndex;
                         if (idx == 0)
                             table->m_primaryIndex = oldIndex;
                         else
                             table->m_secondaryIndexes[oldIndex->GetName()] = oldIndex;
                         GcManager::ClearIndexElements(index->GetIndexId());
                         index->Truncate(true);
                         delete index;
                     }
                 }
                 table->ReplaceRowPool(indexArr->GetRowPool());
                 table->Unlock();
                 delete indexArr;
                 break;
             case DDL_ACCESS_CREATE_INDEX:
                 index = (Index*)ddl_access->GetEntry();
                 table = index->GetTable();
                 MOT_LOG_INFO("Rollback of create index %s for table %s",
                     index->GetName().c_str(),
                     table->GetLongTableName().c_str());
                 table->WrLock();
                 if (index->IsPrimaryKey()) {
                     table->DecIndexColumnUsage(index);
                     table->SetPrimaryIndex(nullptr);
                     table->DeleteIndex(index);
                 } else {
                     table->RemoveSecondaryIndex(index, this);
                 }
                 table->Unlock();
                 break;
             case DDL_ACCESS_DROP_INDEX:
                 index = (Index*)ddl_access->GetEntry();
                 table = index->GetTable();
                 MOT_LOG_INFO("Rollback of drop index %s for table %s",
                     index->GetName().c_str(),
                     table->GetLongTableName().c_str());
                 table->WrLock();
                 if (index->IsPrimaryKey()) {
                     table->IncIndexColumnUsage(index);
                     table->SetPrimaryIndex(index);
                 } else {
                     table->AddSecondaryIndexToMetaData(index);
                 }
                 table->Unlock();
                 break;
             default:
                 break;
         }
     }
 }
 
 void TxnManager::CleanDDLChanges()
 {
     // early exit
     if (m_txnDdlAccess->Size() == 0)
         return;
 
     MOT::Index* index = nullptr;
     MOTIndexArr* indexArr = nullptr;
     Table* table = nullptr;
     for (uint16_t i = 0; i < m_txnDdlAccess->Size(); i++) {
         TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->Get(i);
         switch (ddl_access->GetDDLAccessType()) {
             case DDL_ACCESS_CREATE_TABLE:
                 GetTableManager()->AddTable((Table*)ddl_access->GetEntry());
                 break;
             case DDL_ACCESS_DROP_TABLE:
                 GetTableManager()->DropTable((Table*)ddl_access->GetEntry(), m_sessionContext);
                 break;
             case DDL_ACCESS_TRUNCATE_TABLE:
                 indexArr = (MOTIndexArr*)ddl_access->GetEntry();
                 table = indexArr->GetTable();
                 if (indexArr->GetNumIndexes() > 0) {
                     table->m_rowCount = 0;
                     for (int i = 0; i < indexArr->GetNumIndexes(); i++) {
                         index = indexArr->GetIndex(i);
                         table->DeleteIndex(index);
                     }
                 }
                 table->FreeObjectPool(indexArr->GetRowPool());
                 delete indexArr;
                 break;
             case DDL_ACCESS_CREATE_INDEX:
                 index = (Index*)ddl_access->GetEntry();
                 table = index->GetTable();
                 index->SetIsCommited(true);
                 break;
             case DDL_ACCESS_DROP_INDEX:
                 index = (Index*)ddl_access->GetEntry();
                 table = index->GetTable();
                 table->DeleteIndex(index);
                 break;
             default:
                 break;
         }
     }
 }
 
 Row* TxnManager::RemoveKeyFromIndex(Row* row, Sentinel* sentinel)
 {
     Table* table = row->GetTable();
 
     Row* outputRow = nullptr;
     if (sentinel->GetStable() == nullptr) {
         outputRow = table->RemoveKeyFromIndex(row, sentinel, m_threadId, GetGcSession());
     } else {
         // Checkpoint works on primary-sentinel only!
         if (sentinel->IsPrimaryIndex() == false) {
             outputRow = table->RemoveKeyFromIndex(row, sentinel, m_threadId, GetGcSession());
         }
         outputRow = row;
     }
 
     return outputRow;
 }
 
 RC TxnManager::RowDel()
 {
     return UpdateLastRowState(AccessType::DEL);
 }
 
 // Use this function when we have only the key!
 // Not Used with FDW!
 Row* TxnManager::RowLookupByKey(Table* const& table, const AccessType type, Key* const currentKey)
 {
     TryRecordTimestamp(1, startExec);//ADDBY NEU HW
 
     RC rc = RC_OK;
     Row* originalRow = nullptr;
     Sentinel* pSentinel = nullptr;
     table->FindRow(currentKey, pSentinel, GetThdId());
     if (pSentinel == nullptr) {
         MOT_LOG_DEBUG("Cannot find key:%" PRIu64 " from table:%s", m_key, table->GetLongTableName().c_str());
         return nullptr;
     } else {
         return RowLookup(type, pSentinel, rc);
     }
 }
 
 std::atomic<uint64_t> TxnManager::start_txn_num{0};
 std::atomic<uint64_t> TxnManager::start_interactive_txn_num{0};
 
 // HYBRID_CC: Initialize HybridCC configuration flag (default: disabled)
 bool TxnManager::is_hybrid_cc_enable = true;
 
 TxnManager::TxnManager(SessionContext* session_context)
     : m_latestEpoch(~uint64_t(0)),
       m_threadId((uint64_t)-1),
       m_connectionId((uint64_t)-1),
       m_sessionContext(session_context),
       m_redoLog(this),
       m_occManager(),
       m_gcSession(nullptr),
       m_checkpointPhase(CheckpointPhase::NONE),
       m_checkpointNABit(false),
       m_csn(CSNManager::INVALID_CSN),
       m_transactionId(INVALID_TRANSACTION_ID),
       m_replayLsn(0),
       m_surrogateGen(),
       m_flushDone(false),
       m_internalTransactionId(((uint64_t)m_sessionContext->GetSessionId()) << SESSION_ID_BITS),
       m_internalStmtCount(0),
       m_isolationLevel(READ_COMMITED),
       m_isLightSession(false),
       m_errIx(nullptr),
       m_err(RC_OK)
 {
     m_key = nullptr;
     m_state = TxnState::TXN_START;
 
     // wzy: 测试
     MOTAdaptor::start_num_txn_construct.fetch_add(1);
     start_time = now_to_us();       // wzy: 访问多版本用
     txnId = MOTAdaptor::start_txn_num.fetch_add(1);
     write_cnt = read_cnt = 0;
 
     if (u_sess->storage_cxt.interactiveTxn > 0) {
         InitInteractiveTxn();
 //        MOT_LOG_INFO("TxnManager interactive StartTransactionInteractive thrd_interactiveTxn : %d", u_sess->storage_cxt.interactiveTxn);
     } else {
 //        MOT_LOG_INFO("TxnManager stored process StartTransactionInteractive");
     }
 
     MOTAdaptor::AddActiveTxn(m_internalTransactionId);
     srand(static_cast<unsigned int>(time(nullptr)));  // Seed for random action
 }
 
 TxnManager::~TxnManager()
 {
     // wzy: 从活跃事务列表移除，得在end transaction之前不然internal tid会改变
     MOTAdaptor::RemoveActiveTxn(m_internalTransactionId);
 
     if (GetGlobalConfiguration().m_enableCheckpoint) {
         GetCheckpointManager()->EndCommit(this);
     }
 
     MOT_LOG_DEBUG("txn_man::~txn_man - memory pools released for thread_id=%lu", m_threadId);
 
     if (m_gcSession != nullptr) {
         m_gcSession->GcCleanAll();
         m_gcSession->RemoveFromGcList(m_gcSession);
         m_gcSession->~GcManager();
         MemSessionFree(m_gcSession);
     }
     delete m_key;
     delete m_txnDdlAccess;
 }
 
 bool TxnManager::Init(uint64_t _thread_id, uint64_t connection_id, bool isLightTxn)
 {
     this->m_threadId = _thread_id;
     this->m_connectionId = connection_id;
     m_isLightSession = isLightTxn;
 
     if (!m_occManager.Init())
         return false;
 
     m_txnDdlAccess = new (std::nothrow) TxnDDLAccess(this);
     if (!m_txnDdlAccess) {
         MOT_REPORT_ERROR(MOT_ERROR_OOM, "Initialize Transaction", "Failed to allocate memory for DDL access data");
         return false;
     }
     m_txnDdlAccess->Init();
 
     // make node-local allocations
     if (isLightTxn == false) {
         m_accessMgr = MemSessionAllocAlignedObjectPtr<TxnAccess>(L1_CACHE_LINE);
 
         if (!m_accessMgr->Init(this))
             return false;
 
         m_surrogateGen = GetSurrogateKeyManager()->GetSurrogateSlot(connection_id);
     }
 
     m_gcSession = GcManager::Make(GcManager::GC_MAIN, _thread_id);
     if (!m_gcSession)
         return false;
 
     if (!m_redoLog.Init())
         return false;
 
     static const TxnValidation validation_lock = GetGlobalConfiguration().m_validationLock;
     if (m_key == nullptr) {
         MOT_LOG_DEBUG("Init Key");
         m_key = new (std::nothrow) MaxKey(MAX_KEY_SIZE);
         if (m_key == nullptr)
             MOTAbort();
     }
 
     m_occManager.SetPreAbort(GetGlobalConfiguration().m_preAbort);
     if (validation_lock == TxnValidation::TXN_VALIDATION_NO_WAIT)
         m_occManager.SetValidationNoWait(true);
     else if (validation_lock == TxnValidation::TXN_VALIDATION_WAITING) {
         m_occManager.SetValidationNoWait(false);
     } else {
         MOT_ASSERT(false);
     }
 
     // HYBRID_CC: Initialize the action to 0 (optimistic) by default.
     m_hybridCcAction = 0;
 
     return true;
 }
 
uint64_t TxnManager::GetScore()
{
    if (is_retry_priority_enable) {
        score_ = 0;
        uint64_t f = UINT64_MAX - start_time;
        score_ |= ((uint64_t)retry_cnt << 57);
        score_ |= (f & 0x1FFFFFFFFFFFFFF);
    } else {
        score_ = 0;
        uint64_t f = UINT64_MAX - start_time;
        score_ |= ((uint64_t)0 << 57);
        score_ |= (f & 0x1FFFFFFFFFFFFFF);
    }
    return score_;
}

// HYBRID_CC: Boost transaction priority by increasing retry count
// This increases the transaction's priority in lock competition
void TxnManager::BoostPriority(int boost_level)
{
    // Save old values for logging
    int old_retry_cnt = retry_cnt;
    uint64_t old_score = score_;
    
    // Increase retry count to boost priority
    retry_cnt += boost_level;
    
    // Prevent overflow (7 bits can represent 0-127)
    if (retry_cnt > 127) {
        retry_cnt = 127;
    }
    
    // Recalculate priority score
    GetScore();
    
    if (is_debug_print_enable) {
        std::string csn_temp = std::to_string(pre_csn) + ":0";
        MOT_LOG_INFO("HYBRID_CC: Priority boosted - csn: %s, retry_cnt: %d->%d, score: %llu->%llu", 
                     csn_temp.c_str(), old_retry_cnt, retry_cnt, old_score, score_);
    }
}

// HYBRID_CC: Boost priority relative to current level
// Adaptive boost based on current retry count
void TxnManager::BoostPriorityRelative()
{
    int old_retry_cnt = retry_cnt;
    uint64_t old_score = score_;
    
    // Adaptive boost strategy
    if (retry_cnt == 0) {
        // First boost: jump to medium priority
        retry_cnt = 1;
    } else if (retry_cnt < 5) {
        // Gradual boost for low-medium priority
        retry_cnt += 1;
    } else if (retry_cnt < 10) {
        // Aggressive boost for already high priority
        retry_cnt = std::min(127, retry_cnt + 2);
    } else {
        // Very aggressive boost for repeatedly retried transactions
        retry_cnt = std::min(127, retry_cnt + 3);
    }
    
    GetScore();
    
    if (is_debug_print_enable) {
        std::string csn_temp = std::to_string(pre_csn) + ":0";
        MOT_LOG_INFO("HYBRID_CC: Relative priority boost - csn: %s, retry_cnt: %d->%d, score: %llu->%llu", 
                     csn_temp.c_str(), old_retry_cnt, retry_cnt, old_score, score_);
    }
}

// HYBRID_CC: Set transaction to high priority level
// Directly set to high priority for critical transactions
void TxnManager::SetHighPriority()
{
    int old_retry_cnt = retry_cnt;
    uint64_t old_score = score_;
    
    // Set to high priority level (retry_cnt = 10)
    retry_cnt = 10;
    
    GetScore();
    
    if (is_debug_print_enable) {
        std::string csn_temp = std::to_string(pre_csn) + ":0";
        MOT_LOG_INFO("HYBRID_CC: Set to high priority - csn: %s, retry_cnt: %d->%d, score: %llu->%llu", 
                     csn_temp.c_str(), old_retry_cnt, retry_cnt, old_score, score_);
    }
}
 
// 判断当前行是否满足action，再进行上锁
bool TxnManager::ShouldLock(bool isWrite, uint64_t rowId)
{
    const bool isHot = (hot_rowid_records.count(rowId) != 0);

    switch (this->m_hybridCcAction) {
        case 0:
            return false;                        // 0: 不加锁

        case 1:
            return isWrite && isHot;             // 1: 仅热点写

        case 2:
            return isHot;                        // 2: 所有热点(读/写)

        case 3:
            // 3: 所有写 + 热点读（写无条件加锁，读仅热点加锁）
            return isWrite || isHot;

        case 4:
            return true;                         // 4: 访问到的行一律加锁

        default:
            return false;                        // 未知动作，保守为不加锁或按需调整
    }
}

 // wzy: 检测当前事务是否符合切换策略
 bool TxnManager::ValidateTxnPessimistic(uint64_t cur_time) {
    if (!IsInteractive()) return false;
    if (first_time_pessimistic) first_time_pessimistic = false;
    if (kAllPessimisticLock) {
        if (pessimistic_flag) return true;
      first_time_pessimistic = true;
      pessimistic_flag = true;
      return true;
    }

    // HYBRID_CC: Use HybridCC decision model
    if (is_hybrid_cc_enable) {
        //正常查表模式
        //int action = HybridCcManager::GetInstance().Decide(this);
        //数据收集 action=0-4
        int action = 0;
        if (kTrainAction > 4) {
            action = HybridCcManager::GetInstance().Decide(this);
        } else {
            action = kTrainAction;
        }

        this->m_hybridCcAction = action;

        if (action > 0) {
            pessimistic_flag = true;
            RC rc = HybridCcManager::GetInstance().ExecuteAction(this, action);
            if (rc != MOT::RC_OK) abort_ = true;
            return true;
        }
    }

    return false;

  // HYBRID_CC: Random action for training data generation
  // Use thread-safe random number generation
//   int action = (std::rand() + std::clock() + gettid()) % 6;  // 0-5 random with thread safety
//   m_hybridCcAction = action;
//
//    if (action > 0) {
//        // Execute the random action
//        HybridCcManager::GetInstance().ExecuteAction(this, action);
//        return true;  // Switch to pessimistic if action > 0
//    }
//
//    return false;  // Default to OCC
}
//  bool TxnManager::ValidateTxnPessimistic(uint64_t cur_time) {
//      if (!IsInteractive()) return false;
 
//      if (first_time_pessimistic) first_time_pessimistic = false;
//      if (pessimistic_flag) return true;
 
//     // HYBRID_CC: 数据收集模式 - 只记录状态，不应用LDT决策
//     if (is_hybrid_cc_enable) {
//         // Store action as 0 (optimistic) for logging purposes only
//         // We are in data collection mode, not applying any LDT decisions
//         this->m_hybridCcAction = 0; // HYBRID_CC: Store default action for logging
        
//         // Skip decision-making and action execution
//         // HybridCcManager is not initialized in data collection mode
//         return false;  // 保持乐观模式，不切换到悲观
//     }
 
//      // TODO: 开启RL模型
//      if (is_rl_model_enable) {
//          // test 随机action，保存当前state-action
//          uint64_t execution_time = now_to_us() - start_time;
//          int action = MOTAdaptor::adviser.computeAction(this, execution_time);
//          AddTraj(action);
//          if (action == 1) {
//              first_time_pessimistic = true;
//              pessimistic_flag = true;
//              return true;
//          }
//          return false;
//      }
 
//      // 全悲观测试
//      if (kAllPessimisticLock) {
//          first_time_pessimistic = true;
//          pessimistic_flag = true;
//          return true;
//      }
 
//      // 根据权重计算边界值
//      double txn_avg_time = 0, txn_avg_epoch = 0, txn_avg_readCnt = 0, txn_avg_writeCnt = 0, txn_avg_lockCnt = 0, txn_avg_hotCnt = 0;
//      if(MOTAdaptor::commit_txn_num.load() != 0) {
//          txn_avg_time = MOTAdaptor::txn_total_time.load() / MOTAdaptor::commit_txn_num.load();
//          txn_avg_epoch = MOTAdaptor::txn_total_epoch.load() / MOTAdaptor::commit_txn_num.load();
//          txn_avg_readCnt = MOTAdaptor::txn_total_readCnt.load() / MOTAdaptor::commit_txn_num.load();
//          txn_avg_writeCnt = MOTAdaptor::txn_total_writeCnt.load() / MOTAdaptor::commit_txn_num.load();
//          txn_avg_hotCnt = MOTAdaptor::txn_total_hotCnt.load() / MOTAdaptor::commit_txn_num.load();
//      }
 
//      // TODO: 对近期事务的统计
//  //    double txn_temp_avg_time = 0, txn_temp_avg_epoch = 0, txn_temp_avg_readCnt = 0, txn_temp_avg_writeCnt = 0, txn_temp_avg_lockCnt = 0, txn_temp_avg_hotCnt = 0;
//  //    uint64_t temp_commit_txn_num1 = MOTAdaptor::temp_commit_txn_num.load();
//  //    if(temp_commit_txn_num1 != 0) {
//  //        txn_temp_avg_time = MOTAdaptor::txn_temp_total_time.load() / temp_commit_txn_num1;
//  //        txn_temp_avg_epoch = MOTAdaptor::txn_temp_total_epoch.load() / temp_commit_txn_num1;
//  //        txn_temp_avg_readCnt = MOTAdaptor::txn_temp_total_readCnt.load() / temp_commit_txn_num1;
//  //        txn_temp_avg_writeCnt = MOTAdaptor::txn_temp_total_writeCnt.load() / temp_commit_txn_num1;
//  //        txn_temp_avg_hotCnt = MOTAdaptor::txn_temp_total_hotCnt.load() / temp_commit_txn_num1;
//  //    }
 
//      if (retry_cnt * 10 + (cur_time - start_time) * 0.0001 * kEpochWeight + read_cnt * kReadCntWeight + write_cnt * kWriteCntWeight > kSwitchLimit) {
//          pessimistic_flag = true;
//          first_time_pessimistic = true;
//          std::string csn_temp = std::to_string(pre_csn) + ":0";
//          if (is_debug_print_enable) MOT_LOG_INFO("Change to [Pessimistic1], csn : %s, retry_cnt : %llu, read_cnt : %llu, write_cnt : %llu", csn_temp.c_str(), retry_cnt, read_cnt, write_cnt);
//          MOTAdaptor::pessimisitic_priority_txn_num.fetch_add(1);
//          return true;
//      }
 
//      if (kHotCntLimit > 0 && kHotCntLimit < 100) {
//          if (cur_time - start_time > txn_avg_time + kEpochLimit || (read_cnt > txn_avg_readCnt + kReadCntLimit
//                                                                        && write_cnt > txn_avg_writeCnt + kWriteCntLimit) || hot_cnt >= txn_avg_hotCnt + kHotCntLimit) {
//              pessimistic_flag = true;
//              first_time_pessimistic = true;
//              std::string csn_temp = std::to_string(pre_csn) + ":0";
//              if (is_debug_print_enable) MOT_LOG_INFO("Change to [Pessimistic2], csn : %s, execute time : %llu, retry_cnt : %llu, read_cnt : %llu, write_cnt : %llu, hot_cnt : %llu", csn_temp.c_str(), cur_time - start_time, retry_cnt, read_cnt, write_cnt, hot_cnt);
//              MOTAdaptor::pessimisitic_hot_visits_txn_num.fetch_add(1);
//              return true;
//          }
//      }
//      else if (kHotCntLimit >= 100) {
//          if (cur_time - start_time > txn_avg_time + kEpochLimit || (read_cnt > txn_avg_readCnt + kReadCntLimit
//              && write_cnt > txn_avg_writeCnt + kWriteCntLimit) || hot_cnt >= kHotCntLimit - 100) {
//              pessimistic_flag = true;
//              first_time_pessimistic = true;
//              std::string csn_temp = std::to_string(pre_csn) + ":0";
//              if (is_debug_print_enable) MOT_LOG_INFO("Change to [Pessimistic3], csn : %s, execute time : %llu, retry_cnt : %llu, read_cnt : %llu, write_cnt : %llu, hot_cnt : %llu", csn_temp.c_str(), cur_time - start_time, retry_cnt, read_cnt, write_cnt, hot_cnt);
//              MOTAdaptor::pessimisitic_hot_visits_txn_num.fetch_add(1);
//              return true;
//          }
//      }
//      return false;
//  }
 
 
 RC TxnManager::OverwriteRow(Row* updatedRow, BitmapSet& modifiedColumns)
 {
     MOT::RC rc = MOT::RC_OK;
     if (updatedRow == nullptr) return RC_ERROR;
 
     Access* access = m_accessMgr->GetLastAccess();
     if (access == nullptr) return rc;           // wzy debug
     if (access->m_type == AccessType::WR) {
         access->m_modifiedColumns |= modifiedColumns;
         access->m_stmtCount = GetStmtCount();
     }
 
     auto csn_temp = std::to_string(pre_csn) + ":" + std::to_string(local_ip_index);
     if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) return MOT::RC_ABORT;         // wzy: 被死锁检测abort
 
    AddWriteCnt();      // 统计
    
    // HYBRID_CC: Record operation for interval tracking
    RecordOperation();

//    std::string tmp_rowid = updatedRow->GetTable()->GetTableName() + ":" + to_string(updatedRow->GetRowId());
    std::string tmp_rowid = updatedRow->GetTable()->GetLongTableName() + ":" + to_string(updatedRow->GetRowId());
     // wzy: INS操作没有现存的row id
     MOTAdaptor::dynamic_hot_rows.visit_row(tmp_rowid);      // wzy: 添加统计
 
 
     if (cc_mode == 1) {             // hybrid 只对热数据项上锁
         // wzy: 交互性事务从OCC切换为2PL，判断事务执行时长（是否为长事务），操作数和开始epoch距离现在的时间
         uint64_t cur_time = now_to_us();
         if (kInteractive_Active && IsInteractive() && is_CC_Switch_enable && ValidateTxnPessimistic(cur_time)) {
             // 乐观单版本则读集检验，检测完切换为2PL，对之后的热数据上锁? 把之前的数据也上锁?
             // TODO：先上读锁再读集检验
 
             if (!isMVCC_Active) {
                 if (is_snap_isolation) {
                     if (!m_occManager.ValidateReadInMergeForSnap(this, local_ip_index)) {
                         return RC_ABORT;
                     }
                 } else if (is_read_repeatable) {
                     if (!m_occManager.ValidateReadInMerge(this, local_ip_index)) {
                         return RC_ABORT;
                     }
                 }
             }
 
             // wzy: 应对update，epoch内的混合型事务row不能作为修改位写进row header
             // wzy: 只对交互型事务的hot row上锁
             if (kHotRow_Active && MOTAdaptor::dynamic_hot_rows.isHotRows(tmp_rowid)) {
                 if (GetCommitSequenceNumber() == 0) SetCommitSequenceNumber(now_to_us());
                 updatedRow->SetRowInteractive(true);
                 rc = MOTAdaptor::SendInteractiveLockInfo(updatedRow);
             } else if(!kHotRow_Active) {
                 // 未启用热行上锁策略则全部行上锁
                 if (GetCommitSequenceNumber() == 0) SetCommitSequenceNumber(now_to_us());
                 updatedRow->SetRowInteractive(true);
                 rc = MOTAdaptor::SendInteractiveLockInfo(updatedRow);
             }
         }
 
         // wzy: 应对update，epoch内的混合型事务row不能作为修改位写进row header
         if (kInteractive_Active && IsInteractive() && !is_CC_Switch_enable) {          // wzy: 只对交互型事务的hot row上锁
             pessimistic_flag = true;
             if (kHotRow_Active && MOTAdaptor::dynamic_hot_rows.isHotRows(tmp_rowid)) {
                 if (GetCommitSequenceNumber() == 0) SetCommitSequenceNumber(now_to_us());
                 updatedRow->SetRowInteractive(true);
                 rc = MOTAdaptor::SendInteractiveLockInfo(updatedRow);
             } else if(!kHotRow_Active) {
                 // 未启用热行上锁策略则全部行上锁
                 if (GetCommitSequenceNumber() == 0) SetCommitSequenceNumber(now_to_us());
                 updatedRow->SetRowInteractive(true);
                 rc = MOTAdaptor::SendInteractiveLockInfo(updatedRow);
             }
         }
     }
     else if (cc_mode == 2) {
         // 单机，Plor 对所有事务写操作上锁
         uint64_t cur_time = now_to_us();
         if (kInteractive_Active && IsInteractive() && is_CC_Switch_enable && ValidateTxnPessimistic(cur_time)) {
             // 乐观单版本则读集检验，检测完切换为2PL，对之后的热数据上锁? 把之前的数据也上锁?
             MOT_LOG_INFO("OCC switching to Plor!!!");
             if (first_time_pessimistic) {
                 // wzy：第一次切换为悲观，先上读锁再读集检验
                 MOTAdaptor::pessimisitic_txn_num.fetch_add(1);
                 MOT_LOG_INFO("First time OCC switching to Plor!!!");
                 first_time_pessimistic = false;
                 rc = ReadLockForSwitch_Plor();          // lock read-set and validation
                 if (rc != MOT::RC_OK) return rc;
 
                 if (kHotRow_Active) rc = WriteLockForSwitchHotRows_Plor();
                 else rc = WriteLockForSwitch_Plor();
                 if (rc != MOT::RC_OK) return rc;
             } else {
                 // wzy: 只对交互型事务的hot row上写锁
                 if (kHotRow_Active && MOTAdaptor::dynamic_hot_rows.isHotRows(tmp_rowid)) {
                     if (GetCommitSequenceNumber() == 0) SetCommitSequenceNumber(now_to_us());
                     updatedRow->SetRowInteractive(true);
                     rc = GetWriteLock_Plor(updatedRow);     // Plor上写锁
                 } else if(!kHotRow_Active) {
                     // 未启用热行上锁策略则全部行上锁
                     if (GetCommitSequenceNumber() == 0) SetCommitSequenceNumber(now_to_us());
                     updatedRow->SetRowInteractive(true);
                     rc = GetWriteLock_Plor(updatedRow);     // Plor上写锁
                 }
             }
         }
     }
     else if (cc_mode == 3) {
         // 单机，2PL, wound-wait
         // MOT_LOG_INFO("OCC check to Wound-wait!!!");
         uint64_t cur_time = now_to_us();
         if (kInteractive_Active && IsInteractive() && is_CC_Switch_enable && ValidateTxnPessimistic(cur_time)) {
             if (abort_) return MOT::RC_ABORT;
             // 乐观单版本则读集检验，检测完切换为2PL，对之后的热数据上锁? 把之前的数据也上锁?
             // MOT_LOG_INFO("OCC switching to Wound-wait!!!");
             if (first_time_pessimistic) {
                 rc = SwitchToPCC();
                 if (rc != MOT::RC_OK) return rc;
             } else {
                 // wzy: 只对交互型事务的hot row上写锁
                 if (kHotRow_Active && MOTAdaptor::dynamic_hot_rows.isHotRows(tmp_rowid)) {
                     if (GetCommitSequenceNumber() == 0) {
                         SetCommitSequenceNumber(now_to_us());
                         InitInteractiveTxn();
                     }
                     updatedRow->SetRowInteractive(true);
                     rc = GetWriteLock_WoundWait(updatedRow);     // Plor上写锁
                 } else if(!kHotRow_Active) {
                     // 未启用热行上锁策略则全部行上锁
                     if (GetCommitSequenceNumber() == 0) {
                         SetCommitSequenceNumber(now_to_us());
                         session_id = u_sess->mot_cxt.session_id;
                         InitInteractiveTxn();
                     }
                     updatedRow->SetRowInteractive(true);
                     rc = GetWriteLock_WoundWait(updatedRow);     // Plor上写锁
                 }
             }
         }
     }
     else if (cc_mode == 4) {
         // 单机，Plor 对所有事务写操作上锁
         uint64_t cur_time = now_to_us();
         if (kInteractive_Active && IsInteractive() && is_CC_Switch_enable && ValidateTxnPessimistic(cur_time)) {
             // 乐观单版本则读集检验，检测完切换为2PL，对之后的热数据上锁? 把之前的数据也上锁?
             if (abort_) return MOT::RC_ABORT;
             if (first_time_pessimistic) {
                 // wzy: 切换
                 rc = SwitchToPCC();
                 if (rc != MOT::RC_OK) return rc;
             } else {
                 // wzy: 只对交互型事务的hot row上写锁
                 if ((kHotRow_Active && !is_hybrid_cc_enable && hot_rowid_records.count(updatedRow->GetRowId()) != 0)
                     || (is_hybrid_cc_enable && ShouldLock(true, updatedRow->GetRowId()))) {
                     if (GetCommitSequenceNumber() == 0) {
                         SetCommitSequenceNumber(now_to_us());
                         InitInteractiveTxn();
                     }
                     updatedRow->SetRowInteractive(true);
                     rc = GetWriteLock_Plor(updatedRow);     // Plor上写锁
                 } else if(!kHotRow_Active && !is_hybrid_cc_enable) {
                     // 未启用热行上锁策略则全部行上锁
                     if (GetCommitSequenceNumber() == 0) {
                         SetCommitSequenceNumber(now_to_us());
                         InitInteractiveTxn();
                     }
                     updatedRow->SetRowInteractive(true);
                     rc = GetWriteLock_Plor(updatedRow);     // Plor上写锁
                 }
             }
         }
     } else if (cc_mode == 5) {
         // 单机，Plor 对所有事务写操作上锁
         uint64_t cur_time = now_to_us();
         if (kInteractive_Active && IsInteractive() && is_CC_Switch_enable && ValidateTxnPessimistic(cur_time)) {
             // 乐观单版本则读集检验，检测完切换为2PL，对之后的热数据上锁? 把之前的数据也上锁?
             if (abort_) return MOT::RC_ABORT;
             if (first_time_pessimistic) {
                 // TODO: 切换
                 rc = SwitchToPCC();
                 if (rc != MOT::RC_OK) return rc;
             } else {
                 // wzy: 只对交互型事务的hot row上写锁
                 if (kHotRow_Active && hot_rowid_records.count(updatedRow->GetRowId()) != 0) {
                     if (GetCommitSequenceNumber() == 0) {
                         SetCommitSequenceNumber(now_to_us());
                         InitInteractiveTxn();
                     }
                     updatedRow->SetRowInteractive(true);
                     rc = GetWriteLock_DL(updatedRow);     // Plor上写锁
                 } else if(!kHotRow_Active) {
                     // 未启用热行上锁策略则全部行上锁
                     if (GetCommitSequenceNumber() == 0) {
                         SetCommitSequenceNumber(now_to_us());
                         InitInteractiveTxn();
                     }
                     updatedRow->SetRowInteractive(true);
                     rc = GetWriteLock_DL(updatedRow);     // Plor上写锁
                 }
             }
         }
     }
 
     return rc;
 }
 
 MOT::RC TxnManager::SwitchToPCC() {
     MOT::RC rc = MOT::RC_OK;
     if (first_time_pessimistic) {
         // wzy：第一次切换为悲观，先上读锁再读集检验
         auto time1 = now_to_us();
         MOTAdaptor::pessimisitic_txn_num.fetch_add(1);
         MOT_LOG_INFO("First time OCC switching to Plor!!! csn : %llu", pre_csn);
         first_time_pessimistic = false;
         if (cc_mode == 4) {
             if (kHotRow_Active)
                 rc = ReadLockForSwitchHotRows_Plor();  // lock read-set and validation
             else
                 rc = ReadLockForSwitch_Plor();
         } else if (cc_mode == 5) {
             if (kHotRow_Active)
                 rc = ReadLockForSwitchHotRows_DL();  // lock read-set and validation
             else
                 rc = ReadLockForSwitch_DL();
         } else if (cc_mode == 3) {
             if (kHotRow_Active)
                 rc = ReadLockForSwitchHotRows_WoundWait();
             else
                 rc = ReadLockForSwitch_WoundWait();
         }
 
         if (rc != MOT::RC_OK) return rc;
 
         // 只对热数据项上锁
         if (cc_mode == 4) {
             if (kHotRow_Active)
                 rc = WriteLockForSwitchHotRows_Plor();
             else
                 rc = WriteLockForSwitch_Plor();
         } else if (cc_mode == 5) {
             if (kHotRow_Active)
                 rc = WriteLockForSwitchHotRows_DL();
             else
                 rc = WriteLockForSwitch_DL();
         } else if (cc_mode == 3) {
             if (kHotRow_Active)
                 rc = WriteLockForSwitchHotRows_WoundWait();
             else
                 rc = WriteLockForSwitch_WoundWait();
         }
 
         auto time2 = now_to_us();
 
         // 统计切换耗时
         MOTAdaptor::txn_total_switchTime.fetch_add(time2 - time1);
         MOTAdaptor::txn_total_switchCnt.fetch_add(1);
 
         MOTAdaptor::txn_temp_total_switchTime.fetch_add(time2 - time1);
         MOTAdaptor::txn_temp_total_switchCnt.fetch_add(1);
 
         if (rc != MOT::RC_OK)
             return rc;
     }
     return rc;
 }
 
 void TxnManager::SetTxnState(TxnState envelopeState)
 {
     m_state = envelopeState;
 }
 
 void TxnManager::SetTxnIsoLevel(int envelopeIsoLevel)
 {
     m_isolationLevel = envelopeIsoLevel;
 }
 
 void TxnManager::GcSessionRecordRcu(
     uint32_t index_id, void* object_ptr, void* object_pool, DestroyValueCbFunc cb, uint32_t obj_size)
 {
     return m_gcSession->GcRecordObject(index_id, object_ptr, object_pool, cb, obj_size);
 }
 
 TxnInsertAction::~TxnInsertAction()
 {
     if (m_manager != nullptr && m_insertSet != nullptr) {
         MemSessionFree(m_insertSet);
     }
 }
 
 bool TxnInsertAction::Init(TxnManager* _manager)
 {
     bool rc = true;
     if (this->m_manager)
         return false;
 
     this->m_manager = _manager;
     m_insertSetSize = 0;
     m_insertArraySize = INSERT_ARRAY_DEFAULT_SIZE;
 
     void* ptr = MemSessionAlloc(sizeof(InsItem) * m_insertArraySize);
     if (ptr == nullptr)
         return false;
     m_insertSet = reinterpret_cast<InsItem*>(ptr);
     for (uint64_t item = 0; item < m_insertArraySize; item++) {
         new (&m_insertSet[item]) InsItem();
     }
     return rc;
 }
 
 void TxnInsertAction::ReportError(RC rc, InsItem* currentItem)
 {
 
     switch (rc) {
         case RC_OK:
             break;
         case RC_UNIQUE_VIOLATION:
             // set error
             m_manager->m_err = RC_UNIQUE_VIOLATION;
             m_manager->m_errIx = currentItem->m_index;
             m_manager->m_errIx->BuildErrorMsg(currentItem->m_row->GetTable(),
                 currentItem->m_row,
                 m_manager->m_errMsgBuf,
                 sizeof(m_manager->m_errMsgBuf));
             break;
         case RC_MEMORY_ALLOCATION_ERROR:
             SetLastError(MOT_ERROR_OOM, MOT_SEVERITY_ERROR);
             break;
         case RC_ILLEGAL_ROW_STATE:
             SetLastError(MOT_ERROR_INTERNAL, MOT_SEVERITY_ERROR);
             break;
         default:
             SetLastError(MOT_ERROR_INTERNAL, MOT_SEVERITY_ERROR);
             break;
     }
 }
 
 void TxnInsertAction::CleanupOptimisticInsert(
     InsItem* currentItem, Sentinel* pIndexInsertResult, bool isInserted, bool isMappedToCache)
 {
     // Clean current aborted row and clean secondary indexes that were not inserts
     // Clean first Object! - wither primary or secondary!
     // Return Local Row to pull for PI
     Table* table = currentItem->m_row->GetTable();
     if (currentItem->getIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
         table->DestroyRow(currentItem->m_row);
     }
     if (isInserted == true) {
         if (isMappedToCache == false) {
             RC rc = pIndexInsertResult->RefCountUpdate(DEC, m_manager->GetThdId());
             MOT::Index* index_ = pIndexInsertResult->GetIndex();
             if (rc == RC::RC_INDEX_DELETE) {
                 // Memory reclamation need to release the key from the primary sentinel back to the pool
                 MOT_ASSERT(pIndexInsertResult->GetCounter() == 0);
                 Sentinel* outputSen = index_->IndexRemove(currentItem->m_key, m_manager->GetThdId());
                 MOT_ASSERT(outputSen != nullptr);
                 m_manager->GcSessionRecordRcu(
                     index_->GetIndexId(), outputSen, nullptr, Index::SentinelDtor, SENTINEL_SIZE(index_));
                 m_manager->m_accessMgr->IncreaseTableStat(table);
             }
         }
     }
 }
 
 RC TxnInsertAction::ExecuteOptimisticInsert(Row* row)
 {
     Sentinel* pIndexInsertResult = nullptr;
     Row* accessRow = nullptr;
     RC rc = RC_OK;
     bool isInserted = true;
     bool isMappedToCache = false;
 
     auto currentItem = BeginCursor();
 
     /*
      * 1.Add all sentinels to the Access SET type P_SENTINEL or S_SENTINEL
      * 2.IF Sentinel is committed abort!
      * 3.We perform lookup directly on sentinels
      * 4.We do not attach the row to the index,we map it to the access
      * 5.We can release the row in the case of early abort only for Primary
      * 6.No need to copy the row to the local_access
      */
     while (currentItem != EndCursor()) {
         isInserted = true;
         isMappedToCache = false;
         bool res = reinterpret_cast<MOT::Index*>(currentItem->m_index)
                        ->IndexInsert(pIndexInsertResult, currentItem->m_key, m_manager->GetThdId(), rc);
         if (unlikely(rc == RC_MEMORY_ALLOCATION_ERROR)) {
             ReportError(rc);
             // Failed on Memory
             isInserted = false;
             goto end;
         } else {
             if (currentItem->getIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
                 row->SetAbsentRow();
                 row->SetPrimarySentinel(pIndexInsertResult);
                 MOT_ASSERT(row->IsAbsentRow());
             }
         }
         if (pIndexInsertResult->IsCommited() == true) {
             // Lets check and see if we deleted the row
             Row* local_row = nullptr;
             rc = m_manager->AccessLookup(RD, pIndexInsertResult, local_row);
             switch (rc) {
                 case RC_LOCAL_ROW_DELETED:
                     // promote delete to Insert!
                     // In this case the sentinel is committed and the previous scenario was delete
                     // Insert succeeded Sentinel was not committed before!
                     // At this point the row is not in the cache and can be mapped!
                     MOT_ASSERT(currentItem->m_index->GetUnique() == true);
                     accessRow = m_manager->m_accessMgr->AddInsertToLocalAccess(pIndexInsertResult, row, rc, true);
                     if (accessRow != nullptr) {
                         isMappedToCache = true;
                     }
                     ReportError(rc, currentItem);
                     break;
                 case RC_LOCAL_ROW_NOT_FOUND:
                     // Header is committed
                 case RC_LOCAL_ROW_FOUND:
                     // Found But not deleted = self duplicated!
                     ReportError(RC_UNIQUE_VIOLATION, currentItem);
                     rc = RC_UNIQUE_VIOLATION;
                     goto end;
                 default:
                     goto end;
             }
         } else if (res == true or pIndexInsertResult->IsCommited() == false) {
             // tag all the sentinels the insert metadata
             MOT_ASSERT(pIndexInsertResult->GetCounter() != 0);
             // Reuse the row connected to header
             if (unlikely(pIndexInsertResult->GetData() != nullptr)) {
                 if (pIndexInsertResult->IsCommited() == false) {
                     accessRow = m_manager->m_accessMgr->AddInsertToLocalAccess(pIndexInsertResult, row, rc, true);
                 }
             } else {
                 // Insert succeeded Sentinel was not committed before!
                 accessRow = m_manager->m_accessMgr->AddInsertToLocalAccess(pIndexInsertResult, row, rc);
             }
             if (accessRow == nullptr) {
                 ReportError(rc, currentItem);
                 goto end;
             }
             isMappedToCache = true;
         }
         ++currentItem;
     }
 
 end:
     if ((rc != RC_OK) && (currentItem != EndCursor())) {
         CleanupOptimisticInsert(currentItem, pIndexInsertResult, isInserted, isMappedToCache);
     }
 
     // Clean keys
     currentItem = BeginCursor();
     while (currentItem < EndCursor()) {
         m_manager->DestroyTxnKey(currentItem->m_key);
         currentItem++;
     }
 
     // Clear the current set size;
     m_insertSetSize = 0;
     return rc;
 }
 
 bool TxnInsertAction::ReallocInsertSet()
 {
     bool rc = true;
     uint64_t new_array_size = (uint64_t)m_insertArraySize * INSERT_ARRAY_EXTEND_FACTOR;
     void* ptr = MemSessionAlloc(sizeof(InsItem) * new_array_size);
     if (__builtin_expect(ptr == nullptr, 0)) {
         MOT_LOG_ERROR("%s: failed", __func__);
         MOT_ASSERT(ptr != nullptr);
         return false;
     }
     errno_t erc = memset_s(ptr, sizeof(InsItem) * new_array_size, 0, sizeof(InsItem) * new_array_size);
     securec_check(erc, "\0", "\0");
     erc = memcpy_s(ptr, sizeof(InsItem) * new_array_size, m_insertSet, sizeof(InsItem) * m_insertArraySize);
     securec_check(erc, "\0", "\0");
     MemSessionFree(m_insertSet);
     m_insertSet = reinterpret_cast<InsItem*>(ptr);
     m_insertArraySize = new_array_size;
     return rc;
 }
 
 void TxnInsertAction::ShrinkInsertSet()
 {
     uint64_t new_array_size = INSERT_ARRAY_DEFAULT_SIZE;
     void* ptr = MemSessionAlloc(sizeof(InsItem) * new_array_size);
     if (__builtin_expect(ptr == nullptr, 0)) {
         MOT_LOG_ERROR("%s: failed", __func__);
         return;
     }
     errno_t erc = memcpy_s(ptr, sizeof(InsItem) * new_array_size, m_insertSet, sizeof(InsItem) * new_array_size);
     securec_check(erc, "\0", "\0");
     MemSessionFree(m_insertSet);
     m_insertSet = reinterpret_cast<InsItem*>(ptr);
     m_insertArraySize = new_array_size;
 }
 
 /******************** DDL SUPPORT ********************/
 Table* TxnManager::GetTableByExternalId(uint64_t id)
 {
     TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->GetByOid(id);
     if (ddl_access != nullptr) {
         switch (ddl_access->GetDDLAccessType()) {
             case DDL_ACCESS_CREATE_TABLE:
                 return (Table*)ddl_access->GetEntry();
             case DDL_ACCESS_TRUNCATE_TABLE:
                 return ((MOTIndexArr*)ddl_access->GetEntry())->GetTable();
             case DDL_ACCESS_DROP_TABLE:
                 return nullptr;
             default:
                 break;
         }
     }
 
     return GetTableManager()->GetTableByExternal(id);
 }
 
 Index* TxnManager::GetIndexByExternalId(uint64_t table_id, uint64_t index_id)
 {
     Table* table = GetTableByExternalId(table_id);
     if (table == nullptr) {
         return nullptr;
     } else {
         return table->GetIndexByExtId(index_id);
     }
 }
 
 RC TxnManager::CreateTable(Table* table)
 {
     size_t serializeRedoSize = table->SerializeRedoSize();
     if (serializeRedoSize > RedoLogWriter::REDO_MAX_TABLE_SERIALIZE_SIZE) {
         MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
             "Create Table",
             "Table Serialize size %zu exceeds the maximum allowed limit %u",
             serializeRedoSize,
             RedoLogWriter::REDO_MAX_TABLE_SERIALIZE_SIZE);
         return RC_ERROR;
     }
 
     TxnDDLAccess::DDLAccess* ddl_access =
         new (std::nothrow) TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_CREATE_TABLE, (void*)table);
     if (ddl_access == nullptr) {
         MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Table", "Failed to allocate memory for DDL Access object");
         return RC_MEMORY_ALLOCATION_ERROR;
     }
 
     m_txnDdlAccess->Add(ddl_access);
     return RC_OK;
 }
 
 RC TxnManager::DropTable(Table* table)
 {
     RC res = RC_OK;
 
     // we allocate all memory before action takes place, so that if memory allocation fails, we can report error safely
     TxnDDLAccess::DDLAccess* new_ddl_access =
         new (std::nothrow) TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_DROP_TABLE, (void*)table);
     if (new_ddl_access == nullptr) {
         MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Table", "Failed to allocate DDL Access object");
         return RC_MEMORY_ALLOCATION_ERROR;
     }
 
     if (!m_isLightSession) {
         TxnOrderedSet_t& access_row_set = m_accessMgr->GetOrderedRowSet();
         TxnOrderedSet_t::iterator it = access_row_set.begin();
         while (it != access_row_set.end()) {
             Access* ac = it->second;
             if (ac->GetTxnRow()->GetTable() == table) {
                 if (ac->m_type == INS)
                     RollbackInsert(ac);
                 it = access_row_set.erase(it);
                 // need to perform index clean-up!
                 m_accessMgr->PubReleaseAccess(ac);
             } else {
                 it++;
             }
         }
     }
 
     m_txnDdlAccess->Add(new_ddl_access);
     return RC_OK;
 }
 
 RC TxnManager::TruncateTable(Table* table)
 {
     RC res = RC_OK;
     if (m_isLightSession)  // really?
         return res;
 
     TxnOrderedSet_t& access_row_set = m_accessMgr->GetOrderedRowSet();
     TxnOrderedSet_t::iterator it = access_row_set.begin();
     while (it != access_row_set.end()) {
         Access* ac = it->second;
         if (ac->GetTxnRow()->GetTable() == table) {
             if (ac->m_type == INS)
                 RollbackInsert(ac);
             it = access_row_set.erase(it);
             // need to perform index clean-up!
             m_accessMgr->PubReleaseAccess(ac);
         } else {
             it++;
         }
     }
 
     // clean all GC elements for the table, this call should actually release
     // all elements into an appropriate object pool
     for (uint16_t i = 0; i < table->GetNumIndexes(); i++) {
         MOT::Index* index = table->GetIndex(i);
         GcManager::ClearIndexElements(index->GetIndexId(), false);
     }
 
     TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->GetByOid(table->GetTableExId());
     if (ddl_access == nullptr) {
         MOTIndexArr* indexesArr = nullptr;
         indexesArr = new (std::nothrow) MOTIndexArr(table);
         if (indexesArr == nullptr) {
             // print error, could not allocate memory
             MOT_REPORT_ERROR(MOT_ERROR_OOM,
                 "Truncate Table",
                 "Failed to allocate memory for %u index objects",
                 (unsigned)MAX_NUM_INDEXES);
             return RC_MEMORY_ALLOCATION_ERROR;
         }
         // allocate DDL before work and fail immediately if required
         ddl_access = new (std::nothrow)
             TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_TRUNCATE_TABLE, (void*)indexesArr);
         if (ddl_access == nullptr) {
             MOT_REPORT_ERROR(MOT_ERROR_OOM, "Truncate Table", "Failed to allocate memory for DDL Access object");
             delete indexesArr;
             return RC_MEMORY_ALLOCATION_ERROR;
         }
 
         if (!table->InitRowPool()) {
             table->m_rowPool = indexesArr->GetRowPool();
             delete indexesArr;
             delete ddl_access;
             return RC_MEMORY_ALLOCATION_ERROR;
         }
 
         for (uint16_t i = 0; i < table->GetNumIndexes(); i++) {
             MOT::Index* index = table->GetIndex(i);
             MOT::Index* index_copy = index->CloneEmpty();
             if (index_copy == nullptr) {
                 // print error, could not allocate memory for index
                 MOT_REPORT_ERROR(
                     MOT_ERROR_OOM, "Truncate Table", "Failed to clone empty index %s", index->GetName().c_str());
                 for (uint16_t j = 0; j < indexesArr->GetNumIndexes(); j++) {
                     // cleanup of previous created indexes copy
                     MOT::Index* oldIndex = indexesArr->GetIndex(j);
                     uint16_t oldIx = indexesArr->GetIndexIx(j);
                     MOT::Index* newIndex = table->m_indexes[oldIx];
                     table->m_indexes[oldIx] = oldIndex;
                     if (oldIx != 0)  // is secondary
                         table->m_secondaryIndexes[oldIndex->GetName()] = oldIndex;
                     else  // is primary
                         table->m_primaryIndex = oldIndex;
                     delete newIndex;
                 }
                 delete ddl_access;
                 delete indexesArr;
                 return RC_MEMORY_ALLOCATION_ERROR;
             }
             indexesArr->Add(i, index);
             table->m_indexes[i] = index_copy;
             if (i != 0)  // is secondary
                 table->m_secondaryIndexes[index_copy->GetName()] = index_copy;
             else  // is primary
                 table->m_primaryIndex = index_copy;
         }
         m_txnDdlAccess->Add(ddl_access);
     }
 
     return res;
 }
 
 RC TxnManager::CreateIndex(Table* table, MOT::Index* index, bool is_primary)
 {
     size_t serializeRedoSize = table->SerializeItemSize(index);
     if (serializeRedoSize > RedoLogWriter::REDO_MAX_INDEX_SERIALIZE_SIZE) {
         MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
             "Create Index",
             "Index Serialize size %zu exceeds the maximum allowed limit %u",
             serializeRedoSize,
             RedoLogWriter::REDO_MAX_INDEX_SERIALIZE_SIZE);
         return RC_ERROR;
     }
 
     // allocate DDL before work and fail immediately if required
     TxnDDLAccess::DDLAccess* ddl_access =
         new (std::nothrow) TxnDDLAccess::DDLAccess(index->GetExtId(), DDL_ACCESS_CREATE_INDEX, (void*)index);
     if (ddl_access == nullptr) {
         MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Index", "Failed to allocate DDL Access object");
         return RC_MEMORY_ALLOCATION_ERROR;
     }
 
     table->WrLock();  // for concurrent access
     if (is_primary) {
         if (!table->UpdatePrimaryIndex(index, this, m_threadId)) {
             table->Unlock();
             if (MOT_IS_OOM()) {  // do not report error in "unique violation" scenario
                 MOT_REPORT_ERROR(
                     MOT_ERROR_INTERNAL, "Create Index", "Failed to add primary index %s", index->GetName().c_str());
             }
             delete ddl_access;
             return m_err;
         }
     } else {
         // currently we are still adding the index to the table although
         // is should only be added on successful commit. Assuming that if
         // a client did a create index, all other clients are waiting on a lock
         // until the changes are either commited or aborted
         if (table->GetNumIndexes() == MAX_NUM_INDEXES) {
             table->Unlock();
             MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
                 "Create Index",
                 "Cannot create index in table %s: reached limit of %u indices per table",
                 table->GetLongTableName().c_str(),
                 (unsigned)MAX_NUM_INDEXES);
             delete ddl_access;
             return RC_TABLE_EXCEEDS_MAX_INDEXES;
         }
 
         if (!table->AddSecondaryIndex(index->GetName(), index, this, m_threadId)) {
             table->Unlock();
             if (MOT_IS_OOM()) {  // do not report error in "unique violation" scenario
                 MOT_REPORT_ERROR(
                     MOT_ERROR_INTERNAL, "Create Index", "Failed to add secondary index %s", index->GetName().c_str());
             }
             delete ddl_access;
             return m_err;
         }
     }
     table->Unlock();
     m_txnDdlAccess->Add(ddl_access);
     return RC_OK;
 }
 
 RC TxnManager::DropIndex(MOT::Index* index)
 {
     // allocate DDL before work and fail immediately if required
     TxnDDLAccess::DDLAccess* new_ddl_access =
         new (std::nothrow) TxnDDLAccess::DDLAccess(index->GetExtId(), DDL_ACCESS_DROP_INDEX, (void*)index);
     if (new_ddl_access == nullptr) {
         MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Index", "Failed to allocate DDL Access object");
         return RC_MEMORY_ALLOCATION_ERROR;
     }
 
     RC res = RC_OK;
     Table* table = index->GetTable();
 
     if (!m_isLightSession && !index->IsPrimaryKey()) {
         TxnOrderedSet_t& access_row_set = m_accessMgr->GetOrderedRowSet();
         TxnOrderedSet_t::iterator it = access_row_set.begin();
         while (it != access_row_set.end()) {
             Access* ac = it->second;
             if (ac->GetSentinel()->GetIndex() == index) {
                 if (ac->m_type == INS)
                     RollbackInsert(ac);
                 it = access_row_set.erase(it);
                 // need to perform index clean-up!
                 m_accessMgr->PubReleaseAccess(ac);
             } else {
                 it++;
             }
         }
     }
 
     table->RemoveSecondaryIndexFromMetaData(index);
     m_txnDdlAccess->Add(new_ddl_access);
     return res;
 }
 
 
 
 
 
 
 
 
 
 
 //ADDBY NEU
 bool TxnManager::localMergeValidate(uint64_t csn)
 {
     TxnOrderedSet_t& orderedSet = this->m_accessMgr->GetOrderedRowSet();
     for (const auto& raPair : orderedSet) {
         const Access* ac = raPair.second;
         if (ac->m_type == RD or !ac->m_params.IsPrimarySentinel()) {
             continue;
         }
 
         if (ac->GetRowFromHeader()->GetCommitSequenceNumber()!=csn) {
             return false;
         }
     }
     return true;
 }
 bool TxnManager::isOnlyRead()
 {
     return m_occManager.IsReadOnly(this);
     TxnOrderedSet_t &orderedSet = this->m_accessMgr->GetOrderedRowSet();
     if (orderedSet.size() == 0)
         return false;
     for (const auto &raPair : orderedSet)
     {
         const Access *ac = raPair.second;
         if (ac->m_type != RD)
         {
             return false;
         }
     }
     return true;
 }
 
 
 
 void TxnManager::CommitInternalII()
 {
     // first write to redo log, then write changes
     //不写log
     m_redoLog.Commit();
     m_occManager.WriteChanges(this, 0);
     
     if (GetGlobalConfiguration().m_enableCheckpoint) {
         GetCheckpointManager()->EndCommit(this);
     }
 
     if (!GetGlobalConfiguration().m_enableRedoLog) {
         m_occManager.ReleaseLocks(this);
     }
     
     if(!m_occManager.IsReadOnly(this)){
         auto epoch_mod = GetCommitEpoch() % MOTAdaptor::max_length;
         MOTAdaptor::IncRecordCommittedTxnCounters(epoch_mod, GetIndexPack());
         // MOTAdaptor::Commit(this, GetIndexPack());
         while(!MOTAdaptor::IsRemoteRecordCommitted()) usleep(200);
     }
     // ClearEpochState();
 }
 
 void TxnManager::CommitForRemote(uint64_t server_id)
 {
     m_occManager.updateInsertSetSize(this);
     // MOT_LOG_INFO("m_occManager.IsReadOnly(this) %u", m_occManager.IsReadOnly(this));
     // 对远端事务的update无法进行统计 未添加进入txnmanager中
     // CommitInternalII();
 //    m_redoLog.Commit();           // wzy: 可以不写redolog
     m_occManager.WriteChanges(this, server_id);
 
     if (GetGlobalConfiguration().m_enableCheckpoint) {
         GetCheckpointManager()->EndCommit(this);
     }
 
     if (!GetGlobalConfiguration().m_enableRedoLog) {
         m_occManager.ReleaseLocks(this);
     }
     MOT::DbSessionStatisticsProvider::GetInstance().AddCommitTxn();
 }
 
 //txn.cpp
 /**
  * @brief Commit
  * 改动：改为先写胜利的情况下，不需要precommit阶段，只保留两个计数器
  * 新增 while(MOTAdaptor::GetPhysicalEpoch() != MOTAdaptor::GetLogicalEpoch()) ; 
  * 只有当logical epoch和 physical epoch 相差1时才能继续接收新事务
  * while(!MOTAdaptor::isRemoteExeced()|| !MOTAdaptor::isMergeSent()); 改为while(!MOTAdaptor::isRemoteExeced());
  * 发送线程无影响
  * */
 std::atomic<int> wait_count(0);
 std::atomic<int> wait_count_commit(0);
 RC TxnManager::Commit(){
     if(is_raft_enable == 1 && local_ip_index == kRaftStopServerId && kRaftStopEpoch > 0 && MOTAdaptor::GetPhysicalEpoch() > kRaftStopEpoch) {
         while(MOTAdaptor::GetPhysicalEpoch() < kRaftRestrtEpoch) usleep(kSleepTime);
     }
     
     m_occManager.updateInsertSetSize(this);
     bool result = m_occManager.IsReadOnly(this);
     uint64_t index_pack = GetIndexPack();
 
     // wzy: recovery debug
     if (!MOTAdaptor::isInited.load()) return RC_OK;
     if (kNotifyNum <= 0 || MOTAdaptor::max_length <= 0) kNotifyNum = 1;
 
     uint64_t index_notify = index_pack % kNotifyNum;
     uint64_t index_unique =  MOTAdaptor::IncLocalTxnIndex(GetStartEpoch() % MOTAdaptor::max_length, index_pack);
     uint64_t cnt = 0;
     RC rc = RC_OK;
     if(kDelayRatio > 0) {
         std::default_random_engine random;
         random.seed(time(0));
         if(random() % 100 < kDelayRatio)
             usleep(kDelayTime);
     }
     SetStartMOTCommitTime(now_to_us());
 
     if(is_full_async_exec) {
         if (this->m_accessMgr->m_rowCnt > 0 && !result){
             (*MOTAdaptor::write_total_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             if(is_snap_isolation){
                 if(!m_occManager.ValidateReadInMergeForSnap(this, local_ip_index)){
                     (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
                     MOT_LOG_INFO("write_abort_before_send_txn_num error RC_ABORT");
                     if (IsInteractive()) MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num.fetch_add(1);
                     return RC_ABORT;
                 }
             }
             else if(is_read_repeatable){
                 if(!m_occManager.ValidateReadInMerge(this, local_ip_index)){
                     (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
                     MOT_LOG_INFO("write_abort_before_send_txn_num error RC_ABORT");
                     if (IsInteractive()) MOTAdaptor::ValidateReadInMerge_abort_interactive_num.fetch_add(1);
                     return RC_ABORT;
                 }
             }
 //            if(!MOTAdaptor::InsertTxntoLocalChangeSet(this, index_pack, index_unique)){// 当生成txn失败时abort，为send_before_abort，enqueue必定成功，否则assert
 //                    (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
 //                    MOT_LOG_INFO("write_abort_before_send_txn_num error RC_ABORT");
 //                    return RC_ABORT;
 //            }
             if(!MOTAdaptor::InsertTxntoLocalChangeSet2(this, index_pack, index_unique)){// 当生成txn失败时abort，为send_before_abort，enqueue必定成功，否则assert
                 (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
                 MOT_LOG_INFO("write_abort_before_send_txn_num error RC_ABORT");
                 if (IsInteractive()) MOTAdaptor::InsertTxntoLocalChangeSet_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;
             }
 
             auto time1 = now_to_us();
             auto epoch_mod = GetCommitEpoch() % MOTAdaptor::max_length;
             m_occManager.CommitPhase(this, local_ip_index);
             
             if(rc == RC_OK){
                 MOTAdaptor::IncRecordCommitTxnCounters(epoch_mod, index_pack);
                 (*MOTAdaptor::write_committed_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
                 if(is_breakdown) {
                     auto time2 = now_to_us();
                     MOT_LOG_INFO("full async 事务提交 读写 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                         GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), ((GetStartMOTCommitTime() - GetStartMOTExecTime()) - GetBlockTime()), time2 - GetStartMOTCommitTime(), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());//sync
                 }
             }
             return RC_OK;
         }
         else{
             (*MOTAdaptor::read_total_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             auto time1 = now_to_us();
             if(is_snap_isolation){
                 if(!m_occManager.ValidateReadInMergeForSnap(this, local_ip_index)){
                     (*MOTAdaptor::read_abort_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
                     MOT_LOG_INFO("read_abort_txn_num error RC_ABORT");
                     if (IsInteractive()) MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num.fetch_add(1);
                     return RC_ABORT;
                 }
             }
             else if(is_read_repeatable){
                 if(!m_occManager.ValidateReadInMerge(this, local_ip_index)){
                     (*MOTAdaptor::read_abort_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
                     MOT_LOG_INFO("read_abort_txn_num error RC_ABORT");
                     if (IsInteractive()) MOTAdaptor::ValidateReadInMerge_abort_interactive_num.fetch_add(1);
                     return RC_ABORT;
                 }
             }
             (*MOTAdaptor::read_committed_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             if(is_breakdown) {
                 auto time2 = now_to_us();
                 MOT_LOG_INFO("full async 事务提交 只读 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                         GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), ((GetStartMOTCommitTime() - GetStartMOTExecTime()) - GetBlockTime()), time2 - GetStartMOTCommitTime(), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());//sync
             }
             return RC_OK;
         }
     }
 
     if(is_sync_exec) {
         if (this->m_accessMgr->m_rowCnt > 0 && !result){
             // MOT_LOG_INFO("epoch %llu commit() write", GetStartEpoch());
             (*MOTAdaptor::write_total_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             if(is_snap_isolation){
                 if(!m_occManager.ValidateReadInMergeForSnap(this, local_ip_index)){
                     (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
 //                    MOT_LOG_INFO("write_abort_before_send_txn_num error RC_ABORT");
                     if (IsInteractive()) MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num.fetch_add(1);
                     return RC_ABORT;
                 }
             }
             else if(is_read_repeatable){
                 if(!m_occManager.ValidateReadInMerge(this, local_ip_index)){
                     (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
 //                    MOT_LOG_INFO("write_abort_before_send_txn_num error RC_ABORT");
                     if (IsInteractive()) MOTAdaptor::ValidateReadInMerge_abort_interactive_num.fetch_add(1);
                     return RC_ABORT;
                 }
             }
 //            if(!MOTAdaptor::InsertTxntoLocalChangeSet(this, index_pack, index_unique)){// 当生成txn失败时abort，为send_before_abort，enqueue必定成功，否则assert
 //                    (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
 //                    MOT_LOG_INFO("write_abort_before_send_txn_num error RC_ABORT");
 //                    return RC_ABORT;
 //            }
             if(!MOTAdaptor::InsertTxntoLocalChangeSet2(this, index_pack, index_unique)){// 当生成txn失败时abort，为send_before_abort，enqueue必定成功，否则assert
                 (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
 //                MOT_LOG_INFO("write_abort_before_send_txn_num error RC_ABORT");
                 if (IsInteractive()) MOTAdaptor::InsertTxntoLocalChangeSet_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;
             }
 
             auto time1 = now_to_us();
             cnt = 0;
             while(GetCommitEpoch() > MOTAdaptor::GetLogicalEpoch() || !MOTAdaptor::IsRecordCommitted()){
                 usleep(200);
             }
             auto epoch_mod = GetCommitEpoch() % MOTAdaptor::max_length;
             MOTAdaptor::IncLocalTxnCounters(epoch_mod, index_pack);
             MOTAdaptor::IncLocalTxnExcCounters(epoch_mod, index_pack);
             rc = m_occManager.CommitPhase(this, local_ip_index);
             MOTAdaptor::IncLocalExecedCounters(epoch_mod, index_pack);
 
             if (rc == RC_ABORT){
 //                MOT_LOG_INFO("CommitPhase error RC_ABORT");
                 if (IsInteractive()) MOTAdaptor::CommitPhase_abort_interactive_num.fetch_add(1);
                 MOTAdaptor::CommitPhase_abort_num.fetch_add(1);
             }
 
             if (rc != RC_ABORT){    
                 while(!MOTAdaptor::IsRemoteExeced()) {
                     usleep(200);
                 }
                 auto csn_temp = std::to_string(GetCommitSequenceNumber()) + ":" + std::to_string(local_ip_index);
                 if(MOTAdaptor::abort_transcation_csn_set.contain(csn_temp, csn_temp)){
                     if (IsInteractive()) {
                         MOTAdaptor::Abort_transcation_csn_set_abort_interactive_num.fetch_add(1);
 //                        MOT_LOG_INFO("[Abort] abort_transcation_csn_set() local failed tmp_csn : %s ", csn_temp.c_str());
                     }
                     MOTAdaptor::abort_transcation_csn_set.remove(csn_temp);
                     rc = RC_ABORT;
                 }
                 // wzy:
                 if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
                     if (IsInteractive()) MOTAdaptor::CommitCheck_deadlock_abort_interactive_num.fetch_add(1);
                     rc = RC_ABORT;     // 被死锁检测abort
                 }
             }
             
             if(rc == RC_OK){
                 MOTAdaptor::IncRecordCommitTxnCounters(epoch_mod, index_pack);
                 (*MOTAdaptor::write_committed_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
                 if(is_breakdown) {
                     auto time2 = now_to_us();
                     MOT_LOG_INFO("sync 事务提交 读写 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                         GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), ((GetStartMOTCommitTime() - GetStartMOTExecTime()) - GetBlockTime()), time2 - GetStartMOTCommitTime(), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());//sync
                 }
             }
             else {
                 (*MOTAdaptor::write_abort_after_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             }
 
             MOTAdaptor::IncLocalCommittedCounters(epoch_mod, index_pack);
             return rc;
         }
         else{
             (*MOTAdaptor::read_total_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             auto time1 = now_to_us();
             if(is_snap_isolation){
                 if(!m_occManager.ValidateReadInMergeForSnap(this, local_ip_index)){
                     (*MOTAdaptor::read_abort_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
 //                    MOT_LOG_INFO("read_abort_txn_num error RC_ABORT");
                     if (IsInteractive()) MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num.fetch_add(1);
                     return RC_ABORT;
                 }
             }
             else if(is_read_repeatable){
                 if(!m_occManager.ValidateReadInMerge(this, local_ip_index)){
                     (*MOTAdaptor::read_abort_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
 //                    MOT_LOG_INFO("read_abort_txn_num error RC_ABORT");
                     if (IsInteractive()) MOTAdaptor::ValidateReadInMerge_abort_interactive_num.fetch_add(1);
                     return RC_ABORT;
                 }
             }
             (*MOTAdaptor::read_committed_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             if(is_breakdown) {
                 auto time2 = now_to_us();
                 MOT_LOG_INFO("sync 事务提交 只读 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                         GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), ((GetStartMOTCommitTime() - GetStartMOTExecTime()) - GetBlockTime()), time2 - GetStartMOTCommitTime(), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());//sync
             }
             return RC_OK;
         }
     }
     else {
         if (this->m_accessMgr->m_rowCnt > 0 && !result){
 
             if (!isMVCC_Active) {        // 乐观单版本则读集检验
                  if(is_snap_isolation){
                      if(!m_occManager.ValidateReadInMergeForSnap(this, local_ip_index)){
                          MOT_LOG_INFO("ValidateReadInMergeForSnap error RC_ABORT");
                          if (IsInteractive()) MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num.fetch_add(1);
                          return RC_ABORT;
                      }
                  }
                  else if(is_read_repeatable){
                      if(!m_occManager.ValidateReadInMerge(this, local_ip_index)){
                          MOT_LOG_INFO("ValidateReadInMerge error RC_ABORT");
                          if (IsInteractive()) MOTAdaptor::ValidateReadInMerge_abort_interactive_num.fetch_add(1);
                          return RC_ABORT;
                      }
                  }
             } else {
                 // wzy MVCC需要检测写集是否被修改?
                 // do nothing
             }
 
 
 //            if(!MOTAdaptor::InsertTxntoLocalChangeSet(this, index_pack, index_unique)){
 //                (*MOTAdaptor::write_abort_before_send_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
 //                MOT_LOG_INFO("write_abort_before_send_txn_num error RC_ABORT");
 //                return RC_ABORT;
 //            }
 
             // wzy: 检查事务是否被死锁检测abort
             auto csn_temp = std::to_string(GetCommitSequenceNumber()) + ":" + std::to_string(local_ip_index);
             if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
                 if (IsInteractive()) MOTAdaptor::CommitCheck_deadlock_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;     // 被死锁检测abort，已经被自动解锁
             }
 
             // wzy: 对非交互型事务检查写集是否被上锁
             if (!IsInteractive() && kPreLockCheck_Active) {
                 if (RC_ABORT == m_occManager.CommitLockCheck(this, local_ip_index)) return RC_ABORT;
             }
 
             if(!MOTAdaptor::InsertTxntoLocalChangeSet2(this, index_pack, index_unique)){
                 (*MOTAdaptor::write_abort_before_send_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
 //                MOT_LOG_INFO("write_abort_before_send_txn_num error RC_ABORT");
                 if (IsInteractive()) MOTAdaptor::InsertTxntoLocalChangeSet_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;
             }
 
             auto time1 = now_to_us();
             cnt = 0;
             auto epoch_mod = GetCommitEpoch() % MOTAdaptor::max_length;
             MOTAdaptor::IncLocalTxnCounters(epoch_mod, index_pack);
             MOTAdaptor::IncLocalTxnExcCounters(epoch_mod, index_pack);
 
             // 等待上一個epoch結束
             while(GetCommitEpoch() > MOTAdaptor::GetLogicalEpoch() || !MOTAdaptor::IsRecordCommitted()){
                 usleep(200);
             }
 //            auto epoch_mod = GetCommitEpoch() % MOTAdaptor::max_length;
             auto time3 = now_to_us();
             this->SetBlockTime(time3 - time1);
 //            MOTAdaptor::IncLocalTxnCounters(epoch_mod, index_pack);
 //            MOTAdaptor::IncLocalTxnExcCounters(epoch_mod, index_pack);
 
             // rc = m_occManager.ExecutionPhase(this, local_ip_index);
             rc = m_occManager.CommitPhase(this, local_ip_index);
             MOTAdaptor::IncLocalExecedCounters(epoch_mod, index_pack);
 
             if (IsInteractive() && pessimistic_flag) {
                 // wzy: 在epoch commit check前解锁/abort
                 session_id = u_sess->mot_cxt.session_id;
                 pre_csn = ((start_time & HIGH_MASK) << 16) | (session_id & 0xFFFF);
                 MOTAdaptor::UnlockInteractiveLockInfo(pre_csn, rc != RC_OK);
             }
 
             if (rc == RC_ABORT){
                 if (IsInteractive()) {
 //                    MOT_LOG_INFO("[Abort] CommitPhase() local failed tmp_csn : %s ", csn_temp.c_str());
                     MOTAdaptor::CommitPhase_abort_interactive_num.fetch_add(1);
                 }
                 MOTAdaptor::CommitPhase_abort_num.fetch_add(1);
             }
 
             if(rc != RC_ABORT){
                 while(!MOTAdaptor::IsRemoteExeced()) {
                     usleep(200);
                 }
                 csn_temp = std::to_string(GetCommitSequenceNumber()) + ":" + std::to_string(local_ip_index);
                 if(MOTAdaptor::abort_transcation_csn_set.contain(csn_temp, csn_temp)){
                     if (IsInteractive()) {
                         MOTAdaptor::Abort_transcation_csn_set_abort_interactive_num.fetch_add(1);
 //                        MOT_LOG_INFO("[Abort] abort_transcation_csn_set() local failed tmp_csn : %s ", csn_temp.c_str());
                     }
                     MOTAdaptor::Abort_transcation_csn_set_abort_num.fetch_add(1);
                     rc = RC_ABORT;
                 }
                 // wzy:
                 if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
                     if (IsInteractive()) {
                         MOTAdaptor::CommitCheck_deadlock_abort_interactive_num.fetch_add(1);
 //                        MOT_LOG_INFO("[Abort] deadlock_abort_set() local failed tmp_csn : %s ", csn_temp.c_str());
                     }
                     rc = RC_ABORT;     // 被死锁检测abort
                 }
                 if(m_occManager.CommitCheck(this, local_ip_index) == RC_ABORT) {
                     MOTAdaptor::CommitCheck_abort_num.fetch_add(1);
                     if (IsInteractive()) {
 //                        MOT_LOG_INFO("[Abort] CommitCheck() local failed tmp_csn : %s ", csn_temp.c_str());
                         MOTAdaptor::CommitCheck_abort_interactive_num.fetch_add(1);
                     }
                     rc = RC_ABORT;
                 }
             }
             
             if(rc == RC_OK){
                 MOTAdaptor::IncRecordCommitTxnCounters(epoch_mod, index_pack);
                 (*MOTAdaptor::write_committed_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
                 if(is_breakdown) {
                     auto time2 = now_to_us();
                     MOT_LOG_INFO("async 事务提交 读写 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                         GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), (GetStartMOTCommitTime() - GetStartMOTExecTime()), (time2 - GetStartMOTCommitTime() - GetBlockTime()), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());
                 }
             }
             else {
                 (*MOTAdaptor::write_abort_after_send_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             }
 
 //            if (IsInteractive()) {
 //                // wzy: 在epoch结束前解锁/abort
 //                uint64_t pre_csn = GetCommitSequenceNumber();
 //                MOTAdaptor::UnlockInteractiveLockInfo(pre_csn, rc != RC_OK);
 //            }
 
             MOTAdaptor::IncLocalCommittedCounters(epoch_mod, index_pack);
             return rc;
         }
         else{
             auto time1 = now_to_us();
             if (is_snap_isolation) {
                 if (!m_occManager.ValidateReadInMergeForSnap(this, local_ip_index)) {
 //                    MOT_LOG_INFO("ValidateReadInMergeForSnap error RC_ABORT");
                     if (IsInteractive())
                         MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num.fetch_add(1);
                     return RC_ABORT;
                 }
             } else if (is_read_repeatable) {
                 if (!m_occManager.ValidateReadInMerge(this, local_ip_index)) {
 //                    MOT_LOG_INFO("ValidateReadInMerge error RC_ABORT");
                     if (IsInteractive())
                         MOTAdaptor::ValidateReadInMerge_abort_interactive_num.fetch_add(1);
                     return RC_ABORT;
                 }
             }
 
             if(is_breakdown) {
                 auto time2 = now_to_us();
                 MOT_LOG_INFO("async 事务提交 只读 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                     GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), (GetStartMOTCommitTime() - GetStartMOTExecTime()), (time2 - GetStartMOTCommitTime() - GetBlockTime()), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());
             }
             return RC_OK;
         }
     }
 }
 
 /////////////// PLOR /////////////////////
 RC TxnManager::ReadLockForSwitch_Plor() {
     return m_occManager.SwitchReadPhasePlor(this, local_ip_index, false);
 }
 
 RC TxnManager::ReadLockForSwitchHotRows_Plor() {
     return m_occManager.SwitchReadPhasePlor(this, local_ip_index, true);
 }
 
 RC TxnManager::WriteLockForSwitch_Plor() {
     return m_occManager.SwitchWritePhasePlor(this, local_ip_index, false);
 }
 
 RC TxnManager::WriteLockForSwitchHotRows_Plor() {
     return m_occManager.SwitchWritePhasePlor(this, local_ip_index, true);
 }
 
 RC TxnManager::GetReadLock_Plor(MOT::Row* currRow) {
     return m_occManager.ReadPhasePlor(this, local_ip_index, currRow);
 }
 
 RC TxnManager::GetWriteLock_Plor(MOT::Row* currRow) {
     return m_occManager.WritePhasePlor(this, local_ip_index, currRow);
 }
 
 ///////////////// DL Detect //////////////////
 RC TxnManager::ReadLockForSwitch_DL() {
     return m_occManager.SwitchReadPhaseDL(this, local_ip_index, false);
 }
 
 RC TxnManager::GetReadLock_DL(MOT::Row* currRow) {
     return m_occManager.ReadPhaseDL(this, local_ip_index, currRow);
 }
 
 RC TxnManager::ReadLockForSwitchHotRows_DL() {
     return m_occManager.SwitchReadPhaseDL(this, local_ip_index, true);
 }
 
 RC TxnManager::WriteLockForSwitch_DL() {
     return m_occManager.SwitchWritePhaseDL(this, local_ip_index, false);
 }
 
 RC TxnManager::WriteLockForSwitchHotRows_DL() {
     return m_occManager.SwitchWritePhaseDL(this, local_ip_index, true);
 }
 
 RC TxnManager::GetWriteLock_DL(MOT::Row* currRow) {
     return m_occManager.WritePhaseDL(this, local_ip_index, currRow);
 }
 
 ////////////////////////
 // wzy: 单机不需要
 RC TxnManager::SendLockInfo_Plor(MOT::Row* currRow)
 {
     if (is_raft_enable == 1 && local_ip_index == kRaftStopServerId && kRaftStopEpoch > 0 &&
         MOTAdaptor::GetPhysicalEpoch() > kRaftStopEpoch) {
         while (MOTAdaptor::GetPhysicalEpoch() < kRaftRestrtEpoch)
             usleep(kSleepTime);
     }
 
     m_occManager.updateInsertSetSize(this);
     bool result = m_occManager.IsReadOnly(this);
     uint64_t index_pack = GetIndexPack();
     auto temp = kNotifyNum;
     if (!MOTAdaptor::isInited.load()) return RC_OK;
     if (kNotifyNum <= 0 || MOTAdaptor::max_length <= 0) kNotifyNum = 1;
 
     uint64_t index_notify = index_pack % kNotifyNum;
     uint64_t index_unique = MOTAdaptor::IncLocalTxnIndex(GetStartEpoch() % MOTAdaptor::max_length, index_pack);
     RC rc = RC_OK;
     if (kDelayRatio > 0) {
         std::default_random_engine random;
         random.seed(time(0));
         if (random() % 100 < kDelayRatio)
             usleep(kDelayTime);
     }
 
     // wzy: 对于交互型事务发送lock info并设置为lock
     if (kServerNum > 1) {         // 单机无需发送
         if (!MOTAdaptor::InsertTxntoLocalLockInfo(this,
                 index_pack,
                 index_unique,
                 true, currRow)) {  // 当生成txn失败时abort，为send_before_abort，enqueue必定成功，否则assert
             (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]
                 ->fetch_add(1);
             return RC_ABORT;
         }
     }
 
     // wzy: 在阻塞前就更新计数器
     auto epoch_mod = GetCommitEpoch() % MOTAdaptor::max_length;
     MOTAdaptor::IncLocalLockinfoCounters(epoch_mod, index_pack);
 
     // ============================ 等待事务当前epoch合并 ============================
 
     while (GetCommitEpoch() > MOTAdaptor::GetLogicalEpoch() || !MOTAdaptor::IsLockCommitted()) {
         usleep(200);
     }
 
     MOTAdaptor::IncLocalLockinfoExecedCounters(epoch_mod, index_pack);
 
     // 插入plor 写锁，循环等待
 //    rc = m_occManager.LockPhasePlor(this, local_ip_index, currRow);
 
     auto csn_temp = std::to_string(GetCommitSequenceNumber()) + ":" + std::to_string(local_ip_index);
     if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) return RC_ABORT;         // 被死锁检测abort
     // 获得锁则直接return
     return rc;
 }
 
 // wzy: 单机不需要
 RC TxnManager::SendReadLockInfo_Plor(MOT::Row* currRow)
 {
     if (is_raft_enable == 1 && local_ip_index == kRaftStopServerId && kRaftStopEpoch > 0 &&
         MOTAdaptor::GetPhysicalEpoch() > kRaftStopEpoch) {
         while (MOTAdaptor::GetPhysicalEpoch() < kRaftRestrtEpoch)
             usleep(kSleepTime);
     }
 
     m_occManager.updateInsertSetSize(this);
     bool result = m_occManager.IsReadOnly(this);
     uint64_t index_pack = GetIndexPack();
     auto temp = kNotifyNum;
     if (!MOTAdaptor::isInited.load()) return RC_OK;
     if (kNotifyNum <= 0 || MOTAdaptor::max_length <= 0) kNotifyNum = 1;
 
     uint64_t index_notify = index_pack % kNotifyNum;
     uint64_t index_unique = MOTAdaptor::IncLocalTxnIndex(GetStartEpoch() % MOTAdaptor::max_length, index_pack);
     RC rc = RC_OK;
     if (kDelayRatio > 0) {
         std::default_random_engine random;
         random.seed(time(0));
         if (random() % 100 < kDelayRatio)
             usleep(kDelayTime);
     }
 
     // wzy: 对于交互型事务发送lock info并设置为lock
     if (kServerNum > 1) {         // 单机无需发送
         if (!MOTAdaptor::InsertTxntoLocalLockInfo(this,
                 index_pack,
                 index_unique,
                 true, currRow)) {  // 当生成txn失败时abort，为send_before_abort，enqueue必定成功，否则assert
             (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]
                 ->fetch_add(1);
             return RC_ABORT;
         }
     }
 
     // wzy: 在阻塞前就更新计数器
     auto epoch_mod = GetCommitEpoch() % MOTAdaptor::max_length;
     MOTAdaptor::IncLocalLockinfoCounters(epoch_mod, index_pack);
 
     // ============================ 等待事务当前epoch合并 ============================
 
     while (GetCommitEpoch() > MOTAdaptor::GetLogicalEpoch() || !MOTAdaptor::IsLockCommitted()) {
         usleep(200);
     }
 
     MOTAdaptor::IncLocalLockinfoExecedCounters(epoch_mod, index_pack);
 
     // 插入plor 写锁，循环等待
 //    rc = m_occManager.ReadLockPhasePlor(this, local_ip_index, currRow);
 
     auto csn_temp = std::to_string(GetCommitSequenceNumber()) + ":" + std::to_string(local_ip_index);
     if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) return RC_ABORT;         // 被死锁检测abort
     // 获得锁则直接return
     return rc;
 }
 
 // wzy: plor with no Epoch and CRDT
 RC TxnManager::Commit_Plor(){
 
     m_occManager.updateInsertSetSize(this);
     bool result = m_occManager.IsReadOnly(this);
     uint64_t index_pack = GetIndexPack();
 
     // wzy: recovery debug
     if (!MOTAdaptor::isInited.load()) return RC_OK;
     if (kNotifyNum <= 0 || MOTAdaptor::max_length <= 0) kNotifyNum = 1;
 
     uint64_t index_notify = index_pack % kNotifyNum;
     uint64_t index_unique =  MOTAdaptor::IncLocalTxnIndex(GetStartEpoch() % MOTAdaptor::max_length, index_pack);
     uint64_t cnt = 0;
     RC rc = RC_OK;
 
     SetStartMOTCommitTime(now_to_us());
 
     if (this->m_accessMgr->m_rowCnt > 0 && !result){
         // wzy: 检查事务是否被abort
         auto csn_temp = std::to_string(pre_csn) + ":" + std::to_string(local_ip_index);
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
             if (IsInteractive()) MOTAdaptor::CommitCheck_deadlock_abort_interactive_num.fetch_add(1);
             return RC_ABORT;     // 被死锁检测abort，已经被自动解锁
         }
 
         // wzy: 读写冲突检测，阻塞，成功则已切换为exclusive模式
         if(m_occManager.ValidationPhasePlor(this, local_ip_index) == RC_ABORT) {
             if (IsInteractive()) {
                 if (is_debug_print_enable) MOT_LOG_INFO("[Abort] CommitPhasePlor() local failed tmp_csn : %s ", csn_temp.c_str());
                 MOTAdaptor::CommitPhase_abort_interactive_num.fetch_add(1);
             }
             MOTAdaptor::CommitPhase_abort_num.fetch_add(1);
             return RC_ABORT;
         }
 
         SetCommitEpoch(MOTAdaptor::GetPhysicalEpoch());         // 设置commit epoch
 //        if (!MOTAdaptor::txn_state_map_plor_.cas_element(start_time, 0, 2)) return RC_ABORT;
 
         // 无需再次检验
 
         // 更新到row header，对写集sentinel上锁
         rc = m_occManager.CommitUpdate(this, local_ip_index);       // 如果失败会自动释放锁
 
         if (rc == RC_ABORT){
             // header锁已经释放
             MOTAdaptor::Switch_validation_pcc_abort_num.fetch_add(1);
             if (IsInteractive()) {
                 MOTAdaptor::CommitPhase_abort_interactive_num.fetch_add(1);
             }
             MOTAdaptor::CommitPhase_abort_num.fetch_add(1);
         }
 
         if(rc != RC_ABORT){
             // 移动到Lock释放后，避免死锁，乐观和悲观Plor执行的同步提交，释放锁
             // rc = m_occManager.UnlockReadWriteLockPlor(this, local_ip_index, pre_csn, false);
             // 对未上锁的行进行检查
             auto time1 = now_to_us();
             rc = m_occManager.ValidateOccPlor(this);
             auto time2 = now_to_us();
             MOTAdaptor::txn_total_validate_hotOccTime.fetch_add(time2 - time1);
             MOTAdaptor::txn_total_validate_hotOccCnt.fetch_add(1);
             MOTAdaptor::txn_temp_total_validate_hotOccTime.fetch_add(time2 - time1);
             MOTAdaptor::txn_temp_total_validate_hotOccCnt.fetch_add(1);
         }
 
         if(rc == RC_OK){
             (*MOTAdaptor::write_committed_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             if(is_breakdown) {
                 auto time2 = now_to_us();
                 MOT_LOG_INFO("async 事务提交 读写 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                     GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), (GetStartMOTCommitTime() - GetStartMOTExecTime()), (time2 - GetStartMOTCommitTime() - GetBlockTime()), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());
             }
         }
         else {
             (*MOTAdaptor::write_abort_after_send_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
         }
 
         return rc;
     }
     else{       // 只读
         auto time1 = now_to_us();
         if (is_snap_isolation) {
             if (!m_occManager.ValidateReadInMergeForSnap(this, local_ip_index)) {
                 if (is_debug_print_enable) MOT_LOG_INFO("ValidateReadInMergeForSnap error RC_ABORT");
                 if (IsInteractive())
                     MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;
             }
         } else if (is_read_repeatable) {
             if (!m_occManager.ValidateReadInMerge(this, local_ip_index)) {
                 if (is_debug_print_enable) MOT_LOG_INFO("ValidateReadInMerge error RC_ABORT");
                 if (IsInteractive())
                     MOTAdaptor::ValidateReadInMerge_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;
             }
         }
         // 只读事务解锁
         rc = m_occManager.UnlockReadWriteLockPlor(this, local_ip_index, pre_csn, false);
 
         if(is_breakdown) {
             auto time2 = now_to_us();
             MOT_LOG_INFO("async 事务提交 只读 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                 GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), (GetStartMOTCommitTime() - GetStartMOTExecTime()), (time2 - GetStartMOTCommitTime() - GetBlockTime()), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());
         }
         return RC_OK;
     }
 
 }
 
 
 // wzy: plor 提交，单机
 RC TxnManager::Commit_Plor_Epoch(){
 
     if(is_raft_enable == 1 && local_ip_index == kRaftStopServerId && kRaftStopEpoch > 0 && MOTAdaptor::GetPhysicalEpoch() > kRaftStopEpoch) {
         while(MOTAdaptor::GetPhysicalEpoch() < kRaftRestrtEpoch) usleep(kSleepTime);
     }
 
     m_occManager.updateInsertSetSize(this);
     bool result = m_occManager.IsReadOnly(this);
     uint64_t index_pack = GetIndexPack();
 
     // wzy: recovery debug
     if (!MOTAdaptor::isInited.load()) return RC_OK;
     if (kNotifyNum <= 0 || MOTAdaptor::max_length <= 0) kNotifyNum = 1;
 
     uint64_t index_notify = index_pack % kNotifyNum;
     uint64_t index_unique =  MOTAdaptor::IncLocalTxnIndex(GetStartEpoch() % MOTAdaptor::max_length, index_pack);
     uint64_t cnt = 0;
     RC rc = RC_OK;
     if(kDelayRatio > 0) {
         std::default_random_engine random;
         random.seed(time(0));
         if(random() % 100 < kDelayRatio)
             usleep(kDelayTime);
     }
     SetStartMOTCommitTime(now_to_us());
 
     if (this->m_accessMgr->m_rowCnt > 0 && !result){
         // wzy: 检查事务是否被abort
         auto csn_temp = std::to_string(GetCommitSequenceNumber()) + ":" + std::to_string(local_ip_index);
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
             if (IsInteractive()) MOTAdaptor::CommitCheck_deadlock_abort_interactive_num.fetch_add(1);
             return RC_ABORT;     // 被死锁检测abort，已经被自动解锁
         }
 
         // wzy: 读写冲突检测，阻塞，成功则已切换为exclusive模式
         if(m_occManager.ValidationPhasePlor(this, local_ip_index) == RC_ABORT || MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
             if (IsInteractive()) {
 //                MOT_LOG_INFO("[Abort] CommitPhasePlor() local failed tmp_csn : %s ", csn_temp.c_str());
                 MOTAdaptor::CommitPhase_abort_interactive_num.fetch_add(1);
             }
             MOTAdaptor::CommitPhase_abort_num.fetch_add(1);
             return RC_ABORT;
         }
         SetCommitEpoch(MOTAdaptor::GetPhysicalEpoch());         // 设置commit epoch
         if (!MOTAdaptor::txn_state_map_plor_.cas_element(start_time, 0, 2)) return RC_ABORT;
 
         auto time1 = now_to_us();
         cnt = 0;
         auto epoch_mod = GetCommitEpoch() % MOTAdaptor::max_length;
         MOTAdaptor::IncLocalTxnCounters(epoch_mod, index_pack);
         MOTAdaptor::IncLocalTxnExcCounters(epoch_mod, index_pack);
 
         // TODO: 需要epoch吗? 等待上一個epoch結束
         while(GetCommitEpoch() > MOTAdaptor::GetLogicalEpoch() || !MOTAdaptor::IsRecordCommitted()){
             usleep(200);
         }
 
         auto time3 = now_to_us();
         this->SetBlockTime(time3 - time1);
 
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) rc = RC_ABORT;
         uint64_t pre_csn = GetCommitSequenceNumber();
 
         // 更新到row header
         rc = m_occManager.CommitUpdate(this, local_ip_index);
         MOTAdaptor::IncLocalExecedCounters(epoch_mod, index_pack);
 
         if (rc == RC_ABORT){
             if (IsInteractive()) {
 //                MOT_LOG_INFO("[Abort] CommitPhase() local failed tmp_csn : %s ", csn_temp.c_str());
                 MOTAdaptor::CommitPhase_abort_interactive_num.fetch_add(1);
             }
             MOTAdaptor::CommitPhase_abort_num.fetch_add(1);
         }
 
         if(rc != RC_ABORT){
             while(!MOTAdaptor::IsRemoteExeced()) {
                 usleep(200);
             }
 
             // 乐观和悲观Plor执行的同步提交
             rc = m_occManager.UnlockReadWriteLockPlor(this, local_ip_index, pre_csn, false);
 
             csn_temp = std::to_string(GetCommitSequenceNumber()) + ":" + std::to_string(local_ip_index);
 
             if(MOTAdaptor::abort_transcation_csn_set.contain(csn_temp, csn_temp)){
                 if (IsInteractive()) {
                     MOTAdaptor::Abort_transcation_csn_set_abort_interactive_num.fetch_add(1);
                     MOT_LOG_INFO("[Abort] abort_transcation_csn_set() local failed tmp_csn : %s ", csn_temp.c_str());
                 }
                 MOTAdaptor::Abort_transcation_csn_set_abort_num.fetch_add(1);
                 MOTAdaptor::abort_transcation_csn_set.remove(csn_temp);
                 rc = RC_ABORT;
             }
             // wzy:
             if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
                 if (IsInteractive()) {
                     MOTAdaptor::CommitCheck_deadlock_abort_interactive_num.fetch_add(1);
 //                    MOT_LOG_INFO("[Abort] deadlock_abort_set() local failed tmp_csn : %s ", csn_temp.c_str());
                 }
                 rc = RC_ABORT;     // 被死锁检测abort
             }
         }
 
         if(rc == RC_OK){
             MOTAdaptor::IncRecordCommitTxnCounters(epoch_mod, index_pack);
             (*MOTAdaptor::write_committed_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             if(is_breakdown) {
                 auto time2 = now_to_us();
                 MOT_LOG_INFO("async 事务提交 读写 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                     GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), (GetStartMOTCommitTime() - GetStartMOTExecTime()), (time2 - GetStartMOTCommitTime() - GetBlockTime()), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());
             }
         }
         else {
             (*MOTAdaptor::write_abort_after_send_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
         }
 
         MOTAdaptor::IncLocalCommittedCounters(epoch_mod, index_pack);
         return rc;
     }
     else{       // 只读
         auto time1 = now_to_us();
         if (is_snap_isolation) {
             if (!m_occManager.ValidateReadInMergeForSnap(this, local_ip_index)) {
                 MOT_LOG_INFO("ValidateReadInMergeForSnap error RC_ABORT");
                 if (IsInteractive())
                     MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;
             }
         } else if (is_read_repeatable) {
             if (!m_occManager.ValidateReadInMerge(this, local_ip_index)) {
                 MOT_LOG_INFO("ValidateReadInMerge error RC_ABORT");
                 if (IsInteractive())
                     MOTAdaptor::ValidateReadInMerge_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;
             }
         }
         // 只读事务解锁
         uint64_t pre_csn = GetCommitSequenceNumber();
         rc = m_occManager.UnlockReadWriteLockPlor(this, local_ip_index, pre_csn, false);
 
         if(is_breakdown) {
             auto time2 = now_to_us();
             MOT_LOG_INFO("async 事务提交 只读 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                 GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), (GetStartMOTCommitTime() - GetStartMOTExecTime()), (time2 - GetStartMOTCommitTime() - GetBlockTime()), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());
         }
         return RC_OK;
     }
 
 }
 
 // wzy:解锁本地锁Plor，原csn是否发生变化
 void TxnManager::UnlockLockInfo_Plor(uint64_t csn, bool abort)
 {
     m_occManager.UnlockReadWriteLockPlor(this, local_ip_index, csn, abort);
 }
 
 /////////////// Wound-wait /////////////////
 
 RC TxnManager::ReadLockForSwitch_WoundWait() {
     return m_occManager.SwitchReadPhaseWoundWait(this, local_ip_index, false);
 }
 
 RC TxnManager::ReadLockForSwitchHotRows_WoundWait()
 {
     return m_occManager.SwitchReadPhaseWoundWait(this, local_ip_index, true);
 }
 
 RC TxnManager::WriteLockForSwitch_WoundWait() {
     return m_occManager.SwitchWritePhaseWoundWait(this, local_ip_index, false);
 }
 
 RC TxnManager::WriteLockForSwitchHotRows_WoundWait() {
     return m_occManager.SwitchWritePhaseWoundWait(this, local_ip_index, true);
 }
 
 RC TxnManager::GetReadLock_WoundWait(MOT::Row* currRow) {
     return m_occManager.ReadPhaseWoundWait(this, local_ip_index, currRow);
 }
 
 RC TxnManager::GetWriteLock_WoundWait(MOT::Row* currRow) {
     return m_occManager.WritePhaseWoundWait(this, local_ip_index, currRow);
 }
 
 
 // wzy: 单机不需要
 RC TxnManager::SendLockInfo_WoundWait(MOT::Row* currRow)
 {
     if (is_raft_enable == 1 && local_ip_index == kRaftStopServerId && kRaftStopEpoch > 0 &&
         MOTAdaptor::GetPhysicalEpoch() > kRaftStopEpoch) {
         while (MOTAdaptor::GetPhysicalEpoch() < kRaftRestrtEpoch)
             usleep(kSleepTime);
     }
 
     m_occManager.updateInsertSetSize(this);
     bool result = m_occManager.IsReadOnly(this);
     uint64_t index_pack = GetIndexPack();
     auto temp = kNotifyNum;
     if (!MOTAdaptor::isInited.load()) return RC_OK;
     if (kNotifyNum <= 0 || MOTAdaptor::max_length <= 0) kNotifyNum = 1;
 
     uint64_t index_notify = index_pack % kNotifyNum;
     uint64_t index_unique = MOTAdaptor::IncLocalTxnIndex(GetStartEpoch() % MOTAdaptor::max_length, index_pack);
     RC rc = RC_OK;
     if (kDelayRatio > 0) {
         std::default_random_engine random;
         random.seed(time(0));
         if (random() % 100 < kDelayRatio)
             usleep(kDelayTime);
     }
 
     // wzy: 对于交互型事务发送lock info并设置为lock
     if (kServerNum > 1) {         // 单机无需发送
         if (!MOTAdaptor::InsertTxntoLocalLockInfo(this,
                 index_pack,
                 index_unique,
                 true, currRow)) {  // 当生成txn失败时abort，为send_before_abort，enqueue必定成功，否则assert
             (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]
                 ->fetch_add(1);
             return RC_ABORT;
         }
     }
 
     // wzy: 在阻塞前就更新计数器
     auto epoch_mod = GetCommitEpoch() % MOTAdaptor::max_length;
     MOTAdaptor::IncLocalLockinfoCounters(epoch_mod, index_pack);
 
     // ============================ 等待事务当前epoch合并 ============================
 
     while (GetCommitEpoch() > MOTAdaptor::GetLogicalEpoch() || !MOTAdaptor::IsLockCommitted()) {
         usleep(200);
     }
 
     MOTAdaptor::IncLocalLockinfoExecedCounters(epoch_mod, index_pack);
 
     // 插入plor 写锁，循环等待
     //    rc = m_occManager.LockPhasePlor(this, local_ip_index, currRow);
 
     auto csn_temp = std::to_string(GetCommitSequenceNumber()) + ":" + std::to_string(local_ip_index);
     if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) return RC_ABORT;         // 被死锁检测abort
     // 获得锁则直接return
     return rc;
 }
 
 // wzy: 单机不需要
 RC TxnManager::SendReadLockInfo_WoundWait(MOT::Row* currRow)
 {
     if (is_raft_enable == 1 && local_ip_index == kRaftStopServerId && kRaftStopEpoch > 0 &&
         MOTAdaptor::GetPhysicalEpoch() > kRaftStopEpoch) {
         while (MOTAdaptor::GetPhysicalEpoch() < kRaftRestrtEpoch)
             usleep(kSleepTime);
     }
 
     m_occManager.updateInsertSetSize(this);
     bool result = m_occManager.IsReadOnly(this);
     uint64_t index_pack = GetIndexPack();
     auto temp = kNotifyNum;
     if (!MOTAdaptor::isInited.load()) return RC_OK;
     if (kNotifyNum <= 0 || MOTAdaptor::max_length <= 0) kNotifyNum = 1;
 
     uint64_t index_notify = index_pack % kNotifyNum;
     uint64_t index_unique = MOTAdaptor::IncLocalTxnIndex(GetStartEpoch() % MOTAdaptor::max_length, index_pack);
     RC rc = RC_OK;
     if (kDelayRatio > 0) {
         std::default_random_engine random;
         random.seed(time(0));
         if (random() % 100 < kDelayRatio)
             usleep(kDelayTime);
     }
 
     // wzy: 对于交互型事务发送lock info并设置为lock
     if (kServerNum > 1) {         // 单机无需发送
         if (!MOTAdaptor::InsertTxntoLocalLockInfo(this,
                 index_pack,
                 index_unique,
                 true, currRow)) {  // 当生成txn失败时abort，为send_before_abort，enqueue必定成功，否则assert
             (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]
                 ->fetch_add(1);
             return RC_ABORT;
         }
     }
 
     // wzy: 在阻塞前就更新计数器
     auto epoch_mod = GetCommitEpoch() % MOTAdaptor::max_length;
     MOTAdaptor::IncLocalLockinfoCounters(epoch_mod, index_pack);
 
     // ============================ 等待事务当前epoch合并 ============================
 
     while (GetCommitEpoch() > MOTAdaptor::GetLogicalEpoch() || !MOTAdaptor::IsLockCommitted()) {
         usleep(200);
     }
 
     MOTAdaptor::IncLocalLockinfoExecedCounters(epoch_mod, index_pack);
 
     // 插入plor 写锁，循环等待
     //    rc = m_occManager.ReadLockPhasePlor(this, local_ip_index, currRow);
 
     auto csn_temp = std::to_string(GetCommitSequenceNumber()) + ":" + std::to_string(local_ip_index);
     if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) return RC_ABORT;         // 被死锁检测abort
     // 获得锁则直接return
     return rc;
 }
 
 // wzy: plor 提交，单机
 RC TxnManager::Commit_WoundWait(){
 
     m_occManager.updateInsertSetSize(this);
     bool result = m_occManager.IsReadOnly(this);
     uint64_t index_pack = GetIndexPack();
 
     // wzy: recovery debug
     if (!MOTAdaptor::isInited.load()) return RC_OK;
     if (kNotifyNum <= 0 || MOTAdaptor::max_length <= 0) kNotifyNum = 1;
 
     uint64_t index_notify = index_pack % kNotifyNum;
     uint64_t index_unique =  MOTAdaptor::IncLocalTxnIndex(GetStartEpoch() % MOTAdaptor::max_length, index_pack);
     uint64_t cnt = 0;
     RC rc = RC_OK;
 
     SetStartMOTCommitTime(now_to_us());
 
     if (this->m_accessMgr->m_rowCnt > 0 && !result){
         // wzy: 检查事务是否被abort
         auto csn_temp = std::to_string(pre_csn) + ":" + std::to_string(local_ip_index);
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
             if (IsInteractive()) MOTAdaptor::CommitCheck_deadlock_abort_interactive_num.fetch_add(1);
             return RC_ABORT;     // 被死锁检测abort，已经被自动解锁
         }
 
         if (is_debug_print_enable) MOT_LOG_INFO("CommitPhaseWoundWait() before tmp_csn : %s ", csn_temp.c_str());
 
         if(m_occManager.ValidationPhaseWoundWait(this, local_ip_index) == RC_ABORT) {
             if (IsInteractive()) {
                 if (is_debug_print_enable) MOT_LOG_INFO("[Abort] CommitPhaseWoundWait() local failed tmp_csn : %s ", csn_temp.c_str());
                 MOTAdaptor::CommitPhase_abort_interactive_num.fetch_add(1);
             }
             MOTAdaptor::CommitPhase_abort_num.fetch_add(1);
             return RC_ABORT;
         }
 
         if (is_debug_print_enable) MOT_LOG_INFO("CommitPhaseWoundWait() after tmp_csn : %s ", csn_temp.c_str());
 
         SetCommitEpoch(MOTAdaptor::GetPhysicalEpoch());         // 设置commit epoch
 
         // 更新到row header，对写集sentinel上锁
         rc = m_occManager.CommitUpdate(this, local_ip_index);       // 如果失败会自动释放锁
 
         if (rc == RC_ABORT){
             // header锁已经释放
             MOTAdaptor::Switch_validation_pcc_abort_num.fetch_add(1);
             if (IsInteractive()) {
                 MOTAdaptor::CommitPhase_abort_interactive_num.fetch_add(1);
             }
             MOTAdaptor::CommitPhase_abort_num.fetch_add(1);
         }
 
         if(rc != RC_ABORT){
             // 移动到Lock释放后，避免死锁，乐观和悲观Plor执行的同步提交，释放锁
             // rc = m_occManager.UnlockReadWriteLockPlor(this, local_ip_index, pre_csn, false);
 
             // TODO: 对热点数据进行读写检验，复用silo代码，失败则释放锁
             if (kHotRow_Active) {
                 auto time1 = now_to_us();
                 rc = m_occManager.ValidateOccPlor(this);
                 auto time2 = now_to_us();
                 MOTAdaptor::txn_total_validate_hotOccTime.fetch_add(time2 - time1);
                 MOTAdaptor::txn_total_validate_hotOccCnt.fetch_add(1);
                 MOTAdaptor::txn_temp_total_validate_hotOccTime.fetch_add(time2 - time1);
                 MOTAdaptor::txn_temp_total_validate_hotOccCnt.fetch_add(1);
             }
         }
 
         if(rc == RC_OK){
             (*MOTAdaptor::write_committed_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             if(is_breakdown) {
                 auto time2 = now_to_us();
                 MOT_LOG_INFO("async 事务提交 读写 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                     GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), (GetStartMOTCommitTime() - GetStartMOTExecTime()), (time2 - GetStartMOTCommitTime() - GetBlockTime()), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());
             }
         }
         else {
             (*MOTAdaptor::write_abort_after_send_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
         }
 
         return rc;
     }
     else{       // 只读
         auto time1 = now_to_us();
         if (is_snap_isolation) {
             if (!m_occManager.ValidateReadInMergeForSnap(this, local_ip_index)) {
                 if (is_debug_print_enable) MOT_LOG_INFO("ValidateReadInMergeForSnap error RC_ABORT");
                 if (IsInteractive())
                     MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;
             }
         } else if (is_read_repeatable) {
             if (!m_occManager.ValidateReadInMerge(this, local_ip_index)) {
                 if (is_debug_print_enable) MOT_LOG_INFO("ValidateReadInMerge error RC_ABORT");
                 if (IsInteractive())
                     MOTAdaptor::ValidateReadInMerge_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;
             }
         }
         // 只读事务解锁
         rc = m_occManager.UnlockReadWriteLockPlor(this, local_ip_index, pre_csn, false);
 
         if(is_breakdown) {
             auto time2 = now_to_us();
             MOT_LOG_INFO("async 事务提交 只读 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                 GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), (GetStartMOTCommitTime() - GetStartMOTExecTime()), (time2 - GetStartMOTCommitTime() - GetBlockTime()), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());
         }
         return RC_OK;
     }
 }
 
 // wzy:解锁本地锁Plor，原csn是否发生变化
 void TxnManager::UnlockLockInfo_WoundWait(uint64_t csn, bool abort)
 {
     m_occManager.UnlockReadWriteLockWoundWait(this, local_ip_index, csn, abort);
 }
 
 //////////////// DL Detect //////////////////////
 
 // wzy: plor with no Epoch and CRDT
 RC TxnManager::Commit_DL(){
 
     m_occManager.updateInsertSetSize(this);
     bool result = m_occManager.IsReadOnly(this);
     uint64_t index_pack = GetIndexPack();
 
     // wzy: recovery debug
     if (!MOTAdaptor::isInited.load()) return RC_OK;
     if (kNotifyNum <= 0 || MOTAdaptor::max_length <= 0) kNotifyNum = 1;
 
     uint64_t index_notify = index_pack % kNotifyNum;
     uint64_t index_unique =  MOTAdaptor::IncLocalTxnIndex(GetStartEpoch() % MOTAdaptor::max_length, index_pack);
     uint64_t cnt = 0;
     RC rc = RC_OK;
 
     SetStartMOTCommitTime(now_to_us());
 
     if (this->m_accessMgr->m_rowCnt > 0 && !result){
         // wzy: 检查事务是否被abort
         auto csn_temp = std::to_string(pre_csn) + ":" + std::to_string(local_ip_index);
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
             if (IsInteractive()) MOTAdaptor::CommitCheck_deadlock_abort_interactive_num.fetch_add(1);
             return RC_ABORT;     // 被死锁检测abort，已经被自动解锁
         }
 
         // wzy: 读写冲突检测，阻塞，成功则已切换为exclusive模式，貌似没有Lock
         //        if(m_occManager.ValidationPhasePlor(this, local_ip_index) == RC_ABORT || MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
 //        if(m_occManager.ValidationPhasePlor(this, local_ip_index) == RC_ABORT) {
 //            if (IsInteractive()) {
 //                if (is_debug_print_enable) MOT_LOG_INFO("[Abort] CommitPhasePlor() local failed tmp_csn : %s ", csn_temp.c_str());
 //                MOTAdaptor::CommitPhase_abort_interactive_num.fetch_add(1);
 //            }
 //            MOTAdaptor::CommitPhase_abort_num.fetch_add(1);
 //            return RC_ABORT;
 //        }
 
         SetCommitEpoch(MOTAdaptor::GetPhysicalEpoch());         // 设置commit epoch
         if (!MOTAdaptor::txn_state_map_plor_.cas_element(start_time, 0, 2)) return RC_ABORT;
 
         // TODO：无需再次检验
         // 更新到row header，对写集sentinel上锁
         rc = m_occManager.CommitUpdate(this, local_ip_index);       // 如果失败会自动释放锁
 
         if (rc == RC_ABORT){
             // header锁已经释放
             MOTAdaptor::Switch_validation_pcc_abort_num.fetch_add(1);
             if (IsInteractive()) {
                 MOTAdaptor::CommitPhase_abort_interactive_num.fetch_add(1);
             }
             MOTAdaptor::CommitPhase_abort_num.fetch_add(1);
         }
 
         if(rc != RC_ABORT){
             // 移动到Lock释放后，避免死锁，乐观和悲观Plor执行的同步提交，释放锁
             // rc = m_occManager.UnlockReadWriteLockPlor(this, local_ip_index, pre_csn, false);
             // TODO: 对热点数据进行读写检验，复用silo代码，失败则释放锁
             if (kHotRow_Active) {
                 auto time1 = now_to_us();
                 rc = m_occManager.ValidateOccPlor(this);
                 auto time2 = now_to_us();
                 MOTAdaptor::txn_total_validate_hotOccTime.fetch_add(time2 - time1);
                 MOTAdaptor::txn_total_validate_hotOccCnt.fetch_add(1);
                 MOTAdaptor::txn_temp_total_validate_hotOccTime.fetch_add(time2 - time1);
                 MOTAdaptor::txn_temp_total_validate_hotOccCnt.fetch_add(1);
             }
         }
 
         if(rc == RC_OK){
             (*MOTAdaptor::write_committed_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
             if(is_breakdown) {
                 auto time2 = now_to_us();
                 MOT_LOG_INFO("async 事务提交 读写 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                     GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), (GetStartMOTCommitTime() - GetStartMOTExecTime()), (time2 - GetStartMOTCommitTime() - GetBlockTime()), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());
             }
         }
         else {
             (*MOTAdaptor::write_abort_after_send_txn_num[(GetCommitEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]->fetch_add(1);
         }
 
         return rc;
     }
     else{       // 只读
         auto time1 = now_to_us();
         if (is_snap_isolation) {
             if (!m_occManager.ValidateReadInMergeForSnap(this, local_ip_index)) {
                 if (is_debug_print_enable) MOT_LOG_INFO("ValidateReadInMergeForSnap error RC_ABORT");
                 if (IsInteractive())
                     MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;
             }
         } else if (is_read_repeatable) {
             if (!m_occManager.ValidateReadInMerge(this, local_ip_index)) {
                 if (is_debug_print_enable) MOT_LOG_INFO("ValidateReadInMerge error RC_ABORT");
                 if (IsInteractive())
                     MOTAdaptor::ValidateReadInMerge_abort_interactive_num.fetch_add(1);
                 return RC_ABORT;
             }
         }
         // 只读事务解锁
         rc = m_occManager.UnlockReadWriteLockPlor(this, local_ip_index, pre_csn, false);
 
         if(is_breakdown) {
             auto time2 = now_to_us();
             MOT_LOG_INFO("async 事务提交 只读 epoch:%llu StartMOTExecTime %llu StartMOTCommitTime %llu ValidateFinishTime %llu BlockTime %llu MOTExecTime %llu MOTValidateTime %llu MOTTotalTime %llu ZipTime %llu ZipSize %llu WriteSize %llu",
                 GetCommitEpoch(), GetStartMOTExecTime(), GetStartMOTCommitTime(), time2, GetBlockTime(), (GetStartMOTCommitTime() - GetStartMOTExecTime()), (time2 - GetStartMOTCommitTime() - GetBlockTime()), time2 - GetStartMOTExecTime(), GetZipTime(), GetZipSize(), GetWriteSize());
         }
         return RC_OK;
     }
 
 }
 
 
 ////////////////////////////////////////////
 
 // wzy: 发送lock info，等待本地上锁成功才返回
 RC TxnManager::SendLockInfo(MOT::Row* currRow)
 {
     if (is_raft_enable == 1 && local_ip_index == kRaftStopServerId && kRaftStopEpoch > 0 &&
         MOTAdaptor::GetPhysicalEpoch() > kRaftStopEpoch) {
         while (MOTAdaptor::GetPhysicalEpoch() < kRaftRestrtEpoch)
             usleep(kSleepTime);
     }
 
     m_occManager.updateInsertSetSize(this);
     bool result = m_occManager.IsReadOnly(this);
     uint64_t index_pack = GetIndexPack();
     auto temp = kNotifyNum;
     if (!MOTAdaptor::isInited.load()) return RC_OK;
     if (kNotifyNum <= 0 || MOTAdaptor::max_length <= 0) kNotifyNum = 1;
 
     uint64_t index_notify = index_pack % kNotifyNum;
     uint64_t index_unique = MOTAdaptor::IncLocalTxnIndex(GetStartEpoch() % MOTAdaptor::max_length, index_pack);
     RC rc = RC_OK;
     if (kDelayRatio > 0) {
         std::default_random_engine random;
         random.seed(time(0));
         if (random() % 100 < kDelayRatio)
             usleep(kDelayTime);
     }
 
     // wzy: 对于交互型事务发送lock info并设置为lock
     if (kServerNum > 1 && IsInteractive()) {         // 单机无需发送
         if (!MOTAdaptor::InsertTxntoLocalLockInfo(this,
                 index_pack,
                 index_unique,
                 true, currRow)) {  // 当生成txn失败时abort，为send_before_abort，enqueue必定成功，否则assert
             (*MOTAdaptor::write_abort_before_send_txn_num[(GetStartEpoch() % MOTAdaptor::_max_length)])[GetIndexPack()]
                 ->fetch_add(1);
             return RC_ABORT;
         }
     }
 
     // wzy: 在阻塞前就更新计数器
     auto epoch_mod = GetCommitEpoch() % MOTAdaptor::max_length;
     MOTAdaptor::IncLocalLockinfoCounters(epoch_mod, index_pack);
 
     // 对于交互型事务而言，只有上锁请求成功后才能返回，否则一直等待
     // 等待上一个epoch执行完成（上一个epoch本地和远端都提交）
     // ============================ 等待事务当前epoch合并 ============================
 
     while (GetCommitEpoch() > MOTAdaptor::GetLogicalEpoch() || !MOTAdaptor::IsLockCommitted()) {
         usleep(200);
     }
 
     // 本地更新上锁的metadata（插入metadata中）
     // 若table不存在在rc abort
     rc = m_occManager.LockPhase(this, local_ip_index, currRow);
     MOTAdaptor::IncLocalLockinfoExecedCounters(epoch_mod, index_pack);
 
     while (!MOTAdaptor::IsLockExeced() || !MOTAdaptor::IsLockGranted()) {  // 等待远端合并完成，在merge中有lock插入操作
         usleep(200);
     }
     // 验证自己是否获得锁
     // 没获得锁则继续等待，直到获得锁
     auto csn_temp = std::to_string(GetCommitSequenceNumber()) + ":" + std::to_string(local_ip_index);
     while (m_occManager.LockCheck(this, local_ip_index, currRow) == RC_ABORT) {
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
             MOTAdaptor::LockCheck_abort_num.fetch_add(1);
             return RC_ABORT;     // 被死锁检测abort
         }
         usleep(100);
     }
     if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) return RC_ABORT;         // 被死锁检测abort
     // 获得锁则直接return
     return rc;
 }
 
 // wzy:解锁本地锁，原csn是否发生变化
 void TxnManager::UnlockLockInfo(uint64_t csn, bool abort)
 {
     m_occManager.UnlockPhase(this, local_ip_index, csn, abort);
 }
 
 
 }  // namespace MOT
 