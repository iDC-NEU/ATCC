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
 * occ_transaction_manager.cpp
 *    Optimistic Concurrency Control (OCC) implementation
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/concurrency_control/occ_transaction_manager.h
 *
 * -------------------------------------------------------------------------
 */

 #include "occ_transaction_manager.h"
 #include "../utils/utilities.h"
 #include "cycles.h"
 #include "mot_engine.h"
 #include "row.h"
 #include "row_header.h"
 #include "txn.h"
 #include "txn_access.h"
 #include "checkpoint_manager.h"
 #include "mm_session_api.h"
 #include "mot_error.h"
 #include <pthread.h>
 #include "../../../fdw_adapter/src/mot_internal.h"//ADDBY NEU
 #include "hybrid_cc/hybrid_cc_logger.h" // HYBRID_CC: Include our new logger
 namespace MOT {
 DECLARE_LOGGER(OccTransactionManager, ConcurrenyControl);
 
 OccTransactionManager::OccTransactionManager()
     : m_txnCounter(0),
       m_abortsCounter(0),
       m_writeSetSize(0),
       m_rowsSetSize(0),
       m_deleteSetSize(0),
       m_insertSetSize(0),
       m_dynamicSleep(100),
       m_rowsLocked(false),
       m_preAbort(true),
       m_validationNoWait(true)
 {}
 
 OccTransactionManager::~OccTransactionManager()
 {}
 
 bool OccTransactionManager::Init()
 {
     bool result = true;
     return result;
 }
 
 bool OccTransactionManager::CheckVersion(const Access* access)
 {
     // We always validate on committed rows!
     const Row* row = access->GetRowFromHeader();
     // wzy:
     auto res = (row->m_rowHeader.GetStableCSN() == access->m_tid);
     uint64_t v = row->m_rowHeader.GetStableCSN();
     std::string table_name = row->GetTable()->GetLongTableName();
     if (!res) {
         MOT_LOG_INFO("CheckVersion failed table = %s row id = %llu csnWord = %llu stable_csnWord = %llu, tid = %llu", table_name.c_str(), row->GetRowId(), row->m_rowHeader.GetCSN(), v, access->m_tid);
     }
     return res;
 //    return (row->m_rowHeader.GetCSN() == access->m_tid);
 }
 
 bool OccTransactionManager::QuickHeaderValidation(const Access* access)
 {
     if (access->m_type != INS) {
         // For WR/DEL/RD_FOR_UPDATE lets verify CSN
         return CheckVersion(access);
     } else {
         // Lets verify the inserts
         // For upgrade we verify  the row
         // csn has not changed!
         Sentinel* sent = access->m_origSentinel;
         if (access->m_params.IsUpgradeInsert()) {
             if (access->m_params.IsDummyDeletedRow()) {
                 // Check is sentinel is deleted and CSN is VALID -  ABA problem
                 if (sent->IsCommited() == false) {
                     if (sent->GetData()->GetCommitSequenceNumber() != access->m_tid) {
                         return false;
                     }
                 } else {
                     return false;
                 }
             } else {
                 // We deleted internally!, we only need to check version
                 if (sent->GetData()->GetCommitSequenceNumber() != access->m_tid) {
                     return false;
                 }
             }
         } else {
             // If the sent is committed or inserted-deleted we abort!
             if (sent->IsCommited() or sent->GetData() != nullptr) {
                 return false;
             }
         }
     }
 
     return true;
 }
 
 bool OccTransactionManager::QuickHeaderInsertValidation(const Access* access)
 {
     if (access->m_type == INS) {
         // Lets verify the inserts
         // For upgrade we verify  the row
         // csn has not changed!
         Sentinel* sent = access->m_origSentinel;
         if (access->m_params.IsUpgradeInsert()) {
             if (access->m_params.IsDummyDeletedRow()) {
                 // Check is sentinel is deleted and CSN is VALID -  ABA problem
                 if (sent->IsCommited() == false) {
                     if (sent->GetData()->GetCommitSequenceNumber() != access->m_tid) {
                         return false;
                     }
                 } else {
                     return false;
                 }
             } else {
                 // We deleted internally!, we only need to check version
                 if (sent->GetData()->GetCommitSequenceNumber() != access->m_tid) {
                     return false;
                 }
             }
         } else {
             // If the sent is committed or inserted-deleted we abort!
             if (sent->IsCommited() or sent->GetData() != nullptr) {
                 return false;
             }
         }
     }
     return true;
 }
 
 bool OccTransactionManager::ValidateReadSet(TxnManager* txMan)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     for (const auto& raPair : orderedSet) {
         const Access* ac = raPair.second;
         if (ac->m_type != RD) {
             continue;
         }
         // 最开始锁的sentinel，现在验证也是验的sentinel
 //        if (!ac->GetRowFromHeader()->m_rowHeader.ValidateRead(ac->m_tid) || ac->m_origSentinel->IsLocked()) {
 //            return false;
 //        }
         if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadI(ac->m_tid, 0) || ac->m_origSentinel->IsLocked()) {
             return false;
         }
     }
 
     return true;
 }
 
 bool OccTransactionManager::ValidateWriteSet(TxnManager* txMan)
 {
     // wzy:
     uint64_t currentCSN = txMan->pre_csn;
     std::string csn_tmp = std::to_string(currentCSN) + ":0";
 
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     for (const auto& raPair : orderedSet) {
         const Access* ac = raPair.second;
         if (ac->m_type == RD) {
             continue;
         }
 
         // wzy: 对于上读锁/写锁的数据无法修改
         if (ac->m_type != INS) {
             auto currRow = ac->m_localRow;
             std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
             string res_tid = "";
             if (!MOTAdaptor::IsRowAvailable(tmp_rowid, currentCSN, csn_tmp, res_tid)){
                 if (is_debug_print_enable) MOT_LOG_INFO("ValidateOcc WriteSet IsRowAvailable() not available visited row : %s, cur csn : %s, locked csn : %s",  tmp_rowid.c_str(), csn_tmp.c_str(), res_tid.c_str());
                 return false;
             }
         }
 
         if (!QuickHeaderValidation(ac)) {
             return false;
         }
     }
     return true;
 }
 
 RC OccTransactionManager::LockRows(TxnManager* txMan, uint32_t& numRowsLock)
 {
     RC rc = RC_OK;
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     numRowsLock = 0;
     for (const auto& raPair : orderedSet) {
         const Access* ac = raPair.second;
         if (ac->m_type == RD) {
             continue;
         }
         if (ac->m_params.IsPrimarySentinel()) {
             Row* row = ac->GetRowFromHeader();
             row->m_rowHeader.Lock();
             numRowsLock++;
             MOT_ASSERT(row->GetPrimarySentinel()->IsLocked() == true);
         }
     }
 
     return rc;
 }
 
 //
 bool OccTransactionManager::LockHeadersNoWaitPlor(TxnManager* txMan, uint32_t& numSentinelsLock)
 {
     uint64_t sleepTime = 1;
     uint64_t thdId = txMan->GetThdId();
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     numSentinelsLock = 0;
     while (numSentinelsLock != m_writeSetSize) {
         for (const auto& raPair : orderedSet) {
             const Access* ac = raPair.second;
             if (ac->m_type == RD) {
                 continue;
             }
 
             Sentinel* sent = ac->m_origSentinel;
             if (!sent->TryLock(thdId)) {
                 break;
             }
             numSentinelsLock++;
             if (ac->m_params.IsPrimaryUpgrade()) {
                 ac->m_auxRow->m_rowHeader.Lock();
             }
             // New insert row is already committed!
             // Check if row has changed in sentinel
             // validate insert
             if (!QuickHeaderInsertValidation(ac)) {
                 return false;
             }
         }
 
         if (numSentinelsLock != m_writeSetSize) {
             ReleaseHeaderLocks(txMan, numSentinelsLock);
             numSentinelsLock = 0;
             if (m_preAbort) {
                 for (const auto& acPair : orderedSet) {
                     const Access* ac = acPair.second;
                     if (!QuickHeaderInsertValidation(ac)) {
                         return false;
                     }
                 }
             }
             if (sleepTime > LOCK_TIME_OUT) {
                  return false;
             } else {
                 if (IsHighContention() == false) {
                     CpuCyclesLevelTime::Sleep(5);
                 } else {
                     usleep(m_dynamicSleep);
                 }
                 sleepTime = sleepTime << 1;
             }
         }
     }
     return true;
 }
 
 
 bool OccTransactionManager::LockHeadersNoWait(TxnManager* txMan, uint32_t& numSentinelsLock)
 {
     // wzy:
     uint64_t currentCSN = txMan->pre_csn;
     std::string csn_tmp = std::to_string(currentCSN) + ":0";
 
     uint64_t sleepTime = 1;
     uint64_t thdId = txMan->GetThdId();
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool flag = true;
     numSentinelsLock = 0;
     while (numSentinelsLock != m_writeSetSize) {
         for (const auto& raPair : orderedSet) {
             const Access* ac = raPair.second;
             if (ac->m_type == RD) {
                 continue;
             }
 
             // wzy: 如果已经被WRLock上锁，则不能获取锁
             if (ac->m_type != INS) {
                 auto currRow = ac->m_localRow;
                 std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
                 string res_tid = "";
                 if (!MOTAdaptor::IsRowAvailable(tmp_rowid, currentCSN, csn_tmp, res_tid)) {
                     if (is_debug_print_enable) MOT_LOG_INFO("HeaderLock WriteSet not available visited row : %s, cur csn : %s, locked csn : %s",
                                                 tmp_rowid.c_str(),
                                                 csn_tmp.c_str(),
                                                 res_tid.c_str());
                     MOTAdaptor::Silo_lockheader_abort_by_interactive_num.fetch_add(1);
                     flag = false;
                     break;
                     // return false;           // 直接返回？还是等待下个循环？直到超时？
                 }
             }
 
             Sentinel* sent = ac->m_origSentinel;
             if (!sent->TryLock(thdId)) {
                 break;
             }
             numSentinelsLock++;
             if (ac->m_params.IsPrimaryUpgrade()) {
                 ac->m_auxRow->m_rowHeader.Lock();
             }
             // New insert row is already committed!
             // Check if row has changed in sentinel
             if (!QuickHeaderValidation(ac)) {
                 return false;
             }
         }
 
         if (numSentinelsLock != m_writeSetSize) {
             ReleaseHeaderLocks(txMan, numSentinelsLock);
             numSentinelsLock = 0;
             if (!flag) return false;            // wzy
             if (m_preAbort) {
                 for (const auto& acPair : orderedSet) {
                     const Access* ac = acPair.second;
                     if (!QuickHeaderValidation(ac)) {
                         return false;
                     }
                 }
             }
             if (sleepTime > LOCK_TIME_OUT) {
                 return false;
             } else {
                 if (IsHighContention() == false) {
                     CpuCyclesLevelTime::Sleep(5);
                 } else {
                     usleep(m_dynamicSleep);
                 }
                 sleepTime = sleepTime << 1;
             }
         }
     }
 
     return true;
 }
 
 RC OccTransactionManager::LockHeaders(TxnManager* txMan, uint32_t& numSentinelsLock)
 {
     // wzy: OCC 流程
     uint64_t currentCSN = txMan->pre_csn;
     std::string csn_tmp = std::to_string(currentCSN) + ":0";
 
     RC rc = RC_OK;
     uint64_t thdId = txMan->GetThdId();
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     numSentinelsLock = 0;
     // wzy: 居然是no wait流程
     if (m_validationNoWait) {
         if (!LockHeadersNoWait(txMan, numSentinelsLock)) {
             MOTAdaptor::Silo_lockheader_abort_num.fetch_add(1);
             rc = RC_ABORT;
             goto final;
         }
     } else {
         for (const auto& raPair : orderedSet) {
             const Access* ac = raPair.second;
             if (ac->m_type == RD) {
                 continue;
             }
 
             // wzy: 如果已经被WRLock上锁，则不能获取锁
             if (ac->m_type != INS) {
                 auto currRow = ac->m_localRow;
                 std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
                 string res_tid = "";
                 if (!MOTAdaptor::IsRowAvailable(tmp_rowid, currentCSN, csn_tmp, res_tid)) {
                     if (is_debug_print_enable) MOT_LOG_INFO("HeaderLock WriteSet not available visited row : %s, cur csn : %s, locked csn : %s",
                         tmp_rowid.c_str(),
                         csn_tmp.c_str(),
                         res_tid.c_str());
                     rc = RC_ABORT;
                     goto final;
                 }
             }
 
             Sentinel* sent = ac->m_origSentinel;
             sent->Lock(thdId);
             numSentinelsLock++;
             if (ac->m_params.IsPrimaryUpgrade()) {
                 ac->m_auxRow->m_rowHeader.Lock();
             }
             // New insert row is already committed!
             // Check if row has chained in sentinel
             if (!QuickHeaderValidation(ac)) {
                 rc = RC_ABORT;
                 goto final;
             }
         }
     }
 final:
     return rc;
 }
 
 RC OccTransactionManager::LockHeadersPlor(TxnManager* txMan, uint32_t& numSentinelsLock)
 {
     RC rc = RC_OK;
     uint64_t thdId = txMan->GetThdId();
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     numSentinelsLock = 0;
     if (m_validationNoWait) {
         if (!LockHeadersNoWaitPlor(txMan, numSentinelsLock)) {
             rc = RC_ABORT;
             goto final;
         }
     } else {
         for (const auto& raPair : orderedSet) {
             const Access* ac = raPair.second;
             if (ac->m_type == RD) {
                 continue;
             }
 
             Sentinel* sent = ac->m_origSentinel;
             sent->Lock(thdId);
             numSentinelsLock++;
             if (ac->m_params.IsPrimaryUpgrade()) {
                 ac->m_auxRow->m_rowHeader.Lock();
             }
             // New insert row is already committed!
             // Check if row has chained in sentinel
             // wzy: 只检查insert，无需检查其他
             if (!QuickHeaderInsertValidation(ac)) {
                 rc = RC_ABORT;
                 goto final;
             }
         }
     }
 final:
     return rc;
 }
 
 bool OccTransactionManager::PreAllocStableRow(TxnManager* txMan)
 {
     if (GetGlobalConfiguration().m_enableCheckpoint) {
         GetCheckpointManager()->BeginCommit(txMan);
 
         TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
         for (const auto& raPair : orderedSet) {
             const Access* access = raPair.second;
             if (access->m_type == RD) {
                 continue;
             }
             if (access->m_params.IsPrimarySentinel()) {
                 if (!GetCheckpointManager()->PreAllocStableRow(txMan, access->GetRowFromHeader(), access->m_type)) {
                     GetCheckpointManager()->FreePreAllocStableRows(txMan);
                     GetCheckpointManager()->EndCommit(txMan);
                     return false;
                 }
             }
         }
     }
     return true;
 }
 
 bool OccTransactionManager::QuickVersionCheck(TxnManager* txMan, uint32_t& readSetSize)
 {
     int isolationLevel = txMan->GetTxnIsoLevel();
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     readSetSize = 0;
     for (const auto& raPair : orderedSet) {
         const Access* ac = raPair.second;
         if (ac->m_params.IsPrimarySentinel()) {
             m_rowsSetSize++;
         }
         switch (ac->m_type) {
             case RD_FOR_UPDATE:
             case WR:
                 m_writeSetSize++;
                 break;
             case DEL:
                 m_writeSetSize++;
                 m_deleteSetSize++;
                 break;
             case INS:
                 m_insertSetSize++;
                 m_writeSetSize++;
                 break;
             case RD:
                 if (isolationLevel > READ_COMMITED) {
                     readSetSize++;
                 } else if (cc_mode == 3) {
                     readSetSize++;  // silo SER + PCC
                 } else if (cc_mode == 4) {
                     readSetSize++;  // silo SER + PCC
                 }
                 else {
                     continue;
                 }
                 break;
             default:
                 break;
         }
 
         if (m_preAbort) {
             if (!QuickHeaderValidation(ac)) {
                 return false;
             }
         }
     }
     return true;
 }
 
 bool OccTransactionManager::QuickVersionCheckNoValidation(TxnManager* txMan, uint32_t& readSetSize)
 {
     int isolationLevel = txMan->GetTxnIsoLevel();
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     readSetSize = 0;
     for (const auto& raPair : orderedSet) {
         const Access* ac = raPair.second;
         if (ac->m_params.IsPrimarySentinel()) {
             m_rowsSetSize++;
         }
         switch (ac->m_type) {
             case RD_FOR_UPDATE:
             case WR:
                 m_writeSetSize++;
                 break;
             case DEL:
                 m_writeSetSize++;
                 m_deleteSetSize++;
                 break;
             case INS:
                 m_insertSetSize++;
                 m_writeSetSize++;
                 break;
             case RD:
                 if (isolationLevel > READ_COMMITED) {
                     readSetSize++;
                 } else if (cc_mode == 3) {
                     readSetSize++;  // silo SER + PCC
                 } else if (cc_mode == 4) {
                     readSetSize++;  // silo SER + PCC
                 }
                 else {
                     continue;
                 }
                 break;
             default:
                 break;
         }
     }
     return true;
 }
 
 
 RC OccTransactionManager::ValidateOcc(TxnManager* txMan)
 {
     uint32_t numSentinelLock = 0;
     m_rowsLocked = false;
     TxnAccess* tx = txMan->m_accessMgr.Get();
     RC rc = RC_OK;
     const uint32_t rowCount = tx->m_rowCnt;
 
     m_writeSetSize = 0;
     m_rowsSetSize = 0;
     m_deleteSetSize = 0;
     m_insertSetSize = 0;
     m_txnCounter++;
 
     if (rowCount == 0) {
         // READONLY
         return rc;
     }
 
     uint32_t readSetSize = 0;
     TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
     MOT_ASSERT(rowCount == orderedSet.size());
 
     /* Perform Quick Version check */
     if (!QuickVersionCheck(txMan, readSetSize)) {
         MOTAdaptor::Silo_quick_validation_abort_num.fetch_add(1);
         rc = RC_ABORT;
         goto final;
     }
 
     MOT_LOG_DEBUG("Validate OCC rowCnt=%u RD=%u WR=%u\n", tx->m_rowCnt, tx->m_rowCnt - m_writeSetSize, m_writeSetSize);
 
     // wzy
     rc = LockHeaders(txMan, numSentinelLock);       // 对写集上锁
 
     if (rc != RC_OK) {
         MOT_LOG_DEBUG("Validate OCC LockHeaders failed\n");
         goto final;
     }
 
     // Validate rows in the read set and write set
     if (readSetSize > 0) {
         if (!ValidateReadSet(txMan)) {
             MOTAdaptor::Silo_read_validation_abort_num.fetch_add(1);
             rc = RC_ABORT;
             goto final;
         }
     }
 
     if (!ValidateWriteSet(txMan)) {
         MOTAdaptor::Silo_write_validation_abort_num.fetch_add(1);
         rc = RC_ABORT;
         goto final;
     }
 
     // Pre-allocate stable row according to the checkpoint state.
     if (!PreAllocStableRow(txMan)) {
         rc = RC_MEMORY_ALLOCATION_ERROR;
         goto final;
     }
 
 final:
     if (likely(rc == RC_OK)) {
         txMan->SetTxnState(TxnState::TXN_COMMIT); // HYBRID_CC: Set final state
         MOT_ASSERT(numSentinelLock == m_writeSetSize);
         m_rowsLocked = true;
     } else {
         txMan->SetTxnState(TxnState::TXN_ROLLBACK); // HYBRID_CC: Set final state
         ReleaseHeaderLocks(txMan, numSentinelLock);
         MOTAdaptor::Silo_validation_abort_num.fetch_add(1);
         if (likely(rc == RC_ABORT)) {
             m_abortsCounter++;
         }
     }
 
//     HybridCcLogger::GetInstance().LogTransaction(txMan); // HYBRID_CC: Log transaction details before returning
 
     return rc;
 }
 
 RC OccTransactionManager::ValidateOccPlor(TxnManager* txMan)
 {
     uint32_t numSentinelLock = 0;
     m_rowsLocked = false;
     TxnAccess* tx = txMan->m_accessMgr.Get();
     RC rc = RC_OK;
     const uint32_t rowCount = tx->m_rowCnt;
 
     // 测试前后得出的m_writeSetSize 大小是否一致
     numSentinelLock = m_writeSetSize;
 
     m_writeSetSize = 0;
     m_rowsSetSize = 0;
     m_deleteSetSize = 0;
     m_insertSetSize = 0;
     m_txnCounter++;
 
     if (rowCount == 0) {
         // READONLY
         return rc;
     }
 
     uint32_t readSetSize = 0;
     TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
     MOT_ASSERT(rowCount == orderedSet.size());
 
     /* Perform Quick Version check */
     if (!QuickVersionCheckNoValidation(txMan, readSetSize)) {
         MOTAdaptor::HotRow_quick_validation_abort_num.fetch_add(1);
         rc = RC_ABORT;
         goto final;
     }
 
     if (is_debug_print_enable) MOT_LOG_INFO("Validate OCC for Plor rowCnt=%u RD=%u WR=%u\n", tx->m_rowCnt, tx->m_rowCnt - m_writeSetSize, m_writeSetSize);
 
     // 只对冷数据做读集检查
     if (readSetSize > 0) {
         if (!ValidateReadSetPlor(txMan)) {
             MOTAdaptor::HotRow_read_validation_abort_num.fetch_add(1);
             rc = RC_ABORT;
             goto final;
         }
     }
 
     // 只对冷数据做写集检查
     if (!ValidateWriteSetPlor(txMan)) {
         MOTAdaptor::HotRow_write_validation_abort_num.fetch_add(1);
         rc = RC_ABORT;
         goto final;
     }
 
 final:
     if (likely(rc == RC_OK)) {
         txMan->SetTxnState(TxnState::TXN_COMMIT); // HYBRID_CC: Set final state
         MOT_ASSERT(numSentinelLock == m_writeSetSize);
         m_rowsLocked = true;
     } else {
         txMan->SetTxnState(TxnState::TXN_ROLLBACK); // HYBRID_CC: Set final state
         string tmp_csn = to_string(txMan->pre_csn) + ":0";
         if (is_debug_print_enable) MOT_LOG_INFO("ValidateOccPlor fail tmp_csn : %s, numSentinelLock : %llu, m_writeSetSize : %llu", tmp_csn.c_str(), numSentinelLock, m_writeSetSize);
         MOT_ASSERT(numSentinelLock == m_writeSetSize);
         ReleaseHeaderLocks(txMan, numSentinelLock);     // 直接释放sentinel
         if (likely(rc == RC_ABORT)) {
             m_abortsCounter++;
         }
         m_rowsLocked = false;       // 释放锁
         MOTAdaptor::Switch_validation_occ_abort_num.fetch_add(1);
     }
 
//     HybridCcLogger::GetInstance().LogTransaction(txMan); // HYBRID_CC: Log transaction details before returning
 
     return rc;
 }
 
 // 为冷数据读集检验
 bool OccTransactionManager::ValidateReadSetPlor(TxnManager* txMan)
 {
     uint64_t currentCSN = txMan->pre_csn;
     std::string csn_tmp = std::to_string(currentCSN) + ":0";

     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     for (const auto& raPair : orderedSet) {
         const Access* ac = raPair.second;
         if (ac->m_type != RD) {
             continue;
         }
         if (kHotRow_Active && txMan->hot_rowid_records.count(ac->m_localRow->GetRowId()) != 0) continue;
         if (txMan->read_lock_rowid_records.count(ac->m_localRow->GetRowId()) != 0) continue;

         auto currRow = ac->m_localRow;
         std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());

         if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadI(ac->m_tid, ac->m_server_id) || ac->m_origSentinel->IsLocked()) {
             if (is_debug_print_enable) {
                 uint64_t v = ac->GetRowFromHeader()->m_rowHeader.GetStableCSN();
                 uint64_t e = ac->GetRowFromHeader()->m_rowHeader.GetStableCommitEpoch();
                 bool is_locked = ac->m_origSentinel->IsLocked();
                 bool is_stable_locked = ac->GetRowFromHeader()->m_rowHeader.IsStableLocked();
                 uint64_t stable_server = ac->GetRowFromHeader()->m_rowHeader.GetStableServerId();
                 uint64_t m_cts = ac->m_cts;
                 uint64_t m_tid = ac->m_tid;
                 MOT_LOG_INFO("ValidateReadSetPlor ReadSet failed visited row : %s, cur csn: %s, stable csn: %ld, stable epoch: %ld, m_cts: %ld, m_tid: %ld, is sentinel locked : %ld, is_stable_locked : %ld, stable_server : %ld",
                     tmp_rowid.c_str(), csn_tmp.c_str(), v, e, m_cts, m_tid, is_locked, is_stable_locked, stable_server);
             }
             return false;
         }
     }
     return true;
 }
 
 // 为冷数据进行写集检测
 bool OccTransactionManager::ValidateWriteSetPlor(TxnManager* txMan)
 {
     // wzy:
     uint64_t currentCSN = txMan->pre_csn;
     std::string csn_tmp = std::to_string(currentCSN) + ":0";
 
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     for (const auto& raPair : orderedSet) {
         const Access* ac = raPair.second;
         if (ac->m_type == RD) {
             continue;
         }
         if (ac->m_localRow == nullptr) continue;
 
         if (kHotRow_Active && txMan->hot_rowid_records.count(ac->m_localRow->GetRowId()) != 0) continue;
//         if (txMan->write_lock_rowid_records.count(ac->m_localRow->GetRowId()) != 0) continue;

         if (kHotRow_Active && ac->m_type != INS && txMan->hot_rowid_records.count(ac->m_localRow->GetRowId()) == 0) {
             if (!CheckVersion(ac)) {
                 return false;
             }
         } else if (ac->m_type == INS) {
             if (!QuickHeaderValidation(ac)) {
                 return false;
             }
         }
     }
     return true;
 }
 
 
 void OccTransactionManager::RollbackInserts(TxnManager* txMan)
 {
     return txMan->UndoInserts();
 }
 
 void OccTransactionManager::ApplyWrite(TxnManager* txMan)
 {
     if (GetGlobalConfiguration().m_enableCheckpoint) {
         TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
         for (const auto& raPair : orderedSet) {
             const Access* access = raPair.second;
             if (access->m_type == RD) {
                 continue;
             }
             if (access->m_params.IsPrimarySentinel()) {
                 // Pass the actual global row (access->GetRowFromHeader()), so that the stable row will have the
                 // same CSN, rowid, etc as the original row before the modifications are applied.
                 GetCheckpointManager()->ApplyWrite(txMan, access->GetRowFromHeader(), access->m_type);
             }
         }
     }
 }
 
 // original function
 void OccTransactionManager::WriteChanges(TxnManager* txMan)
 {
     if (m_writeSetSize == 0 && m_insertSetSize == 0) {
         return;
     }
 
     // 对header上锁
     LockRows(txMan, m_rowsSetSize);
 
     // Stable rows for checkpoint needs to be created (copied from original row) before modifying the global rows.
     ApplyWrite(txMan);
 
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
 
     // Update CSN with all relevant information on global rows
     // For deletes invalidate sentinels - rows still locked!
 
     TxnAccess* tx = txMan->m_accessMgr.Get();       // wzy
 
     for (const auto& raPair : orderedSet) {
         const Access* access = raPair.second;
 //        access->GetRowFromHeader()->m_rowHeader.WriteChangesToRow(access, txMan->GetCommitSequenceNumber());
         access->GetRowFromHeader()->m_rowHeader.WriteChangesToRow(access, txMan, tx, txMan->GetCommitSequenceNumber(), 0);  // wzy
     }
 
     // Treat Inserts
     if (m_insertSetSize > 0) {
         for (const auto& raPair : orderedSet) {
             Access* access = raPair.second;
             if (access->m_type != INS) {
                 continue;
             }
             MOT_ASSERT(access->m_origSentinel->IsLocked() == true);
             if (access->m_params.IsUpgradeInsert() == false) {
                 if (access->m_params.IsPrimarySentinel()) {
                     MOT_ASSERT(access->m_origSentinel->IsDirty() == true);
                     // Connect row and sentinel, row is set to absent and locked
                     access->m_origSentinel->SetNextPtr(access->GetRowFromHeader());
                     // Current state: row is set to absent,sentinel is locked and not dirty
                     // Readers will not see the row
                     access->GetTxnRow()->GetTable()->UpdateRowCount(1);
                 } else {
                     // We only set the in the secondary sentinel!
                     access->m_origSentinel->SetNextPtr(access->GetRowFromHeader()->GetPrimarySentinel());
                 }
             } else {
                 MOT_ASSERT(access->m_params.IsUniqueIndex() == true);
                 // Rows are locked and marked as deleted
                 if (access->m_params.IsPrimarySentinel()) {
                     /* Switch the locked row's in the sentinel
                      * The old row is locked and marked deleted
                      * The new row is locked
                      * Save previous row in the access!
                      * We need it for the row release!
                      */
                     Row* row = access->GetRowFromHeader();
                     access->m_localInsertRow = row;
                     access->m_origSentinel->SetNextPtr(access->m_auxRow);
                     // Add row to GC!
                     txMan->GetGcSession()->GcRecordObject(row->GetTable()->GetPrimaryIndex()->GetIndexId(),
                         row,
                         nullptr,
                         Row::RowDtor,
                         ROW_SIZE_FROM_POOL(row->GetTable()));
                 } else {
                     // Set Sentinel for
                     access->m_origSentinel->SetNextPtr(access->m_auxRow->GetPrimarySentinel());
                 }
                 // upgrade should not change the reference count!
                 if (access->m_origSentinel->IsCommited()) {
                     access->m_origSentinel->SetUpgradeCounter();
                 }
             }
         }
     }
 
     // Treat Inserts
     if (m_insertSetSize > 0) {
         for (const auto& raPair : orderedSet) {
             const Access* access = raPair.second;
             if (access->m_type != INS) {
                 continue;
             }
             access->m_origSentinel->UnSetDirty();
         }
     }
 
     CleanRowsFromIndexes(txMan);
 }
 
 void OccTransactionManager::WriteChanges(TxnManager* txMan, uint64_t server_id)
 {
     if (m_writeSetSize == 0 && m_insertSetSize == 0) {
         return;
     }
 
     int lockCnt = 0;
     //ADDBY NEU
     // LockRows(txMan, m_rowsSetSize);
     std::map<MOT::Row*, bool> lock_map;
     lock_map.clear();
     auto csn = txMan->GetCommitSequenceNumber();
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     for (const auto& raPair : orderedSet) {
         const Access* access = raPair.second;
         Row* row = access->GetRowFromHeader();
         AccessType type = access->m_type;
         if (type == RD) {
             continue;
         }
         if(lock_map[row] == false) {
             if(is_full_async_exec == true && row->GetRowHeader()->GetCSN() == csn
                 && row->GetRowHeader()->GetServerId() == server_id){
                 row->GetRowHeader()->Lock();
                 row->GetRowHeader()->LockStable();
             }
             else {
                 row->GetRowHeader()->Lock();
                 row->GetRowHeader()->LockStable();
             }
             lockCnt++;
             lock_map[row] = true;
             if (is_debug_print_enable) MOT_LOG_INFO("pre_csn = %llu csn = %llu Lock & LockStable row = %llu success row stable_csn = %llu stable_csnWord = %llu", txMan->pre_csn, txMan->GetCommitSequenceNumber(), row->GetRowId(), row->GetRowHeader()->GetStableCSN(), row->GetRowHeader()->GetStableCsnWord());
         }
     }
 
 
 
     // Stable rows for checkpoint needs to be created (copied from original row) before modifying the global rows.
     ApplyWrite(txMan);
     TxnAccess* tx = txMan->m_accessMgr.Get();
     // Update CSN with all relevant information on global rows
     // For deletes invalidate sentinels - rows still locked!
 
 
     for (const auto& raPair : orderedSet) {
         const Access* access = raPair.second;
         if (isMVCC_Active) access->GetRowFromHeader()->m_rowHeader.WriteChangesToRow(access, txMan, tx, txMan->GetCommitSequenceNumber(), server_id);  // wzy
         else access->GetRowFromHeader()->m_rowHeader.WriteChangesToRow(access, txMan->GetCommitSequenceNumber(), server_id);
     }
 
     // Treat Inserts
     if (m_insertSetSize > 0) {
         for (const auto& raPair : orderedSet) {
             Access* access = raPair.second;
             if (access->m_type != INS) {
                 continue;
             }
             MOT_ASSERT(access->m_origSentinel->IsLocked() == true);
             if (access->m_params.IsUpgradeInsert() == false) {
                 if (access->m_params.IsPrimarySentinel()) {
                     MOT_ASSERT(access->m_origSentinel->IsDirty() == true);
                     // Connect row and sentinel, row is set to absent and locked
                     access->m_origSentinel->SetNextPtr(access->GetRowFromHeader());
                     // Current state: row is set to absent,sentinel is locked and not dirty
                     // Readers will not see the row
                     access->GetTxnRow()->GetTable()->UpdateRowCount(1);
                 } else {
                     // We only set the in the secondary sentinel!
                     access->m_origSentinel->SetNextPtr(access->GetRowFromHeader()->GetPrimarySentinel());
                 }
             } else {
                 MOT_ASSERT(access->m_params.IsUniqueIndex() == true);
                 // Rows are locked and marked as deleted
                 if (access->m_params.IsPrimarySentinel()) {
                     /* Switch the locked row's in the sentinel
                      * The old row is locked and marked deleted
                      * The new row is locked
                      * Save previous row in the access!
                      * We need it for the row release!
                      */
                     Row* row = access->GetRowFromHeader();
                     access->m_localInsertRow = row;
                     access->m_origSentinel->SetNextPtr(access->m_auxRow);
                     // Add row to GC!
                     txMan->GetGcSession()->GcRecordObject(row->GetTable()->GetPrimaryIndex()->GetIndexId(),
                         row,
                         nullptr,
                         Row::RowDtor,
                         ROW_SIZE_FROM_POOL(row->GetTable()));
                 } else {
                     // Set Sentinel for
                     access->m_origSentinel->SetNextPtr(access->m_auxRow->GetPrimarySentinel());
                 }
                 // upgrade should not change the reference count!
                 if (access->m_origSentinel->IsCommited()) {
                     access->m_origSentinel->SetUpgradeCounter();
                 }
             }
         }
     }
 
     // Treat Inserts
     if (m_insertSetSize > 0) {
         for (const auto& raPair : orderedSet) {
             const Access* access = raPair.second;
             if (access->m_type != INS) {
                 continue;
             }
             access->m_origSentinel->UnSetDirty();
         }
     }
 
     // 清除索引
     CleanRowsFromIndexes(txMan);
         
     for (const auto& raPair : orderedSet) {
         const Access* access = raPair.second;
         Row* row = access->GetRowFromHeader();
         AccessType type = access->m_type;
         if (type == RD) {
             continue;
         }
         if(lock_map[row] == true) {
             if(is_full_async_exec == true && row->GetRowHeader()->GetCSN() == csn 
                 && row->GetRowHeader()->GetServerId() == server_id) {
                 row->GetRowHeader()->ReleaseStable();
                 row->GetRowHeader()->Release();
             }
             else {
                 row->GetRowHeader()->ReleaseStable();
                 if (row->GetRowHeader()->IsStableLocked()) {
                     if (is_debug_print_enable) ("Write changes error pre_csn = %llu csn = %llu row = %llu stable_csn = %llu stable_csnWord = %llu", txMan->pre_csn, txMan->GetCommitSequenceNumber(), row->GetRowId(), row->GetRowHeader()->GetStableCSN(), row->GetRowHeader()->GetStableCsnWord());
                     row->GetRowHeader()->ReleaseStable();
                 }
                 if (is_debug_print_enable) MOT_LOG_INFO("pre_csn = %llu csn = %llu UnLockStable row = %llu success row stable_csn = %llu stable_csnWord = %llu", txMan->pre_csn, txMan->GetCommitSequenceNumber(), row->GetRowId(), row->GetRowHeader()->GetStableCSN(), row->GetRowHeader()->GetStableCsnWord());
                 row->GetRowHeader()->Release();
             }
             lockCnt--;
             lock_map[row] = false;
 //            MOT_LOG_INFO("csn = %llu UnLock & UnLockStable row = %llu success ", txMan->GetCommitSequenceNumber(), row->GetRowId());
         }
     }
 
 
     if (lockCnt != 0) {
         MOT_LOG_INFO("Write changes error lock Unreleased : %d", lockCnt);
     }
 }
 
 

 void OccTransactionManager::CleanRowsFromIndexes(TxnManager* txMan)
 {
     if (m_deleteSetSize == 0) {
         return;
     }
 
     TxnAccess* tx = txMan->m_accessMgr.Get();
     TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
     uint32_t numOfDeletes = m_deleteSetSize;
     // use local counter to optimize
     for (const auto& raPair : orderedSet) {
         const Access* access = raPair.second;
         if (access->m_type == DEL) {
             numOfDeletes--;
             // wzy: rowCnt不修改，不然下次找不到row
 //            access->GetTxnRow()->GetTable()->UpdateRowCount(-1);
             MOT_ASSERT(access->m_params.IsUpgradeInsert() == false);
             // Use Txn Row as row may change INSERT after DELETE leaves residue
             // wzy: 删除时不删index
 //            txMan->RemoveKeyFromIndex(access->GetTxnRow(), access->m_origSentinel);
         }
         if (!numOfDeletes) {
             break;
         }
     }
 }
 
 void OccTransactionManager::ReleaseHeaderLocks(TxnManager* txMan, uint32_t numOfLocks)
 {
     if (numOfLocks == 0) {
         return;
     }
 
     string tmp_csn = to_string(txMan->pre_csn) + ":0";
     if (is_debug_print_enable) MOT_LOG_INFO("ReleaseHeaderLocks tmp_csn : %s, numSentinelLock : %llu, m_writeSetSize : %llu", tmp_csn.c_str(), numOfLocks, m_writeSetSize);
 
     TxnAccess* tx = txMan->m_accessMgr.Get();
     TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
     // use local counter to optimize
     for (const auto& raPair : orderedSet) {
         const Access* access = raPair.second;
         if (access->m_type == RD) {
             continue;
         } else {
             numOfLocks--;
             access->m_origSentinel->Release();
         }
         if (!numOfLocks) {
             break;
         }
     }
 }
 
 void OccTransactionManager::ReleaseHeaderLocksPlor(TxnManager* txMan, uint32_t numOfLocks)
 {
     if (numOfLocks == 0) {
         return;
     }
 
     TxnAccess* tx = txMan->m_accessMgr.Get();
     TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
     // use local counter to optimize
     for (const auto& raPair : orderedSet) {
         const Access* access = raPair.second;
         numOfLocks--;
         access->m_origSentinel->Release();
         if (!numOfLocks) {
             break;
         }
     }
 }
 
 void OccTransactionManager::ReleaseRowsLocks(TxnManager* txMan, uint32_t numOfLocks)
 {
     if (numOfLocks == 0) {
         return;
     }
 
     TxnAccess* tx = txMan->m_accessMgr.Get();
     TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
 
     // use local counter to optimize
     for (const auto& raPair : orderedSet) {
         const Access* access = raPair.second;
         if (access->m_type == RD) {
             continue;
         }
 
         if (access->m_params.IsPrimarySentinel()) {
             numOfLocks--;
             access->GetRowFromHeader()->m_rowHeader.Release();
             if (access->m_params.IsUpgradeInsert()) {
                 // This is the global row that we switched!
                 // Currently it's in the gc!
                 access->m_localInsertRow->m_rowHeader.Release();
             }
         }
         if (!numOfLocks) {
             break;
         }
     }
 }
 
 void OccTransactionManager::CleanUp()
 {
     m_writeSetSize = 0;
     m_insertSetSize = 0;
     m_rowsSetSize = 0;
 }
 
 
 //ADDBY NEU
 
 bool OccTransactionManager::ValidateReadInMerge(TxnManager * txMan, uint32_t server_id){
     if(txMan->GetStartLogicalEpoch() == MOTAdaptor::GetLogicalEpoch() && is_full_async_exec == false) return true;
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     for (const auto &raPair : orderedSet)
     {
         const Access *ac = raPair.second;
         if (ac->m_type == RD)
         {
             // if (!ac->GetRowFromHeader()->m_rowHeader.ValidateRead(ac->m_cts))
             if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadI(ac->m_cts, ac->m_server_id))
             {
                 return false;
             }
         }
     }
     return result;
 }
 
 bool OccTransactionManager::ValidateReadInMergeForSnap(TxnManager * txMan, uint32_t server_id){
     auto start_logical_epoch = txMan->GetStartLogicalEpoch(); 
     if(start_logical_epoch == MOTAdaptor::GetLogicalEpoch() && is_full_async_exec == false) return true;
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     for (const auto &raPair : orderedSet)
     {
         const Access *ac = raPair.second;
         if (ac->m_type == RD)
         {
             // if (!ac->GetRowFromHeader()->m_rowHeader.ValidateRead(ac->m_cts))
             if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadForSnap(ac->m_cts, start_logical_epoch, ac->m_server_id))
             {
                 return false;
             }
         }
     }
     return result;
 }
 
 // wzy
 bool OccTransactionManager::ValidateWriteSetForMVCC(TxnManager * txMan, uint32_t server_id){
     auto start_logical_epoch = txMan->GetStartLogicalEpoch();
     if(start_logical_epoch == MOTAdaptor::GetLogicalEpoch() && is_full_async_exec == false) return true;
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     for (const auto &raPair : orderedSet)
     {
         const Access *ac = raPair.second;
         if (ac->m_type == WR)
         {
             if (!ac->GetRowFromHeader()->m_rowHeader.ValidateWriteForMVCC(ac->m_cts, start_logical_epoch, ac->m_server_id)) {
                 return false;
             }
         }
     }
     return result;
 }
 
 
 void OccTransactionManager::recoverRowHeader(TxnManager * txMan, uint32_t server_id){
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Key* key_ptr;
     MOT::Row* row;
     void* buf;
     currentCSN = txMan->GetCommitSequenceNumber();
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     for (const auto &raPair : orderedSet){
         const Access *ac = raPair.second;
         if (ac->m_type == RD){
             continue;
         }
         if(ac->m_type == INS){
             table_name = ac->m_localInsertRow->GetTable()->GetLongTableName();
             key_ptr = ac->m_localInsertRow->GetTable()->BuildKeyByRow(ac->m_localInsertRow, txMan, buf);
             if(key_ptr == nullptr) assert(false);
             key = key_ptr->GetKeyStr();
             key_temp = table_name + key;
             MOTAdaptor::insertSet.remove(key_temp, csn_temp);
             MOT::MemSessionFree(buf);
             continue;
         }
         if (!ac->GetRowFromHeader()->m_rowHeader.GetCSN() != txMan->GetCommitSequenceNumber()){
             continue; //already modify by other txn , no need to recover
         }
         else{
             ac->GetRowFromHeader()->m_rowHeader.RecoverToStable(); 
         }
     }
 }
 
 RC OccTransactionManager::CommitPhase(TxnManager *txMan, uint32_t server_id)
 {
     bool result = ValidateAndSetWriteSet(txMan, server_id);
     if (result) {
         return RC::RC_OK;
     }
     //是否需要进行recovery
     // recoverRowHeader(txMan,server_id);//txn abort, need to recover the RowHeader
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::ValidateAndSetWriteSet(TxnManager *txMan, uint32_t server_id)//Commit Phase set csn
 {
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
     currentCSN = txMan->GetCommitSequenceNumber();
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     for (const auto &raPair : orderedSet){
         const Access *ac = raPair.second;
         if (ac->m_type == RD){
             continue;
         }
         else if(ac->m_type == INS) { //已经生成了localInsertRow 问题在于先后插入，先插入的已经完成后将dirty置为true
             if(ac->m_localInsertRow->GetTable() == nullptr){
                 result = false;
                 continue;
             }
             table_name = ac->m_localInsertRow->GetTable()->GetLongTableName();
             void* buf;
             MOT::Key* key_ptr = ac->m_localInsertRow->GetTable()->BuildKeyByRow(ac->m_localInsertRow, txMan, buf);
             // MOT::Key* key_ptr = txMan->GetTxnKey(ac->m_localInsertRow->GetTable()->GetPrimaryIndex());
             if(key_ptr == nullptr) assert(false);
             key = key_ptr->GetKeyStr();
             row = ac->GetSentinel()->GetData();
             row = ac->GetSentinel()->GetData();
             if (!(row == nullptr || row->IsAbsentRow())) {
                 result = false;
             }
             key_temp = table_name + key;
             if(!MOTAdaptor::insertSetForCommit.insert(key_temp, csn_temp, &csn_result)){
                 result = false;
             }
             MOTAdaptor::abort_transcation_csn_set.insert(csn_result, csn_result);
             // MOT::MemSessionFree(buf);   
         }
         else{ // update or delete
             if (ac->m_localRow->GetTable() == nullptr){
                 result = false;
                 continue;
             }
             if (ac->m_origSentinel == nullptr || ac->m_origSentinel->IsDirty()) {
                 result = false;
                 if (txMan->IsInteractive()) MOTAdaptor::CommitPhase_origSentinel_abort_interactive_num.fetch_add(1);
                 MOTAdaptor::CommitPhase_origSentinel_abort_num.fetch_add(1);
                 continue;
             }
 
             // wzy: 已被上锁则abort
             auto currRow = ac->m_localRow;
             std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
 
             string res_tid = "";
             if (!MOTAdaptor::IsRowAvailable(tmp_rowid, currentCSN, csn_temp, res_tid)){
                 result = false;
                 MOT_LOG_INFO("Commit Phase IsRowAvailable() not available visited row : %s, cur csn : %s, locked csn : %s",  tmp_rowid.c_str(), csn_temp.c_str(), res_tid.c_str());
                 if (txMan->IsInteractive()) MOTAdaptor::CommitPhase_IsRowAvailable_abort_interactive_num.fetch_add(1);
                 MOTAdaptor::CommitPhase_IsRowAvailable_abort_num.fetch_add(1);
                 continue;
             }
 
             // wzy
             if (kPriority_Active) {
                 if(!ac->GetRowFromHeader()->m_rowHeader.ValidateAndSetWriteForCommitInteractive(txMan->GetCommitSequenceNumber(), txMan->GetStartEpoch(), txMan->GetCommitEpoch(), server_id, txMan->IsInteractive())){
                     if (txMan->IsInteractive()) {
                         MOTAdaptor::CommitPhase_ValidateAndSetWriteForCommit_abort_interactive_num.fetch_add(1);
                         MOT_LOG_INFO("[Abort] ValidateAndSetWriteSet() local validate failed tmp_csn : %s , tmp_rowid : %s ", csn_temp.c_str(), tmp_rowid.c_str());
                     }
                     MOTAdaptor::CommitPhase_ValidateAndSetWriteForCommit_abort_num.fetch_add(1);
                     result = false;
                 }
             } else {
                 if(!ac->GetRowFromHeader()->m_rowHeader.ValidateAndSetWriteForCommit(txMan->GetCommitSequenceNumber(), txMan->GetStartEpoch(), txMan->GetCommitEpoch(), server_id)){
                     if (txMan->IsInteractive()) {
                         MOTAdaptor::CommitPhase_ValidateAndSetWriteForCommit_abort_interactive_num.fetch_add(1);
                         MOT_LOG_INFO("[Abort] ValidateAndSetWriteSet() local validate failed tmp_csn : %s , tmp_rowid : %s ", csn_temp.c_str(), tmp_rowid.c_str());
                     }
                     MOTAdaptor::CommitPhase_ValidateAndSetWriteForCommit_abort_num.fetch_add(1);
                     result = false;
                 }
             }
 //            if (txMan->IsInteractive()){
 //                // 直接设置为交互型事务的csn serverid
 //                ac->GetRowFromHeader()->m_rowHeader.InteractiveSetWriteForCommit(txMan->GetCommitSequenceNumber(), txMan->GetStartEpoch(), txMan->GetCommitEpoch(), server_id);
 //                continue;
 //            }
 
             /////////////
         }
     }
     if(result == false){
         MOTAdaptor::abort_transcation_csn_set.insert(csn_temp, csn_temp);
     } 
     return result;
 }
 
 
 RC OccTransactionManager::CommitLockCheck(TxnManager *txMan, uint32_t server_id)
 {
     bool result = ValidateLockWriteSet(txMan, server_id);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 
 bool OccTransactionManager::ValidateLockWriteSet(TxnManager *txMan, uint32_t server_id)//Commit Phase set csn
 {
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
     currentCSN = txMan->GetCommitSequenceNumber();
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     for (const auto &raPair : orderedSet){
         const Access *ac = raPair.second;
         if (ac->m_type == RD || INS) {
             continue;
         }
         else{ // update or delete
             if (ac->m_localRow->GetTable() == nullptr){
                 result = false;
                 break;
             }
             // wzy: 已被上锁则abort
             auto currRow = ac->m_localRow;
             std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
             string res_tid = "";
             if (!MOTAdaptor::IsRowAvailable(tmp_rowid, currentCSN, csn_temp, res_tid)){
                 if (is_debug_print_enable)  MOT_LOG_INFO("IsRowAvailable() not available visited row : %s, cur csn : %s, locked csn : %s",  tmp_rowid.c_str(), csn_temp.c_str(), res_tid.c_str());
                 MOTAdaptor::CommitLockCheck_IsRowAvailable_abort_num.fetch_add(1);
                 result = false;
                 break;
             }
         }
     }
     return result;
 }
 
 
 /////////////////////// Plor /////////////////////
 RC OccTransactionManager::WritePhasePlor(TxnManager* txMan, uint32_t server_id, void* currRow)
 {
     bool result = GetWriteLockPlor(txMan, server_id, currRow);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetWriteLockPlor(TxnManager* txMan, uint32_t server_id, void* row)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(txMan->pre_csn) + ":" + to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> new_queue = nullptr;
     auto currRow = static_cast<Row*>(row);
 
     if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) {
         if (!txMan->abort_) {
             txMan->abort_ = true;
             MOTAdaptor::WriteLock_pcc_abort_num.fetch_add(1);
         }
         if (is_debug_print_enable) MOT_LOG_INFO("GetWriteLockPlor() LockRD [abort] because of wound_wait tmp_csn : %s, tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
         return false;
     }

     auto table = currRow->GetTable();
     if (table == nullptr) {
         result = false;
         return result;
     }
     table_name = table->GetLongTableName();
     rowId = currRow->GetRowId();
     tmp_rowid = table_name + ":" + to_string(rowId);
 
     if (kHotRow_Active && txMan->hot_rowid_records.count(rowId) == 0) return true;

     // 提前检验是否上过锁
//     if (txMan->write_lock_rowid_records.count(rowId) != 0) return true;

//     auto [it, inserted] = txMan->write_lock_rowid_records.emplace(rowId);
//     if (inserted) {
//         MOT_LOG_INFO("Inserted rowid: %llu address: %p", *it, &txMan->write_lock_rowid_records);
//     } else {
//         MOT_LOG_INFO("Rowid already exists: %llu",*it);
//     }
//     if (txMan->write_lock_rowid_records.count(rowId) == 0) {
//         MOT_LOG_INFO("GetWriteLockPlor() continue row is not Wlocked tmp_csn : %s , rowid : %llu write_lock_rowid_records address: %p", csn_temp.c_str(), rowId, &txMan->write_lock_rowid_records);
//     }
 
     if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue){    // 未找到则创建新lock request
         new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
     }
 
     auto time1 = now_to_us();
     if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue){
         auto res = tmp_queue->LockWR(tmp_rowid, txMan, server_id);
         if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) {
             if (!txMan->abort_) {
                 txMan->abort_ = true;
                 MOTAdaptor::WriteLock_pcc_abort_num.fetch_add(1);
             }
             return false;
         }
         // 添加到新的 epoch lock request queue 中，没有取模
         // MOTAdaptor::AddEpochActiveQueue(tmp_queue, MOTAdaptor::GetLogicalEpoch());
         // MOTAdaptor::AddActiveQueue(tmp_queue, rowId);
         if (!res) {
             if (is_debug_print_enable) MOT_LOG_INFO("GetWriteLockPlor() LockWR [failed] because of wound_wait tmp_csn : %s, tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             if (!txMan->abort_) {
                 txMan->abort_ = true;
                 MOTAdaptor::WriteLock_pcc_abort_num.fetch_add(1);
             }
             return false;
         } else {
             // 插入csn + queue
             MOTAdaptor::txn_total_lockCnt.fetch_add(1);
             MOTAdaptor::local_lock_num.fetch_add(1);
 //            MOTAdaptor::AddCsnRequestQueue(tmp_csn, tmp_queue);
             if (is_debug_print_enable) MOT_LOG_INFO("GetWriteLockPlor() LockWR tmp_csn : %s , epoch : %llu , pre_csn : %llu , tmp_rowid : %s ", tmp_csn.c_str(), MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1), txMan->pre_csn, tmp_rowid.c_str());
         }
     }
     auto time2 = now_to_us();
     MOTAdaptor::txn_total_write_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_total_write_lockCnt.fetch_add(1);
 
     MOTAdaptor::txn_temp_total_write_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_temp_total_write_lockCnt.fetch_add(1);
 
     new_queue.reset();
     tmp_queue = nullptr;
     return result;
 }
 
 RC OccTransactionManager::ReadPhasePlor(TxnManager* txMan, uint32_t server_id, void* currRow)
 {
     bool result = GetReadLockPlor(txMan, server_id, currRow);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetReadLockPlor(TxnManager* txMan, uint32_t server_id, void* row)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(txMan->pre_csn) + ":" + to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> new_queue = nullptr;
     auto currRow = static_cast<Row*>(row);
 
     if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) {
         if (!txMan->abort_) {
             txMan->abort_ = true;
             MOTAdaptor::ReadLock_pcc_abort_num.fetch_add(1);
         }
         if (is_debug_print_enable) MOT_LOG_INFO("GetReadLockPlor() LockRD [abort] because of wound_wait tmp_csn : %s, tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
         return false;
     }
 
     if (kHotRow_Active && txMan->hot_rowid_records.count(currRow->GetRowId()) == 0) return true;
     // 提前检验是否上过锁
//     if (txMan->read_lock_rowid_records.count(currRow->GetRowId()) != 0) return true;
     txMan->read_lock_rowid_records.emplace(currRow->GetRowId());

     auto table = currRow->GetTable();
     if (table == nullptr) {
         result = false;
         return result;
     }
     table_name = table->GetLongTableName();
     rowId = currRow->GetRowId();
     tmp_rowid = table_name + ":" + to_string(rowId);
 
     if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue){    // 未找到则创建新lock request
         new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
     }
 
     auto time1 = now_to_us();
     if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue){
         auto res = tmp_queue->LockRD(tmp_rowid, txMan, server_id, false);
//         if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) {
//             if (!txMan->abort_) {
//                 txMan->abort_ = true;
//                 MOTAdaptor::ReadLock_pcc_abort_num.fetch_add(1);
//             }
//             if (is_debug_print_enable) MOT_LOG_INFO("GetReadLockPlor() LockRD [abort] because of wound_wait tmp_csn : %s, tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
//             return false;
//         }
         // 添加到新的 epoch lock request queue 中，没有取模
         // MOTAdaptor::AddEpochActiveQueue(tmp_queue, MOTAdaptor::GetLogicalEpoch());
 //        MOTAdaptor::AddActiveQueue(tmp_queue, rowId);
         if (!res) {
             if (is_debug_print_enable) MOT_LOG_INFO("GetReadLockPlor() LockRD [failed] because of wound_wait tmp_csn : %s, tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             if (!txMan->abort_) {
                 txMan->abort_ = true;
                 MOTAdaptor::ReadLock_pcc_abort_num.fetch_add(1);
             }
             return false;
         } else {
             // 插入csn + queue
             MOTAdaptor::txn_total_lockCnt.fetch_add(1);
             MOTAdaptor::local_lock_num.fetch_add(1);
             if (is_debug_print_enable) MOT_LOG_INFO("GetReadLockPlor() LockRD tmp_csn : %s , epoch : %llu , pre_csn : %llu , tmp_rowid : %s ", tmp_csn.c_str(), MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1), txMan->pre_csn, tmp_rowid.c_str());
         }
     }
     auto time2 = now_to_us();
     MOTAdaptor::txn_total_read_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_total_read_lockCnt.fetch_add(1);
 
     MOTAdaptor::txn_temp_total_read_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_temp_total_read_lockCnt.fetch_add(1);
 
     new_queue.reset();
     tmp_queue = nullptr;
     // wzy : 一次只发送一个lockinfo
     return result;
 }
 
 //// Switch phase
 RC OccTransactionManager::SwitchReadPhasePlor(TxnManager* txMan, uint32_t server_id, bool hot_rows) {
     bool result = true;
     if (cc_mode == 2) result = GetSwitchReadLockPlor(txMan, server_id);
     else if (cc_mode == 4) result = GetSwitchReadLockPlorPrevRLock(txMan, server_id, hot_rows);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetSwitchReadLockPlorPrevRLock(MOT::TxnManager* txMan, uint32_t server_id, bool hot_rows)
 {
     // 对读集上锁
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
 //    currentCSN = txMan->GetCommitSequenceNumber();
     currentCSN = txMan->pre_csn;
     auto start_logical_epoch = txMan->GetStartLogicalEpoch();
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr, new_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
     // 尝试锁住sentinel
     uint64_t thdId = txMan->GetThdId();
 
     for (const auto &raPair : orderedSet){
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
             MOTAdaptor::ReadLock_switch_pcc_abort_num.fetch_add(1);
             return false;
         }
         const Access *ac = raPair.second;
         Row* row_from_header = ac->GetRowFromHeader();
         if (ac->m_type == RD){
             if (ac->m_localRow->GetTable() == nullptr) {
                 result = false;
                 break;
             }
 
             auto table = ac->m_localRow->GetTable();
             auto currRow = ac->m_localRow;

             // 提前检验是否上过锁
//             if (txMan->read_lock_rowid_records.count(currRow->GetRowId()) != 0) {
//                 continue;
//             }
 
             uint64_t start_time = now_to_us();
 
             // RLock
             if (!hot_rows) {
                 txMan->read_lock_rowid_records.emplace(currRow->GetRowId());           // 添加read lock set
                 std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
                 if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) ||
                     !tmp_queue) {  // 未找到则创建新lock request
                     new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
                 }
                 if (MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue) {
                     auto res = tmp_queue->LockRD(tmp_rowid, txMan, server_id, true);
                      if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
                          if (!txMan->abort_) {
                              txMan->abort_ = true;
                              MOTAdaptor::ReadLock_pcc_abort_num.fetch_add(1);
                          }
                          if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchReadLockPlorPrevRLock() LockRD [abort] because of wound_wait tmp_csn : %s, tmp_rowid : %s ", csn_temp.c_str(), tmp_rowid.c_str());
                          result = false;
                      }
                     if (!res) {
                         if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchReadLockPlorPrevRLock() LockRD [error] tmp_csn : %s  , tmp_rowid : %s",
                                 csn_temp.c_str(),
                                 tmp_rowid.c_str());
                         MOTAdaptor::ReadLock_switch_pcc_abort_num.fetch_add(1);
                         result = false;
                     } else {
                         if (is_debug_print_enable) MOT_LOG_INFO(
                                 "GetSwitchReadLockPlorPrevRLock() LockRD tmp_csn : %s , epoch : %llu , tmp_rowid : %s",
                                 csn_temp.c_str(),
                                 epoch_mod,
                                 tmp_rowid.c_str());
                     }
                 } else {
                     // do nothing
                 }
             }
             else if (hot_rows && txMan->hot_rowid_records.count(currRow->GetRowId()) != 0) {
                 txMan->read_lock_rowid_records.emplace(currRow->GetRowId());           // 添加read lock set
                 std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
                 if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) ||
                     !tmp_queue) {  // 未找到则创建新lock request
                     new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
                 }
                 if (MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue) {
                     auto res = tmp_queue->LockRD(tmp_rowid, txMan, server_id, true);
                     if (!res) {
                         if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchReadLockPlorPrevRLock() LockRD [error] tmp_csn : %s  , tmp_rowid : %s",
                             csn_temp.c_str(),
                             tmp_rowid.c_str());
                         MOTAdaptor::ReadLock_switch_pcc_abort_num.fetch_add(1);
                         result = false;
                     } else {
                         if (is_debug_print_enable) MOT_LOG_INFO(
                             "GetSwitchReadLockPlorPrevRLock() LockRD tmp_csn : %s , epoch : %llu , tmp_rowid : %s",
                             csn_temp.c_str(),
                             epoch_mod,
                             tmp_rowid.c_str());
                     }
                 } else {
                     // do nothing
                 }
             }
 
             MOTAdaptor::txn_total_switchTime_RLock.fetch_add(now_to_us() - start_time);
             MOTAdaptor::txn_total_switchTime_RLockCnt.fetch_add(1);
 
             if (!result) return false;
 
             start_time = now_to_us();
             // LOCK sentinel
             ac->m_origSentinel->Lock(thdId);
 
 
             // Validation
             if (!isMVCC_Active) {
                 if (is_snap_isolation) {
                     if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadForSnap_Switching(ac->m_cts, start_logical_epoch, ac->m_server_id)) result = false;
                 } else if (is_read_repeatable) {
                     if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadI_Switching(ac->m_cts, ac->m_server_id)) result = false;
                 }
             }
 
             // UNLOCK
             ac->m_origSentinel->Release();
 
             MOTAdaptor::txn_total_switchTime_Sentinel.fetch_add(now_to_us() - start_time);
             MOTAdaptor::txn_total_switchTime_SentinelCnt.fetch_add(1);
 
             if (!result) {
                 MOTAdaptor::Switch_validation_abort_num.fetch_add(1);
                 return false;
             }
         }
     }
 
     return result;
 }
 
 // plor + CRDT 上锁顺序有问题
 bool OccTransactionManager::GetSwitchReadLockPlor(TxnManager* txMan, uint32_t server_id) {
     // 对读集上锁
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
     currentCSN = txMan->pre_csn;
     auto start_logical_epoch = txMan->GetStartLogicalEpoch();
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr, new_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
     for (const auto &raPair : orderedSet){
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
             MOTAdaptor::ReadLock_switch_pcc_abort_num.fetch_add(1);
             return false;
         }
         const Access *ac = raPair.second;
         Row* row_from_header = ac->GetRowFromHeader();
         if (ac->m_type == RD){
             if (ac->m_localRow->GetTable() == nullptr) {
                 result = false;
                 break;
             }
 
             if (!result) return false;
 
             // LOCK
             row_from_header->GetRowHeader()->Lock();
             row_from_header->GetRowHeader()->LockStable();
 
             // Validation
             if (!isMVCC_Active) {
                 if (is_snap_isolation) {
                     if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadForSnap_Switching(ac->m_cts, start_logical_epoch, ac->m_server_id)) result = false;
                 } else if (is_read_repeatable) {
                     if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadI_Switching(ac->m_cts, ac->m_server_id)) result = false;
                 }
             }

 
             // RLock
             if (result) {
                 auto table = ac->m_localRow->GetTable();
                 auto currRow = ac->m_localRow;
                 // 提前检验是否上过锁
//                 if (txMan->read_lock_rowid_records.count(currRow->GetRowId()) != 0) {
//                     continue;
//                 }
                 txMan->read_lock_rowid_records.emplace(currRow->GetRowId());           // 添加read lock set
                 std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
                 if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue) {    // 未找到则创建新lock request
                     new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
                 }
 
                 if (is_debug_print_enable) MOT_LOG_INFO("Before GetSwitchReadLockPlor() LockRD tmp_csn : %s , epoch : %llu , tmp_rowid : %s", csn_temp.c_str(), epoch_mod, tmp_rowid.c_str());
 
                 if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue) {
                     auto res = tmp_queue->LockRD(tmp_rowid, txMan, server_id, true);
                     if(!res) {
                         if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchReadLockPlor() LockRD [error] tmp_csn : %s  , tmp_rowid : %s", csn_temp.c_str(), tmp_rowid.c_str());
                         MOTAdaptor::ReadLock_switch_pcc_abort_num.fetch_add(1);
                         result = false;
                     } else {
                         if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchReadLockPlor() LockRD tmp_csn : %s , epoch : %llu , tmp_rowid : %s", csn_temp.c_str(), epoch_mod, tmp_rowid.c_str());
                     }
                 } else {
                     // do nothing
                 }
             }
 
             // UNLOCK
             row_from_header->GetRowHeader()->ReleaseStable();
             row_from_header->GetRowHeader()->Release();
 
             if (!result) return false;
         }
     }
 
     return result;
 }
 
 RC OccTransactionManager::SwitchWritePhasePlor(TxnManager* txMan, uint32_t server_id, bool hot_rows) {
     bool result = GetSwitchWriteLockPlor(txMan, server_id, hot_rows);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetSwitchWriteLockPlor(TxnManager* txMan, uint32_t server_id, bool hot_rows) {
     // 对写集上锁
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
     currentCSN = txMan->pre_csn;
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr, new_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
     for (const auto &raPair : orderedSet){
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
             if (!txMan->abort_) {
                 txMan->abort_ = true;
                 MOTAdaptor::WriteLock_switch_pcc_abort_num.fetch_add(1);
             }
             return false;
         }
         const Access *ac = raPair.second;
         if (ac->m_type == WR) {

             if (ac->m_localRow->GetTable() == nullptr) {
                 result = false;
                 break;
             }
             auto table = ac->m_localRow->GetTable();
             auto currRow = ac->m_localRow;
             rowId = currRow->GetRowId();

             // 提前检验是否上过锁
//             if (txMan->write_lock_rowid_records.count(rowId) != 0) {
//                 continue;
//             }
 
             // 统计时是根据GetTableName
 //            std::string tmp_rowid = currRow->GetTable()->GetTableName() + ":" + to_string(currRow->GetRowId());
             std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
             // 只对热数据项上锁
             if (hot_rows && txMan->hot_rowid_records.count(rowId) == 0) continue;
//             auto [it, inserted] = txMan->write_lock_rowid_records.emplace(rowId);
//             if (inserted) {
//                 MOT_LOG_INFO("Inserted rowid: %llu address: %p", *it, &txMan->write_lock_rowid_records);
//             } else {
//                 MOT_LOG_INFO("Rowid already exists: %llu",*it);
//             }
//             if (txMan->write_lock_rowid_records.count(rowId) == 0) {
//                 MOT_LOG_INFO("GetSwitchWriteLockPlor() continue row is not Wlocked tmp_csn : %s , rowid : %llu write_lock_rowid_records address: %p", csn_temp.c_str(), rowId, &txMan->write_lock_rowid_records);
//             }


             if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue){    // 未找到则创建新lock request
                 new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
             }
 
             if (is_debug_print_enable) MOT_LOG_INFO("[Before] GetSwitchWriteLockPlor() LockWR tmp_csn : %s , epoch : %llu , tmp_rowid : %s", csn_temp.c_str(), epoch_mod, tmp_rowid.c_str());
 
             uint64_t start_time = now_to_us();
 
             if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue){
                 auto res = tmp_queue->LockWR(tmp_rowid, txMan, server_id);
                 if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
                     if (!txMan->abort_) {
                         txMan->abort_ = true;
                         MOTAdaptor::WriteLock_switch_pcc_abort_num.fetch_add(1);
                     }
                     return false;
                 }
                 if(!res) {
                     if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchWriteLockPlor() LockWR [error] tmp_csn : %s  , tmp_rowid : %s", csn_temp.c_str(), tmp_rowid.c_str());
                     if (!txMan->abort_) {
                         txMan->abort_ = true;
                         MOTAdaptor::WriteLock_switch_pcc_abort_num.fetch_add(1);
                     }
                     return false;
                 } else {
                     if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchWriteLockPlor() LockWR tmp_csn : %s , epoch : %llu , tmp_rowid : %s", csn_temp.c_str(), epoch_mod, tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchWriteLockPlor() LockWR not found tmp_csn : %s  , tmp_rowid : %s ", csn_temp.c_str(), tmp_rowid.c_str());
             }
 
             MOTAdaptor::txn_total_switchTime_WLock.fetch_add(now_to_us() - start_time);
             MOTAdaptor::txn_total_switchTime_WLockCnt.fetch_add(1);
         }
     }
     return result;
 }
 
 //// end Switch phase
 
 RC OccTransactionManager::ValidationPhasePlor(TxnManager *txMan, uint32_t server_id)
 {
     bool result = ValidateReadWriteConflict(txMan, server_id);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::ValidateReadWriteConflict(TxnManager* txMan, uint32_t server_id)
 {
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
     currentCSN = txMan->pre_csn;
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
     m_writeSetSize = 0;
     auto time1 = now_to_us();
     for (const auto &raPair : orderedSet){
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) return false;
         const Access *ac = raPair.second;
         if (ac->m_type == WR){
             if (ac->m_localRow->GetTable() == nullptr) {
                 result = false;
                 break;
             }
 
             auto table = ac->m_localRow->GetTable();
             auto currRow = ac->m_localRow;
             m_writeSetSize++;       // 统计write大小?
 
             // 非热数据，跳过
             if (kHotRow_Active && txMan->hot_rowid_records.count(currRow->GetRowId()) == 0) {
                 MOT_LOG_INFO("ValidateWR() continue row is not Wlocked tmp_csn : %s , rowid : %llu ", csn_temp.c_str(), rowId);
                 continue;
             }

             table_name = table->GetLongTableName();
             rowId = ac->m_localRow->GetRowId();
             tmp_rowid = table_name + ":" + to_string(rowId);

//             if (txMan->write_lock_rowid_records.count(rowId) == 0) {
//                 MOT_LOG_INFO("ValidateWR() continue row is not Wlocked tmp_csn : %s , rowid : %llu address : %p", csn_temp.c_str(), rowId, &txMan->write_lock_rowid_records);
//                 continue;
//             }


             tmp_queue = nullptr;
             if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
                 auto res = tmp_queue->ValidateWR(tmp_rowid, txMan, server_id);      // wait
                 if(!res) {
                     if (is_debug_print_enable) MOT_LOG_INFO("ValidateWR() lock_request_queue [error] tmp_csn : %s  , tmp_rowid : %s", csn_temp.c_str(), tmp_rowid.c_str());
                     return false;
                 } else {
                     if (is_debug_print_enable) MOT_LOG_INFO("ValidateWR() pass tmp_csn : %s , epoch : %llu , pre_csn : %llu , tmp_rowid : %s", csn_temp.c_str(), epoch_mod, txMan->pre_csn, tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 if (is_debug_print_enable) MOT_LOG_INFO("ValidateWR() lock_request_queue not found tmp_csn : %s , tmp_rowid : %s ", csn_temp.c_str(), tmp_rowid.c_str());
             }
 
         }
     }
     auto time2 = now_to_us();
     MOTAdaptor::txn_total_validate_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_total_validate_lockCnt.fetch_add(1);
 
     MOTAdaptor::txn_temp_total_validate_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_temp_total_validate_lockCnt.fetch_add(1);
 
     return result;
 }
 
 RC OccTransactionManager::UnlockReadWriteLockPlor(TxnManager* txMan, uint32_t server_id, uint64_t csn, bool abort)
 {
     bool result = UnlockReadWriteRowPlor(txMan, server_id, csn, abort);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::UnlockReadWriteRowPlor(TxnManager* txMan, uint32_t server_id, uint64_t csn, bool abort)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(csn) + ":" + to_string(server_id);
     string res_csn = "";
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
     csn_temp = to_string(txMan->pre_csn) + ":" + to_string(server_id);

     std::string write_records = "";
//     for (auto v : txMan->write_lock_rowid_records) {
//         write_records += (std::to_string(v) + " ");
//     }
//     if (is_debug_print_enable) MOT_LOG_INFO("UnlockReadWriteSet() write_lock_rowid_records : %s", write_records.c_str());


 //    if (abort) MOT_LOG_INFO("UnlockWriteSet() txn [Abort] tmp_csn : %s", tmp_csn.c_str());
     for (const auto& raPair : orderedSet) {
         // orderedSet中重复的read会被write覆盖
         const Access* ac = raPair.second;
         if (ac->m_type == WR) {
             auto table = ac->m_localRow->GetTable();
             if (table == nullptr) {
                 result = false;
                 continue;
             }

             table_name = table->GetLongTableName();
             rowId = ac->m_localRow->GetRowId();
             res_csn = "";
             tmp_rowid = table_name + ":" + to_string(rowId);

             // 非热数据，跳过
//             if (kHotRow_Active && txMan->hot_rowid_records.count(rowId) == 0) {
//                 MOT_LOG_INFO("UnlockReadWriteRowPlor() continue row is not Wlocked tmp_csn : %s , rowid : %llu ", csn_temp.c_str(), rowId);
//                 continue;
//             }
//
//             if (txMan->write_lock_rowid_records.count(rowId) == 0) {
//                 MOT_LOG_INFO("UnlockReadWriteRowPlor() continue row is not Wlocked tmp_csn : %s , rowid : %llu write_lock_rowid_records address: %p", csn_temp.c_str(), rowId, &txMan->write_lock_rowid_records);
//                 continue;
//             }

             tmp_queue = nullptr;
 
             if (csn == 0) {
                 if (is_debug_print_enable) MOT_LOG_INFO("UnlockWR() [error] start_time : %llu, pre_csn : %llu, csn : %llu", txMan->start_time, txMan->pre_csn, csn);
                 csn = txMan->start_time;
             }
 
             if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
                 auto res = tmp_queue->UnlockWR(tmp_rowid, csn, res_csn, server_id);
                 MOTAdaptor::local_unlock_num.fetch_add(1);
                 if(abort) {
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockWR() abort lock_request_queue grant_csn : %s, tmp_csn : %s  , tmp_rowid : %s", res_csn.c_str(), tmp_csn.c_str(), tmp_rowid.c_str());
                 } else {
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockWR() unlock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s", tmp_csn.c_str(), epoch_mod, tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 if (is_debug_print_enable) MOT_LOG_INFO("UnlockWriteSet() lock_request_queue not found tmp_csn : %s  , tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             }
 
             tmp_queue = nullptr;
         } else if (ac->m_type == RD) {
             auto table = ac->m_localRow->GetTable();
             if (table == nullptr) {
                 result = false;
                 continue;
             }

             table_name = table->GetLongTableName();
             rowId = ac->m_localRow->GetRowId();
             res_csn = "";
             tmp_rowid = table_name + ":" + to_string(rowId);

             // 非热数据，跳过
//             if (kHotRow_Active && txMan->hot_rowid_records.count(rowId) == 0) {
//                 MOT_LOG_INFO("UnlockReadWriteRowPlor() continue row is not Rlocked tmp_csn : %s , rowid : %llu ", csn_temp.c_str(), rowId);
//                 continue;
//             }
//             if (txMan->read_lock_rowid_records.count(rowId) == 0) {
//                 MOT_LOG_INFO("UnlockReadWriteRowPlor() continue row is not Rlocked tmp_csn : %s , rowid : %llu ", csn_temp.c_str(), rowId);
//                 continue;
//             }

             tmp_queue = nullptr;
             if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
                 auto res = tmp_queue->UnlockRD(tmp_rowid, csn);
 //                MOTAdaptor::RemoveActiveQueue(tmp_queue, rowId);    // 移除活跃队列
                 MOTAdaptor::local_unlock_num.fetch_add(1);
                 if(abort) {
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockRD() abort lock_request_queue grant_csn : %s, tmp_csn : %s  , tmp_rowid : %s", res_csn.c_str(), tmp_csn.c_str(), tmp_rowid.c_str());
                 } else{
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockRD() unlock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s", tmp_csn.c_str(), epoch_mod, tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 if (is_debug_print_enable) MOT_LOG_INFO("UnlockReadSet() lock_request_queue not found tmp_csn : %s  , tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             }
         } else
             continue;
     }
     MOTAdaptor::deadlock_abort_set.remove(tmp_csn);
     //    MOTAdaptor::csn_requests_map.remove(tmp_csn);           // 清除tid对应的上锁队列指针
 //    MOTAdaptor::txn_rowid_map.remove(tmp_csn);
     return result;
 }

 bool OccTransactionManager::BoostPriority(TxnManager* txMan, uint32_t server_id)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(txMan->pre_csn) + ":" + to_string(server_id);
     string res_csn = "";
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
     csn_temp = to_string(txMan->pre_csn) + ":" + to_string(server_id);
     std::string write_records = "";
     for (const auto& raPair : orderedSet) {
         const Access* ac = raPair.second;
         if (ac->m_type == WR || ac->m_type == RD) {
             auto table = ac->m_localRow->GetTable();
             if (table == nullptr) {
                 result = false;
                 continue;
             }

             table_name = table->GetLongTableName();
             rowId = ac->m_localRow->GetRowId();
             res_csn = "";
             tmp_rowid = table_name + ":" + to_string(rowId);
             tmp_queue = nullptr;

             if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
                 auto res = tmp_queue->BoostPriority(txMan, txMan->GetCurrentPriority(), server_id);
                 if(!res) {
                     if (is_debug_print_enable) MOT_LOG_INFO("BoostPriority() failed lock_request_queue grant_csn : %s, tmp_csn : %s  , tmp_rowid : %s", res_csn.c_str(), tmp_csn.c_str(), tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 if (is_debug_print_enable) MOT_LOG_INFO("BoostPriority() lock_request_queue not found tmp_csn : %s  , tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             }
             tmp_queue = nullptr;
         } else
             continue;
     }
     return result;
 }
 
 
 /////////////////////// Wound-wait /////////////////////
 
 RC OccTransactionManager::WritePhaseWoundWait(TxnManager* txMan, uint32_t server_id, void* currRow)
 {
     bool result = GetWriteLockWoundWait(txMan, server_id, currRow);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetWriteLockWoundWait(TxnManager* txMan, uint32_t server_id, void* row)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(txMan->pre_csn) + ":" + to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> new_queue = nullptr;
     auto currRow = static_cast<Row*>(row);
 
     if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) {
         if (!txMan->abort_) {
             txMan->abort_ = true;
             MOTAdaptor::WriteLock_pcc_abort_num.fetch_add(1);
         }
         return false;
     }
 
     auto table = currRow->GetTable();
     if (table == nullptr) {
         result = false;
         return result;
     }
     table_name = table->GetLongTableName();
     rowId = currRow->GetRowId();
     tmp_rowid = table_name + ":" + to_string(rowId);
 
     if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue){    // 未找到则创建新lock request
         new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
     }
 
     auto time1 = now_to_us();
     if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue){
         auto res = tmp_queue->LockWR_WoundWait(tmp_rowid, txMan, server_id);
         if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) {
             if (!txMan->abort_) {
                 txMan->abort_ = true;
                 MOTAdaptor::WriteLock_pcc_abort_num.fetch_add(1);
             }
             return false;
         }
         // 添加到新的 epoch lock request queue 中，没有取模
         // MOTAdaptor::AddEpochActiveQueue(tmp_queue, MOTAdaptor::GetLogicalEpoch());
         // MOTAdaptor::AddActiveQueue(tmp_queue, rowId);
         if (!res) {
             if (is_debug_print_enable) MOT_LOG_INFO("GetWriteLockWoundWait() lock_row_local [failed] because of wound_wait tmp_csn : %s, tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             if (!txMan->abort_) {
                 txMan->abort_ = true;
                 MOTAdaptor::WriteLock_pcc_abort_num.fetch_add(1);
             }
             // 移除等待图
             MOTAdaptor::wait_for_graph.removeNode(csn_temp);
             return false;
         } else {
             // 插入csn + queue
             MOTAdaptor::txn_total_lockCnt.fetch_add(1);
             MOTAdaptor::local_lock_num.fetch_add(1);
 //            MOTAdaptor::AddCsnRequestQueue(tmp_csn, tmp_queue);
             if (is_debug_print_enable) MOT_LOG_INFO("GetWriteLockWoundWait() lock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s ", tmp_csn.c_str(), MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1), tmp_rowid.c_str());
         }
     }
 
     auto time2 = now_to_us();
     MOTAdaptor::txn_total_write_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_total_write_lockCnt.fetch_add(1);
 
     MOTAdaptor::txn_temp_total_write_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_temp_total_write_lockCnt.fetch_add(1);
 
     new_queue.reset();
     tmp_queue = nullptr;
     return result;
 }
 
 RC OccTransactionManager::ReadPhaseWoundWait(TxnManager* txMan, uint32_t server_id, void* currRow)
 {
     bool result = GetReadLockWoundWait(txMan, server_id, currRow);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetReadLockWoundWait(TxnManager* txMan, uint32_t server_id, void* row)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(txMan->pre_csn) + ":" + to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> new_queue = nullptr;
     auto currRow = static_cast<Row*>(row);
 
     if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) {
         if (!txMan->abort_) {
             txMan->abort_ = true;
             MOTAdaptor::ReadLock_pcc_abort_num.fetch_add(1);
         }
         return false;
     }
 
     auto table = currRow->GetTable();
     if (table == nullptr) {
         result = false;
         return result;
     }
     table_name = table->GetLongTableName();
     rowId = currRow->GetRowId();
     tmp_rowid = table_name + ":" + to_string(rowId);
 
     if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue){    // 未找到则创建新lock request
         new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
     }
 
     auto time1 = now_to_us();
     if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue){
         auto res = tmp_queue->LockRD_WoundWait(tmp_rowid, txMan, server_id, false);
         if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) {
             if (!txMan->abort_) {
                 txMan->abort_ = true;
                 MOTAdaptor::ReadLock_pcc_abort_num.fetch_add(1);
             }
             return false;
         }
 
         // 添加到新的 epoch lock request queue 中，没有取模
         // MOTAdaptor::AddEpochActiveQueue(tmp_queue, MOTAdaptor::GetLogicalEpoch());
         MOTAdaptor::AddActiveQueue(tmp_queue, rowId);
         if (!res) {
             if (is_debug_print_enable) MOT_LOG_INFO("GetReadLockWoundWait() lock_row_local [failed] because of wound_wait tmp_csn : %s, tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             if (!txMan->abort_) {
                 txMan->abort_ = true;
                 MOTAdaptor::ReadLock_pcc_abort_num.fetch_add(1);
             }
             // 移除等待图
             MOTAdaptor::wait_for_graph.removeNode(csn_temp);
             return false;
         } else {
             // 插入csn + queue
             MOTAdaptor::txn_total_lockCnt.fetch_add(1);
             MOTAdaptor::local_lock_num.fetch_add(1);
             if (is_debug_print_enable) MOT_LOG_INFO("GetReadLockWoundWait() lock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s ", tmp_csn.c_str(), MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1), tmp_rowid.c_str());
         }
     }
 
     auto time2 = now_to_us();
     MOTAdaptor::txn_total_read_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_total_read_lockCnt.fetch_add(1);
 
     MOTAdaptor::txn_temp_total_read_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_temp_total_read_lockCnt.fetch_add(1);
 
     new_queue.reset();
     tmp_queue = nullptr;
     // wzy : 一次只发送一个lockinfo
     return result;
 }
 
 //// Switch phase
 RC OccTransactionManager::SwitchReadPhaseWoundWait(TxnManager* txMan, uint32_t server_id, bool hot_rows) {
     bool result = GetSwitchReadLockWoundWait(txMan, server_id, hot_rows);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetSwitchReadLockWoundWait(TxnManager* txMan, uint32_t server_id, bool hot_rows) {
     // 对读集上锁
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
     currentCSN = txMan->pre_csn;
     auto start_logical_epoch = txMan->GetStartLogicalEpoch();
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr, new_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
     // 尝试锁住sentinel
     uint64_t thdId = txMan->GetThdId();
 
     for (const auto &raPair : orderedSet){
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
             MOTAdaptor::ReadLock_switch_pcc_abort_num.fetch_add(1);
             return false;
         }
         const Access *ac = raPair.second;
         Row* row_from_header = ac->GetRowFromHeader();
         if (ac->m_type == RD){
             if (ac->m_localRow->GetTable() == nullptr) {
                 result = false;
                 break;
             }
 
             auto table = ac->m_localRow->GetTable();
             auto currRow = ac->m_localRow;
 
             uint64_t start_time = now_to_us();
 
             // RLock
             if (!hot_rows) {
                 std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
                 if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) ||
                     !tmp_queue) {  // 未找到则创建新lock request
                     new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
                 }
                 if (MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue) {
                     auto res = tmp_queue->LockRD_WoundWait(tmp_rowid, txMan, server_id, true);
                     if (!res) {
                         if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchReadLockWoundWait() LockRD [error] tmp_csn : %s  , tmp_rowid : %s",
                                 csn_temp.c_str(),
                                 tmp_rowid.c_str());
                         MOTAdaptor::ReadLock_switch_pcc_abort_num.fetch_add(1);
                         result = false;
                     } else {
                         if (is_debug_print_enable) MOT_LOG_INFO(
                                 "GetSwitchReadLockWoundWait() LockRD tmp_csn : %s , epoch : %llu , tmp_rowid : %s",
                                 csn_temp.c_str(),
                                 epoch_mod,
                                 tmp_rowid.c_str());
                     }
                 } else {
                     // do nothing
                 }
             }
             else if (hot_rows && txMan->hot_rowid_records.count(currRow->GetRowId()) != 0) {
                 std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
                 if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) ||
                     !tmp_queue) {  // 未找到则创建新lock request
                     new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
                 }
                 if (MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue) {
                     auto res = tmp_queue->LockRD_WoundWait(tmp_rowid, txMan, server_id, true);
                     if (!res) {
                         if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchReadLockWoundWait() LockRD [error] tmp_csn : %s  , tmp_rowid : %s",
                                 csn_temp.c_str(),
                                 tmp_rowid.c_str());
                         MOTAdaptor::ReadLock_switch_pcc_abort_num.fetch_add(1);
                         result = false;
                     } else {
                         if (is_debug_print_enable) MOT_LOG_INFO(
                                 "GetSwitchReadLockWoundWait() LockRD tmp_csn : %s , epoch : %llu , tmp_rowid : %s",
                                 csn_temp.c_str(),
                                 epoch_mod,
                                 tmp_rowid.c_str());
                     }
                 } else {
                     // do nothing
                 }
             }
 
             MOTAdaptor::txn_total_switchTime_RLock.fetch_add(now_to_us() - start_time);
             MOTAdaptor::txn_total_switchTime_RLockCnt.fetch_add(1);
 
             if (!result) return false;
 
             start_time = now_to_us();
             // LOCK sentinel
             ac->m_origSentinel->Lock(thdId);
 
 
             // Validation
             if (!isMVCC_Active) {
                 if (is_snap_isolation) {
                     if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadForSnap_Switching(ac->m_cts, start_logical_epoch, ac->m_server_id)) result = false;
                 } else if (is_read_repeatable) {
                     if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadI_Switching(ac->m_cts, ac->m_server_id)) result = false;
                 }
             }
 
             // UNLOCK
             ac->m_origSentinel->Release();
 
             MOTAdaptor::txn_total_switchTime_Sentinel.fetch_add(now_to_us() - start_time);
             MOTAdaptor::txn_total_switchTime_SentinelCnt.fetch_add(1);
 
             if (!result) {
                 MOTAdaptor::Switch_validation_abort_num.fetch_add(1);
                 return false;
             }
         }
     }
 
     return result;
 }
 
 RC OccTransactionManager::SwitchWritePhaseWoundWait(TxnManager* txMan, uint32_t server_id, bool hot_rows) {
     bool result = GetSwitchWriteLockWoundWait(txMan, server_id, hot_rows);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetSwitchWriteLockWoundWait(TxnManager* txMan, uint32_t server_id, bool hot_rows) {
     // 对写集上锁
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
     currentCSN = txMan->pre_csn;
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr, new_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
     for (const auto &raPair : orderedSet){
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
             if (!txMan->abort_) {
                 txMan->abort_ = true;
                 MOTAdaptor::WriteLock_switch_pcc_abort_num.fetch_add(1);
             }
             return false;
         }
         const Access *ac = raPair.second;
         if (ac->m_type == WR) {
             if (ac->m_localRow->GetTable() == nullptr) {
                 result = false;
                 break;
             }
             auto table = ac->m_localRow->GetTable();
             auto currRow = ac->m_localRow;
 
             // 统计时是根据GetTableName
             //            std::string tmp_rowid = currRow->GetTable()->GetTableName() + ":" + to_string(currRow->GetRowId());
             std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
             if (hot_rows && !MOTAdaptor::dynamic_hot_rows.isHotRows(tmp_rowid))
                 continue;
 
             if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue){    // 未找到则创建新lock request
                 new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
             }
 
             uint64_t start_time = now_to_us();
             if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue){
                 auto res = tmp_queue->LockWR_WoundWait(tmp_rowid, txMan, server_id);
                 if(!res) {
                     if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchWriteLockWoundWait() lock_request_queue [error] tmp_csn : %s  , tmp_rowid : %s", csn_temp.c_str(), tmp_rowid.c_str());
                     if (!txMan->abort_) {
                         txMan->abort_ = true;
                         MOTAdaptor::WriteLock_switch_pcc_abort_num.fetch_add(1);
                     }
                     // 移除等待图
                     MOTAdaptor::wait_for_graph.removeNode(csn_temp);
                     return false;
                 } else {
                     if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchWriteLockWoundWait() unlock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s", csn_temp.c_str(), epoch_mod, tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchWriteLockWoundWait() LockWR not found tmp_csn : %s  , tmp_rowid : %s ", csn_temp.c_str(), tmp_rowid.c_str());
             }
             MOTAdaptor::txn_total_switchTime_WLock.fetch_add(now_to_us() - start_time);
             MOTAdaptor::txn_total_switchTime_WLockCnt.fetch_add(1);
         }
     }
     return result;
 }
 
 //// end Switch phase
 
 RC OccTransactionManager::ValidationPhaseWoundWait(TxnManager *txMan, uint32_t server_id)
 {
     bool result = ValidateReadWriteConflictWoundWait(txMan, server_id);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::ValidateReadWriteConflictWoundWait(TxnManager* txMan, uint32_t server_id)
 {
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
     currentCSN = txMan->pre_csn;
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
     auto time1 = now_to_us();
 
     for (const auto &raPair : orderedSet){
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) return false;
         const Access *ac = raPair.second;
         if (ac->m_type == WR){
             if (ac->m_localRow->GetTable() == nullptr) {
                 result = false;
                 break;
             }
             auto table = ac->m_localRow->GetTable();
             // wzy: 遍历写集
             auto currRow = ac->m_localRow;
             std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
 
             table_name = table->GetLongTableName();
             rowId = ac->m_localRow->GetRowId();
             tmp_rowid = table_name + ":" + to_string(rowId);
             tmp_queue = nullptr;
             if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
                 auto res = tmp_queue->ValidateWR_WoundWait(tmp_rowid, txMan, server_id);
                 if(!res) {
                     if (is_debug_print_enable) MOT_LOG_INFO("ValidateReadWriteConflict() lock_request_queue [error] tmp_csn : %s  , tmp_rowid : %s", csn_temp.c_str(), tmp_rowid.c_str());
                     // 移除等待图
                     MOTAdaptor::wait_for_graph.removeNode(csn_temp);
                     return false;
                 } else {
                     if (is_debug_print_enable) MOT_LOG_INFO("ValidateReadWriteConflict() lock_request_queue success tmp_csn : %s  , tmp_rowid : %s", csn_temp.c_str(), tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 // MOT_LOG_INFO("UnlockWriteSet() lock_request_queue not found tmp_csn : %s  , tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             }
         }
     }
 
     auto time2 = now_to_us();
     MOTAdaptor::txn_total_validate_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_total_validate_lockCnt.fetch_add(1);
 
     MOTAdaptor::txn_temp_total_validate_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_temp_total_validate_lockCnt.fetch_add(1);
 
     return result;
 }
 
 RC OccTransactionManager::UnlockReadWriteLockWoundWait(TxnManager* txMan, uint32_t server_id, uint64_t csn, bool abort)
 {
     bool result = UnlockReadWriteRowWoundWait(txMan, server_id, csn, abort);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::UnlockReadWriteRowWoundWait(TxnManager* txMan, uint32_t server_id, uint64_t csn, bool abort)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(csn) + ":" + to_string(server_id);
     string res_csn = "";
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
 //    if (abort) MOT_LOG_INFO("UnlockWriteSet() txn [Abort] tmp_csn : %s", tmp_csn.c_str());
     for (const auto& raPair : orderedSet) {
         // orderedSet中重复的read会被write覆盖
         const Access* ac = raPair.second;
         if (ac->m_type == WR) {
             auto table = ac->m_localRow->GetTable();
             if (table == nullptr) {
                 result = false;
                 continue;
             }
 
             // 非热数据，跳过
             if (kHotRow_Active && txMan->hot_rowid_records.count(ac->m_localRow->GetRowId()) == 0) {
                 continue;
             }
 
             table_name = table->GetLongTableName();
             rowId = ac->m_localRow->GetRowId();
             res_csn = "";
             tmp_rowid = table_name + ":" + to_string(rowId);
             tmp_queue = nullptr;
 
             if (csn == 0) {
                 if (is_debug_print_enable) MOT_LOG_INFO("UnlockWR() [error] start_time : %llu, pre_csn : %llu, csn : %llu", txMan->start_time, txMan->pre_csn, csn);
                 csn = txMan->start_time;
             }
 
             if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
                 auto res = tmp_queue->UnlockWR_WoundWait(tmp_rowid, csn, res_csn, server_id);
                 MOTAdaptor::local_unlock_num.fetch_add(1);
                 if(!res) {
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockWR() lock_request_queue [error] grant_csn : %s, tmp_csn : %s  , tmp_rowid : %s", res_csn.c_str(), tmp_csn.c_str(), tmp_rowid.c_str());
                 } else {
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockWR() unlock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s", tmp_csn.c_str(), epoch_mod, tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 // MOT_LOG_INFO("UnlockWriteSet() lock_request_queue not found tmp_csn : %s  , tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             }
 
             tmp_queue = nullptr;
         } else if (ac->m_type == RD) {
             auto table = ac->m_localRow->GetTable();
             if (table == nullptr) {
                 result = false;
                 continue;
             }
 
             // 非热数据，跳过
             if (kHotRow_Active && txMan->hot_rowid_records.count(ac->m_localRow->GetRowId()) == 0) {
                 continue;
             }
 
             table_name = table->GetLongTableName();
             rowId = ac->m_localRow->GetRowId();
             res_csn = "";
             tmp_rowid = table_name + ":" + to_string(rowId);
             tmp_queue = nullptr;
             if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
                 auto res = tmp_queue->UnlockRD_WoundWait(tmp_rowid, csn);
                 MOTAdaptor::local_unlock_num.fetch_add(1);
                 if(!res && !abort) {
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockRD() lock_request_queue [error] grant_csn : %s, tmp_csn : %s  , tmp_rowid : %s", res_csn.c_str(), tmp_csn.c_str(), tmp_rowid.c_str());
                 } else if (!abort){
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockRD() unlock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s", tmp_csn.c_str(), epoch_mod, tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 // MOT_LOG_INFO("UnlockWriteSet() lock_request_queue not found tmp_csn : %s  , tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             }
         } else
             continue;
     }
 
     // 移除等待图
     uint64_t node_num = MOTAdaptor::wait_for_graph.vertex_num.load();
     MOTAdaptor::wait_for_graph.removeNode(tmp_csn);
     if (is_debug_print_enable) MOT_LOG_INFO("RemoveNode() tmp_csn : %s , epoch : %llu , wait_for_graph before node : %llu , wait_for_graph after node : %llu", tmp_csn.c_str(), epoch_mod, node_num, MOTAdaptor::wait_for_graph.vertex_num.load());
 
     MOTAdaptor::deadlock_abort_set.remove(tmp_csn);
     //    MOTAdaptor::csn_requests_map.remove(tmp_csn);           // 清除tid对应的上锁队列指针
 //    MOTAdaptor::txn_rowid_map.remove(tmp_csn);
     return result;
 }
 
 //////////////////////// Wound-wait End /////////////////////////
 
 ////////////////////// DL detect ////////////////////////////
 
 RC OccTransactionManager::WritePhaseDL(TxnManager* txMan, uint32_t server_id, void* currRow)
 {
     bool result = GetWriteLockDL(txMan, server_id, currRow);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetWriteLockDL(TxnManager* txMan, uint32_t server_id, void* row)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(txMan->pre_csn) + ":" + to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> new_queue = nullptr;
     auto currRow = static_cast<Row*>(row);
 
     if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) return false;
     if (kHotRow_Active && txMan->hot_rowid_records.count(currRow->GetRowId()) == 0) return true;
 
     auto table = currRow->GetTable();
     if (table == nullptr) {
         result = false;
         return result;
     }
     table_name = table->GetLongTableName();
     rowId = currRow->GetRowId();
     tmp_rowid = table_name + ":" + to_string(rowId);
 
     if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue){    // 未找到则创建新lock request
         new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
     }
 
     auto time1 = now_to_us();
     if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue){
         auto res = tmp_queue->LockWRDL(tmp_rowid, txMan, server_id);
         if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) return false;
         // 添加到新的 epoch lock request queue 中，没有取模
         if (!res) {
             if (is_debug_print_enable) MOT_LOG_INFO("GetWriteLockPlor() LockWR [failed] because of wound_wait tmp_csn : %s, tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             MOTAdaptor::WriteLock_pcc_abort_num.fetch_add(1);
             return false;
         } else {
             // 插入csn + queue
             MOTAdaptor::txn_total_lockCnt.fetch_add(1);
             MOTAdaptor::local_lock_num.fetch_add(1);
 //            MOTAdaptor::AddCsnRequestQueue(tmp_csn, tmp_queue);
             if (is_debug_print_enable) MOT_LOG_INFO("GetWriteLockPlor() LockWR tmp_csn : %s , epoch : %llu , tmp_rowid : %s ", tmp_csn.c_str(), MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1), tmp_rowid.c_str());
         }
     }
     auto time2 = now_to_us();
     MOTAdaptor::txn_total_write_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_total_write_lockCnt.fetch_add(1);
 
     MOTAdaptor::txn_temp_total_write_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_temp_total_write_lockCnt.fetch_add(1);
 
     new_queue.reset();
     tmp_queue = nullptr;
     return result;
 }
 
 RC OccTransactionManager::ReadPhaseDL(TxnManager* txMan, uint32_t server_id, void* currRow)
 {
     bool result = GetReadLockDL(txMan, server_id, currRow);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetReadLockDL(TxnManager* txMan, uint32_t server_id, void* row)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(txMan->pre_csn) + ":" + to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> new_queue = nullptr;
     auto currRow = static_cast<Row*>(row);
 
     if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) return false;
 
     if (kHotRow_Active && txMan->hot_rowid_records.count(currRow->GetRowId()) == 0) return true;
 
     auto table = currRow->GetTable();
     if (table == nullptr) {
         result = false;
         return result;
     }
     table_name = table->GetLongTableName();
     rowId = currRow->GetRowId();
     tmp_rowid = table_name + ":" + to_string(rowId);
 
     if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue){    // 未找到则创建新lock request
         new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
     }
 
     auto time1 = now_to_us();
     if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue){
         auto res = tmp_queue->LockRDDL(tmp_rowid, txMan, server_id, false);
         if (MOTAdaptor::deadlock_abort_set.contain(tmp_csn, tmp_csn)) return false;
         // 添加到新的 epoch lock request queue 中，没有取模
         // MOTAdaptor::AddEpochActiveQueue(tmp_queue, MOTAdaptor::GetLogicalEpoch());
         //        MOTAdaptor::AddActiveQueue(tmp_queue, rowId);
         if (!res) {
             if (is_debug_print_enable) MOT_LOG_INFO("GetReadLockPlor() LockRD [failed] because of wound_wait tmp_csn : %s, tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             MOTAdaptor::ReadLock_pcc_abort_num.fetch_add(1);
             return false;
         } else {
             // 插入csn + queue
             MOTAdaptor::txn_total_lockCnt.fetch_add(1);
             MOTAdaptor::local_lock_num.fetch_add(1);
             if (is_debug_print_enable) MOT_LOG_INFO("GetReadLockPlor() LockRD tmp_csn : %s , epoch : %llu , tmp_rowid : %s ", tmp_csn.c_str(), MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1), tmp_rowid.c_str());
         }
     }
     auto time2 = now_to_us();
     MOTAdaptor::txn_total_read_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_total_read_lockCnt.fetch_add(1);
 
     MOTAdaptor::txn_temp_total_read_lockTime.fetch_add(time2 - time1);
     MOTAdaptor::txn_temp_total_read_lockCnt.fetch_add(1);
 
     new_queue.reset();
     tmp_queue = nullptr;
     // wzy : 一次只发送一个lockinfo
     return result;
 }
 
 //// Switch phase
 RC OccTransactionManager::SwitchReadPhaseDL(TxnManager* txMan, uint32_t server_id, bool hot_rows) {
     bool result = true;
     if (cc_mode == 5) result = GetSwitchReadLockDLPrevRLock(txMan, server_id, hot_rows);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetSwitchReadLockDLPrevRLock(MOT::TxnManager* txMan, uint32_t server_id, bool hot_rows)
 {
     // 对读集上锁
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
     currentCSN = txMan->pre_csn;
     auto start_logical_epoch = txMan->GetStartLogicalEpoch();
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr, new_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
     // 尝试锁住sentinel
     uint64_t thdId = txMan->GetThdId();
 
     for (const auto &raPair : orderedSet){
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) return false;
         const Access *ac = raPair.second;
         Row* row_from_header = ac->GetRowFromHeader();
         if (ac->m_type == RD){
             if (ac->m_localRow->GetTable() == nullptr) {
                 result = false;
                 break;
             }
 
             auto table = ac->m_localRow->GetTable();
             auto currRow = ac->m_localRow;
 
             // RLock
             if (hot_rows && txMan->hot_rowid_records.count(currRow->GetRowId()) != 0) {
                 std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
                 if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) ||
                     !tmp_queue) {  // 未找到则创建新lock request
                     new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
                 }
                 if (MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue) {
                     auto res = tmp_queue->LockRDDL(tmp_rowid, txMan, server_id, true);
                     if (!res) {
                         if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchReadLockPlorPrevRLock() LockRD [error] tmp_csn : %s  , tmp_rowid : %s",
                                 csn_temp.c_str(),
                                 tmp_rowid.c_str());
                         MOTAdaptor::ReadLock_switch_pcc_abort_num.fetch_add(1);
                         result = false;
                     } else {
                         if (is_debug_print_enable) MOT_LOG_INFO(
                                 "GetSwitchReadLockPlorPrevRLock() LockRD tmp_csn : %s , epoch : %llu , tmp_rowid : %s",
                                 csn_temp.c_str(),
                                 epoch_mod,
                                 tmp_rowid.c_str());
                     }
                 } else {
                     // do nothing
                 }
             }
 
             if (!result) return false;
 
             // LOCK sentinel
             ac->m_origSentinel->Lock(thdId);
 
             // row_from_header->GetRowHeader()->Lock();
             // row_from_header->GetRowHeader()->LockStable();
 
             // Validation
             if (!isMVCC_Active) {
                 if (is_snap_isolation) {
                     if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadForSnap_Switching(ac->m_cts, start_logical_epoch, ac->m_server_id)) result = false;
                 } else if (is_read_repeatable) {
                     if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadI_Switching(ac->m_cts, ac->m_server_id)) result = false;
                 }
             }
 
             // UNLOCK
             // row_from_header->GetRowHeader()->ReleaseStable();
             // row_from_header->GetRowHeader()->Release();
             ac->m_origSentinel->Release();
 
             if (!result) {
                 MOTAdaptor::Switch_validation_abort_num.fetch_add(1);
                 return false;
             }
         }
     }
 
     return result;
 }
 
 bool OccTransactionManager::GetSwitchReadLockDL(TxnManager* txMan, uint32_t server_id) {
     // 对读集上锁
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
     currentCSN = txMan->pre_csn;
     auto start_logical_epoch = txMan->GetStartLogicalEpoch();
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr, new_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
     for (const auto &raPair : orderedSet){
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) return false;
         const Access *ac = raPair.second;
         Row* row_from_header = ac->GetRowFromHeader();
         if (ac->m_type == RD){
             if (ac->m_localRow->GetTable() == nullptr) {
                 result = false;
                 break;
             }
 
             if (!result) return false;
 
             // LOCK
             row_from_header->GetRowHeader()->Lock();
             row_from_header->GetRowHeader()->LockStable();
 
             // Validation
             if (!isMVCC_Active) {
                 if (is_snap_isolation) {
                     if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadForSnap_Switching(ac->m_cts, start_logical_epoch, ac->m_server_id)) result = false;
                 } else if (is_read_repeatable) {
                     if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadI_Switching(ac->m_cts, ac->m_server_id)) result = false;
                 }
             }
 
             // RLock
             if (result) {
                 auto table = ac->m_localRow->GetTable();
                 auto currRow = ac->m_localRow;
                 std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
                 if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue) {    // 未找到则创建新lock request
                     new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
                 }
 
                 if (is_debug_print_enable) MOT_LOG_INFO("Before GetSwitchReadLockPlor() LockRD tmp_csn : %s , epoch : %llu , tmp_rowid : %s", csn_temp.c_str(), epoch_mod, tmp_rowid.c_str());
 
                 if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue) {
                     auto res = tmp_queue->LockRDDL(tmp_rowid, txMan, server_id, true);
                     if(!res) {
                         if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchReadLockPlor() LockRD [error] tmp_csn : %s  , tmp_rowid : %s", csn_temp.c_str(), tmp_rowid.c_str());
                         MOTAdaptor::ReadLock_switch_pcc_abort_num.fetch_add(1);
                         result = false;
                     } else {
                         if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchReadLockPlor() LockRD tmp_csn : %s , epoch : %llu , tmp_rowid : %s", csn_temp.c_str(), epoch_mod, tmp_rowid.c_str());
                     }
                 } else {
                     // do nothing
                 }
             }
 
             // UNLOCK
             row_from_header->GetRowHeader()->ReleaseStable();
             row_from_header->GetRowHeader()->Release();
 
             if (!result) return false;
         }
     }
 
     return result;
 }
 
 RC OccTransactionManager::SwitchWritePhaseDL(TxnManager* txMan, uint32_t server_id, bool hot_rows) {
     bool result = GetSwitchWriteLockDL(txMan, server_id, hot_rows);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::GetSwitchWriteLockDL(TxnManager* txMan, uint32_t server_id, bool hot_rows) {
     // 对写集上锁
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
     currentCSN = txMan->pre_csn;
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr, new_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
     for (const auto &raPair : orderedSet){
         if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) return false;
         const Access *ac = raPair.second;
         if (ac->m_type == WR) {
             if (ac->m_localRow->GetTable() == nullptr) {
                 result = false;
                 break;
             }
             auto table = ac->m_localRow->GetTable();
             auto currRow = ac->m_localRow;
 
             // 统计时是根据GetTableName
             //            std::string tmp_rowid = currRow->GetTable()->GetTableName() + ":" + to_string(currRow->GetRowId());
             std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
             // 只对热数据项上锁
             if (hot_rows && txMan->hot_rowid_records.count(currRow->GetRowId()) == 0)
                 continue;
 
             if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue){    // 未找到则创建新lock request
                 new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
             }
 
             if (is_debug_print_enable) MOT_LOG_INFO("[Before] GetSwitchWriteLockPlor() LockWR tmp_csn : %s , epoch : %llu , tmp_rowid : %s", csn_temp.c_str(), epoch_mod, tmp_rowid.c_str());
 
             if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue){
                 auto res = tmp_queue->LockWRDL(tmp_rowid, txMan, server_id);
                 if(!res) {
                     if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchWriteLockPlor() LockWR [error] tmp_csn : %s  , tmp_rowid : %s", csn_temp.c_str(), tmp_rowid.c_str());
                     MOTAdaptor::WriteLock_switch_pcc_abort_num.fetch_add(1);
                     return false;
                 } else {
                     if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchWriteLockPlor() LockWR tmp_csn : %s , epoch : %llu , tmp_rowid : %s", csn_temp.c_str(), epoch_mod, tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 if (is_debug_print_enable) MOT_LOG_INFO("GetSwitchWriteLockPlor() LockWR not found tmp_csn : %s  , tmp_rowid : %s ", csn_temp.c_str(), tmp_rowid.c_str());
             }
         }
     }
     return result;
 }
 
 
 RC OccTransactionManager::UnlockReadWriteLockDL(TxnManager* txMan, uint32_t server_id, uint64_t csn, bool abort)
 {
     bool result = UnlockReadWriteRowDL(txMan, server_id, csn, abort);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::UnlockReadWriteRowDL(TxnManager* txMan, uint32_t server_id, uint64_t csn, bool abort)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(csn) + ":" + to_string(server_id);
     string res_csn = "";
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
     //    if (abort) MOT_LOG_INFO("UnlockWriteSet() txn [Abort] tmp_csn : %s", tmp_csn.c_str());
     for (const auto& raPair : orderedSet) {
         // orderedSet中重复的read会被write覆盖
         const Access* ac = raPair.second;
         if (ac->m_type == WR) {
             auto table = ac->m_localRow->GetTable();
             if (table == nullptr) {
                 result = false;
                 continue;
             }
 
             // 非热数据，跳过
             if (kHotRow_Active && txMan->hot_rowid_records.count(ac->m_localRow->GetRowId()) == 0) {
                 continue;
             }
             table_name = table->GetLongTableName();
             rowId = ac->m_localRow->GetRowId();
             res_csn = "";
             tmp_rowid = table_name + ":" + to_string(rowId);
             tmp_queue = nullptr;
 
             if (csn == 0) {
                 if (is_debug_print_enable) MOT_LOG_INFO("UnlockWR() [error] start_time : %llu, pre_csn : %llu, csn : %llu", txMan->start_time, txMan->pre_csn, csn);
                 csn = txMan->start_time;
             }
 
             if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
                 auto res = tmp_queue->UnlockRowDL(tmp_rowid, txMan, res_csn, server_id);
                 MOTAdaptor::local_unlock_num.fetch_add(1);
                 if(abort) {
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockWR() abort lock_request_queue grant_csn : %s, tmp_csn : %s  , tmp_rowid : %s", res_csn.c_str(), tmp_csn.c_str(), tmp_rowid.c_str());
                 } else {
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockWR() unlock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s", tmp_csn.c_str(), epoch_mod, tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 if (is_debug_print_enable) MOT_LOG_INFO("UnlockWriteSet() lock_request_queue not found tmp_csn : %s  , tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             }
             tmp_queue = nullptr;
         } else if (ac->m_type == RD) {
             auto table = ac->m_localRow->GetTable();
             if (table == nullptr) {
                 result = false;
                 continue;
             }
 
             // 非热数据，跳过
             if (kHotRow_Active && txMan->hot_rowid_records.count(ac->m_localRow->GetRowId()) == 0) {
                 continue;
             }
 
             table_name = table->GetLongTableName();
             rowId = ac->m_localRow->GetRowId();
             res_csn = "";
             tmp_rowid = table_name + ":" + to_string(rowId);
             tmp_queue = nullptr;
             if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
                 auto res = tmp_queue->UnlockRowDL(tmp_rowid, txMan, res_csn, server_id);
                 //                MOTAdaptor::RemoveActiveQueue(tmp_queue, rowId);    // 移除活跃队列
                 MOTAdaptor::local_unlock_num.fetch_add(1);
                 if(!res && !abort) {
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockRD() lock_request_queue [error] grant_csn : %s, tmp_csn : %s  , tmp_rowid : %s", res_csn.c_str(), tmp_csn.c_str(), tmp_rowid.c_str());
                 } else if (!abort) {
                     if (is_debug_print_enable) MOT_LOG_INFO("UnlockRD() unlock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s", tmp_csn.c_str(), epoch_mod, tmp_rowid.c_str());
                 }
             } else {
                 // do nothing
                 if (is_debug_print_enable) MOT_LOG_INFO("UnlockWriteSet() lock_request_queue not found tmp_csn : %s  , tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             }
         } else
             continue;
     }
     //    MOTAdaptor::csn_requests_map.remove(tmp_csn);           // 清除tid对应的上锁队列指针
     MOTAdaptor::deadlock_abort_set.remove(tmp_csn);
     MOTAdaptor::wait_for_graph.removeNode(tmp_csn);         // 清除等待图
 //    MOTAdaptor::txn_rowid_map.remove(tmp_csn);
     return result;
 }
 
 
 ////////////////////////////////////////////////////////////////
 
 // wzy: 给本地交互型事务上锁（主要是update）
 RC OccTransactionManager::LockPhase(TxnManager* txMan, uint32_t server_id, void* currRow)
 {
     bool result = LockWriteSet(txMan, server_id, currRow);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 // wzy: 上锁单个lockinfo，而不是累计
 bool OccTransactionManager::LockWriteSet(TxnManager* txMan, uint32_t server_id, void* row)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(txMan->GetCommitSequenceNumber()) + ":" + to_string(server_id);
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> new_queue = nullptr;
     auto currRow = static_cast<Row*>(row);
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);

     if (is_wound_wait_enable) {
         // 检查是否该中止，不检查当前epoch，是因为所有锁请求都应该执行后，再判断，除非该锁请求在之前的epoch就被中止
         if (MOTAdaptor::wound_wait_abort_map.should_abort(tmp_csn, txMan->GetCommitEpoch())) {
             MOT_LOG_INFO("[Wound] lock_row_local [failed] because of wound tmp_csn : %s epoch : %llu", tmp_csn.c_str(), txMan->GetCommitEpoch());
             return false;  // 被死锁检测abort
         }
         if (MOTAdaptor::abort_transcation_csn_set.contain(tmp_csn, tmp_csn)) {
             return false;  // 被死锁检测abort
         }
     }

     // 只有WR操作会调用
     auto table = currRow->GetTable();
     if (table == nullptr) {
         result = false;
         return result;
     }
     table_name = table->GetLongTableName();
     rowId = currRow->GetRowId();
     tmp_rowid = table_name + ":" + to_string(rowId);
 
     if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue){    // 未找到则创建新lock request
         new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
     }
 
     if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue){
         auto res = tmp_queue->lock_row_local(tmp_rowid, txMan, server_id);
         // 添加到新的 epoch lock request queue 中，没有取模
         // MOTAdaptor::AddEpochActiveQueue(tmp_queue, MOTAdaptor::GetLogicalEpoch());
         MOTAdaptor::AddActiveQueue(tmp_queue, rowId);
         if (!res) {
             MOT_LOG_INFO("LockWriteSet() lock_row_local [failed] because of duplicate tmp_csn : %s, tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
         } else {
             // 插入csn + queue
             MOTAdaptor::txn_total_lockCnt.fetch_add(1);
             MOTAdaptor::local_lock_num.fetch_add(1);
             MOTAdaptor::AddCsnRequestQueue(tmp_csn, tmp_queue);
//             MOT_LOG_INFO("LockWriteSet() lock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s ", tmp_csn.c_str(), MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1), tmp_rowid.c_str());
         }
     }
     MOT_LOG_INFO("LockWriteSet() lock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s", tmp_csn.c_str(), epoch_mod, tmp_rowid.c_str());
     new_queue.reset();
     tmp_queue = nullptr;
     return result;
 }
 
 RC OccTransactionManager::LockCheck(TxnManager* txMan, uint32_t server_id, void* currRow)
 {
     bool result = CheckForLockSet(txMan, server_id, currRow);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::CheckForLockSet(TxnManager* txMan, uint32_t server_id, void* row)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     std::string tmp_csn = to_string(txMan->GetCommitSequenceNumber()) + ":" + to_string(server_id);
     string locked_csn = "-1";
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
 
     auto currRow = static_cast<Row*>(row);
     auto table = currRow->GetTable();
     if (table == nullptr) {
         result = false;
         return result;
     }
     table_name = table->GetLongTableName();
     rowId = currRow->GetRowId();
     tmp_rowid = table_name + ":" + to_string(rowId);
     locked_csn = "-1";
 
     if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
         auto res = tmp_queue->get_grant_csn(tmp_csn, locked_csn);
         if(!res) {
             MOTAdaptor::abort_transcation_csn_set.insert(tmp_csn, tmp_csn);
             MOTAdaptor::deadlock_abort_set.insert(tmp_csn, tmp_csn);
             MOT_LOG_INFO("CheckForLockSet() lock_request [error] not found tid : %s , tmp_rowid ： %s ", tmp_csn.c_str(), tmp_rowid.c_str());
             return false;
         } else {
             // do nothing
             // if (locked_csn == "") MOTAdaptor::AddEpochActiveQueue(tmp_queue, MOTAdaptor::GetLogicalEpoch());
//             MOT_LOG_INFO("CheckForLockSet() lock_request get success locked_csn : %s , tid : %s , tmp_rowid ： %s ", locked_csn.c_str(), tmp_csn.c_str(), tmp_rowid.c_str());
         }
     } else {
         locked_csn = "-1";
     }
     if (locked_csn != tmp_csn) {
         result = false;
     }
     // wzy : 一次只验证一个lockinfo
     return result;
 }
 
 RC OccTransactionManager::UnlockPhase(TxnManager* txMan, uint32_t server_id, uint64_t csn, bool abort)
 {
 //    bool result = UnlockWriteSet(txMan, server_id, csn);
     bool result = UnlockWriteSetRow(txMan, server_id, csn, abort);
     if (result) {
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::UnlockWriteSet(TxnManager* txMan, uint32_t server_id, uint64_t csn)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(csn) + ":" + to_string(server_id);
     string res_csn = "";
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
 
     for (const auto& raPair : orderedSet) {
         const Access* ac = raPair.second;
         if (ac->m_type == WR) {
             auto table = ac->m_localRow->GetTable();
             if (table == nullptr) {
                 result = false;
                 continue;
             }
             table_name = table->GetLongTableName();
             rowId = ac->m_localRow->GetRowId();
             res_csn = "";
             tmp_rowid = table_name + ":" + to_string(rowId);
             tmp_queue = nullptr;
             if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
                 auto res = tmp_queue->unlock_row(tmp_rowid, tmp_csn, res_csn);
                 if(!res) {
                     MOT_LOG_INFO("UnlockWriteSet() lock_request_queue [error] grant_csn : %s, tmp_csn : %s  , tmp_rowid : %s ", res_csn.c_str(), tmp_csn.c_str(), tmp_rowid.c_str());
                 } else {
//                     MOT_LOG_INFO("UnlockWriteSet() unlock_row_local tmp_csn : %s", tmp_csn.c_str());
                 }
             } else {
                 MOT_LOG_INFO("UnlockWriteSet() lock_request_queue not found row id = %s", tmp_rowid.c_str());
             }
             tmp_queue = nullptr;
         } else if (ac->m_type == RD) {
             // 对于读操作从版本活跃事务链表中移除
 //             ac->GetRowFromHeader()->m_rowHeader.RemoveTxnList(txMan->GetInternalTransactionId(), server_id);
         } else
             continue;
     }
     MOTAdaptor::deadlock_abort_set.remove(tmp_csn);
     if (is_wound_wait_enable) MOTAdaptor::wound_wait_abort_map.clear_tid(tmp_csn);
     else MOTAdaptor::wait_for_graph.removeNode(tmp_csn);
     MOTAdaptor::txn_rowid_map.remove(tmp_csn);
     return result;
 }
 
 bool OccTransactionManager::UnlockWriteSetRow(MOT::TxnManager* txMan, uint32_t server_id, uint64_t csn, bool abort)
 {
     TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     string tmp_csn = to_string(csn) + ":" + to_string(server_id);
     string res_csn = "";
     uint64_t rowId;
     std::string tmp_rowid;
     std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
     uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
 
     if (abort) MOT_LOG_INFO("UnlockWriteSet() txn [Abort] tmp_csn : %s", tmp_csn.c_str());
     for (const auto& raPair : orderedSet) {
         const Access* ac = raPair.second;
         if (ac->m_type == WR) {
             auto table = ac->m_localRow->GetTable();
             if (table == nullptr) {
                 result = false;
                 continue;
             }
             // 只有interactive行会解锁，update row和local row不相同?
             if (ac->m_localRow->GetRowInteractive()) {
                 table_name = table->GetLongTableName();
                 rowId = ac->m_localRow->GetRowId();
                 res_csn = "";
                 tmp_rowid = table_name + ":" + to_string(rowId);
                 tmp_queue = nullptr;
                 if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue){
                     auto res = tmp_queue->unlock_row(tmp_rowid, tmp_csn, res_csn);
                     MOTAdaptor::RemoveActiveQueue(tmp_queue, rowId);    // 移除活跃队列
                     MOTAdaptor::local_unlock_num.fetch_add(1);
                     if(!res && !abort) {
                         MOT_LOG_INFO("UnlockWriteSet() lock_request_queue [error] grant_csn : %s, tmp_csn : %s  , tmp_rowid : %s", res_csn.c_str(), tmp_csn.c_str(), tmp_rowid.c_str());
                     } else if (!abort){
                         MOT_LOG_INFO("UnlockWriteSet() unlock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s", tmp_csn.c_str(), epoch_mod, tmp_rowid.c_str());
                     }
                     MOT_LOG_INFO("UnlockWriteSet() unlock_row_local tmp_csn : %s , epoch : %llu , tmp_rowid : %s", tmp_csn.c_str(), epoch_mod, tmp_rowid.c_str());
                 } else {
                     // do nothing
                     // MOT_LOG_INFO("UnlockWriteSet() lock_request_queue not found tmp_csn : %s  , tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
                 }
             }
             tmp_queue = nullptr;
         } else if (ac->m_type == RD) {
             // 对于读操作从版本活跃事务链表中移除
             //             ac->GetRowFromHeader()->m_rowHeader.RemoveTxnList(txMan->GetInternalTransactionId(), server_id);
         } else
             continue;
     }
     MOTAdaptor::deadlock_abort_set.remove(tmp_csn);
     if (is_wound_wait_enable) MOTAdaptor::wound_wait_abort_map.clear_tid(tmp_csn);
     else MOTAdaptor::wait_for_graph.removeNode(tmp_csn);         // 清除等待图
     MOTAdaptor::txn_rowid_map.remove(tmp_csn);
     return result;
 }
 
 ///////////////// End Write Lock////////////////
 // CRDT时用header字符，silo时用上锁解决
 RC OccTransactionManager::CommitUpdate(TxnManager *txMan, uint32_t server_id){
     m_rowsLocked = false;
     // wzy
     m_writeSetSize = 0;
     m_rowsSetSize = 0;
     m_deleteSetSize = 0;
     m_insertSetSize = 0;
     uint32_t readSetSize = 0;
     if (!QuickVersionCheckNoValidation(txMan, readSetSize)) {
         return RC::RC_ABORT;
     }
 
     bool result = true;
     if (cc_mode == 2) result = UpdateWriteHeaderForPCC(txMan, server_id);
     else if (cc_mode == 3) result = LockWriteHeaderForPCC(txMan, server_id);
     else if (cc_mode == 4) result = LockWriteHeaderForPCC(txMan, server_id);
     if (result){
         m_rowsLocked = true;
         return RC::RC_OK;
     }
     MOTAdaptor::CommitUpdate_abort_interactive_num.fetch_add(1);
     return RC::RC_ABORT;
 }
 
 RC OccTransactionManager::UnlockCommitUpdate(TxnManager *txMan, uint32_t server_id){
     bool result = true;
     if (cc_mode == 2) result = true;
     else if (cc_mode == 3) result = UnlockWriteHeaderForPCC(txMan, server_id);
     else if (cc_mode == 4) result = UnlockWriteHeaderForPCC(txMan, server_id);
     if (result){
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }

 bool OccTransactionManager::UpdateWriteHeaderForPCC(TxnManager *txMan, uint32_t server_id)  //Commit Phase set csn
 {
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     bool result = true;
     std::string table_name, key, key_temp, csn_temp, csn_result;
     uint64_t currentCSN;
     MOT::Row* row;
 //    currentCSN = txMan->GetCommitSequenceNumber();
     currentCSN = txMan->pre_csn;
     csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
     for (const auto &raPair : orderedSet){
         const Access *ac = raPair.second;
         if (ac->m_type == RD){
             continue;
         }
         else if(ac->m_type == INS) { //已经生成了localInsertRow 问题在于先后插入，先插入的已经完成后将dirty置为true
             if(ac->m_localInsertRow->GetTable() == nullptr){
                 result = false;
                 continue;
             }
             table_name = ac->m_localInsertRow->GetTable()->GetLongTableName();
             void* buf;
             MOT::Key* key_ptr = ac->m_localInsertRow->GetTable()->BuildKeyByRow(ac->m_localInsertRow, txMan, buf);
             // MOT::Key* key_ptr = txMan->GetTxnKey(ac->m_localInsertRow->GetTable()->GetPrimaryIndex());
             if(key_ptr == nullptr) assert(false);
             key = key_ptr->GetKeyStr();
             row = ac->GetSentinel()->GetData();
             if (!(row == nullptr || row->IsAbsentRow())) {
                 result = false;
             }
             key_temp = table_name + key;
             if(!MOTAdaptor::insertSetForCommit.insert(key_temp, csn_temp, &csn_result)){
                 result = false;
             }
             MOTAdaptor::abort_transcation_csn_set.insert(csn_result, csn_result);
             // MOT::MemSessionFree(buf);
         }
         else{ // update or delete
             if (ac->m_localRow->GetTable() == nullptr){
                 result = false;
                 continue;
             }
             if (ac->m_origSentinel == nullptr || ac->m_origSentinel->IsDirty()) {
                 result = false;
                 continue;
             }
 
             if(!ac->GetRowFromHeader()->m_rowHeader.SetWriteForCommitPCC(txMan->GetCommitSequenceNumber(), txMan->GetStartEpoch(), txMan->GetCommitEpoch(), server_id, txMan->IsInteractive())){
                 result = false;
             }
         }
     }
     if(result == false){
         MOTAdaptor::abort_transcation_csn_set.insert(csn_temp, csn_temp);
     }
     return result;
 }
 
 bool OccTransactionManager::LockWriteHeaderForPCC(TxnManager *txMan, uint32_t server_id)    //Commit Phase lock header
 {
     string tmp_csn = to_string(txMan->pre_csn) + ":0";
     uint32_t numSentinelLock = 0;
     bool result = true;
 
     if(LockHeadersPlor(txMan, numSentinelLock) != RC_OK) {
         result = false;
         if (is_debug_print_enable)MOT_LOG_INFO("LockWriteHeaderForPCC failed tmp_csn : %s", tmp_csn.c_str());
     }
     if (!result) {
         ReleaseHeaderLocks(txMan, numSentinelLock);
     } else {
         if (is_debug_print_enable) MOT_LOG_INFO("LockWriteHeaderForPCC success tmp_csn : %s, numSentinelLock : %llu, m_writeSetSize : %llu", tmp_csn.c_str(), numSentinelLock, m_writeSetSize);
     }
     return result;
 }
 
 bool OccTransactionManager::UnlockWriteHeaderForPCC(TxnManager *txMan, uint32_t server_id)    //Commit Phase unlock header
 {
     // 上锁失败时，header锁就已经全部释放了
     // 热点数据验证失败，则需要unlock
     uint32_t numSentinelLock = 10;
     ReleaseHeaderLocks(txMan, numSentinelLock);
     return true;
 }
 
 RC OccTransactionManager::CommitCheck(TxnManager *txMan, uint32_t server_id){ //not use
     bool result = ValidateWriteSetForCommit(txMan, server_id);
     if (result){
         return RC::RC_OK;
     }
     return RC::RC_ABORT;
 }
 
 bool OccTransactionManager::ValidateWriteSetForCommit(TxnManager *txMan, uint32_t server_id){ //not use
     std::string table_name, key, key_temp, csn_temp;
     MOT::Key* key_ptr;
     void* buf;
     uint64_t currentCSN = txMan->GetCommitSequenceNumber();
     csn_temp = std::to_string(txMan->GetCommitSequenceNumber())+ ":" + std::to_string(server_id);
     uint64_t csn_tmp = txMan->GetCommitSequenceNumber();
     TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
     for (const auto &raPair : orderedSet)
     {
         const Access *ac = raPair.second;
         if (ac->m_type == RD or !ac->m_params.IsPrimarySentinel()){
             continue;
         }
         else if(ac->m_type == INS){
             table_name = ac->m_localInsertRow->GetTable()->GetLongTableName();
             key_ptr = ac->m_localInsertRow->GetTable()->BuildKeyByRow(ac->m_localInsertRow, txMan, buf);
             if(key_ptr == nullptr) assert(false);
             key_temp = table_name + key_ptr->GetKeyStr();
             // MOT::MemSessionFree(buf);
             if(MOTAdaptor::insertSetForCommit.contain(key_temp, csn_temp) == false){
                 return false;
             }
         }
         else{       // update and delete
             // wzy: double check locks
             auto currRow = ac->m_localRow;
             std::string tmp_rowid = currRow->GetTable()->GetLongTableName() + ":" + to_string(currRow->GetRowId());
 
             string res_tid = "";
             if (!MOTAdaptor::IsRowAvailable(tmp_rowid, currentCSN, csn_temp, res_tid)){
                 MOT_LOG_INFO("Commit Check IsRowAvailable() not available visited row : %s, cur csn : %s, locked csn : %s",  tmp_rowid.c_str(), csn_temp.c_str(), res_tid.c_str());
                 return false;
             }
 
             MOT::RowHeader& row_header = ac->GetRowFromHeader()->m_rowHeader;
             if(row_header.GetCSN() != csn_tmp  || row_header.GetServerId() != server_id) {
                 MOT_LOG_INFO("abort %llu %llu %llu", row_header.GetCSN(), csn_tmp, row_header.Getcsn());
                 return false;
             }
         }
     }
     return true;
 }
 
 void OccTransactionManager::updateInsertSetSize(TxnManager * txMan){
     TxnAccess *tx = txMan->m_accessMgr.Get();
     const uint32_t rowCount = tx->m_rowCnt;
 
     m_writeSetSize = 0;
     m_rowsSetSize = 0;
     m_deleteSetSize = 0;
     m_insertSetSize = 0;
     m_txnCounter++;
 
     TxnOrderedSet_t &orderedSet = tx->GetOrderedRowSet();
     MOT_ASSERT(rowCount == orderedSet.size());
 
     for (const auto &raPair : orderedSet)
     {
         const Access *ac = raPair.second;
         if (ac->m_params.IsPrimarySentinel())
         {
             m_rowsSetSize++;
         }
         switch (ac->m_type)
         {
             case WR:
                 m_writeSetSize++;
                 break;
             case DEL:
                 m_writeSetSize++;
                 m_deleteSetSize++;
                 break;
             case INS:
                 m_insertSetSize++;
                 m_writeSetSize++;
                 break;
             case RD: /// now only support the "READ-COMMITTED" isolation
                 break;
             default:
                 break;
         }
     }
 }
 
 bool OccTransactionManager::IsReadOnly(TxnManager * txMan){
     // return txMan->m_accessMgr.Get()->m_rowCnt == 0;
     return m_writeSetSize == 0;
 }
 
 }  // namespace MOT
 