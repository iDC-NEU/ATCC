/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of tfhe Mulan PSL v2.
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
 * mot_internal.h
 *    MOT Foreign Data Wrapper internal interfaces to the MOT engine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/src/mot_internal.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_INTERNAL_H
#define MOT_INTERNAL_H

#include <map>
#include <string>
#include "catalog_column_types.h"
#include "foreign/fdwapi.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "pgstat.h"
#include "global.h"
#include "mot_fdw_error.h"
#include "mot_fdw_xlog.h"
#include "system/mot_engine.h"
#include "bitmapset.h"
#include "storage/mot/jit_exec.h"
#include "mot_match_index.h"
#include <atomic>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <mutex>
#include <condition_variable>
#include "pthread.h"
#include "table.h"
#include "row.h"
#include "neu_concurrency_tools/blockingconcurrentqueue.h"
#include "neu_concurrency_tools/blocking_mpmc_queue.h"
#include "neu_concurrency_tools/readerwriterqueue.h"
#include <vector>
#include <typeinfo>
#include <random>
#include <stdlib.h>
#include <set>
#include <queue>
// #include "message.pb.h"      // 不能引用
// #include <semaphore.h>
// #include <semaphore.h>

using std::map;
using std::string;

#define MAX_TXN_NUM 100000
#define ADD_NUM 10000000001
#define MOD_NUM 10000000000

extern void EpochLogicalTimerManagerThreadMain(uint64_t id);
extern void EpochPhysicalTimerManagerThreadMain(uint64_t id);
extern void EpochMessageManagerThreadMain(uint64_t id);
extern void EpochMessageCacheManagerThreadMain(uint64_t id);

extern void EpochNotifyThreadMain(uint64_t id);
extern void EpochPackThreadMain(uint64_t id);

extern void EpochRaftSendThreadMain(uint64_t id);
extern void EpochSendThreadMain(uint64_t id);

extern void EpochRaftListenThreadMain(uint64_t id);
extern void EpochListenThreadMain(uint64_t id);
extern void EpochUnseriThreadMain(uint64_t id);
extern void EpochUnpackThreadMain(uint64_t id);
extern void EpochMergeThreadMain(uint64_t id);
extern void EpochCommitThreadMain(uint64_t id);
extern void EpochRecordCommitThreadMain(uint64_t id);

extern void EpochMessageSendThreadMain(uint64_t id);
extern void EpochMessageListenThreadMain(uint64_t id);

extern void MultiRaftThreadMain(uint64_t id);

extern void EpochLockThreadMain(uint64_t id);   // wzy:
extern void EpochLockThreadMain_WoundWait(uint64_t id);   // wzy:
extern void EpochLockThreadMain_Wait(uint64_t id);   // wzy:

extern void TryGetServerInfo();                    // wzy: 接收Server Info
extern void ReGetServerInfo();                    // wzy: 接收Server Info


extern void EpochCleanVersionThreadMain(uint64_t id);   // wzy:

namespace aum {
    class SpinLock {
        public:
        // constructors
        SpinLock() = default;

        SpinLock(const SpinLock &) = delete;            // non construction-copyable
        SpinLock &operator=(const SpinLock &) = delete; // non copyable

        // Modifiers
        void lock() {
            while (lock_.test_and_set());
        }

        void unlock() { lock_.clear(std::memory_order_release); }

        private:
        std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
    };

    template<typename key, typename value, typename pointer>
    class atomic_unordered_map {
    public:
        typedef typename std::unordered_map<key, value>::iterator map_iterator;
        typedef typename std::unordered_map<key, value>::size_type size_type;
        // typedef typename std::map<key, value>::iterator map_iterator;
        // typedef typename std::map<key, value>::size_type size_type;
    public:

        bool insert(key &k, value &v, pointer *ptr, std::function<bool(const value &, const value &)> 
            cmp = [](const value& o, const value& n){return n < o;}) //为insert服务
        {
            bool result = true;
            lock.lock();
            map_iterator iter = map.find(k);
            if (iter == map.end()) {
                map[k] = v;
                *ptr = "0";
                result = true;
            } else {
                // cmp(oldCsn, newCsn) -> bool
                // if true: 新的事务覆盖旧的事务
                if (iter->second > v) {
                    *ptr = map[k];
                    map[k] = v;
                    result = true;
                } 
                else if(iter->second == v){//相同则为同一事务执行的插入，不能将map[k]返回abort掉
                    *ptr = "0";
                    result = true;
                }
                else{
                    *ptr = v;
                    result = false;
                }
            }
            lock.unlock();
            return result;
        }

        void insert(key &k, value &v){//为update服务
            lock.lock();
            map[k] = v;
            lock.unlock();
        }


        void remove(key &k, value &v) 
        {
            lock.lock();
            map_iterator iter = map.find(k);
            if (iter != map.end() && iter->second == v) {
                //if the abort txn has insert row and has not been modifid by ohters 
                //then remove it from map;or keep it 
                map.erase(iter);
            }
            lock.unlock();
        }

        void clear() {
            lock.lock();
            map.clear();
            lock.unlock();
        }

        void unsafe_clear() { map.clear(); }

        map_iterator unsafe_begin() {
            return map.begin();
        }

        map_iterator unsafe_end() {
            return map.end();
        }

        map_iterator unsafe_find(key &k) {
            return map.find(k);
        }

        bool contain(key &k, value &v){
            map_iterator iter = map.find(k);
            if(iter != map.end() && iter->second == v){
                return true;
            }
            return false;
        }

        size_type size() { return map.size(); }

    public:
        std::unordered_map<key, value> map;
        // std::map<key, value> map;
        aum::SpinLock lock;
    };

    template<typename key, typename value, typename pointer>
    class concurrent_unordered_map {
    public:
        typedef typename std::unordered_map<key, value>::iterator map_iterator;
        typedef typename std::unordered_map<key, value>::size_type size_type;

        bool insert(key &k, value &v, pointer *ptr, std::function<bool(const value &, const value &)> 
            cmp = [](const value& o, const value& n){return n < o;}) //为insert服务
        {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            bool result = true;
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if (iter == _map_temp.end()) {
                _map_temp[k] = v;
                *ptr = "0";
                result = true;
            } else {
                // cmp(oldCsn, newCsn) -> bool
                // if true: 新的事务覆盖旧的事务
                if (iter->second > v) {
                    *ptr = _map_temp[k];
                    _map_temp[k] = v;
                    result = true;
                } 
                else if(iter->second == v){//相同则为同一事务执行的插入，不能将map[k]返回abort掉
                    *ptr = "0"; //暂且只支持string
                    result = true;
                }
                else{
                    *ptr = v;
                    result = false;
                }
            }
            lock.unlock();
            return result;
        }

        void insert(key &k, value &v){
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            _map_temp[k] = v;
        }

        // wzy: v为vector<string>
        void insert_vector(key& k, std::string& e)
        {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            bool result = false;
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            std::shared_ptr<std::vector<std::string>> m_vec;
            value v;                        // pair
            if (iter == _map_temp.end()) {  // 没有找到k的情况
                m_vec = std::make_shared<std::vector<std::string>>();
                m_vec->emplace_back(e);
                _map_temp[k] = m_vec;
                result = true;
            } else {  // 找到k的情况，向k中添加
                m_vec = _map_temp[k];
                // 遍历v，确认tid是否在其中，若不在则插入，若在则跳过，不能重复
                for (const auto& temp : *m_vec) {
                    if (temp == e) {
                        result = true;
                        break;
                    }
                }
                if (!result) {
                    m_vec->emplace_back(e);
                }
                _map_temp[k] = m_vec;
                result = true;
            }
            lock.unlock();
        }

        void remove(key &k, value &v) 
        {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                if (iter->second == v) {
                    //if the abort txn has insert row and has not been modifid by ohters 
                    //then remove it from map;or keep it 
                    _map_temp.erase(iter);
                } 
            }
            lock.unlock();
        }

        void remove(key &k) 
        {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                _map_temp.erase(iter);
            }
            lock.unlock();
        }

        void clear() {
            for(uint64_t i = 0; i < _N; i ++){
                std::unique_lock<std::mutex> lock(_mutex[i]);
                _map[i].clear();
            }
        }

        void unsafe_clear() { 
            for(uint64_t i = 0; i < _N; i ++){
                _map[i].clear();
            }
        }

        bool contain(key &k, value &v){
            std::mutex& _mutex_temp = GetMutexRef(k);   // wzy: 保险起见先上锁
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(iter->second == v){
                    return true;
                }
            }
            return false;
        }

        bool contain(key &k){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                return true;
            }
            return false;
        }

        bool unsafe_contain(key &k, value &v){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(iter->second == v){
                    return true;
                }
            }
            return false;
        }

        size_type size() {
            size_type ans = 0;
            for(uint64_t i = 0; i < _N; i ++){
                std::unique_lock<std::mutex> lock(_mutex[i]);
                ans += _map[i].size();
            }
            return ans;
        }

        // wzy: 获取到元素
        bool get_element(key& k, value& v)
        {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                v = iter->second;
                return true;
            }
            return false;
        }

        // wzy: 获取queue return true，若不存在则创建 return false
        bool get_or_create_queue(key& k, value& v, value& new_v)
        {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                v = iter->second;
                return true;
            } else {
                _map_temp[k] = new_v;
                v = new_v;
                return true;
            }
        }

        void remove_queue(key &k)
        {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                auto v = iter->second;
                _map_temp.erase(iter);
            }
            lock.unlock();
        }

        bool add_visit(key& k, value& v) {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            value old_v;
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                old_v = iter->second;
                old_v += v;
                _map_temp[k] = old_v;
                return true;
            } else {
                _map_temp[k] = v;
                return false;
            }
        }

        bool contain_lock(key &k, value &v){
//            std::mutex& _mutex_temp = GetMutexRef(k);   // wzy: 保险起见先上锁
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
//            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(iter->second == v){
                    return true;
                } else return false;
            }
            return true;
        }

        // wzy: cas states
        bool cas_element(key& k, value old_v, value new_v) {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                if (iter->second == old_v) {
                    _map_temp[k] = new_v;
                    return true;
                } else return false;
            }
            return true;       // 不存在元素
        }

    protected:
        inline std::unordered_map<key, value>& GetMapRef(const key k){ return _map[(_hash(k) % _N)]; }
        inline std::unordered_map<key, value>& GetMapRef(const key k) const { return _map[(_hash(k) % _N)]; }
        inline std::mutex& GetMutexRef(const key k) { return _mutex[(_hash(k) % _N)]; }
        inline std::mutex& GetMutexRef(const key k) const {return _mutex[(_hash(k) % _N)]; }

    private:
        const static uint64_t _N = 997;//1217 12281 122777 prime
        std::hash<key> _hash;
        std::unordered_map<key, value> _map[_N];
        std::mutex _mutex[_N];
    };

}; // NAMESPACE AUM


#define MIN_DYNAMIC_PROCESS_MEMORY 2 * 1024 * 1024

#define MOT_INSERT_FAILED_MSG "Insert failed"
#define MOT_UPDATE_FAILED_MSG "Update failed"
#define MOT_DELETE_FAILED_MSG "Delete failed"

#define MOT_UNIQUE_VIOLATION_MSG "duplicate key value violates unique constraint \"%s\""
#define MOT_UNIQUE_VIOLATION_DETAIL "Key %s already exists."
#define MOT_TABLE_NOTFOUND "Table \"%s\" doesn't exist"
#define MOT_UPDATE_INDEXED_FIELD_NOT_SUPPORTED "Update indexed field \"%s\" in table \"%s\" is not supported"

#define NULL_DETAIL ((char*)nullptr)
#define abortParentTransaction(msg, detail) \
    ereport(ERROR,                          \
        (errmodule(MOD_MOT), errcode(ERRCODE_FDW_ERROR), errmsg(msg), (detail != nullptr ? errdetail(detail) : 0)));

#define abortParentTransactionParams(error, msg, msg_p, detail, detail_p) \
    ereport(ERROR, (errmodule(MOD_MOT), errcode(error), errmsg(msg, msg_p), errdetail(detail, detail_p)));

#define abortParentTransactionParamsNoDetail(error, msg, ...) \
    ereport(ERROR, (errmodule(MOD_MOT), errcode(error), errmsg(msg, __VA_ARGS__)));

#define isMemoryLimitReached()                                                                                         \
    {                                                                                                                  \
        if (MOTAdaptor::m_engine->IsSoftMemoryLimitReached()) {                                                        \
            MOT_LOG_ERROR("Maximum logical memory capacity %lu bytes of allowed %lu bytes reached",                    \
                (uint64_t)MOTAdaptor::m_engine->GetCurrentMemoryConsumptionBytes(),                                    \
                (uint64_t)MOTAdaptor::m_engine->GetHardMemoryLimitBytes());                                            \
            ereport(ERROR,                                                                                             \
                (errmodule(MOD_MOT),                                                                                   \
                    errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),                                                            \
                    errmsg("You have reached a maximum logical capacity"),                                             \
                    errdetail("Only destructive operations are allowed, please perform database cleanup to free some " \
                              "memory.")));                                                                            \
        }                                                                                                              \
    }

namespace MOT {
class Table;
class Index;
class IndexIterator;
class Column;
class MOTEngine;
}  // namespace MOT

#ifndef MOTFdwStateSt
typedef struct MOTFdwState_St MOTFdwStateSt;
#endif

typedef enum : uint8_t { SORTDIR_NONE = 0, SORTDIR_ASC = 1, SORTDIR_DESC = 2 } SORTDIR_ENUM;
typedef enum : uint8_t { FDW_LIST_STATE = 1, FDW_LIST_BITMAP = 2 } FDW_LIST_TYPE;

typedef struct Order_St {
    SORTDIR_ENUM m_order;
    int m_lastMatch;
    int m_cols[MAX_KEY_COLUMNS];

    void init()
    {
        m_order = SORTDIR_NONE;
        m_lastMatch = -1;
        for (uint32_t i = 0; i < MAX_KEY_COLUMNS; i++)
            m_cols[i] = 0;
    }
} OrderSt;

#define MOT_REC_TID_NAME "ctid"

typedef struct MOTRecConvert {
    union {
        uint64_t m_ptr;
        ItemPointerData m_self; /* SelfItemPointer */
    } m_u;
} MOTRecConvertSt;

#define SORT_STRATEGY(x) ((x == BTGreaterStrategyNumber) ? SORTDIR_DESC : SORTDIR_ASC)

struct MOTFdwState_St {
    ::TransactionId m_txnId;
    bool m_allocInScan;
    CmdType m_cmdOper;
    SORTDIR_ENUM m_order;
    bool m_hasForUpdate;
    Oid m_foreignTableId;
    AttrNumber m_numAttrs;
    AttrNumber m_ctidNum;
    uint16_t m_numExpr;
    uint8_t* m_attrsUsed;
    uint8_t* m_attrsModified;  // this will be merged into attrs_used in BeginModify
    List* m_remoteConds;
    List* m_remoteCondsOrig;
    List* m_localConds;
    List* m_execExprs;
    ExprContext* m_econtext;
    double m_startupCost;
    double m_totalCost;

    MatchIndex* m_bestIx;
    MatchIndex* m_paramBestIx;
    MatchIndex m_bestIxBuf;

    // ENGINE
    MOT::Table* m_table;
    MOT::IndexIterator* m_cursor[2] = {nullptr, nullptr};
    MOT::TxnManager* m_currTxn;
    void* m_currItem = nullptr;
    uint32_t m_rowsFound = 0;
    bool m_cursorOpened = false;
    MOT::MaxKey m_stateKey[2];
    bool m_forwardDirectionScan;
    MOT::AccessType m_internalCmdOper;
};

class MOTAdaptor {
public:
    static void Init();
    static void Destroy();
    static void NotifyConfigChange();
    static void InitDataNodeId();

    static inline void GetCmdOper(MOTFdwStateSt* festate)
    {
        switch (festate->m_cmdOper) {
            case CMD_SELECT:
                if (festate->m_hasForUpdate) {
                    festate->m_internalCmdOper = MOT::AccessType::RD_FOR_UPDATE;
                } else {
                    festate->m_internalCmdOper = MOT::AccessType::RD;
                }
                break;
            case CMD_DELETE:
                festate->m_internalCmdOper = MOT::AccessType::DEL;
                break;
            case CMD_UPDATE:
                festate->m_internalCmdOper = MOT::AccessType::WR;
                break;
            case CMD_INSERT:
                festate->m_internalCmdOper = MOT::AccessType::INS;
                break;
            case CMD_UNKNOWN:
            case CMD_MERGE:
            case CMD_UTILITY:
            case CMD_NOTHING:
            default:
                festate->m_internalCmdOper = MOT::AccessType::INV;
                break;
        }
    }

    static MOT::TxnManager* InitTxnManager(
        const char* callerSrc, MOT::ConnectionId connection_id = INVALID_CONNECTION_ID);
    static void DestroyTxn(int status, Datum ptr);
    static void DeleteTablePtr(MOT::Table* t);

    static MOT::RC CreateTable(CreateForeignTableStmt* table, TransactionId tid);
    static MOT::RC CreateIndex(IndexStmt* index, TransactionId tid);
    static MOT::RC DropIndex(DropForeignStmt* stmt, TransactionId tid);
    static MOT::RC DropTable(DropForeignStmt* stmt, TransactionId tid);
    static MOT::RC TruncateTable(Relation rel, TransactionId tid);
    static MOT::RC VacuumTable(Relation rel, TransactionId tid);
    static uint64_t GetTableIndexSize(uint64_t tabId, uint64_t ixId);
    static MotMemoryDetail* GetMemSize(uint32_t* nodeCount, bool isGlobal);
    static MotSessionMemoryDetail* GetSessionMemSize(uint32_t* sessionCount);
    static MOT::RC ValidateCommit();

    static MOT::RC SendInteractiveLockInfo(MOT::Row* currRow);  // wzy: 模拟事务执行层
    static void UnlockInteractiveLockInfo(uint64_t csn, bool abort);   // wzy: 模拟事务执行层解锁

    ////////////// PLOR ///////////////
    static MOT::RC SwitchPlor();
    static MOT::RC ValidateCommitPlor();            // wzy: Plor
    static void UnlockInteractiveLockInfoPlor(uint64_t csn, bool abort);   // wzy: 模拟事务执行层解锁

    ///////////// Wound-wait ////////////////
    static MOT::RC SwitchWoundWait();
    static MOT::RC ValidateCommitWoundWait();            // wzy: wound-wait
    static void UnlockInteractiveLockInfoWoundWait(uint64_t csn, bool abort);   // wzy: 模拟事务执行层解锁

    ///////////// DL Detect ////////////
    static MOT::RC ValidateCommitDL();
    static void UnlockInteractiveLockInfoDL(uint64_t csn, bool abort);   // wzy: 模拟事务执行层解锁
    /////////////////////////////////////

    static void RecordCommit(uint64_t csn);
    static MOT::RC Commit(uint64_t csn);  // Does both ValidateCommit and RecordCommit
    static void EndTransaction();
    static void Rollback();
    static MOT::RC Prepare();
    static void CommitPrepared(uint64_t csn);
    static void RollbackPrepared();
    static MOT::RC InsertRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot);
    static MOT::RC UpdateRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot, MOT::Row* currRow);
    static MOT::RC DeleteRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot);

    /* Convertors */
    inline static void PGNumericToMOT(const Numeric n, MOT::DecimalSt& d)
    {
        int sign = NUMERIC_SIGN(n);

        d.m_hdr.m_flags = 0;
        d.m_hdr.m_flags |= (sign == NUMERIC_POS
                                ? DECIMAL_POSITIVE
                                : (sign == NUMERIC_NEG ? DECIMAL_NEGATIVE : ((sign == NUMERIC_NAN) ? DECIMAL_NAN : 0)));
        d.m_hdr.m_ndigits = NUMERIC_NDIGITS(n);
        d.m_hdr.m_scale = NUMERIC_DSCALE(n);
        d.m_hdr.m_weight = NUMERIC_WEIGHT(n);
        d.m_round = 0;
        if (d.m_hdr.m_ndigits > 0) {
            errno_t erc = memcpy_s(d.m_digits,
                DECIMAL_MAX_SIZE - sizeof(MOT::DecimalSt),
                (void*)NUMERIC_DIGITS(n),
                d.m_hdr.m_ndigits * sizeof(NumericDigit));
            securec_check(erc, "\0", "\0");
        }
    }

    inline static Numeric MOTNumericToPG(MOT::DecimalSt* d)
    {
        NumericVar v;

        v.ndigits = d->m_hdr.m_ndigits;
        v.dscale = d->m_hdr.m_scale;
        v.weight = (int)(int16_t)(d->m_hdr.m_weight);
        v.sign = (d->m_hdr.m_flags & DECIMAL_POSITIVE
                      ? NUMERIC_POS
                      : (d->m_hdr.m_flags & DECIMAL_NEGATIVE ? NUMERIC_NEG
                                                             : ((d->m_hdr.m_flags & DECIMAL_NAN) ? DECIMAL_NAN : 0)));
        v.buf = (NumericDigit*)&d->m_round;
        v.digits = (NumericDigit*)d->m_digits;

        return makeNumeric(&v);
    }

    // data conversion
    static void DatumToMOT(MOT::Column* col, Datum datum, Oid type, uint8_t* data);
    static void DatumToMOTKey(MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len,
        KEY_OPER oper, uint8_t fill = 0x00);
    static void MOTToDatum(MOT::Table* table, const Form_pg_attribute attr, uint8_t* data, Datum* value, bool* is_null);

    static void PackRow(TupleTableSlot* slot, MOT::Table* table, uint8_t* attrs_used, uint8_t* destRow);
    static void PackUpdateRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* destRow);
    static void UnpackRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* srcRow);

    // scan helpers
    static void OpenCursor(Relation rel, MOTFdwStateSt* festate);
    static bool IsScanEnd(MOTFdwStateSt* festate);
    static void CreateKeyBuffer(Relation rel, MOTFdwStateSt* festate, int start);

    // planning helpers
    static bool SetMatchingExpr(MOTFdwStateSt* state, MatchIndexArr* marr, int16_t colId, KEY_OPER op, Expr* expr,
        Expr* parent, bool set_local);
    static MatchIndex* GetBestMatchIndex(
        MOTFdwStateSt* festate, MatchIndexArr* marr, int numClauses, bool setLocal = true);
    inline static int32_t AddParam(List** params, Expr* expr)
    {
        int32_t index = 0;
        ListCell* cell = nullptr;

        foreach (cell, *params) {
            ++index;
            if (equal(expr, (Node*)lfirst(cell))) {
                break;
            }
        }
        if (cell == nullptr) {
            /* add the parameter to the list */
            ++index;
            *params = lappend(*params, expr);
        }

        return index;
    }

    static MOT::MOTEngine* m_engine;
    static bool m_initialized;
    static bool m_callbacks_initialized;

private:
    /**
     * @brief Adds all the columns.
     * @param table Table object being created.
     * @param tableElts Column definitions list.
     * @param[out] hasBlob Whether any column is a blob.
     * NOTE: On failure, table object will be deleted and ereport will be done.
     */
    static void AddTableColumns(MOT::Table* table, List *tableElts, bool& hasBlob);

    static void ValidateCreateIndex(IndexStmt* index, MOT::Table* table, MOT::TxnManager* txn);

    static void VarcharToMOTKey(MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len,
        KEY_OPER oper, uint8_t fill);
    static void FloatToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
    static void NumericToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
    static void TimestampToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
    static void TimestampTzToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
    static void DateToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);





































private:
    typedef typename std::unordered_set<MOT::TxnManager *> TxnBuffer;
    typedef typename TxnBuffer::iterator TxnBufferIter;
    static TxnBuffer txnBuffer;
    
    static bool timerStop;
    //ADDBY NUE Concurrency
    static volatile bool remote_execed, record_committed, remote_record_committed, is_current_epoch_abort;
    static volatile bool lock_granted, lock_execed, lock_committed, deadlock_detectted; /// wzy :lockinfo
    static std::atomic<uint64_t> lock_grant_num, lock_should_grant_num;

    static volatile uint64_t logical_epoch;
    static volatile uint64_t physical_epoch;

public:
    static uint64_t max_length, pack_num;
    static std::default_random_engine random_mot;
    static std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> 
        local_txn_counters, local_txn_exc_counters, local_txn_execed_counters, local_txn_committed_counters,
        local_txn_index, record_commit_txn_counters, record_committed_txn_counters, remote_merged_txn_counters, remote_commit_txn_counters,
        remote_committed_txn_counters, limite_txn_num;

    static std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> local_lockinfo_counters, merge_lockinfo_counters, local_lockinfo_execed_counters;    /// wzy :lockinfo
    static std::map<uint64_t, std::unique_ptr<std::vector<MOT::Row*>>> remote_row_ptr_map;
    static aum::concurrent_unordered_map<std::string, std::string, std::string> insertSet;
    static aum::concurrent_unordered_map<std::string, std::string, std::string> insertSetForCommit;
    static aum::concurrent_unordered_map<std::string, std::string, std::string> abort_transcation_csn_set;


    // wzy: 记录对应的tablename Rowid以及csn_server_id 队列和获取到锁的csn_server_id，用cv来唤醒
    class LockRequestQueue;
    class WaitForGraph;
    class DynamicHotRow;
    static aum::concurrent_unordered_map<std::string, std::shared_ptr<LockRequestQueue>, std::string>
        row_lockrequest_map;

    static aum::concurrent_unordered_map<std::string, std::string, std::string> epoch_lock_set;     // wzy: 优化

    // wzy : csn + server id - row lock request queue 处理abort的交互型事务
    static aum::concurrent_unordered_map<std::string, std::shared_ptr<std::vector<std::shared_ptr<LockRequestQueue>>>, std::string>
        csn_requests_map;

    // wzy: 快速中止事务
    static aum::concurrent_unordered_map<std::string, std::shared_ptr<std::vector<std::string>>, std::string>
        txn_rowid_map;

    // wzy: 事务状态用于Wound-wait plus中 0-running 1-abort 2-CommitPhase
    static aum::concurrent_unordered_map<uint64_t, int, std::string> txn_state_map_plor_;


    // wzy: 记录已有LockRequest队列的rowid
    static std::set<std::string> rowid_set;     // 去重   // delete
    static std::vector<std::string> rowid_vec;  // 遍历   // delete
    static std::mutex rowid_set_mutex;      // delete

    // wzy: 等待图，解决死锁，上锁参考
    static std::map<std::string, std::set<std::string>> wait_for;       // delete
    static std::mutex wait_for_mutex;       // delete

    static WaitForGraph wait_for_graph;
    static aum::concurrent_unordered_map<std::string, std::string, std::string> deadlock_abort_set;        // 记录由死锁检测abort的事务tid

    static std::set<TransactionId> active_txn_list;     /// wzy: 活跃事务id
    static std::mutex active_txn_list_mutex;
    static std::atomic<TransactionId> min_active_txn_id;      // wzy: 最小活跃事务id
    static std::list<MOT::Row*> clean_row_list;             /// wzy: 记录有多版本的row
    static std::unordered_map<std::string, std::list<MOT::Row*>::iterator> clean_row_list_map;        // 去重
    static std::mutex clean_row_list_mutex;

    static std::vector<std::shared_ptr<moodycamel::BlockingConcurrentQueue<std::shared_ptr<LockRequestQueue>>>> active_lock_queues;          // wzy: 分epoch记录活跃lock queue
    static std::vector<std::shared_ptr<std::unordered_set<std::string>>> active_lock_queues_set;
    static std::vector<std::mutex> active_lock_queues_mutex;

    static uint64_t lock_thread_num;
    static std::vector<std::shared_ptr<std::list<std::shared_ptr<MOTAdaptor::LockRequestQueue>>>> active_lock_list;         // wzy: 用于多线程
    static std::vector<std::shared_ptr<std::unordered_map<std::shared_ptr<LockRequestQueue>, std::list<std::shared_ptr<LockRequestQueue>>::iterator>>> active_lock_list_set;
    static std::vector<std::mutex> active_lock_list_mutex;
    static std::atomic<uint64_t> active_lock_list_size;
    static std::vector<bool> active_lock_list_exced;

    static std::list<std::shared_ptr<LockRequestQueue>> active_queue_list;             /// wzy: 记录活跃的queue
    static std::unordered_map<std::shared_ptr<LockRequestQueue>, std::list<std::shared_ptr<LockRequestQueue>>::iterator> active_queue_set;
    static std::mutex active_queue_list_mutex;

    static DynamicHotRow dynamic_hot_rows;

    // wzy: recovery debug
    static std::atomic<bool> isInited;
    static std::atomic<bool> need_clean;

    // wzy: 统计
    static std::atomic<uint64_t> txn_total_epoch;
    static std::atomic<uint64_t> txn_total_readCnt;
    static std::atomic<uint64_t> txn_total_writeCnt;
    static std::atomic<uint64_t> txn_total_hotCnt;
    static std::atomic<uint64_t> txn_total_lockCnt;

    // wzy: 30s statistics
    static std::atomic<uint64_t> temp_commit_txn_num;
    static std::atomic<uint64_t> txn_temp_total_time;
    static std::atomic<uint64_t> txn_temp_total_readCnt;
    static std::atomic<uint64_t> txn_temp_total_writeCnt;
    static std::atomic<uint64_t> txn_temp_total_hotCnt;
    static std::atomic<uint64_t> txn_temp_total_epoch;

    static std::atomic<uint64_t> txn_total_switchTime;
    static std::atomic<uint64_t> txn_total_read_lockTime;
    static std::atomic<uint64_t> txn_total_write_lockTime;
    static std::atomic<uint64_t> txn_total_validate_lockTime;
    static std::atomic<uint64_t> txn_total_validate_hotOccTime;

    static std::atomic<uint64_t> txn_total_switchCnt;
    static std::atomic<uint64_t> txn_total_read_lockCnt;
    static std::atomic<uint64_t> txn_total_write_lockCnt;
    static std::atomic<uint64_t> txn_total_validate_lockCnt;
    static std::atomic<uint64_t> txn_total_validate_hotOccCnt;

    static std::atomic<uint64_t> txn_total_switchTime_RLock;
    static std::atomic<uint64_t> txn_total_switchTime_WLock;
    static std::atomic<uint64_t> txn_total_switchTime_Sentinel;

    static std::atomic<uint64_t> txn_total_switchTime_RLockCnt;
    static std::atomic<uint64_t> txn_total_switchTime_WLockCnt;
    static std::atomic<uint64_t> txn_total_switchTime_SentinelCnt;

    static std::atomic<uint64_t> txn_temp_total_switchTime;
    static std::atomic<uint64_t> txn_temp_total_read_lockTime;
    static std::atomic<uint64_t> txn_temp_total_write_lockTime;
    static std::atomic<uint64_t> txn_temp_total_validate_lockTime;
    static std::atomic<uint64_t> txn_temp_total_validate_hotOccTime;

    static std::atomic<uint64_t> txn_temp_total_switchCnt;
    static std::atomic<uint64_t> txn_temp_total_read_lockCnt;
    static std::atomic<uint64_t> txn_temp_total_write_lockCnt;
    static std::atomic<uint64_t> txn_temp_total_validate_lockCnt;
    static std::atomic<uint64_t> txn_temp_total_validate_hotOccCnt;

    static std::atomic<uint64_t> start_num_start_txn;
    static std::atomic<uint64_t> start_num_txn_construct;

    static std::atomic<uint64_t> pessimisitic_txn_num;              // 本地PCC事务
    static std::atomic<uint64_t> pessimisitic_priority_txn_num;
    static std::atomic<uint64_t> pessimisitic_hot_visits_txn_num;

    static std::atomic<uint64_t> start_txn_num;
    static std::atomic<uint64_t> start_interactive_txn_num;
    static std::atomic<uint64_t> commit_txn_num;
    static std::atomic<uint64_t> commit_stored_txn_num;
    static std::atomic<uint64_t> commit_interactive_txn_num;        // 本地交互型事务
    static std::atomic<uint64_t> commit_pcc_interactive_txn_num;
    static std::atomic<uint64_t> commit_occ_interactive_txn_num;

    static std::atomic<uint64_t> txn_total_time;
    static std::atomic<uint64_t> stored_txn_total_time;
    static std::atomic<uint64_t> interactive_txn_total_time;
    static std::atomic<uint64_t> interactive_pcc_txn_total_time;
    static std::atomic<uint64_t> interactive_occ_txn_total_time;
    static std::atomic<uint64_t> txn_abort_time;
    static std::atomic<uint64_t> interactive_txn_abort_time;

    static std::atomic<uint64_t> LockCheck_abort_num;
    static std::atomic<uint64_t> Commit_abort_num;             // commit阶段的abort数
    static std::atomic<uint64_t> DeadLock_abort_num;
    static std::atomic<uint64_t> Abort_txn_num;
    static std::atomic<uint64_t> Abort_interactive_txn_num;
    static std::atomic<uint64_t> Abort_pcc_interactive_txn_num;
    static std::atomic<uint64_t> Remote_Abort_interactive_txn_num;

    static std::atomic<uint64_t> CommitPhase_abort_num;             // 记录abort的所有 txn
    static std::atomic<uint64_t> CommitCheck_abort_num;             // commit阶段的abort数
    static std::atomic<uint64_t> Abort_transcation_csn_set_abort_num;

    static std::atomic<uint64_t> Remote_ValidateAndSetWriteForRemote_abort_num; // 远端验证abort
    static std::atomic<uint64_t> Remote_CommitCheck_abort_num; // 远端验证abort


    static std::atomic<uint64_t> CommitPhase_ValidateAndSetWriteForCommit_abort_num;
    static std::atomic<uint64_t> CommitPhase_IsRowAvailable_abort_num;
    static std::atomic<uint64_t> CommitPhase_origSentinel_abort_num;
    static std::atomic<uint64_t> CommitLockCheck_IsRowAvailable_abort_num;

    static std::atomic<uint64_t> Commit_abort_pcc_total_interactive_num;
    static std::atomic<uint64_t> Commit_abort_occ_total_interactive_num;
    static std::atomic<uint64_t> Commit_abort_interactive_num;
    static std::atomic<uint64_t> CommitPhase_abort_interactive_num;
    static std::atomic<uint64_t> CommitCheck_abort_interactive_num;
    static std::atomic<uint64_t> CommitUpdate_abort_interactive_num;
    static std::atomic<uint64_t> CommitCheck_deadlock_abort_interactive_num;
    static std::atomic<uint64_t> ValidateReadInMergeForSnap_abort_interactive_num;
    static std::atomic<uint64_t> ValidateReadInMerge_abort_interactive_num;
    static std::atomic<uint64_t> InsertTxntoLocalChangeSet_abort_interactive_num;
    static std::atomic<uint64_t> Abort_transcation_csn_set_abort_interactive_num;

    static std::atomic<uint64_t> CommitPhase_ValidateAndSetWriteForCommit_abort_interactive_num;
    static std::atomic<uint64_t> CommitPhase_IsRowAvailable_abort_interactive_num;
    static std::atomic<uint64_t> CommitPhase_origSentinel_abort_interactive_num;

    static std::atomic<uint64_t> Switch_validation_abort_num;
    static std::atomic<uint64_t> Switch_validation_occ_abort_num;
    static std::atomic<uint64_t> Switch_validation_pcc_abort_num;

    static std::atomic<uint64_t> HotRow_quick_validation_abort_num;
    static std::atomic<uint64_t> HotRow_read_validation_abort_num;
    static std::atomic<uint64_t> HotRow_write_validation_abort_num;

    static std::atomic<uint64_t> ReadLock_pcc_abort_num;
    static std::atomic<uint64_t> WriteLock_pcc_abort_num;
    static std::atomic<uint64_t> ReadLock_switch_pcc_abort_num;
    static std::atomic<uint64_t> WriteLock_switch_pcc_abort_num;

    // Silo abort统计
    static std::atomic<uint64_t> Silo_validation_abort_num;
    static std::atomic<uint64_t> Silo_quick_validation_abort_num;
    static std::atomic<uint64_t> Silo_lockheader_abort_num;
    static std::atomic<uint64_t> Silo_lockheader_abort_by_interactive_num;
    static std::atomic<uint64_t> Silo_write_validation_abort_num;
    static std::atomic<uint64_t> Silo_read_validation_abort_num;

    //LOCKINFO 上锁请求添加成功和解锁统计
    static std::atomic<uint64_t> local_lock_num;
    static std::atomic<uint64_t> remote_lock_num;
    static std::atomic<uint64_t> local_unlock_num;
    static std::atomic<uint64_t> remote_unlock_num;
    static std::atomic<uint64_t> send_lock_num;
    static std::atomic<uint64_t> receive_lock_num;


    //////////////////////////////////////////////////
    // wzy: 添加全局活跃事务
    static void AddActiveTxnRow(TransactionId tid, MOT::Row* row) {
        std::lock_guard<std::mutex> txn_lock(active_txn_list_mutex);
        active_txn_list.insert(tid);
    }

    static void AddCleanRow(MOT::Row* row) {
        std::lock_guard<std::mutex> row_lock(clean_row_list_mutex);
        auto table_name = row->GetTable()->GetLongTableName();
        auto tmp_rowid = table_name + ":" + to_string(row->GetRowId());
        if (!clean_row_list_map.count(tmp_rowid)) {
            clean_row_list.push_back(row);
            clean_row_list_map[tmp_rowid] = std::prev(clean_row_list.end());
        }
    }

    // wzy: 添加活跃queue，内部判断request queue的请求 > 0，去重，有问题
    static void AddEpochActiveQueue(std::shared_ptr<LockRequestQueue> &queue, uint64_t epoch) {
        uint64_t epoch_mod = epoch % 2;
        std::lock_guard<std::mutex> lock(active_lock_queues_mutex[epoch_mod]);
        if (active_lock_queues_set[epoch_mod]->count(queue->m_row_id) == 0 && !queue->empty()) {
            active_lock_queues[epoch_mod]->enqueue(queue);
            active_lock_queues[epoch_mod]->enqueue(std::shared_ptr<LockRequestQueue>());
            active_lock_queues_set[epoch_mod]->insert(queue->m_row_id);
        }
    }
    // wzy: 清空活跃queue，有问题
    static void ClearEpochActiveQueue(uint64_t epoch) {
        uint64_t epoch_mod = epoch % 2;
        std::lock_guard<std::mutex> lock(active_lock_queues_mutex[epoch_mod]);
        active_lock_queues_set[epoch_mod]->clear();
    }

    static uint64_t GetActiveQueueNum(uint64_t n) {
        return active_lock_list_size.load();
    }

    static void SetActiveLockListExced(uint64_t id, bool value) {
        active_lock_list_exced[id] = value;
    }

    static bool IsActiveLockListExced(uint64_t id) {
        return active_lock_list_exced[id];
    }

    static bool IsAllActiveLockListExced() {
        for (int i = 0; i < lock_thread_num; i++) {
            if (!active_lock_list_exced[i]) return false;
        }
        return true;
    }

    // wzy: 添加活跃queue，多线程
    static void AddActiveQueue(std::shared_ptr<LockRequestQueue> &queue, uint64_t id) {
        id = id % lock_thread_num;
        std::lock_guard<std::mutex> queue_lock(active_lock_list_mutex[id]);
        if (!active_lock_list_set[id]->count(queue)) {
            active_lock_list[id]->push_back(queue);
            auto iter = std::prev(active_lock_list[id]->end());     // iter为空？
            active_lock_list_set[id]->emplace(queue, iter);
            active_lock_list_size.fetch_add(1);
        }
    }

    // wzy: 移除活跃queue，多线程
    static void RemoveActiveQueue(std::shared_ptr<LockRequestQueue> &queue, uint64_t id) {
        id = id % lock_thread_num;
        std::lock_guard<std::mutex> queue_lock(active_lock_list_mutex[id]);
        if (queue->lock_request_queue_.empty()) {
            auto iter = active_lock_list_set[id]->find(queue);
            if (iter != active_lock_list_set[id]->end()) {
                active_lock_list[id]->erase(iter->second);
                active_lock_list_set[id]->erase(iter);
                active_lock_list_size.fetch_sub(1);
            }
        }
    }

    // wzy: 添加活跃queue
    static void AddActiveQueue(std::shared_ptr<LockRequestQueue> &queue) {
        std::lock_guard<std::mutex> queue_lock(active_queue_list_mutex);
        if (!active_queue_set.count(queue)) {
            active_queue_list.push_back(queue);
            active_queue_set[queue] = std::prev(active_queue_list.end());
        }
    }

    // TODO: tmp_vec->push_back(queue)并发问题
    // wzy: 加入csn - request queue
    static void AddCsnRequestQueue(std::string& csn, const std::shared_ptr<MOTAdaptor::LockRequestQueue> &queue) {
//        std::shared_ptr<std::vector<std::shared_ptr<MOTAdaptor::LockRequestQueue>>> tmp_vec = nullptr;
//        std::shared_ptr<std::vector<std::shared_ptr<MOTAdaptor::LockRequestQueue>>> new_vec = nullptr;
//        if (!MOTAdaptor::csn_requests_map.get_element(csn, tmp_vec) || !tmp_vec){    // 未找到则创建新lock request
//            new_vec = std::make_shared<std::vector<std::shared_ptr<MOTAdaptor::LockRequestQueue>>>();
//        }
//        if(MOTAdaptor::csn_requests_map.get_or_create_queue(csn, tmp_vec, new_vec) && tmp_vec) {
//            if (queue) tmp_vec->push_back(queue);
//        }
//        new_vec.reset();
//        tmp_vec = nullptr;

        // TODO: 插入txn_rowid_map
        if (queue) txn_rowid_map.insert_vector(csn, queue->m_row_id);
//        std::shared_ptr<std::vector<std::string>> tmp_vec_1 = nullptr;
//        std::shared_ptr<std::vector<std::string>> new_vec_1 = nullptr;
//        if (!MOTAdaptor::txn_rowid_map.get_element(csn, tmp_vec_1) || !tmp_vec_1){    // 未找到则创建新lock request
//            new_vec_1 = std::make_shared<std::vector<std::string>>();
//        }
//        if(MOTAdaptor::txn_rowid_map.get_or_create_queue(csn, tmp_vec_1, new_vec_1) && tmp_vec_1) {
//            if (queue) tmp_vec_1->push_back(queue->m_row_id);
//        }
//        new_vec_1.reset();
//        tmp_vec_1.reset();
    }

    // wzy: 移除活跃queue
    static void RemoveActiveQueue(std::shared_ptr<LockRequestQueue> &queue) {
        std::lock_guard<std::mutex> queue_lock(active_queue_list_mutex);
        if (queue->lock_request_queue_.empty()) {
            auto iter = active_queue_set.find(queue);
            active_queue_list.erase(iter->second);
            active_queue_set.erase(iter);
        }
    }

    static bool IsRowAvailable(string &rowid, uint64_t &tid, string &csn, string &res) {
        uint64_t epoch_mod = GetLogicalEpoch() % (UINT64_MAX - 1);
        std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
        MOTAdaptor::row_lockrequest_map.get_element(rowid, tmp_queue);
        // if (tmp_queue && !tmp_queue->available_row(rowid, csn)) return false;
        // 已经被锁则abort
        if (cc_mode == 1) {
            if (tmp_queue && !tmp_queue->available_row(rowid, csn, res, epoch_mod)) return false;
            return MOTAdaptor::epoch_lock_set.contain_lock(rowid, csn);
        }
        else if (cc_mode == 2 || cc_mode == 3 || cc_mode == 4) {
            if (tmp_queue && !tmp_queue->AvailableRowPlor(rowid, tid, csn, res)) return false;
            return true;
        } else if (cc_mode == 5) {
            if (tmp_queue && !tmp_queue->AvailableRowDL(rowid, tid, csn, res)) return false;
            return true;
        }
    }

    static bool IsRowAvailable(string &rowid, string &csn, string &res) {
        uint64_t epoch_mod = GetLogicalEpoch() % (UINT64_MAX - 1);
        std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
        MOTAdaptor::row_lockrequest_map.get_element(rowid, tmp_queue);
        // if (tmp_queue && !tmp_queue->available_row(rowid, csn)) return false;
        // 已经被锁则abort
        if (tmp_queue && !tmp_queue->available_row(rowid, csn, res, epoch_mod)) return false;
        return MOTAdaptor::epoch_lock_set.contain_lock(rowid, csn);   
    }

    // TODO: wzy 测试
    static void EpochCleanVersion() {
        TransactionId min_active_txn = INT64_MAX_VALUE;
        if (!active_txn_list.empty()) {
            min_active_txn = *MOTAdaptor::active_txn_list.begin();
        }
        std::unique_lock<std::mutex> row_lock(clean_row_list_mutex);
        // 更新最小活跃txn id，并进行清除
        for (auto row : clean_row_list) {
            row->GetRowHeader()->CleanupVersions(min_active_txn);
        }
        row_lock.unlock();
        SetNeedClean(false);
    }

    static void RemoveActiveTxn(TransactionId tid) {
        std::lock_guard<std::mutex> txn_lock(active_txn_list_mutex);
        active_txn_list.erase(tid);
        if (min_active_txn_id.load() >= tid) SetNeedClean(true);
//        EpochCleanVersion();    // 测试
    }

    static void AddActiveTxn(TransactionId tid) {
        // recovery 会空转生成多个tid = 0的事务，并且之后不会提交
        std::lock_guard<std::mutex> txn_lock(active_txn_list_mutex);
        if (tid != 0) {
            active_txn_list.insert(tid);
            if (min_active_txn_id.load() == 0 && tid != 0) min_active_txn_id.store(tid);
        }
    }



    static void SetTimerStop(bool value) {timerStop = value;}
    static bool IsTimerStop() {return timerStop;}
    
    static bool IsRemoteExeced() {return remote_execed;}
    static void SetRemoteExeced(bool value) {remote_execed = value;}

    // wzy: 本地和远端锁已插入
    static bool IsLockExeced() {return lock_execed;}
    static void SetLockExeced(bool value) {lock_execed = value;}

    // wzy:
    static bool IsLockGranted() {return lock_granted;}
    static void SetLockGranted(bool value) {lock_granted = value;}

    // wzy:
    static bool IsDeadLockDetected() {return deadlock_detectted;}
    static void SetDeadLockDetected(bool value) {deadlock_detectted = value;}

    // wzy:
    static bool IsNeedClean() {return need_clean.load();}
    static void SetNeedClean(bool value) {need_clean.store(value);}

    // wzy:
    static bool IsLockCommitted(){ return lock_committed;}
    static void SetLockCommitted(bool value){ lock_committed = value;}

    static bool IsRecordCommitted(){ return record_committed;}
    static void SetRecordCommitted(bool value){ record_committed = value;}
    
    static bool IsRemoteRecordCommitted(){ return remote_record_committed;}
    static void SetRemoteRecordCommitted(bool value){ remote_record_committed = value;}

    static bool IsCurrentEpochAbort(){ return is_current_epoch_abort;}
    static void SetCurrentEpochAbort(bool value){ is_current_epoch_abort = value;}
    
    static void SetPhysicalEpoch(int value){ physical_epoch = value;}
    static uint64_t AddPhysicalEpoch(){ return ++ physical_epoch;}
    static uint64_t GetPhysicalEpoch(){ return physical_epoch;}

    static void SetLogicalEpoch(int value){ logical_epoch = value;}
    static uint64_t AddLogicalEpoch(){ return ++ logical_epoch;}
    static uint64_t GetLogicalEpoch(){ return logical_epoch;}


    static uint64_t IncLocalTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}

    // wzy:
    static uint64_t IncLocalLockinfoCounters(uint64_t epoch_mod, uint64_t index){ return (*local_lockinfo_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t IncLocalLockinfoExecedCounters(uint64_t epoch_mod, uint64_t index){ return (*local_lockinfo_execed_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetLocalLockinfoExecedCounters(uint64_t epoch_mod) {
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++){
            ans += (*local_lockinfo_execed_counters[epoch_mod])[i]->load();
        }
        return ans;
    }
    static uint64_t GetLocalLockinfoCounters(uint64_t epoch_mod){
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++){
            ans += (*local_lockinfo_counters[epoch_mod])[i]->load();
        }
        return ans;
    }
    static bool IsLocalLockinfoCountersExced(uint64_t epoch_mod){
        return GetLocalLockinfoExecedCounters(epoch_mod) >= GetLocalLockinfoCounters(epoch_mod);
    }
    static uint64_t GetLockGrantedNum() {return lock_grant_num.load();}
    static void SetLockGrantedNum(uint64_t num) {lock_grant_num.store(num);}
    static void AddLockGrantedNum(){lock_grant_num.fetch_add(1);}
    static uint64_t GetShouldLockGrantedNum() {return lock_should_grant_num.load();}
    static void SetShouldLockGrantedNum(uint64_t num) {lock_should_grant_num.store(num);}

    ////////////


    static uint64_t IncLocalExecedCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_execed_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t IncLocalCommittedCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_committed_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetLocalExecedCounters(uint64_t epoch_mod) {
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++){
            ans += (*local_txn_execed_counters[epoch_mod])[i]->load();
        }
        return ans;
    }

    static uint64_t GetLocalCommittedCounters(uint64_t epoch_mod) {
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++){
            ans += (*local_txn_committed_counters[epoch_mod])[i]->load();
        }
        return ans;
    }
    static bool IsLocalTxnCountersExced(uint64_t epoch_mod){
        return GetLocalExecedCounters(epoch_mod) >= GetLocalTxnCounters(epoch_mod);
    }
    static bool IsLocalTxnCountersCommitted(uint64_t epoch_mod){
        return GetLocalCommittedCounters(epoch_mod) >= GetLocalTxnCounters(epoch_mod);
    }
    static bool IsLocalTxnCountersExcEqualZero(uint64_t epoch_mod){
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++){
            if( (*local_txn_counters[epoch_mod])[i]->load() % static_cast<uint64_t>(1e10) != 0) return false;
        }
        return true;
    }
    static bool IsLocalTxnCountersComEqualZero(uint64_t epoch_mod){
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++){
            if( (*local_txn_counters[epoch_mod])[i]->load() != 0) return false;
        }
        return true;
    }
    static uint64_t GetLocalTxnCounters(uint64_t epoch_mod){
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++){
            ans += (*local_txn_counters[epoch_mod])[i]->load();
        }
        return ans;
    }


    static void SetLocalTxnExcCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*local_txn_exc_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncLocalTxnExcCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_exc_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetLocalTxnExcCounters(uint64_t epoch_mod){
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*local_txn_exc_counters[epoch_mod])[i]->load();
        return ans;
    }

    static void SetRecordCommitTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*record_commit_txn_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncRecordCommitTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*record_commit_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetRecordCommitTxnCounters(uint64_t epoch_mod){ 
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*record_commit_txn_counters[epoch_mod])[i]->load();
        return ans;
    }

    // wzy:
    static uint64_t IncMergeLockinfoCounters(uint64_t epoch_mod, uint64_t index){ return (*merge_lockinfo_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetMergeLockinfoCounters(uint64_t epoch_mod){
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*merge_lockinfo_counters[epoch_mod])[i]->load();
        return ans;
    }

    static void SetRecordCommittedTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*record_committed_txn_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncRecordCommittedTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*record_committed_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetRecordCommittedTxnCounters(uint64_t epoch_mod){ 
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*record_committed_txn_counters[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t IncLocalTxnIndex(uint64_t epoch_mod, uint64_t index){
        return (*local_txn_index[epoch_mod % max_length])[index]->fetch_add(1);
    }

    static void SetRemoteMergedTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*remote_merged_txn_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncRemoteMergedTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*remote_merged_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetRemoteMergedTxnCounters(uint64_t epoch_mod){ 
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*remote_merged_txn_counters[epoch_mod])[i]->load();
        return ans;
    }

    static void SetRemoteCommitTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*remote_commit_txn_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncRemoteCommitTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*remote_commit_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetRemoteCommitTxnCounters(uint64_t epoch_mod){ 
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*remote_commit_txn_counters[epoch_mod])[i]->load();
        return ans;
    }

    static void SetRemoteCommittedTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*remote_committed_txn_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncRemoteCommittedTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*remote_committed_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetRemoteCommittedTxnCounters(uint64_t epoch_mod){ 
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*remote_committed_txn_counters[epoch_mod])[i]->load();
        return ans;
    }

    static void SetLimiteTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*limite_txn_num[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncLimiteTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*limite_txn_num[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetLimiteTxnCounters(uint64_t epoch_mod){ 
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*limite_txn_num[epoch_mod])[i]->load();
        return ans;
    }



    static uint64_t _max_length, _pack_num;
    static std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> txn_num_ptrs,
        write_abort_before_send_txn_num,
        write_abort_after_send_txn_num, total_abort_txn_num, read_abort_txn_num, read_committed_txn_num, write_committed_txn_num, 
        read_total_txn_num, write_total_txn_num, pack_txn_num, packd_txn_num_ptrs;

    /// wzy: lockinfo
    static std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>>  lockinfo_num_ptrs, packd_lockinfo_num_ptrs, pack_lockinfo_num;

    static void Init(uint64_t pack_num, uint64_t length);
    
    
    static void PushBack(uint64_t epoch, std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr2, 
        std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr3) {
        auto epoch_mod = epoch % _max_length;
        txn_num_ptrs[epoch_mod] = ptr2;
        packd_txn_num_ptrs[epoch_mod] = ptr3;
    }
    
    static uint64_t LoadChangeSet(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*txn_num_ptrs[epoch_mod])[i]->load();
        return ans;
    }

    // wzy:
    static uint64_t LoadLockinfoSet(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*lockinfo_num_ptrs[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadTotalAbortTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*total_abort_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadWriteAbortBeforeSendTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*write_abort_before_send_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadWriteAbortAfterSendTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*write_abort_after_send_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadWriteCommittedTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*write_committed_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadReadAbortTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*read_abort_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadReadCommittedTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*read_committed_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadReadTotalTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*read_total_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadPackTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*pack_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    // wzy:
    static uint64_t LoadPackLockinfoNum(uint64_t epoch)
    {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for (int i = 0; i < (int)_pack_num; i++)
            ans += (*pack_lockinfo_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadWriteTotalTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*write_total_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadPackedTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*packd_txn_num_ptrs[epoch_mod])[i]->load();
        return ans;
    }

    // wzy: lockinfo packed_lockinfo_num_ptrs
    static uint64_t LoadPackedLockinfoNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*packd_lockinfo_num_ptrs[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadShouldSendTxnNum(uint64_t epoch) {
        if(is_sync_exec) {
            // if(LoadChangeSet(epoch) < LoadReadCommittedTxnNum(epoch) - (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch))) {
            //     return LoadPackedTxnNum(epoch);
            // }
            return LoadChangeSet(epoch) - LoadReadCommittedTxnNum(epoch) - 
                (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch)) ;
        }
        else 
            return LoadChangeSet(epoch);
    }

    // wzy:
    static uint64_t LoadShouldSendLockinfoNum(uint64_t epoch) {
         return LoadLockinfoSet(epoch);
    }

    static uint64_t LoadShouldExecTxnNum(uint64_t epoch) {// 只在 is_sync_exec中使用
        // if(LoadChangeSet(epoch) < LoadReadCommittedTxnNum(epoch) - (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch))) {
        //     return MOTAdaptor::GetLocalTxnExcCounters();
        // }
        if(is_sync_exec)
            return LoadChangeSet(epoch) - LoadReadCommittedTxnNum(epoch) - 
                (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch));
        else return LoadChangeSet(epoch);
    }

    static bool IsCurrentEpochFinished(uint64_t epoch) {
        usleep(200);
        return ((LoadPackedTxnNum(epoch) >= LoadShouldSendTxnNum(epoch)) &&  epoch < MOTAdaptor::GetPhysicalEpoch());
    }

    // wzy:
    static bool IsCurrentEpochFinishedInteractive(uint64_t epoch) {
        usleep(200);
        bool flag = ((LoadPackedTxnNum(epoch) >= LoadShouldSendTxnNum(epoch)) &&  epoch < MOTAdaptor::GetPhysicalEpoch());
        return flag && (LoadPackedLockinfoNum(epoch) >= LoadShouldSendLockinfoNum(epoch));
    }

    static bool TryAddNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
        return true;
    }

    // wzy:
    static bool TryAddLockinfoNum(uint64_t epoch_num, uint64_t index, uint64_t value)
    {
        (*(lockinfo_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
        return true;
    }
    static bool AddPackedLockinfoNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(packd_lockinfo_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
        return true;
    }

    static bool AddNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
        return true;
    }

    // packd_txn_num_ptrs
    static bool AddPackedNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(packd_txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
        return true;
    }




    static void ClearMergeEpochState() {
        remote_row_ptr_map.clear();
        insertSet.clear();
        insertSetForCommit.clear();
        abort_transcation_csn_set.clear();
        remote_execed = false;
        // wzy: lockinfo
        lock_granted = false;
        lock_execed = false;
        lock_committed = true;
        lock_grant_num.store(0);
        lock_should_grant_num.store(0);
        deadlock_detectted = true;
        epoch_lock_set.clear();

        auto epoch = logical_epoch % max_length;
        for(int i = 0; i <= (int)pack_num; i++) {
            (*limite_txn_num[epoch])[i]->store(0);
        }
        for (int i = 0; i < lock_thread_num; i++) {
            active_lock_list_exced[i] = false;
        }
    }

    static void ClearEpochState(uint64_t epoch_mod){
        epoch_mod %= max_length;
        for(int i = 0; i <= (int)pack_num; i++){
            (*local_txn_counters[epoch_mod])[i]->store(0);
            (*local_txn_exc_counters[epoch_mod])[i]->store(0);
            (*local_txn_index[epoch_mod])[i]->store(1);
            (*remote_merged_txn_counters[epoch_mod])[i]->store(0);
            (*remote_commit_txn_counters[epoch_mod])[i]->store(0);
            (*remote_committed_txn_counters[epoch_mod])[i]->store(0);
            
            (*record_commit_txn_counters[epoch_mod])[i]->store(0);
            (*record_committed_txn_counters[epoch_mod])[i]->store(0);

            (*txn_num_ptrs[epoch_mod])[i]->store(0);
            (*packd_txn_num_ptrs[epoch_mod])[i]->store(0);
            (*write_abort_before_send_txn_num[epoch_mod])[i]->store(0);
            (*write_abort_after_send_txn_num[epoch_mod])[i]->store(0);
            (*total_abort_txn_num[epoch_mod])[i]->store(0);
            (*read_abort_txn_num[epoch_mod])[i]->store(0);
            (*read_committed_txn_num[epoch_mod])[i]->store(0);
            (*write_committed_txn_num[epoch_mod])[i]->store(0);
            (*read_total_txn_num[epoch_mod])[i]->store(0);
            (*write_total_txn_num[epoch_mod])[i]->store(0);
            (*pack_txn_num[epoch_mod])[i]->store(0);

            /// wzy: lockinfo
            (*local_lockinfo_counters[epoch_mod])[i]->store(0);
            (*lockinfo_num_ptrs[epoch_mod])[i]->store(0);
            (*packd_lockinfo_num_ptrs[epoch_mod])[i]->store(0);
            (*pack_lockinfo_num[epoch_mod])[i]->store(0);
            (*merge_lockinfo_counters[epoch_mod])[i]->store(0);
        }
    }
    
    static bool InsertTxntoLocalChangeSet(MOT::TxnManager* txMan, const uint64_t& index_pack, const uint64_t& index_unique);
    // wzy:
    static bool InsertTxntoLocalChangeSet2(MOT::TxnManager* txMan, const uint64_t& index_pack, const uint64_t& index_unique);
    static bool InsertTxntoLocalLockInfo(MOT::TxnManager* txMan, const uint64_t& index_pack, const uint64_t& index_unique, const bool& lock, MOT::Row* currRow);
    static bool TryIncLocalChangeSetNum(uint64_t epoch, uint64_t index_pack, uint64_t value);
    static bool IncLocalChangeSetNum(uint64_t epoch, uint64_t index_pack, uint64_t value);
    static bool InsertRowToSet(MOT::TxnManager* txMan, void* txn_void, const uint64_t& index_pack, const uint64_t& index_unique);
    static bool InsertTxnIntoRecordCommitQueue(MOT::TxnManager* txMan, void* txn_void, MOT::RC &rc);
    static void LocalTxnSafeExit(const uint64_t& index_pack, void* txn_void);
    static void Output(std::string v);
    static void Merge(MOT::TxnManager* txMan, uint64_t& index_pack);
    static void Commit(MOT::TxnManager* txMan, uint64_t& index_pack);
    
    
    static std::unique_ptr<std::atomic<uint64_t>> should_receive_pack_num, online_server_num;
    static std::vector<std::unique_ptr<std::atomic<uint64_t>>> is_server_online;
    static std::vector<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> 
        received_pack_num, received_txn_num, should_receive_txn_num;

    /// wzy: lockinfo
    static std::vector<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> should_receive_lockinfo_num, received_lockinfo_num;

    static std::vector<std::unique_ptr<std::atomic<uint64_t>>>
    received_total_pack_num, received_total_txn_num;

    /// wzy: lockinfo
    static std::vector<std::unique_ptr<std::atomic<uint64_t>>> received_total_lockinfo_num;

    static uint64_t AddShouldReceiveTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        return should_receive_txn_num[epoch % _max_length][index]->fetch_add(value);
    }
    static void StoreShouldReceiveTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        should_receive_txn_num[epoch % _max_length][index]->store(value);
    }
    // wzy:
    static void StoreShouldReceiveLockinfoNum(uint64_t epoch, uint64_t index, uint64_t value) {
        should_receive_lockinfo_num[epoch % _max_length][index]->store(value);
    }
    static uint64_t GetShouldReceiveLockinfoNum(uint64_t epoch, uint64_t index) {
        return should_receive_lockinfo_num[epoch % _max_length][index]->load();
    }
    // wzy:
    static uint64_t GetShouldReceiveLockinfoNum(uint64_t epoch) {
        epoch %= _max_length;
        uint64_t ans = 0;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            if(received_pack_num[epoch][i]->load() == 1)
                ans += should_receive_lockinfo_num[epoch][i]->load();
        }
        return ans;
    }


    static uint64_t GetShouldReceiveTxnNum(uint64_t epoch, uint64_t index) {
        return should_receive_txn_num[epoch % _max_length][index]->load();
    }

    static uint64_t GetShouldReceiveTxnNum(uint64_t epoch) {
        epoch %= _max_length;
        uint64_t ans = 0;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            if(received_pack_num[epoch][i]->load() == 1)
                ans += should_receive_txn_num[epoch][i]->load();
        }
        return ans;
    }


    static uint64_t AddReceivedPackNum(uint64_t epoch, uint64_t index, uint64_t value) {
        return received_pack_num[epoch % _max_length][index]->fetch_add(value);
    }
    static void StoreReceivedPackNum(uint64_t epoch, uint64_t index, uint64_t value) {
        received_pack_num[epoch % _max_length][index]->store(value);
    }
    static uint64_t GetReceivedPackNum(uint64_t epoch, uint64_t index) {
        return received_pack_num[epoch % _max_length][index]->load();
    }
    static uint64_t GetReceivedPackNum(uint64_t epoch) {
        epoch %= _max_length;
        uint64_t ans = 0;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            if(received_pack_num[epoch][i]->load() == 1)
                ans ++;
        }
        return ans;
    }

    static uint64_t AddReceivedTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        return received_txn_num[epoch % _max_length][index]->fetch_add(value);
    }
    // wzy: 接收到的lockinfo计数
    static uint64_t AddReceivedLockinfoNum(uint64_t epoch, uint64_t index, uint64_t value) {
        return received_lockinfo_num[epoch % _max_length][index]->fetch_add(value);
    }
    static uint64_t GetReceivedLockinfoNum(uint64_t epoch, uint64_t index) {
        return received_lockinfo_num[epoch % _max_length][index]->load();
    }
    static uint64_t GetReceivedLockinfoNum(uint64_t epoch) {
        epoch %= _max_length;
        uint64_t ans = 0;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            if(received_pack_num[epoch][i]->load() == 1)
                ans += received_lockinfo_num[epoch][i]->load();
        }
        return ans;
    }

    static void StoreReceivedTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        received_txn_num[epoch % _max_length][index]->store(value);
    }
    static uint64_t GetReceivedTxnNum(uint64_t epoch, uint64_t index) {
        return received_txn_num[epoch % _max_length][index]->load();
    }

    static uint64_t GetReceivedTxnNum(uint64_t epoch) {
        epoch %= _max_length;
        uint64_t ans = 0;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            if(received_pack_num[epoch][i]->load() == 1)
                ans += received_txn_num[epoch][i]->load();
        }
        return ans;
    }

    static uint64_t AddReceivedLockinfoNumTotal(uint64_t epoch, uint64_t value) {
        return received_total_lockinfo_num[epoch % _max_length]->fetch_add(value);
    }
    static uint64_t GetReceivedLockinfoNumTotal(uint64_t epoch) {
        return received_total_lockinfo_num[epoch % _max_length]->load();
    }

    static uint64_t AddReceivedPackNumTotal(uint64_t epoch, uint64_t value) {
        return received_total_pack_num[epoch % _max_length]->fetch_add(value);
    }
    static uint64_t GetReceivedPackNumTotal(uint64_t epoch) {
        return received_total_pack_num[epoch % _max_length]->load();
    }
    static uint64_t AddReceivedTxnNumTotal(uint64_t epoch, uint64_t value) {
        return received_total_txn_num[epoch % _max_length]->fetch_add(value);
    }

    static uint64_t GetReceivedTxnNumTotal(uint64_t epoch) {
        return received_total_txn_num[epoch % _max_length]->load();
    }


    static uint64_t AddShouldReceivePackNum(uint64_t value) {
        return should_receive_pack_num->fetch_add(value);
    }
    static uint64_t SubShouldReceivePackNum(uint64_t value) {
        return should_receive_pack_num->fetch_sub(value);
    }
    static void StoreShouldReceivePackNum(uint64_t value) {
        should_receive_pack_num->store(value);
    }
    static uint64_t GetShouldReceivePackNum() {
        return should_receive_pack_num->load();
    }

    static uint64_t AddOnLineServerNum(uint64_t value) {
        return online_server_num->fetch_add(value);
    }
    static uint64_t SubOnLineServerNum(uint64_t value) {
        return online_server_num->fetch_sub(value);
    }
    static void StoreOnLineServerNum(uint64_t value) {
        online_server_num->store(value);
    }
    static uint64_t GetOnLineServerNum() {
        return online_server_num->load();
    }

    
    static void RemoteCacheClear(uint64_t epoch) {
        epoch %= _max_length;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            received_pack_num[epoch][i]->store(0);
            received_txn_num[epoch][i]->store(0);
            should_receive_txn_num[epoch][i]->store(0);

            received_lockinfo_num[epoch][i]->store(0);      /// wzy
            should_receive_lockinfo_num[epoch][i]->store(0);        /// wzy
        }
        received_total_pack_num[epoch]->store(0);
        received_total_txn_num[epoch]->store(0);
        received_total_lockinfo_num[epoch]->store(0);       /// wzy
    }

    static void SetServerOnLine(std::string ip) {
        for(int i = 0; i < (int)kServerIp.size(); i++) {
            if(ip == kServerIp[i]) {
                is_server_online[i]->store(1);
            }
        }
    }

    static void SetServerOffLine(std::string ip) {
        for(int i = 0; i < (int)kServerIp.size(); i++) {
            if(ip == kServerIp[i]) {
                is_server_online[i]->store(0);
            }
        }
    }

    static void SetServerOnLine(uint64_t index) {
        is_server_online[index]->store(1);
    }

    static void SetServerOffLine(uint64_t index) {
        is_server_online[index]->store(0);
    }

    static bool IsServerOnLine(uint64_t index) {
        return (is_server_online[index]->load() == 1);
    }

    static void ReceiveAMessage() {}

    static std::vector<std::unique_ptr<std::atomic<uint64_t>>>  received_epoch;
    
    static bool IsCacheServerStored(uint64_t epoch) {
        if(received_epoch[epoch % _max_length]->load() == static_cast<uint64_t>(1)) {
            return true;
        }
        else {
            return false;
        }
    }

    static void SetCacheServerStored(uint64_t epoch, uint64_t value) {
        received_epoch[epoch % _max_length]->store(value);
    }

public:
    //////////////// RL State List /////////////////

    class RLState {
    public:
        uint64_t tid_;      // 查看tid是否能对应上
        int action_;

        int retry_cnt_;
        uint64_t runtime_;
        int read_cnt_;
        int write_cnt_;
        int hot_visited_;

        RLState(uint64_t tid, uint64_t action, uint64_t retry_cnt, uint64_t runtime, uint64_t read_cnt, uint64_t write_cnt, uint64_t hot_visited)
            :tid_(tid), action_(action), retry_cnt_(retry_cnt), runtime_(runtime), read_cnt_(read_cnt), write_cnt_(write_cnt), hot_visited_(hot_visited)
        {}

    };

    ////////////////////////////////////////
    // wzy : 上锁和死锁检测
    class LockRequest {
    public:
        uint64_t server_id_;
        uint64_t csn_;
        uint64_t start_epoch_;
        uint64_t commit_epoch_;
        int retry_cnt_;
        uint64_t score_;
        std::string tid_;  // csn + serverid
        bool exclusive_;

        std::string key_;
        std::string table_name_;
        LockRequest(uint64_t csn, uint64_t server_id, uint64_t start_epoch)
            : csn_(csn), server_id_(server_id), start_epoch_(start_epoch)
        {
            tid_ = to_string(csn_) + ":" + to_string(server_id_);
            uint64_t f = UINT64_MAX - csn_;
            score_ = 0;
            score_ |= ((uint64_t)retry_cnt_ << 57);
            score_ |= (f & 0x1FFFFFFFFFFFFFFF);
        }
        LockRequest(uint64_t server_id, uint64_t csn, uint64_t start_epoch, uint64_t commit_epoch)
        {
            server_id_ = server_id;
            csn_ = csn;
            start_epoch_ = start_epoch;
            commit_epoch_ = commit_epoch;
            tid_ = to_string(csn_) + ":" + to_string(server_id_);
            uint64_t f = UINT64_MAX - csn_;
            score_ = 0;
            score_ |= ((uint64_t)retry_cnt_ << 57);
            score_ |= (f & 0x1FFFFFFFFFFFFFFF);
        }

        LockRequest(MOT::TxnManager* txMan, uint32_t server_id)
        {
            server_id_ = server_id;
            csn_ = txMan->pre_csn;
            start_epoch_ = txMan->GetStartEpoch();
            commit_epoch_ = txMan->GetCommitEpoch();
            tid_ = to_string(csn_) + ":" + to_string(server_id_);
            retry_cnt_ = txMan->retry_cnt;
            uint64_t f = UINT64_MAX - csn_;
            score_ = txMan->score_;         // test: get score from txMan
//            score_ = (uint64_t ) retry_cnt_;
//            score_ = 0;
//            score_ |= ((uint64_t)retry_cnt_ << 57);
//            score_ |= (f & 0x1FFFFFFFFFFFFFF);           // Use the lower 57 bits for (max - csn_)
            exclusive_ = false;     // no use
        }

        LockRequest(MOT::TxnManager* txMan, uint32_t server_id, bool exclusive)
        {
            server_id_ = server_id;
            csn_ = txMan->pre_csn;
            start_epoch_ = txMan->GetStartEpoch();
            commit_epoch_ = txMan->GetCommitEpoch();
            tid_ = to_string(csn_) + ":" + to_string(server_id_);
            retry_cnt_ = txMan->retry_cnt;
            uint64_t f = UINT64_MAX - csn_;
            //            score_ = (uint64_t ) retry_cnt_;
            score_ = 0;
            score_ |= ((uint64_t)retry_cnt_ << 57);
            score_ |= (f & 0x1FFFFFFFFFFFFFF);           // Use the lower 57 bits for (max - csn_)
            exclusive_ = exclusive;
        }
    };

    struct alignas(64) LockEntry {
        std::atomic<bool> valid{false};
        std::atomic<uint64_t> tid{0};
        std::atomic<uint64_t> score{0};
        uint32_t server_id{0};

        bool operator<(const LockEntry& other) const {
            return score < other.score; // 按年龄降序排列
        }
    };

    struct WriteRequest {
        int session;
        uint64_t score;
        LockEntry* request;  // 对应的请求指针

        bool operator<(const WriteRequest& other) const {
            return score < other.score;  // 最大堆，score大优先
        }
    };

    // wzy: LockRequestQueue
    class LockRequestQueue {
    public:
        std::list<std::string> lock_request_string_queue_;            // ordered
        std::list<std::shared_ptr<LockRequest>> lock_request_queue_;  // ordered
        std::unordered_set<std::string> lock_request_queue_set_;        // 去重
        std::unordered_map<std::string, std::list<std::shared_ptr<LockRequest>>::iterator> lock_request_queue_map_;        // 去重


        std::condition_variable cv_;
        std::mutex latch_;          // lock queue mutex

        std::string grant_csn;    // epoch结束成功上锁的csn
        std::shared_ptr<LockRequest> grant_request;
        std::string m_grant_csn;  // 正在上锁的server id + csn
        std::shared_ptr<LockRequest> m_grant_request;
        std::string m_row_id;       // 当前queue对应的rowid

        uint64_t epoch_;            // 当前锁开始生效的epoch(该epoch之后的会判定锁)

        std::thread lock_thread_;
        std::thread print_thread_;
        bool ready;

        std::atomic<uint64_t> request_num;        // request个数统计

        // 不启动debug 线程
        // LockRequestQueue()
        //     : lock_thread_(&LockRequestQueue::processLockRequests, this),
        //       print_thread_(&LockRequestQueue::processPrintRequests, this),
        //       ready(false)
        // {
        //     lock_request_queue_ = std::list<std::shared_ptr<LockRequest>>();
        //     grant_request = nullptr;
        // }

        uint64_t now_to_us(){
            return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        }

        ////////////////// PLOR //////////////////////
        const static uint64_t INVALID_TID = 0;

        std::list<std::shared_ptr<LockRequest>> reader_list_;  // 读者表       // 实现plor需要, 提交中的事务等待reader 表中tid小的提交后才能继续
        std::unordered_map<uint64_t, std::list<std::shared_ptr<LockRequest>>::iterator> reader_list_map_;        // 读者去重

        // helper
        std::list<uint64_t> snapshot_reader_list_;
        std::unordered_map<uint64_t, uint64_t> snapshot_reader_score_map_;
        std::unordered_map<uint64_t, std::list<uint64_t>::iterator> snapshot_reader_list_map_;        // snapshot读者去重
        ////

        std::list<std::shared_ptr<LockRequest>> writer_list_;     // 实现plor需要, 提交中的事务等待reader 表中tid小的提交后才能继续
        std::unordered_map<uint64_t, std::list<std::shared_ptr<LockRequest>>::iterator> writer_list_map_;        // 读者去重
        std::mutex p_latch_;            // reader list mutex

        std::atomic<uint64_t> writer_{INVALID_TID};               // 获得写锁
        std::atomic<uint64_t> m_writer_{INVALID_TID};             // 备选获取写锁

        std::atomic<uint64_t> writer_score_{INVALID_TID};                // 写锁的score
        std::atomic<uint64_t> m_writer_score_{INVALID_TID};              // 备选写锁的score


        /////////////// priority queue ///////////////
        std::priority_queue<WriteRequest> pq;              // 优先队列替换candidate
        std::mutex pq_latch_;
        void pq_push(WriteRequest &item) {
            pq_latch_.lock();
            pq.push(item);
            pq_latch_.unlock();
        }
        void pq_remove(int session) {
            pq_latch_.lock();
            std::vector<WriteRequest> tempQueue;
            while (!pq.empty()) {
                WriteRequest top = pq.top();
                pq.pop();
                if (top.session != session) {
                    tempQueue.push_back(top);
                }
            }
            for (const auto& item : tempQueue) {
                pq.push(item);
            }
            pq_latch_.unlock();
        }
        void pq_top(WriteRequest &item) {
            pq_latch_.lock();
            WriteRequest top = {INVALID_TID, 0, nullptr};
            if (!pq.empty()) {
                top = pq.top();
            }
            pq_latch_.unlock();
        }
        //////////////////////////////////////////////

        std::atomic<bool> excl_sig{false};          // 升级为写锁(提交时候), 归latch 控制
        std::atomic<uint64_t> excl_tid{INVALID_TID};          // 升级为写锁(提交时候), 归latch 控制

        void SetExcl(uint64_t& tid) {
            if (writer_.load() == tid) {
                excl_sig.store(true);
                excl_tid.store(tid);
            }
        }

        bool LockRD(std::string& row_id, MOT::TxnManager*& txMan, uint32_t& server_id, bool switch_phase) {
            uint64_t tid = txMan->pre_csn;
            std::string s_tid = to_string(tid) + ":" + to_string(server_id);
            if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) return false;
            if (tid == writer_.load()) return true;

            std::unique_lock<std::mutex> lock(p_latch_);
            if (reader_list_map_.count(tid)) return true;           // 重复则不管
            if (writer_list_map_.count(tid)) return true;           // 重复则不管

            auto new_request = std::make_shared<LockRequest>(txMan, server_id);
            uint64_t tid_score = new_request->score_;

            // switch阶段上读锁 采用no-wait
            if (writer_.load() != INVALID_TID && switch_phase) {
                RemoveReaderRequest(tid);
                RemoveWriterRequest(tid);
                return false;
            }

            // switch阶段上锁
            if (IsSmallerThanWriter(tid, tid_score) && switch_phase) {
                RemoveReaderRequest(tid);
                RemoveWriterRequest(tid);
                return false;
            }

//            reader_list_.push_back(new_request);
//            reader_list_map_[tid] = std::prev(reader_list_.end());        // 插入迭代器
//
//            snapshot_reader_list_.push_back(tid);
//            snapshot_reader_score_map_[tid] = tid_score;
//            snapshot_reader_list_map_[tid] = std::prev(snapshot_reader_list_.end());        // 插入迭代器
//            lock.unlock();

            uint64_t start_time = now_to_us();

            if (!is_wound_wait_enable) {
                // PLOR 算法，若当前reader tid < 写者tid，则写者abort
                while (excl_sig.load()) {      // 其他事务的写集，进入commit阶段// lock.lock();
                    if (!excl_sig.load()) break;
                    if (writer_.load() != INVALID_TID && !IsSmallerThanWriter(tid, tid_score)) {
                        AbortTransactinRequest(writer_.load());
                    }
                    if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                        RemoveReaderRequest(tid);
                        RemoveWriterRequest(tid);
                        return false;
                    }
                    lock.unlock();

                    if (is_debug_print_enable && now_to_us() - start_time > 3000000) {
                        DebugMessage();
                        //                        return false;
                    }
                    if (now_to_us() - start_time > 3000000) return false;
                    std::this_thread::yield();
                    lock.lock();        // test
                }
            } else {
                // wound-wait block
                while (writer_.load() != INVALID_TID) {
                    if (!excl_sig.load() && !IsSmallerThanWriter(tid, tid_score)) break;
                    if (excl_sig.load() && !IsSmallerThanWriter(tid, tid_score)) {
                        AbortTransactinRequest(writer_.load());
                        break;
                    }
                    if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                        RemoveReaderRequest(tid);
                        RemoveWriterRequest(tid);
                        return false;
                    }

                    lock.unlock();
                    if (is_debug_print_enable && now_to_us() - start_time > 3000000) {
                        DebugMessage();
                        // return false;
                    }
                    if (now_to_us() - start_time > 3000000) return false;
                    std::this_thread::yield();

                    lock.lock();        // test
                }
            }

            reader_list_.push_back(new_request);
            reader_list_map_[tid] = std::prev(reader_list_.end());        // 插入迭代器

            snapshot_reader_list_.push_back(tid);
            snapshot_reader_score_map_[tid] = tid_score;
            snapshot_reader_list_map_[tid] = std::prev(snapshot_reader_list_.end());        // 插入迭代器

            return true;
        }

        bool LockWR(std::string& row_id, MOT::TxnManager*& txMan, uint32_t& server_id) {
            // 分配tid不相同，直接用uint64_t
            uint64_t tid = txMan->pre_csn;
            std::string s_tid = to_string(tid) + ":" + to_string(server_id);
            if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) return false;
            if (writer_.load() == tid) return true;

            std::unique_lock<std::mutex> lock(p_latch_);
            if (writer_list_map_.count(tid)) return true;           // 重复则不管

            auto new_request = std::make_shared<LockRequest>(txMan, server_id);
            uint64_t tid_score = new_request->score_;
            if (tid_score > m_writer_score_) {
                m_writer_ = tid;
                m_writer_score_ = tid_score;
            }
            writer_list_.push_back(new_request);
            writer_list_map_[tid] = std::prev(writer_list_.end());
            request_num.fetch_add(1);
            lock.unlock();

            uint64_t start_time = now_to_us();

            // PLOR 算法等待成功上锁后再继续执行
            uint64_t expected = 0L;
            if (!writer_.compare_exchange_weak(expected, tid)) {
                while (writer_.load() != tid) {
                    lock.lock();
                    if (writer_.load() == INVALID_TID) writer_.store(tid);
                    if (tid == writer_.load()) break;
                    if (!IsSmallerThanWriter(tid, tid_score)) {
                        if (tid != writer_.load()) AbortTransactinRequest(writer_.load());
                    }
                    if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                        RemoveReaderRequest(tid);
                        RemoveWriterRequest(tid);
                        return false;
                    }
                    lock.unlock();
                    if (is_debug_print_enable && now_to_us() - start_time > 3000000) {
                        DebugMessage();
                        // MOT_LOG_INFO("LockWR() csn : %s , rowid : %s", s_tid.c_str(), row_id.c_str());
//                        return false;
                    }
                    if (now_to_us() - start_time > 3000000) return false;
                    std::this_thread::yield();
                }
            }
            writer_score_.store(tid_score);         // 自己获得锁
            return true;
        }

        bool UnlockRD(std::string& row_id, uint64_t& tid) {
            std::lock_guard<std::mutex> lock(p_latch_);
            RemoveReaderRequest(tid);
            return true;
        }

        bool UnlockWR(std::string& row_id, uint64_t& tid, std::string& res_tid, uint32_t& server_id) {
            std::string s_tid = to_string(tid) + ":" + to_string(server_id);
            std::lock_guard<std::mutex> lock(p_latch_);
            bool res = true;
            if (writer_.load() == INVALID_TID || writer_.load() != tid) {
                res_tid = to_string(writer_.load()) + ":" + to_string(server_id);
                RemoveReaderRequest(tid);
                RemoveWriterRequest(tid);
                res = false;            // 同一事务不同操作解锁同一行，可能遇到该情况
            } else {
                writer_.store(INVALID_TID);
//                MOTAdaptor::epoch_lock_set.insert(row_id, s_tid);     // 先插入当前epoch有锁集, 需要吗?
                RemoveReaderRequest(tid);
                RemoveWriterRequest(tid);
                // 消除exclusive模式
                excl_sig.store(false);
            }
            if (writer_.load() == INVALID_TID) {
                // 获取优先级最高的tid，获取写锁
                if (!writer_list_.empty() && m_writer_.load() == INVALID_TID) {  // 如果队里有request，则取第一个作为grant
                    // 找到最大的元素
                    auto max_element = std::max_element(writer_list_.begin(), writer_list_.end(), cmp);
                    m_writer_.store((*max_element)->csn_);
                    m_writer_score_.store((*max_element)->score_);
                }
                writer_score_.store(m_writer_score_.load());
                writer_.store(m_writer_.load());
            }
            return res;
        }

        bool ValidateWR(std::string& row_id, MOT::TxnManager*& txMan, uint32_t& server_id) {
            uint64_t tid = txMan->pre_csn;
            std::string s_tid = to_string(tid) + ":" + to_string(server_id);
            if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) return false;
            std::unique_lock<std::mutex> lock(p_latch_);
            if (writer_.load() != tid) return false;

            SetExcl(tid);

            std::vector<uint64_t> delayed_abort_list(64);
            std::list<uint64_t> snapshot_queue(snapshot_reader_list_);
            std::unordered_map<uint64_t, uint64_t> snapshot_score_map(snapshot_reader_score_map_);

            uint64_t start_time = now_to_us();
            for (auto reader : snapshot_queue) {
                uint64_t r_score = snapshot_score_map[reader];
                if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                    RemoveReaderRequest(tid);
                    RemoveWriterRequest(tid);
                    return false;
                }
                if (reader == tid) continue;
                if (IsSmallerThanWriter(reader, r_score)){
                    // 延迟abort?
                    delayed_abort_list.emplace_back(reader);
//                    AbortTransactinRequest(reader);
                } else {
                    // 等待该reader commit
                    auto r = reader;
                    while (r != tid && reader_list_map_.count(r)) {
                        if (IsSmallerThanWriter(r, r_score)){
                            AbortTransactinRequest(r);
                        }
                        lock.unlock();
                        if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                            lock.lock();
                            RemoveReaderRequest(tid);
                            RemoveWriterRequest(tid);
                            lock.unlock();
                            return false;
                        }
                        if (is_debug_print_enable && now_to_us() - start_time > 3000000) {
                            // debug snapshot
                            DebugMessage();
//                            return false;
                        }
                        if (now_to_us() - start_time > 3000000) return false;
                        std::this_thread::yield();
                        lock.lock();
                    }
                }
            }

            // 对delayed list进行中止
            for (auto r : delayed_abort_list) {
                if (reader_list_map_.count(r)) AbortTransactinRequest(r);
            }

//            if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
//                RemoveReaderRequest(tid);
//                RemoveWriterRequest(tid);
//                return false;
//            }
            return true;
        }

        // 对OCC写操作进行检测 -> 有读者则返回false
        bool AvailableRowPlor(std::string& row_id, uint64_t& tid, std::string& s_tid, std::string& res)
        {
            bool result = true;
            if (writer_.load() == tid) return true;         // 当前线程是写者，则可以无视读者（因为进入独占模式，读者无法读取）
            std::lock_guard<std::mutex> lock(p_latch_);
//            p_lock();
            if (!reader_list_.empty()) result = false;        // 没有读者
            if (writer_list_.size() > 0) result = false;
            if (writer_.load() != INVALID_TID && writer_.load() != tid) {
                res = writer_.load();
                result = false;
            }
//            p_unlock();
            return result;
        }

        ////////////// Spin lock//////////////

//        std::atomic<bool> lock_flag{false};
        std::atomic_flag lock_flag = ATOMIC_FLAG_INIT;
        void p_lock() {
//            bool v = false;
//            while (!lock_flag.compare_exchange_weak(v, true)) {
//                v = false;
//            }
            while (lock_flag.test_and_set(std::memory_order_acquire)) { /* spin */ }
        }
        // 释放锁
        void p_unlock() {
//            lock_flag.store(false);
            lock_flag.clear(std::memory_order_release);
        }

        /////////////////// Wound-wait ////////////////////

        std::atomic<uint64_t> reader_score_{INVALID_TID};       // 读者score最大值
                                                                // helper
         std::list<uint64_t> waiting_reader_list_;
         std::unordered_map<uint64_t, uint64_t> waiting_reader_score_map_;
         std::unordered_map<uint64_t, std::list<uint64_t>::iterator> waiting_reader_list_map_;        // waiting读者去重

        // helper : writer > tid returns true
        bool IsSmallerThanWriter(uint64_t& tid, uint64_t& tid_score) {
            if (writer_score_.load(std::memory_order_seq_cst) > tid_score) return true;
            else if (writer_score_.load(std::memory_order_seq_cst) == tid_score) {
                if (writer_.load(std::memory_order_seq_cst) < tid) return true;
                else return false;
            }
            return false;
        }

        inline std::string makeSid(uint64_t tid, uint32_t server_id) {
            return std::to_string(tid) + ":" + std::to_string(server_id);
        }

        // 比较优先级，这里用 score 越大优先级越高
        inline bool higherPriority(uint64_t a_score, uint64_t b_score) {
            return a_score > b_score;
        }

        bool LockRD_WoundWait(std::string& row_id, MOT::TxnManager*& txMan, uint32_t& server_id, bool switch_phase) {

            uint64_t tid = txMan->pre_csn;
            std::string s_tid = to_string(tid) + ":" + to_string(server_id);
            if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) return false;
            if (tid == writer_.load()) return true;

            std::unique_lock<std::mutex> lock(p_latch_);
//            p_lock();

            if (reader_list_map_.count(tid)) {
//                p_unlock();
                return true;           // 重复则不管
            }
            if (writer_list_map_.count(tid)) {
//                p_unlock();
                return true;           // 重复则不管
            }

            auto new_request = std::make_shared<LockRequest>(txMan, server_id);
            uint64_t tid_score = new_request->score_;

            // switch阶段上读锁 采用no-wait
            if (writer_.load() != INVALID_TID && switch_phase) {
                RemoveReaderRequest(tid);
                RemoveWriterRequest(tid);
//                p_unlock();
                return false;
            }

            // switch阶段上锁
            if (IsSmallerThanWriter(tid, tid_score) && switch_phase) {
                RemoveReaderRequest(tid);
                RemoveWriterRequest(tid);
//                p_unlock();
                return false;
            }
            uint64_t start_time = now_to_us();

            if (is_wound_wait_enable) {
                // 比较writer优先级，waitForGraph添加边
                uint64_t w_tid = writer_.load();
                if (w_tid != INVALID_TID) {
                    std::string s_w = makeSid(w_tid, server_id);
                    // 低优先级 reader 等待读写者
                    if (!higherPriority(tid_score, writer_score_.load()) || excl_sig.load()) {
                        wait_for_graph.addEdge(s_tid, s_w, tid_score, writer_score_.load(), 5);
                    }
                }

                // wzy: 在环内等待的reader
                waiting_reader_list_.push_back(tid);
                waiting_reader_score_map_[tid] = tid_score;
                waiting_reader_list_map_[tid] = std::prev(waiting_reader_list_.end());  // 插入迭代器

                // wound-wait block
                while (writer_.load() != INVALID_TID || excl_sig.load()) {
                    // 优先级高，且没有在验证阶段的不被阻塞
                    if (!IsSmallerThanWriter(tid, tid_score) && !excl_sig.load())
                        break;
                    if (writer_.load() != INVALID_TID && !IsSmallerThanWriter(tid, tid_score) && excl_sig.load()) {
                        AbortTransactinRequest(writer_.load());
                    }
                    if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                        wait_for_graph.removeNode(s_tid);
                        RemoveReaderRequest(tid);
                        RemoveWriterRequest(tid);
//                        p_unlock();
                        return false;
                    }
//                    p_unlock();
                    lock.unlock();

                    if (is_debug_print_enable && now_to_us() - start_time > 3000000) {
                        DebugMessage();
                    }
                    //                if (now_to_us() - start_time > 3000000) {
                    //                    wait_for_graph.removeNode(s_tid);
                    //                    return false;
                    //                }
                    std::this_thread::yield();

//                    p_lock();
                    lock.lock();
                }

                // 删除等待
                // wzy: 在环内等待的reader
                if (waiting_reader_list_map_.count(tid)) {
                    auto iter1 = waiting_reader_list_map_.find(tid);
                    if (iter1 != waiting_reader_list_map_.end()) {
                        waiting_reader_list_.erase(iter1->second);
                        waiting_reader_list_map_.erase(iter1);
                    }
                    auto iter2 = waiting_reader_score_map_.find(tid);
                    if (iter2 != waiting_reader_score_map_.end()) {
                        waiting_reader_score_map_.erase(iter2);
                    }
                }

                // 无需等待任何事务
                wait_for_graph.removeEdgesFrom(s_tid);
            } else {
                // plor
                while (excl_sig.load()) {      // 其他事务的写集，进入commit阶段// lock.lock();
                    if (!excl_sig.load()) break;
                    if (writer_.load() != INVALID_TID && !IsSmallerThanWriter(tid, tid_score)) {
                        AbortTransactinRequest(writer_.load());
                    }
                    if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                        RemoveReaderRequest(tid);
                        RemoveWriterRequest(tid);
//                        p_unlock();
                        return false;
                    }
//                    p_unlock();
                    lock.unlock();

                    if (is_debug_print_enable && now_to_us() - start_time > 3000000) {
                        DebugMessage();
                    }
                    //                if (now_to_us() - start_time > 3000000) {
                    //                    wait_for_graph.removeNode(s_tid);
                    //                    return false;
                    //                }
                    std::this_thread::yield();
//                    p_lock();
                    lock.lock();
                }
            }

            reader_list_.push_back(new_request);
            reader_list_map_[tid] = std::prev(reader_list_.end());        // 插入迭代器

            snapshot_reader_list_.push_back(tid);
            snapshot_reader_score_map_[tid] = tid_score;
            snapshot_reader_list_map_[tid] = std::prev(snapshot_reader_list_.end());        // 插入迭代器

//            p_unlock();

            return true;
        }

        bool LockWR_WoundWait(std::string& row_id, MOT::TxnManager*& txMan, uint32_t& server_id) {
            // 分配tid不相同，直接用uint64_t
            uint64_t tid = txMan->pre_csn;
            std::string s_tid = to_string(tid) + ":" + to_string(server_id);
            if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) return false;
            if (writer_.load() == tid) return true;

            std::unique_lock<std::mutex> lock(p_latch_);
//            p_lock();

            if (writer_list_map_.count(tid)) {
//                p_unlock();
                return true;           // 重复则不管
            }

            auto new_request = std::make_shared<LockRequest>(txMan, server_id);
            uint64_t tid_score = new_request->score_;
            if (tid_score > m_writer_score_) {
                m_writer_ = tid;
                m_writer_score_ = tid_score;
            }
            writer_list_.push_back(new_request);
            writer_list_map_[tid] = std::prev(writer_list_.end());
            request_num.fetch_add(1);

            uint64_t start_time = now_to_us();

            uint64_t cur_owner = writer_.load();
            if (cur_owner != INVALID_TID) {
                std::string s_owner = makeSid(cur_owner, server_id);
                wait_for_graph.addEdge(s_tid, s_owner, tid_score, writer_score_.load(), 0);
            }

            // 等待成功上锁后再继续执行
            uint64_t expected = 0L;
            if (!writer_.compare_exchange_weak(expected, tid)) {
                while (writer_.load() != tid) {
                    if (writer_.load() == INVALID_TID) writer_.store(tid);
                    if (tid == writer_.load()) break;

                    if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                        wait_for_graph.removeNode(s_tid);
                        RemoveReaderRequest(tid);
                        RemoveWriterRequest(tid);
//                        p_unlock();
                        return false;
                    }
//                    p_unlock();
                    lock.unlock();

                    if (is_debug_print_enable && now_to_us() - start_time > 3000000) {
                        DebugMessage();
                    }
//                    if (now_to_us() - start_time > 3000000) {
//                        wait_for_graph.removeNode(s_tid);
//                        return false;
//                    }
                    std::this_thread::yield();

                    lock.lock();
//                    p_lock();
                }
            }

//            if (cur_owner != INVALID_TID) {
//                std::string s_owner = makeSid(cur_owner, server_id);
//                wait_for_graph.removeEdge(s_tid, s_owner);
//            }

            // 无需等待任何事务
            wait_for_graph.removeEdgesFrom(s_tid);

            // 加入 writer list 中的每个writer
            for (auto quest : writer_list_) {
                if (quest->csn_ == tid) continue;
                std::string s_writer = quest->tid_;
                wait_for_graph.addEdge(s_writer, s_tid, quest->score_, tid_score, 1);
            }

            if (is_wound_wait_enable) {
                // 加入 reader list 中的每个 reader
                for (auto rid : waiting_reader_list_) {
                    std::string s_r = makeSid(rid, server_id);
                    uint64_t r_score = waiting_reader_score_map_[rid];
                    if (!higherPriority(r_score, tid_score)) {
                        // 低优先级 reader：reader 等待写者
                        wait_for_graph.addEdge(s_r, s_tid, r_score, tid_score, 2);
                    }
                }
            }

            writer_score_.store(tid_score);         // 自己获得锁
//            p_unlock();
            return true;
        }

        bool UnlockRD_WoundWait(std::string& row_id, uint64_t& tid) {
//            p_lock();
            std::lock_guard<std::mutex> lock(p_latch_);
            RemoveReaderRequest(tid);
//            p_unlock();
            return true;
        }

        bool UnlockWR_WoundWait(std::string& row_id, uint64_t& tid, std::string& res_tid, uint32_t& server_id) {
            std::string s_tid = to_string(tid) + ":" + to_string(server_id);
            std::lock_guard<std::mutex> lock(p_latch_);
//            p_lock();

            bool res = true;
            if (writer_.load() == INVALID_TID || writer_.load() != tid) {
                res_tid = to_string(writer_.load()) + ":" + to_string(server_id);
                RemoveReaderRequest(tid);
                RemoveWriterRequest(tid);
                res = false;            // 同一事务不同操作解锁同一行，可能遇到该情况
            } else {
                writer_.store(INVALID_TID);
                RemoveReaderRequest(tid);
                RemoveWriterRequest(tid);
                // 消除exclusive模式
                excl_sig.store(false);
            }
            if (writer_.load() == INVALID_TID) {
                // 获取优先级最高的tid，获取写锁
                if (!writer_list_.empty() && m_writer_.load() == INVALID_TID) {  // 如果队里有request，则取第一个作为grant
                    // 找到最大的元素
                    auto max_element = std::max_element(writer_list_.begin(), writer_list_.end(), cmp);
                    m_writer_.store((*max_element)->csn_);
                    m_writer_score_.store((*max_element)->score_);
                }
                writer_score_.store(m_writer_score_.load());
                writer_.store(m_writer_.load());
            }
//            p_unlock();
            return res;
        }

        bool ValidateWR_WoundWait(std::string& row_id, MOT::TxnManager*& txMan, uint32_t& server_id) {
            uint64_t tid = txMan->pre_csn;
            std::string s_tid = to_string(tid) + ":" + to_string(server_id);
            if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) return false;
            if (writer_.load() != tid) return false;
            std::unique_lock<std::mutex> lock(p_latch_);

//            p_lock();

            uint64_t tid_score = writer_score_.load();

            SetExcl(tid);

            int d_index = 0;
            std::vector<uint64_t> delayed_abort_list(64);
            std::list<uint64_t> snapshot_queue(snapshot_reader_list_);
            std::unordered_map<uint64_t, uint64_t> snapshot_score_map(snapshot_reader_score_map_);

            uint64_t start_time = now_to_us();
            for (auto reader : snapshot_queue) {
                uint64_t r_score = snapshot_score_map[reader];
                if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                    wait_for_graph.removeNode(s_tid);
                    RemoveReaderRequest(tid);
                    RemoveWriterRequest(tid);
//                    p_unlock();
                    return false;
                }
                if (reader == tid) continue;
                if (!reader_list_map_.count(reader)) continue;

                if (IsSmallerThanWriter(reader, r_score)){
                    // 延迟abort?
                    delayed_abort_list[d_index++] = reader;
                } else {
                    // 等待该reader commit
                    auto r = reader;
                    std::string s_reader = makeSid(r, server_id);
                    wait_for_graph.addEdge(s_tid, s_reader, tid_score, r_score, 3);
                    while (r != tid && reader_list_map_.count(r)) {
                        if (IsSmallerThanWriter(r, r_score)){
                            delayed_abort_list.emplace_back(r);
                            break;
                        }
                        if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                            wait_for_graph.removeNode(s_tid);
                            RemoveReaderRequest(tid);
                            RemoveWriterRequest(tid);
//                            p_unlock();
                            return false;
                        }
                        lock.unlock();
//                        p_unlock();

                        // TODO: 触发死锁检测，或者死锁检测线程

                        if (is_debug_print_enable && now_to_us() - start_time > 3000000) {
                            DebugMessage();
                        }
//                        if (now_to_us() - start_time > 3000000) {
//                            wait_for_graph.removeNode(s_tid);
//                            return false;
//                        }
                        std::this_thread::yield();
                        lock.lock();
//                        p_lock();
                    }
                    // 不再等待
                    wait_for_graph.removeEdgesFrom(s_tid);
                }
            }

            // 对delayed list进行中止
            for (auto r : delayed_abort_list) {
                if (reader_list_map_.count(r)) AbortTransactinRequest(r);
            }

//            p_unlock();
            return true;
        }

        ////////////////// DL detect /////////////////////
        std::atomic<bool> excl;
        std::atomic<uint64_t> reader_cnt;
        std::atomic_flag flag = ATOMIC_FLAG_INIT;
        std::unordered_map<uint64_t , std::list<std::shared_ptr<LockRequest>>::iterator> lock_request_map_dl_;        // 去重

        void lock() {
            // 不断尝试设置 flag，当设置成功表示获得锁
            while (flag.test_and_set(std::memory_order_acquire)) {
                std::this_thread::yield(); // 主动让出 CPU，降低资源消耗
            }
        }

        void unlock() {
            flag.clear(std::memory_order_release);
        }

        bool AvailableRowDL(std::string& row_id, uint64_t& tid, std::string& s_tid, std::string& res)
        {
            bool result = false;
            if (writer_.load() == tid || writer_.load() == INVALID_TID) return true;         // 当前线程是写者，则可以无视读者（因为进入独占模式，读者无法读取）
            if (excl.load()) result = false;          // 有写者
            res = writer_.load();
            return result;
        }

        // 读锁
        bool LockRDDL(std::string& row_id, MOT::TxnManager*& txMan, uint32_t& server_id, bool switch_phase)
        {
            uint64_t tid = txMan->pre_csn;
            std::string s_tid = to_string(tid) + ":" + to_string(server_id);
            std::unique_lock<std::mutex> lock(latch_);
            if (switch_phase && excl.load() && writer_.load() != tid) return false;
            if (writer_.load() == tid) return true;
            if (lock_request_map_dl_.count(tid)) return true;           // 重复则不管

            if (m_writer_.load() == INVALID_TID && !lock_request_map_dl_.empty()) {  // 无候选则找到一个候选
                // temp queue做排序，防止迭代器失效
                // 找到最早的元素
                auto max_element = std::max_element(lock_request_queue_.begin(), lock_request_queue_.end(), cmp);
                m_grant_request.reset();
                m_grant_request = *max_element;
                m_writer_.store(m_grant_request->csn_);
            }

            auto new_request = std::make_shared<LockRequest>(txMan, server_id, false);
            // 设置自己为candidate
            if (cmp(new_request, m_grant_request)) {
                m_writer_.store(tid);
                m_grant_request.reset();
                m_grant_request = new_request;
            }

            lock_request_queue_.push_back(new_request);
            lock_request_map_dl_[tid] = std::prev(lock_request_queue_.end());        // 插入迭代器
            request_num.fetch_add(1);
            if (writer_.load() == INVALID_TID) {
                writer_.store(tid);
                excl.store(false);
                return true;
            } else MOTAdaptor::wait_for_graph.addEdge(s_tid, grant_csn);      // 添加边
            lock.unlock();

            // 轮询，是否获取锁，是否被中止
            while (excl.load()) {
                if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                    // 移除? 之后移除
                    return false;
                }
                std::this_thread::yield();
            }

            reader_cnt.fetch_add(1);
            return true;
        }

        // 写锁
        bool LockWRDL(std::string& row_id, MOT::TxnManager*& txMan, uint32_t& server_id)
        {
            uint64_t tid = txMan->pre_csn;
            std::string s_tid = to_string(tid) + ":" + to_string(server_id);
            std::unique_lock<std::mutex> lock(latch_);
            if (writer_.load() == tid) return true;
            if (lock_request_map_dl_.count(tid)) return true;           // 重复则不管

            if (m_writer_.load() == INVALID_TID && !lock_request_map_dl_.empty()) {  // 无候选则找到一个候选
                // temp queue做排序，防止迭代器失效
                // 找到最早的元素
                auto max_element = std::max_element(lock_request_queue_.begin(), lock_request_queue_.end(), cmp);
                m_grant_request.reset();
                m_grant_request = *max_element;
                m_writer_.store(m_grant_request->csn_);
            }

            auto new_request = std::make_shared<LockRequest>(txMan, server_id, true);
            // 设置自己为candidate
            if (cmp(new_request, m_grant_request)) {
                m_writer_.store(tid);
                m_grant_request.reset();
                m_grant_request = new_request;
            }

            lock_request_queue_.push_back(new_request);
            lock_request_map_dl_[tid] = std::prev(lock_request_queue_.end());        // 插入迭代器
            request_num.fetch_add(1);
            if (writer_.load() == INVALID_TID && reader_cnt.load() == 0) {
                writer_.store(tid);
                excl.store(true);
                return true;
            } else MOTAdaptor::wait_for_graph.addEdge(s_tid, grant_csn);      // 添加边
            lock.unlock();

            // 轮询，是否获取锁，是否被中止
            while (writer_.load() != tid) {
                if (MOTAdaptor::deadlock_abort_set.contain(s_tid, s_tid)) {
                    // 移除? 之后移除
                    return false;
                }
                std::this_thread::yield();
            }
            return true;
        }

        bool UnlockRowDL(std::string& row_id, MOT::TxnManager*& txMan, std::string& res_tid, uint32_t& server_id)
        {
            uint64_t tid = txMan->pre_csn;
            std::string s_tid = to_string(tid) + ":" + to_string(server_id);

            std::lock_guard<std::mutex> lock(latch_);
            bool res = true;
            if (writer_.load() == INVALID_TID || writer_.load() != tid) {
                res_tid = to_string(writer_.load()) + ":" + to_string(server_id);
                res = false;            // 同一事务不同操作解锁同一行，可能遇到该情况
            } else {
                writer_.store(INVALID_TID);
                auto iter = lock_request_map_dl_.find(tid);
                if (iter != lock_request_map_dl_.end()) {
                    lock_request_queue_.erase(iter->second);
                    lock_request_map_dl_.erase(iter);
                    request_num.fetch_sub(1);
                }
            }
            if (writer_.load() == INVALID_TID) {
                // 获取优先级最高的tid，获取写锁
                if (!lock_request_queue_.empty() && m_writer_.load() == INVALID_TID) {  // 如果队里有request，则取第一个作为grant
                    // 找到最大的元素
                    auto max_element = std::max_element(lock_request_queue_.begin(), lock_request_queue_.end(), cmp);
                    m_grant_request.reset();
                    m_grant_request = *max_element;
                    m_writer_.store(m_grant_request->csn_);
                }
                writer_.store(m_writer_.load());

                if (m_grant_request->exclusive_ && reader_cnt.load() == 0) excl.store(true);
                else excl.store(false);
            }
            return res;
        }


        ////////////////////////////////////
        LockRequestQueue(std::string& row_id)
        {
            m_row_id = row_id;
            ready = false;
            epoch_ = UINT64_MAX;
            lock_request_queue_ = std::list<std::shared_ptr<LockRequest>>();
            lock_request_queue_set_ = std::unordered_set<std::string>();
            lock_request_queue_map_ =  std::unordered_map<std::string, std::list<std::shared_ptr<LockRequest>>::iterator>();
            m_grant_request = nullptr;
            grant_request = nullptr;
            request_num.store(0);
        }

        ~LockRequestQueue()
        {
            lock_request_queue_.clear();
            lock_request_queue_set_.clear();
            lock_request_queue_map_.clear();
//            lock_thread_.join();
        }

        // 优化： 实时更新grant_csn，更新为最优先的request，其他的存在queue中
        bool lock_row_remote(std::string& row_id, uint64_t server_id, uint64_t csn, uint64_t start_epoch, uint64_t commit_epoch)
        {
            std::lock_guard<std::mutex> lock(latch_);
            std::string tid = to_string(csn) + ":" + to_string(server_id);

            if (m_grant_csn == "" && lock_request_queue_.size() > 0) {  // 如果队里有request，则取第一个作为grant
                // temp queue做排序，防止迭代器失效
//                std::list<std::shared_ptr<LockRequest>> temp_list = lock_request_queue_;
//                temp_list.sort(cmp);
//                m_grant_request.reset();
//                m_grant_request = temp_list.front();
//                m_grant_csn = m_grant_request->tid_;

                // 找到最早的元素
                auto max_element = std::max_element(lock_request_queue_.begin(), lock_request_queue_.end(), cmp);
                m_grant_request.reset();
                m_grant_request = *max_element;
                m_grant_csn = m_grant_request->tid_;
            }
            if (grant_csn == tid) return false;
            if (lock_request_queue_map_.count(tid)) return false;           // 重复则不管

            auto new_request = std::make_shared<LockRequest>(server_id, csn, start_epoch, commit_epoch);
            if (cmp(new_request, m_grant_request)) {
                m_grant_csn = tid;
                m_grant_request.reset();
                m_grant_request = new_request;
            }
            lock_request_queue_.push_back(new_request);
            lock_request_queue_map_[tid] = std::prev(lock_request_queue_.end());        // 插入迭代器
            request_num.fetch_add(1);

            if (grant_csn != "") MOTAdaptor::wait_for_graph.addEdge(tid, grant_csn);      // 添加边

            return true;
        }

        // 优化：实时更新grant_csn，更新为最优先的request，其他的存在queue中
        bool lock_row_local(std::string& row_id, MOT::TxnManager*& txMan, uint32_t& server_id)
        {
            std::string tid = to_string(txMan->GetCommitSequenceNumber()) + ":" + to_string(server_id);
            std::lock_guard<std::mutex> lock(latch_);

            if (m_grant_csn.empty() && !lock_request_queue_.empty()) {  // 如果队里有request，则取第一个作为grant
                // temp queue做排序，防止迭代器失效
                // 找到最早的元素
                auto max_element = std::max_element(lock_request_queue_.begin(), lock_request_queue_.end(), cmp);
                m_grant_request.reset();
                m_grant_request = *max_element;
                m_grant_csn = m_grant_request->tid_;
            }
            if (grant_csn == tid) return false;
            if (lock_request_queue_map_.count(tid)) return false;           // 重复则不管

            auto new_request = std::make_shared<LockRequest>(txMan, server_id);
            if (cmp(new_request, m_grant_request)) {
                m_grant_csn = tid;
                m_grant_request.reset();
                m_grant_request = new_request;
            }
            lock_request_queue_.push_back(new_request);
            lock_request_queue_map_[tid] = std::prev(lock_request_queue_.end());        // 插入迭代器

            request_num.fetch_add(1);
            if (grant_csn != "") MOTAdaptor::wait_for_graph.addEdge(tid, grant_csn);      // 添加边

            return true;
        }

        bool get_grant_csn(std::string& tid)
        {
            std::lock_guard<std::mutex> lock(latch_);
            tid = grant_csn;
            return !lock_request_queue_.empty();
        }

        bool unlock_row(std::string& row_id, std::string& tid, std::string& res_tid)
        {
            std::lock_guard<std::mutex> lock(latch_);
            if (grant_csn == "" || grant_csn != tid) {
                res_tid = grant_csn;
                if (tid == m_grant_csn) {
                    m_grant_csn = "";
                    m_grant_request.reset();
                }
                auto iter = lock_request_queue_map_.find(tid);
                if (iter != lock_request_queue_map_.end()) {
                    lock_request_queue_.erase(iter->second);
                    lock_request_queue_map_.erase(iter);
                    request_num.fetch_sub(1);
                }
                return false;     // 同一事务不同操作解锁同一行，可能遇到该情况
            } else {
                grant_csn = "";
                MOTAdaptor::epoch_lock_set.insert(row_id, tid);     // 先插入当前epoch有锁集
                auto iter = lock_request_queue_map_.find(tid);
                if (iter != lock_request_queue_map_.end()) {
                    lock_request_queue_.erase(iter->second);
                    lock_request_queue_map_.erase(iter);
                    request_num.fetch_sub(1);
                }
                grant_request.reset();
                if (tid == m_grant_csn) {
                    m_grant_csn = "";
                    m_grant_request.reset();
                }
                // 消除生效epoch
                epoch_ = UINT64_MAX;
                return true;
            }
        }

        // 返回false，则当前上锁的事务从tid返回
        bool available_row(std::string& row_id, std::string& tid)
        {
            bool result = false;
            if (lock_request_queue_.size() <= 0)
                return true;
            if (grant_csn == "" || grant_csn == tid) {
                return true;
            } else {
                tid = grant_csn;
                result = false;
            }
            return result;
        }

        // 带有epoch判断锁是否生效，返回false，则当前上锁的事务从tid返回
        bool available_row(std::string& row_id, std::string& tid, std::string& res, uint64_t epoch)
        {
            std::lock_guard<std::mutex> lock(latch_);
            bool result = false;
            if (epoch <= epoch_) return true;
            if (lock_request_queue_.size() <= 0)
                return true;
            if (grant_csn == "" || grant_csn == tid) {
                return true;
            } else {
                res = grant_csn;
                result = false;
            }
            return result;
        }

        // 当前行是否被上锁
        bool is_row_locked()
        {
            bool result = false;
            if (lock_request_queue_.size() <= 0)
                return true;
            if (grant_csn == "") {
                result = true;
            } else {
                result = false;
            }
            return result;
        }

        bool empty() {
            return lock_request_queue_.size() <= 0;
        }

        void remove_lock_request(std::string& tid)
        {
            std::lock_guard<std::mutex> lock(latch_);
            if (tid == grant_csn) {
                grant_csn = "";
                grant_request.reset();
            }
            if(tid ==  m_grant_csn) {
                m_grant_csn = "";
                m_grant_request.reset();
            }
            auto iter = lock_request_queue_map_.find(tid);
            if (iter != lock_request_queue_map_.end()) {
                lock_request_queue_.erase(iter->second);
                lock_request_queue_map_.erase(iter);
                request_num.fetch_sub(1);
            }
        }

        // 在完成上锁后，插入进wait_for
        void generateWaitFor()
        {
            std::lock_guard<std::mutex> lock(latch_);
            if (lock_request_queue_.size() <= 0) return;
            for (auto & iter : lock_request_queue_) {
                if (iter->tid_ == grant_csn) continue;
                wait_for_graph.addEdge(iter->tid_, grant_csn);
            }
        }

        static bool cmp(const std::shared_ptr<LockRequest> a, const std::shared_ptr<LockRequest> b)
        {
            // 排序
            if(b == nullptr) return true;
            bool bigger = false;
            if (a->score_ > b->score_) {
                bigger = true;
            } else if (a->commit_epoch_ < b->commit_epoch_) {     // a->commit_epoch_ > b->commit_epoch_
                bigger = true;
            } else if (a->commit_epoch_ == b->commit_epoch_) {
                if (a->start_epoch_ > b->start_epoch_) {
                    bigger = true;
                } else if (a->start_epoch_ == b->start_epoch_) {
                    if (a->csn_ < b->csn_) {
                        bigger = true;
                    } else if (a->csn_ == b->csn_) {
                        if (a->server_id_ < b->server_id_)
                            bigger = true;
                    }
                }
            }
            return bigger;
        }

        // 对该row进行上锁操作
        bool grantLocks()
        {
            std::lock_guard<std::mutex> lock(latch_);
            // 锁未释放，或者没有剩余request则返回
            if ((grant_csn != "") || (grant_csn == "" && m_grant_csn == "" && lock_request_queue_.size() <= 0)) return false;
            if (m_grant_csn == "" && lock_request_queue_.size() > 0) {  // 如果队里有request，则取第一个作为grant
                // 找到最大的元素
                auto max_element = std::max_element(lock_request_queue_.begin(), lock_request_queue_.end(), cmp);
                m_grant_request.reset();
                m_grant_request = *max_element;
                m_grant_csn = m_grant_request->tid_;
            }
            grant_csn = m_grant_csn;
            grant_request = m_grant_request;
            return true;
        }

        // 对该row进行上锁操作，加入生效epoch判断
        bool grantLocks(uint64_t epoch)
        {
            std::lock_guard<std::mutex> lock(latch_);
            // 锁未释放，或者没有剩余request则返回
            if ((grant_csn != "") || (grant_csn == "" && m_grant_csn == "" && lock_request_queue_.size() <= 0)) return false;
            if (m_grant_csn == "" && lock_request_queue_.size() > 0) {  // 如果队里有request，则取第一个作为grant
                // 找到最大的元素
                auto max_element = std::max_element(lock_request_queue_.begin(), lock_request_queue_.end(), cmp);
                m_grant_request.reset();
                m_grant_request = *max_element;
                m_grant_csn = m_grant_request->tid_;
            }
            grant_csn = m_grant_csn;
            grant_request = m_grant_request;
            epoch_ = epoch;         // 设置生效epoch
            return true;
        }

        // 对该row进行上锁操作，wound-wait剥夺锁，返回被剥夺锁的tid
        std::string grantLocks_woundWait(uint64_t epoch)
        {
            std::lock_guard<std::mutex> lock(latch_);
            if (m_grant_csn == "" && lock_request_queue_.size() > 0) {  // 如果队里有request，则取第一个作为grant
                // 找到最大的元素
                auto max_element = std::max_element(lock_request_queue_.begin(), lock_request_queue_.end(), cmp);
                m_grant_request.reset();
                m_grant_request = *max_element;
                m_grant_csn = m_grant_request->tid_;
            }
            std::string abort_csn = "";
            if (grant_csn != m_grant_csn) {
                abort_csn = grant_csn;
                // 删除abort_csn的相关请求
                RemoveLockRequest(abort_csn);

                // 授予锁
                grant_csn = m_grant_csn;
                grant_request = m_grant_request;
                epoch_ = epoch;         // 设置生效epoch
            }
            return abort_csn;
        }

    private:
        // bg线程选取上锁，指定为epoch commit结尾进行上锁
        void processLockRequests()
        {
            while (true) {
                std::unique_lock<std::mutex> lock(latch_);
//                cv_.wait(lock,
//                    [&] { return lock_request_queue_.size() >= 0 && grant_csn == "" && IsRemoteRecordCommitted(); });
                lock_request_queue_.sort(cmp);
                grant_csn = lock_request_queue_.front()->tid_;
                lock_request_queue_.pop_front();
                generateWaitFor();
                lock.unlock();
                usleep(200);
            }
        }

        // bg线程打印
        void processPrintRequests()
        {
            while (true) {
                latch_.lock();
                std::cout << "lock_request_queue_ : [" << grant_csn << "] = >";
                for (const auto& tmp : lock_request_queue_) {
                    std::cout << tmp->tid_ << " ";
                }
                std::cout << "\n";
                latch_.unlock();
                usleep(10000);
            }
        }

        void AbortTransactinRequest(uint64_t abort_csn) {
            std::string tid = to_string(abort_csn) + ":" + to_string(0);
            MOTAdaptor::deadlock_abort_set.insert(tid, tid);
            RemoveReaderRequest(abort_csn);
            RemoveWriterRequest(abort_csn);
        }

        void RemoveReaderRequest(uint64_t abort_csn) {
            // 删除abort_csn的相关请求
            if (reader_list_map_.count(abort_csn)){
                auto iter = reader_list_map_.find(abort_csn);
                if (iter != reader_list_map_.end()) {
                    reader_list_.erase(iter->second);
                    reader_list_map_.erase(iter);
                }
            }

            if (snapshot_reader_list_map_.count(abort_csn)) {
                auto iter1 = snapshot_reader_list_map_.find(abort_csn);
                if (iter1 != snapshot_reader_list_map_.end()) {
                    snapshot_reader_list_.erase(iter1->second);
                    snapshot_reader_list_map_.erase(iter1);
                }
                auto iter2 = snapshot_reader_score_map_.find(abort_csn);
                if (iter2 != snapshot_reader_score_map_.end()) {
                    snapshot_reader_score_map_.erase(iter2);
                }
            }

            // wzy: 在环内等待的reader
            if (waiting_reader_list_map_.count(abort_csn)) {
                auto iter1 = waiting_reader_list_map_.find(abort_csn);
                if (iter1 != waiting_reader_list_map_.end()) {
                    waiting_reader_list_.erase(iter1->second);
                    waiting_reader_list_map_.erase(iter1);
                }
                auto iter2 = waiting_reader_score_map_.find(abort_csn);
                if (iter2 != waiting_reader_score_map_.end()) {
                    waiting_reader_score_map_.erase(iter2);
                }
            }
        }

        void RemoveWriterRequest(uint64_t abort_csn) {
            // 删除abort_csn的相关请求
            if (!writer_list_map_.count(abort_csn)) return;
            auto iter = writer_list_map_.find(abort_csn);
            if (iter != writer_list_map_.end()) {
                writer_list_.erase(iter->second);
                writer_list_map_.erase(iter);
            }
            if (abort_csn == m_writer_.load()) {
                m_writer_.store(INVALID_TID);
                m_writer_score_.store(INVALID_TID);
            }
        }

        void RemoveLockRequest(std::string abort_csn) {
            if (abort_csn == grant_csn) {
                grant_csn = "";
                grant_request.reset();
            }
            if(abort_csn ==  m_grant_csn) {
                m_grant_csn = "";
                m_grant_request.reset();
            }
            // 删除abort_csn的相关请求
            auto iter = lock_request_queue_map_.find(abort_csn);
            if (iter != lock_request_queue_map_.end()) {
                lock_request_queue_.erase(iter->second);
                lock_request_queue_map_.erase(iter);
                request_num.fetch_sub(1);
            }
        }

        void DebugMessage() {
//            p_lock();
            std::unique_lock<std::mutex> lock(p_latch_);
            bool temp_excl = excl_sig.load();
            uint64_t temp_writer = writer_.load();
            uint64_t temp_score = writer_score_.load();
            uint64_t temp_m_writer = m_writer_.load();
            uint64_t temp_m_score = m_writer_score_.load();
            std::list<shared_ptr<LockRequest>> temp_reader_queue(reader_list_);
            std::list<shared_ptr<LockRequest>> temp_writer_queue(writer_list_);
            std::list<uint64_t> temp_waiting_reader_queue(waiting_reader_list_);
            WaitForGraph w;
            MOTAdaptor::wait_for_graph.CopyGraph(w);
//            p_unlock();
            lock.unlock();
        }
    };

    // wzy: 等待图
    class WaitForGraph {
        ////////////// Spin lock//////////////

        std::atomic_flag lock_flag = ATOMIC_FLAG_INIT;
        void p_lock() {
            //            bool v = false;
            //            while (!lock_flag.compare_exchange_weak(v, true)) {
            //                v = false;
            //            }
            while (lock_flag.test_and_set(std::memory_order_acquire)) { /* spin */ }
        }
        // 释放锁
        void p_unlock() {
            //            lock_flag.store(false);
            lock_flag.clear(std::memory_order_release);
        }

        //////////////////////////////////////

    public:
        std::unordered_map<std::string, std::list<std::string>> graph;
        std::unordered_map<std::string, std::unordered_set<std::string>> graph_set;       // 去重
        std::list<std::string> vertex;                  // 遍历
        std::unordered_set<std::string> vertex_set;   // 去重
        std::unordered_map<std::string, uint64_t> vertex_score;
        std::unordered_map<std::string, uint64_t> vertex_debug_mode;        // 被插入的mode

        std::atomic<uint64_t> vertex_num;
        std::atomic<uint64_t> edge_num;

        std::mutex mutex;

        WaitForGraph() {
            vertex_num.store(0);
            edge_num.store(0);
        }

        WaitForGraph(WaitForGraph& src) {
            graph = src.graph;
            graph_set = src.graph_set;
            vertex = src.vertex;
            vertex_set = src.vertex_set;
        }

        ~WaitForGraph() {
            graph.clear();
            graph_set.clear();
            vertex.clear();
            vertex_set.clear();
        }

        void CopyGraph(WaitForGraph& to) {
            std::lock_guard<std::mutex> graph_lock(mutex);
//            p_lock();
            to.graph = graph;
            to.graph_set = graph_set;
            to.vertex = vertex;
            to.vertex_set = vertex_set;
//            p_unlock();
        }

        uint64_t getEdgeNum() {
            std::lock_guard<std::mutex> graph_lock(mutex);
//            p_lock();
            uint64_t size = 0;
            for(const auto& v : vertex) {
                size += graph[v].size();
            }
//            p_unlock();
            return size;
        }

        uint64_t getVertexNum() {
            return vertex_num.load();
        }

        // 内部调用
        void addNode(const std::string& node, const uint64_t& score) {
            if (node == "") return;
            if (!vertex_set.count(node)){
                vertex.push_back(node);
                graph[node] = std::list<std::string>();
                vertex_set.insert(node);
                graph_set[node] = std::unordered_set<std::string>();
                vertex_num.fetch_add(1);
                vertex_score[node] = score;
            }
            modify.store(true);
        }

        // 内部调用
        void addNode(const std::string& node, const uint64_t& score, const uint64_t& mode) {
            if (node == "") return;
            if (!vertex_set.count(node)){
                vertex.push_back(node);
                graph[node] = std::list<std::string>();
                vertex_set.insert(node);
                graph_set[node] = std::unordered_set<std::string>();
                vertex_num.fetch_add(1);
                vertex_score[node] = score;
                vertex_debug_mode[node] = mode;
            }
            modify.store(true);
        }

        void addEdge(const std::string& from, const std::string& to, const uint64_t& from_score, const uint64_t& to_score) {
            if (from == "" || to == "") return;
            if (from == to) return;
            std::lock_guard<std::mutex> graph_lock(mutex);
            addNode(from, from_score);
            addNode(to, to_score);
            if (!graph_set[from].count(to)){
                graph[from].push_back(to);
                graph_set[from].insert(to);
            }
            modify.store(true);
        }

        void addEdge(const std::string& from, const std::string& to, const uint64_t& from_score, const uint64_t& to_score, const uint64_t& mode) {
            if (from == "" || to == "") return;
            if (from == to) return;
//            p_lock();
            std::lock_guard<std::mutex> graph_lock(mutex);
            addNode(from, from_score, mode);
            addNode(to, to_score, mode);
            if (!graph_set[from].count(to)){
                graph[from].push_back(to);
                graph_set[from].insert(to);
            }
            modify.store(true);
//            p_unlock();
        }


        // 内部调用
        void addNode(const std::string& node) {
            if (node == "") return;
            if (!vertex_set.count(node)){
                vertex.push_back(node);
                graph[node] = std::list<std::string>();
                vertex_set.insert(node);
                graph_set[node] = std::unordered_set<std::string>();
                vertex_num.fetch_add(1);
            }
            modify.store(true);
        }

        void addEdge(const std::string& from, const std::string& to) {
            if (from == "" || to == "") return;
            std::lock_guard<std::mutex> graph_lock(mutex);
            addNode(from);
            addNode(to);
            if (!graph_set[from].count(to)){
                graph[from].push_back(to);
                graph_set[from].insert(to);
            }
            modify.store(true);
        }

        void removeNode(const std::string& node) {
//            p_lock();
            std::lock_guard<std::mutex> graph_lock(mutex);
            if (vertex_set.count(node)) vertex_num.fetch_sub(1);
            vertex.remove(node);
            vertex_set.erase(node);
            graph.erase(node);
            graph_set.erase(node);
            vertex_score.erase(node);
            vertex_debug_mode.erase(node);

            for (auto &u : vertex) {
                graph[u].remove(node);
                graph_set[u].erase(node);
            }
            modify.store(true);
//            p_unlock();
        }

        // 只删除所有以 from 为起点的边（用于锁释放或放弃时清理自己的出边）
        void removeEdgesFrom(const std::string& from) {
//            p_lock();
            std::lock_guard<std::mutex> lk(mutex);
            if (!vertex_set.count(from)) {
//                p_unlock();
                return;
            }
            auto &outs = graph[from];
            for (const auto &to : outs) {
                graph_set[from].erase(to);
            }
            outs.clear();
            modify.store(true);
//            p_unlock();
        }

        // 只删除所有以 to 为终点的边（用于锁释放或放弃时清理指向自己的入边）
        void removeEdgesTo(const std::string& to) {
//            p_lock();
            std::lock_guard<std::mutex> lk(mutex);
            if (!vertex_set.count(to)) {
//                p_unlock();
                return;
            }
            for (auto &v : vertex) {
                auto &outs = graph[v];
                if (!outs.empty()) {
                    // 删除 v -> to
                    outs.remove(to);
                    graph_set[v].erase(to);
                }
            }
            modify.store(true);
//            p_unlock();
        }

        void removeEdge(const std::string& from, const std::string& to){
            std::lock_guard<std::mutex> graph_lock(mutex);
            if (graph_set[from].count(to)){
                graph[from].remove(to);
                graph_set[from].erase(to);
            }
            modify.store(true);
        }

        void removeInvalid() {
            for (auto &v : vertex1) {
                auto &outs = graph[v];
                if (outs.empty()) {
                    // TODO: from, to都没有才会被移除
                    graph.erase(v);
                    graph_set.erase(v);
                    vertex.remove(v);
                    vertex_set.erase(v);
                    vertex_score.erase(v);
                    vertex_num.fetch_sub(1);
                    modify.store(true);
                }
            }
        }

        void print() {
            for (auto& node : graph) {
                std::cout << node.first << " waits for: ";
                for (auto& edge : node.second) {
                    std::cout << edge << " ";
                }
                std::cout << std::endl;
            }
        }

        enum Color { WHITE, GRAY, BLACK };

        void findCycle(const std::string &start, const std::string &current,
            const std::unordered_map<std::string, std::string> &parent,
            std::vector<std::string> &target_tids, std::unordered_set<std::string> &target_set) {
            std::string node = current;
            std::string target_tid;
            while (node != start) {
                if (target_tid.empty()) target_tid = node;
                if (target_tid < node) target_tid = node;
                node = parent.at(node);
            }
            if (!target_set.count(target_tid)) {
                target_tids.push_back(target_tid);
                target_set.insert(target_tid);
            }
        }

        void DFS_VISIT(const std::string &u, std::unordered_map<std::string, Color> &color,
            std::unordered_map<std::string, std::string> &parent,
            std::vector<std::string> &target_tids, std::unordered_set<std::string> &target_set) {
            color[u] = GRAY;
            for (const auto &v : graph[u]) {
                if (color[v] == WHITE) {
                    parent[v] = u;
                    DFS_VISIT(v, color, parent, target_tids, target_set);
                } else if (color[v] == GRAY) {
                    findCycle(v, u, parent, target_tids, target_set);
                }
            }
            color[u] = BLACK;
        }

        bool CLRS_Cycles(std::vector<std::string> &target_tids, std::unordered_set<std::string> &target_set) {
            std::lock_guard<std::mutex> graph_lock(mutex);
            std::unordered_map<std::string, Color> color;
            std::unordered_map<std::string, std::string> parent;
            if (vertex.empty()) return false;
            for (const auto &node : vertex) {
                color[node] = WHITE;
                parent[node] = "";
            }
            for (const auto &node : vertex) {
                if (color[node] == WHITE) {
                    DFS_VISIT(node, color, parent, target_tids, target_set);
                }
            }
            return !target_tids.empty();
        }


        //////////////////// COPY AND DETECT ////////////////////////
        uint64_t now_to_us(){
            return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        }

        std::unordered_map<std::string, std::list<std::string>> graph1;
        std::list<std::string> vertex1;                  // 遍历
        std::unordered_map<std::string, uint64_t> vertex_score1;
        std::atomic<uint64_t> copy_time;
        std::atomic<uint64_t> detect_time;
        std::atomic<uint64_t> detect_count;
        std::atomic<bool> modify;

        void findCycle1(const std::string &start, const std::string &current,
            const std::unordered_map<std::string, std::string> &parent,
            std::vector<std::string> &target_tids, std::unordered_set<std::string> &target_set) {
            std::string node = current;

            uint64_t min_score = UINT64_MAX;  // 初始化一个极大的 score
            std::string min_node;

            while (node != start) {
                if (vertex_score1[node] < min_score) {
                    min_score = vertex_score1[node];
                    min_node = node;
                }
                node = parent.at(node);
            }

            if (!target_set.count(min_node)) {
                target_tids.push_back(min_node);
                target_set.insert(min_node);
            }
        }

        void DFS_VISIT1(const std::string &u, std::unordered_map<std::string, Color> &color,
            std::unordered_map<std::string, std::string> &parent,
            std::vector<std::string> &target_tids, std::unordered_set<std::string> &target_set) {
            color[u] = GRAY;
            for (const auto &v : graph1[u]) {
                if (color[v] == WHITE) {
                    parent[v] = u;
                    DFS_VISIT1(v, color, parent, target_tids, target_set);
                } else if (color[v] == GRAY) {
                    findCycle1(v, u, parent, target_tids, target_set);
                }
            }
            color[u] = BLACK;
        }

        bool CLRS_Cycles1(std::vector<std::string> &target_tids, std::unordered_set<std::string> &target_set) {
            uint64_t start_time = now_to_us();

            if (modify.load()) {
                std::unique_lock<std::mutex> graph_lock(mutex);
                graph1 = graph;
                vertex1 = vertex;
                vertex_score1 = vertex_score;
                modify.store(false);
//                removeInvalid();        // 移除无效node/edge
                graph_lock.unlock();
            }

            copy_time.fetch_add(now_to_us() - start_time);

            std::unordered_map<std::string, Color> color;
            std::unordered_map<std::string, std::string> parent;
            if (vertex1.empty()) return false;
            for (const auto &node : vertex1) {
                color[node] = WHITE;
                parent[node] = "";
            }
            for (const auto &node : vertex1) {
                if (color[node] == WHITE) {
                    DFS_VISIT1(node, color, parent, target_tids, target_set);
                }
            }

            detect_time.fetch_add(now_to_us() - start_time);
            detect_count.fetch_add(1);
            return !target_tids.empty();
        }

        bool JOHNSON_Cycles1(std::vector<std::string> &target_tids, std::unordered_set<std::string> &target_set) {
            uint64_t start_time = now_to_us();

            if (modify.load()) {
                std::unique_lock<std::mutex> graph_lock(mutex);
//                p_lock();
                graph1 = graph;
                vertex1 = vertex;
                vertex_score1 = vertex_score;
                modify.store(false);
//                p_unlock();
                graph_lock.unlock();
            }

            copy_time.fetch_add(now_to_us() - start_time);

            std::unordered_map<std::string, bool> blocked;
            std::unordered_map<std::string, std::unordered_set<std::string>> B;
            std::vector<std::string> stk, minStk;
            std::unordered_map<std::string, bool> processed; // 新增：记录已处理的节点

            // 解除阻塞的辅助函数
            std::function<void(const std::string&)> unblock = [&](const std::string& u) {
                blocked[u] = false;
                for (auto w : B[u]) {
                    B[u].erase(w);
                    if (blocked[w]) unblock(w);
                }
            };

            // 环检测的核心递归函数
            std::function<bool(const std::string&, const std::string&)> circuit =
                [&](const std::string& v, const std::string& s) -> bool {
                bool found = false;
                stk.push_back(v);
                blocked[v] = true;

                if (minStk.empty() || vertex_score1[v] < vertex_score1[minStk.back()]) {
                    minStk.push_back(v);
                } else {
                    minStk.push_back(minStk.back());
                }

                for (auto w : graph1[v]) {
                    if (w == s) {
                        if (target_set.count(minStk.back()) == 0) {
                            // 找到一个环，直接记录最小分节点
                            target_tids.push_back(minStk.back());
                            target_set.insert(minStk.back());  // 确保唯一
                            found = true;
                        }
                    }
                    else if (!blocked[w] && !processed[w]) { // 检查是否已处理
                        if (circuit(w, s)) found = true;
                    }
                }

                if (found) {
                    unblock(v);
                } else {
                    for (auto w : graph1[v]) {
                        B[w].insert(v);
                    }
                }

                stk.pop_back();
                minStk.pop_back();
                return found;
            };

            // 遍历所有顶点
            for (auto s : vertex1) {
                if (processed[s]) continue;  // 如果节点已经处理过，跳过

                // 重置 blocked 和 B
                for (auto& u : vertex1) {
                    blocked[u] = false;
                    B[u].clear();
                }
                stk.clear();
                minStk.clear();

                circuit(s, s);

                // 标记该节点已处理
                processed[s] = true;
            }

            detect_time.fetch_add(now_to_us() - start_time);
            detect_count.fetch_add(1);
            return !target_tids.empty();
        }
    };

    // wzy: 动态统计最热门row（作为可交互型row）
    class DynamicHotRow {
    public:
        // 记录当前的row统计
        aum::concurrent_unordered_map<std::string, uint64_t , std::string> row_freq_map_;       // row 的访问次数
        std::vector<std::string> active_rows;
        std::mutex active_rows_mutex;
        uint64_t visits_num;

        // 目前hot rows(之前10个epoch)
        std::vector<std::shared_ptr<std::unordered_set<std::string>>> hot_rows_set_;           // 去重
        uint64_t hot_rows_visit_num;
        std::mutex hot_rows_mutex_;

        uint64_t maxSize_;
        uint64_t freq_;         // 每n个epoch选出大于freq的row作为hot rows
        uint64_t epoch_;

        DynamicHotRow() {
            maxSize_ = 100;
            freq_ = kHotRowsFreq;
            epoch_ = 1;
            visits_num = 0;
            hot_rows_visit_num = 0;
            hot_rows_set_.reserve(2);
            for(int i = 0; i < 2; i ++) {
                hot_rows_set_.emplace_back(std::make_shared<std::unordered_set<std::string>>());
            }
        }

        DynamicHotRow(uint64_t maxSize, uint64_t freq) {
            maxSize_ = maxSize;
            freq_ = freq;
            hot_rows_set_.reserve(2);
            for(int i = 0; i < 2; i ++) {
                hot_rows_set_.emplace_back(std::make_shared<std::unordered_set<std::string>>());
            }
        }

        // 增加访问数 table + rowId
        void visit_row(std::string rowId) {
            std::lock_guard<std::mutex> lock(active_rows_mutex);
            visits_num++;
            uint64_t freq = 1;
            if(!row_freq_map_.add_visit(rowId, freq)){
                active_rows.push_back(rowId);
            }
        }

        // 按照固定次数选举hot rows
        void get_hot_rows_by_freq() {
            std::lock_guard<std::mutex> lock(active_rows_mutex);
            std::unique_lock<std::mutex> hot_lock(hot_rows_mutex_);
            uint64_t tmp_epoch = (epoch_ + 1) % 2;     // 更新到下一轮
            // TODO: 如果会继承上一批的热点数据呢
//            hot_rows_set_[tmp_epoch]->clear();      // 清空内容
            uint64_t f = 0;
            for (std::string r : active_rows) {
                row_freq_map_.get_element(r, f);
                if(f >= freq_ && hot_rows_set_[tmp_epoch]->count(r) == 0) {
                    hot_rows_set_[tmp_epoch]->insert(r);
                }
            }
            // active row 和 freq map 清空
            row_freq_map_.clear();
            active_rows.clear();
            epoch_ = tmp_epoch;
        }

        // TODO: 堆排序百分比
        void get_hot_rows_by_percent() {
        }

        bool isHotRows(std::string rowId) {
            if (hot_rows_set_[epoch_]->count(rowId)) hot_rows_visit_num++;
            return hot_rows_set_[epoch_]->count(rowId);
        }

        uint64_t size() {
            return hot_rows_set_[epoch_]->size();
        }

    };

    // wzy: 存储写入CSV的记录
//    class CSVMessage {
//        uint64_t tid;
//        uint64_t read_cnt;
//        uint64_t write_cnt;
//        uint64_t start_time;
//        uint64_t execution_time;
//        bool pessimistic_flag;
//        bool commit_flag;
//        uint64_t hot_cnt;
//        uint64_t retry_cnt;
//
//        CSVMessage(uint64_t _tid, uint64_t _read_cnt, uint64_t _write_cnt, uint64_t _start_time, uint64_t _execution_time, bool _pessimistic_flag, bool _commit_flag)
//            : tid(_tid), read_cnt(_read_cnt), write_cnt(_write_cnt), start_time(_start_time), execution_time(_execution_time), pessimistic_flag(_pessimistic_flag), commit_flag(_commit_flag)
//        {
//        }
//
//        // TODO:
//        std::string toString() {
//
//        }
//
//    };

    ///////////////////////////////////////////


    // wzy: 死锁检测等相关操作，可能有死锁
    // 在grant lock的时候生成
    static void insertIntoWaitfor(const std::string& txn, const std::string& wait)
    {
//        std::lock_guard<std::mutex> lock(wait_for_mutex);
        if (txn.empty() || txn == wait) return;
        if (wait_for[txn].empty()) {
            wait_for[txn] = std::set<std::string>();
        }
        wait_for[txn].insert(wait);
    }

    // 死锁后的abort删去wait for
    static void removeFromWaitFor(std::string& txn, std::string& wait)
    {}

};







inline MOT::TxnManager* GetSafeTxn(const char* callerSrc, ::TransactionId txn_id = 0)
{
    if (!u_sess->mot_cxt.txn_manager) {
        MOTAdaptor::InitTxnManager(callerSrc);
        if (u_sess->mot_cxt.txn_manager != nullptr) {
            if (txn_id != 0) {
                u_sess->mot_cxt.txn_manager->SetTransactionId(txn_id);
            }
        } else {
            report_pg_error(MOT_GET_ROOT_ERROR_RC());
        }
    }
    return u_sess->mot_cxt.txn_manager;
}

extern void EnsureSafeThreadAccess();

inline List* BitmapSerialize(List* result, uint8_t* bitmap, int16_t len)
{
    // set list type to FDW_LIST_BITMAP
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, FDW_LIST_BITMAP, false, true));
    for (int i = 0; i < len; i++)
        result = lappend(result, makeConst(INT1OID, -1, InvalidOid, 1, Int8GetDatum(bitmap[i]), false, true));

    return result;
}

inline void BitmapDeSerialize(uint8_t* bitmap, int16_t len, ListCell** cell)
{
    if (cell != nullptr && *cell != nullptr) {
        int type = ((Const*)lfirst(*cell))->constvalue;
        if (type == FDW_LIST_BITMAP) {
            *cell = lnext(*cell);
            for (int i = 0; i < len; i++) {
                bitmap[i] = (uint8_t)((Const*)lfirst(*cell))->constvalue;
                *cell = lnext(*cell);
            }
        }
    }
}

inline void CleanCursors(MOTFdwStateSt* state)
{
    for (int i = 0; i < 2; i++) {
        if (state->m_cursor[i]) {
            state->m_cursor[i]->Invalidate();
            state->m_cursor[i]->Destroy();
            delete state->m_cursor[i];
            state->m_cursor[i] = NULL;
        }
    }
}

inline void CleanQueryStatesOnError(MOT::TxnManager* txn)
{
    if (txn != nullptr) {
        for (auto& itr : txn->m_queryState) {
            MOTFdwStateSt* state = (MOTFdwStateSt*)itr.second;
            if (state != nullptr) {
                CleanCursors(state);
            }
        }
        txn->m_queryState.clear();
    }
}

MOTFdwStateSt* InitializeFdwState(void* fdwState, List** fdwExpr, uint64_t exTableID);
void* SerializeFdwState(MOTFdwStateSt* state);
void ReleaseFdwState(MOTFdwStateSt* state);









template<typename T>
using BlockingConcurrentQueue =  moodycamel::BlockingConcurrentQueue<T>;

struct send_thread_params {
    uint64_t current_epoch;
    uint64_t tot;
    std::string* merge_request_ptr;
    send_thread_params(uint64_t ce, uint64_t tot_temp, std::string* ptr1):
        current_epoch(ce), tot(tot_temp), merge_request_ptr(ptr1){}
    send_thread_params(){}
};


#endif  // MOT_INTERNAL_H
