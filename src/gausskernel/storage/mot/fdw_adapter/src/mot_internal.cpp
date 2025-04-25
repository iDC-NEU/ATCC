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
 * mot_internal.cpp
 *    MOT Foreign Data Wrapper internal interfaces to the MOT engine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/src/mot_internal.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <google/protobuf/io/gzip_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <ostream>
#include <istream>
#include <iomanip>
#include <pthread.h>
#include <cstring>
#include "message.pb.h"
#include "postgres.h"
#include "access/dfs/dfs_query.h"
#include "access/sysattr.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/syscache.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "commands/dbcommands.h"
#include "knl/knl_session.h"
#include "mot_internal.h"
#include "row.h"
#include "log_statistics.h"
#include "spin_lock.h"
#include "txn.h"
#include "table.h"
#include "utilities.h"
#include "mot_engine.h"
#include "sentinel.h"
#include "txn.h"
#include "txn_access.h"
#include "index_factory.h"
#include "column.h"
#include "mm_raw_chunk_store.h"
#include "ext_config_loader.h"
#include "config_manager.h"
#include "mot_error.h"
#include "utilities.h"
#include "jit_context.h"
#include "mm_cfg.h"
#include "jit_statistics.h"
#include "gaussdb_config_loader.h"
#include <cstdio> 
#include <stdio.h>
#include <iostream>
#include "string"
#include "sstream"
#include <fstream>
#include <atomic>
#include <sys/time.h>
// #include "neu_concurrency_tools/blockingconcurrentqueue.h"
// #include "neu_concurrency_tools/blocking_mpmc_queue.h"
// #include "neu_concurrency_tools/ThreadPool.h"
// #include "epoch_merge.h"
#include "zmq.hpp"
#include "zmq.h"
#include <sched.h>
#include "utils/timestamp.h"
#include "postmaster/postmaster.h"

// wzy: 测试
#include "postmaster/tinyxml2.h"

/** @define masks for CSN word   */
#define CSN_BITS 0x1FFFFFFFFFFFFFFFUL

#define STATUS_BITS 0xE000000000000000UL

#define IS_CHAR_TYPE(oid) (oid == VARCHAROID || oid == BPCHAROID || oid == TEXTOID || oid == CLOBOID || oid == BYTEAOID)
#define IS_INT_TYPE(oid)                                                                                           \
    (oid == BOOLOID || oid == CHAROID || oid == INT8OID || oid == INT2OID || oid == INT4OID || oid == FLOAT4OID || \
        oid == FLOAT8OID || oid == INT1OID || oid == DATEOID || oid == TIMEOID || oid == TIMESTAMPOID ||           \
        oid == TIMESTAMPTZOID)

MOT::MOTEngine* MOTAdaptor::m_engine = nullptr;
static XLOGLogger xlogger;

// enable MOT Engine logging facilities
DECLARE_LOGGER(InternalExecutor, FDW)

/** @brief on_proc_exit() callback for cleaning up current thread - only when thread pool is ENABLED. */
static void MOTCleanupThread(int status, Datum ptr);

/** @brief Helper for cleaning up all JIT context objects stored in all CachedPlanSource of the current session. */
static void DestroySessionJitContexts();

// in a thread-pooled environment we need to ensure thread-locals are initialized properly
static inline void EnsureSafeThreadAccessInline()
{
    if (MOTCurrThreadId == INVALID_THREAD_ID) {
        MOT_LOG_DEBUG("Initializing safe thread access for current thread");
        MOT::AllocThreadId();
        // register for cleanup only once - not having a current thread id is the safe indicator we never registered
        // proc-exit callback for this thread
        if (g_instance.attr.attr_common.enable_thread_pool) {
            on_proc_exit(MOTCleanupThread, PointerGetDatum(nullptr));
            MOT_LOG_DEBUG("Registered current thread for proc-exit callback for thread %p", (void*)pthread_self());
        }
    }
    if (MOTCurrentNumaNodeId == MEM_INVALID_NODE) {
        MOT::InitCurrentNumaNodeId();
    }
    MOT::InitMasstreeThreadinfo();
}

extern void EnsureSafeThreadAccess()
{
    EnsureSafeThreadAccessInline();
}

static void DestroySession(MOT::SessionContext* sessionContext)
{
    MOT_ASSERT(MOTAdaptor::m_engine);
    MOT_LOG_DEBUG("Destroying session context %p, connection_id %u", sessionContext, sessionContext->GetConnectionId());

    if (u_sess->mot_cxt.jit_session_context_pool) {
        JitExec::FreeSessionJitContextPool(u_sess->mot_cxt.jit_session_context_pool);
    }
    MOT::GetSessionManager()->DestroySessionContext(sessionContext);
}

// Global map of PG session identification (required for session statistics)
// This approach is safer than saving information in the session context
static pthread_spinlock_t sessionDetailsLock;
typedef std::map<MOT::SessionId, pair<::ThreadId, pg_time_t>> SessionDetailsMap;
static SessionDetailsMap sessionDetailsMap;

static void InitSessionDetailsMap()
{
    pthread_spin_init(&sessionDetailsLock, 0);
}

static void DestroySessionDetailsMap()
{
    pthread_spin_destroy(&sessionDetailsLock);
}

static void RecordSessionDetails()
{
    MOT::SessionId sessionId = u_sess->mot_cxt.session_id;
    if (sessionId != INVALID_SESSION_ID) {
        pthread_spin_lock(&sessionDetailsLock);
        sessionDetailsMap.emplace(sessionId, std::make_pair(t_thrd.proc->pid, t_thrd.proc->myStartTime));
        pthread_spin_unlock(&sessionDetailsLock);
    }
}

static void ClearSessionDetails(MOT::SessionId sessionId)
{
    if (sessionId != INVALID_SESSION_ID) {
        pthread_spin_lock(&sessionDetailsLock);
        SessionDetailsMap::iterator itr = sessionDetailsMap.find(sessionId);
        if (itr != sessionDetailsMap.end()) {
            sessionDetailsMap.erase(itr);
        }
        pthread_spin_unlock(&sessionDetailsLock);
    }
}

inline void ClearCurrentSessionDetails()
{
    ClearSessionDetails(u_sess->mot_cxt.session_id);
}

static void GetSessionDetails(MOT::SessionId sessionId, ::ThreadId* gaussSessionId, pg_time_t* sessionStartTime)
{
    // although we have the PGPROC in the user data of the session context, we prefer not to use
    // it due to safety (in some unknown constellation we might hold an invalid pointer)
    // it is much safer to save a copy of the two required fields
    pthread_spin_lock(&sessionDetailsLock);
    SessionDetailsMap::iterator itr = sessionDetailsMap.find(sessionId);
    if (itr != sessionDetailsMap.end()) {
        *gaussSessionId = itr->second.first;
        *sessionStartTime = itr->second.second;
    }
    pthread_spin_unlock(&sessionDetailsLock);
}

// provide safe session auto-cleanup in case of missing session closure
// This mechanism relies on the fact that when a session ends, eventually its thread is terminated
// ATTENTION: in thread-pooled envelopes this assumption no longer holds true, since the container thread keeps
// running after the session ends, and a session might run each time on a different thread, so we
// disable this feature, instead we use this mechanism to generate thread-ended event into the MOT Engine
static pthread_key_t sessionCleanupKey;

static void SessionCleanup(void* key)
{
    MOT_ASSERT(!g_instance.attr.attr_common.enable_thread_pool);

    // in order to ensure session-id cleanup for session 0 we use positive values
    MOT::SessionId sessionId = (MOT::SessionId)(((uint64_t)key) - 1);
    if (sessionId != INVALID_SESSION_ID) {
        MOT_LOG_WARN("Encountered unclosed session %u (missing call to DestroyTxn()?)", (unsigned)sessionId);
        ClearSessionDetails(sessionId);
        MOT_LOG_DEBUG("SessionCleanup(): Calling DestroySessionJitContext()");
        DestroySessionJitContexts();
        if (MOTAdaptor::m_engine) {
            MOT::SessionContext* sessionContext = MOT::GetSessionManager()->GetSessionContext(sessionId);
            if (sessionContext != nullptr) {
                DestroySession(sessionContext);
            }
            // since a call to on_proc_exit(destroyTxn) was probably missing, we should also cleanup thread-locals
            // pay attention that if we got here it means the thread pool is disabled, so we must ensure thread-locals
            // are cleaned up right now. Due to these complexities, onCurrentThreadEnding() was designed to be proof
            // for repeated calls.
            MOTAdaptor::m_engine->OnCurrentThreadEnding();
        }
    }
}

static void InitSessionCleanup()
{
    pthread_key_create(&sessionCleanupKey, SessionCleanup);
}

static void DestroySessionCleanup()
{
    pthread_key_delete(sessionCleanupKey);
}

static void ScheduleSessionCleanup()
{
    pthread_setspecific(sessionCleanupKey, (const void*)(uint64_t)(u_sess->mot_cxt.session_id + 1));
}

static void CancelSessionCleanup()
{
    pthread_setspecific(sessionCleanupKey, nullptr);
}

static GaussdbConfigLoader* gaussdbConfigLoader = nullptr;

bool MOTAdaptor::m_initialized = false;
bool MOTAdaptor::m_callbacks_initialized = false;

static void WakeupWalWriter()
{
    if (g_instance.proc_base->walwriterLatch != nullptr) {
        SetLatch(g_instance.proc_base->walwriterLatch);
    }
}

void MOTAdaptor::Init()
{
    if (m_initialized) {
        // This is highly unexpected, and should especially be guarded in scenario of switch-over to standby.
        elog(FATAL, "Double attempt to initialize MOT engine, it is already initialized");
    }

    m_engine = MOT::MOTEngine::CreateInstanceNoInit(g_instance.attr.attr_common.MOTConfigFileName, 0, nullptr);
    if (m_engine == nullptr) {
        elog(FATAL, "Failed to create MOT engine");
    }

    MOT::MOTConfiguration& motCfg = MOT::GetGlobalConfiguration();
    motCfg.SetTotalMemoryMb(g_instance.attr.attr_memory.max_process_memory / KILO_BYTE);

    gaussdbConfigLoader = new (std::nothrow) GaussdbConfigLoader();
    if (gaussdbConfigLoader == nullptr) {
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to allocate memory for GaussDB/MOTEngine configuration loader.");
    }
    MOT_LOG_TRACE("Adding external configuration loader for GaussDB");
    if (!m_engine->AddConfigLoader(gaussdbConfigLoader)) {
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to add GaussDB/MOTEngine configuration loader");
    }

    if (!m_engine->LoadConfig()) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to load configuration for MOT engine.");
    }

    // Check max process memory here - we do it anyway to protect ourselves from miscalculations.
    // Attention: the following values are configured during the call to MOTEngine::LoadConfig() just above
    uint64_t globalMemoryKb = MOT::g_memGlobalCfg.m_maxGlobalMemoryMb * KILO_BYTE;
    uint64_t localMemoryKb = MOT::g_memGlobalCfg.m_maxLocalMemoryMb * KILO_BYTE;
    uint64_t maxReserveMemoryKb = globalMemoryKb + localMemoryKb;

    // check whether the 2GB gap between MOT and envelope is still kept
    if ((g_instance.attr.attr_memory.max_process_memory < (int32)maxReserveMemoryKb) ||
        ((g_instance.attr.attr_memory.max_process_memory - maxReserveMemoryKb) < MIN_DYNAMIC_PROCESS_MEMORY)) {
        // we allow one extreme case: GaussDB is configured to its limit, and zero memory is left for us
        if (maxReserveMemoryKb <= motCfg.MOT_MIN_MEMORY_USAGE_MB * KILO_BYTE) {
            MOT_LOG_WARN("Allowing MOT to work in minimal memory mode");
        } else {
            m_engine->RemoveConfigLoader(gaussdbConfigLoader);
            delete gaussdbConfigLoader;
            gaussdbConfigLoader = nullptr;
            MOT::MOTEngine::DestroyInstance();
            elog(FATAL,
                "The value of pre-reserved memory for MOT engine is not reasonable: "
                "Request for a maximum of %" PRIu64 " KB global memory, and %" PRIu64
                " KB session memory (total of %" PRIu64 " KB) is invalid since max_process_memory is %u KB",
                globalMemoryKb,
                localMemoryKb,
                maxReserveMemoryKb,
                g_instance.attr.attr_memory.max_process_memory);
        }
    }

    if (!m_engine->Initialize()) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to initialize MOT engine.");
    }

    if (!JitExec::JitStatisticsProvider::CreateInstance()) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to initialize JIT statistics.");
    }

    // make sure current thread is cleaned up properly when thread pool is enabled
    EnsureSafeThreadAccessInline();

    if (motCfg.m_enableRedoLog && motCfg.m_loggerType == MOT::LoggerType::EXTERNAL_LOGGER) {
        m_engine->GetRedoLogHandler()->SetLogger(&xlogger);
        m_engine->GetRedoLogHandler()->SetWalWakeupFunc(WakeupWalWriter);
    }

    InitSessionDetailsMap();
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        InitSessionCleanup();
    }
    InitDataNodeId();
    InitKeyOperStateMachine();
    m_initialized = true;
}

void MOTAdaptor::NotifyConfigChange()
{
    if (gaussdbConfigLoader != nullptr) {
        gaussdbConfigLoader->MarkChanged();
    }
}

void MOTAdaptor::InitDataNodeId()
{
    MOT::GetGlobalConfiguration().SetPgNodes(1, 1);
}

void MOTAdaptor::Destroy()
{
    if (!m_initialized) {
        return;
    }

    JitExec::JitStatisticsProvider::DestroyInstance();
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        DestroySessionCleanup();
    }
    DestroySessionDetailsMap();
    if (gaussdbConfigLoader != nullptr) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
    }

    EnsureSafeThreadAccessInline();
    MOT::MOTEngine::DestroyInstance();
    m_engine = nullptr;
    knl_thread_mot_init();  // reset all thread-locals, mandatory for standby switch-over
    m_initialized = false;
}

MOT::TxnManager* MOTAdaptor::InitTxnManager(
    const char* callerSrc, MOT::ConnectionId connection_id /* = INVALID_CONNECTION_ID */)
{
    if (!u_sess->mot_cxt.txn_manager) {
        bool attachCleanFunc =
            (MOTCurrThreadId == INVALID_THREAD_ID ? true : !g_instance.attr.attr_common.enable_thread_pool);

        // First time we handle this connection
        if (m_engine == nullptr) {
            elog(ERROR, "initTxnManager: MOT engine is not initialized");
            return nullptr;
        }

        // create new session context
        MOT::SessionContext* session_ctx =
            MOT::GetSessionManager()->CreateSessionContext(IS_PGXC_COORDINATOR, 0, nullptr, connection_id);
        if (session_ctx == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Session Initialization", "Failed to create session context in %s", callerSrc);
            ereport(ERROR, (errmsg("Session startup: failed to create session context.")));
            return nullptr;
        }
        MOT_ASSERT(u_sess->mot_cxt.session_context == session_ctx);
        MOT_ASSERT(u_sess->mot_cxt.session_id == session_ctx->GetSessionId());
        MOT_ASSERT(u_sess->mot_cxt.connection_id == session_ctx->GetConnectionId());

        // make sure we cleanup leftovers from other session
        u_sess->mot_cxt.jit_context_count = 0;

        // record session details for statistics report
        RecordSessionDetails();

        if (attachCleanFunc) {
            // schedule session cleanup when thread pool is not used
            if (!g_instance.attr.attr_common.enable_thread_pool) {
                on_proc_exit(DestroyTxn, PointerGetDatum(session_ctx));
                ScheduleSessionCleanup();
            } else {
                on_proc_exit(MOTCleanupThread, PointerGetDatum(nullptr));
                MOT_LOG_DEBUG("Registered current thread for proc-exit callback for thread %p", (void*)pthread_self());
            }
        }

        u_sess->mot_cxt.txn_manager = session_ctx->GetTxnManager();
        elog(DEBUG1, "Init TXN_MAN for thread %u", MOTCurrThreadId);
    }

    return u_sess->mot_cxt.txn_manager;
}

static void DestroySessionJitContexts()
{
    // we must release all JIT context objects associated with this session now.
    // it seems that when thread pool is disabled, all cached plan sources for the session are not
    // released explicitly, but rather implicitly as part of the release of the memory context of the session.
    // in any case, we guard against repeated destruction of the JIT context by nullifying it
    MOT_LOG_DEBUG("Cleaning up all JIT context objects for current session");
    CachedPlanSource* psrc = u_sess->pcache_cxt.first_saved_plan;
    while (psrc != nullptr) {
        if (psrc->mot_jit_context != nullptr) {
            MOT_LOG_DEBUG("DestroySessionJitContexts(): Calling DestroyJitContext(%p)", psrc->mot_jit_context);
            JitExec::DestroyJitContext(psrc->mot_jit_context);
            psrc->mot_jit_context = nullptr;
        }
        psrc = psrc->next_saved;
    }
    MOT_LOG_DEBUG("DONE Cleaning up all JIT context objects for current session");
}

/** @brief Notification from thread pool that a session ended (only when thread pool is ENABLED). */
extern void MOTOnSessionClose()
{
    MOT_LOG_TRACE("Received session close notification (current session id: %u, current connection id: %u)",
        u_sess->mot_cxt.session_id,
        u_sess->mot_cxt.connection_id);
    if (u_sess->mot_cxt.session_id != INVALID_SESSION_ID) {
        ClearCurrentSessionDetails();
        MOT_LOG_DEBUG("MOTOnSessionClose(): Calling DestroySessionJitContexts()");
        DestroySessionJitContexts();
        if (!MOTAdaptor::m_engine) {
            MOT_LOG_ERROR("MOTOnSessionClose(): MOT engine is not initialized");
        } else {
            EnsureSafeThreadAccessInline();  // this is ok, it wil be cleaned up when thread exits
            MOT::SessionContext* sessionContext = u_sess->mot_cxt.session_context;
            if (sessionContext == nullptr) {
                MOT_LOG_WARN("Received session close notification, but no current session is found. Current session id "
                             "is %u. Request ignored.",
                    u_sess->mot_cxt.session_id);
            } else {
                DestroySession(sessionContext);
                MOT_ASSERT(u_sess->mot_cxt.session_id == INVALID_SESSION_ID);
            }
        }
    }
}

/** @brief Notification from thread pool that a pooled thread ended (only when thread pool is ENABLED). */
static void MOTOnThreadShutdown()
{
    if (!MOTAdaptor::m_initialized) {
        return;
    }

    MOT_LOG_TRACE("Received thread shutdown notification");
    if (!MOTAdaptor::m_engine) {
        MOT_LOG_ERROR("MOTOnThreadShutdown(): MOT engine is not initialized");
    } else {
        MOTAdaptor::m_engine->OnCurrentThreadEnding();
    }
    knl_thread_mot_init();  // reset all thread locals
}

/**
 * @brief on_proc_exit() callback to handle thread-cleanup - regardless of whether thread pool is enabled or not.
 * registration to on_proc_exit() is triggered by first call to EnsureSafeThreadAccessInline().
 */
static void MOTCleanupThread(int status, Datum ptr)
{
    MOT_ASSERT(g_instance.attr.attr_common.enable_thread_pool);
    MOT_LOG_TRACE("Received thread cleanup notification (thread-pool ON)");

    // when thread pool is used we just cleanup current thread
    // this might be a duplicate because thread pool also calls MOTOnThreadShutdown() - this is still ok
    // because we guard against repeated calls in MOTEngine::onCurrentThreadEnding()
    MOTOnThreadShutdown();
}

void MOTAdaptor::DestroyTxn(int status, Datum ptr)
{
    MOT_ASSERT(!g_instance.attr.attr_common.enable_thread_pool);

    // cleanup session
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        CancelSessionCleanup();
    }
    ClearCurrentSessionDetails();
    MOT_LOG_DEBUG("DestroyTxn(): Calling DestroySessionJitContexts()");
    DestroySessionJitContexts();
    MOT::SessionContext* session = (MOT::SessionContext*)DatumGetPointer(ptr);
    if (m_engine == nullptr) {
        elog(ERROR, "destroyTxn: MOT engine is not initialized");
    }

    if (session != MOT_GET_CURRENT_SESSION_CONTEXT()) {
        MOT_LOG_WARN("Ignoring request to delete session context: already deleted");
    } else if (session != nullptr) {
        elog(DEBUG1, "Destroy SessionContext, connection_id = %u \n", session->GetConnectionId());
        EnsureSafeThreadAccessInline();  // may be accessed from new thread pool worker
        MOT::GcManager* gc = MOT_GET_CURRENT_SESSION_CONTEXT()->GetTxnManager()->GetGcSession();
        if (gc != nullptr) {
            gc->GcEndTxn();
        }
        DestroySession(session);
    }

    // clean up thread
    MOTOnThreadShutdown();
}

MOT::RC MOTAdaptor::SendInteractiveLockInfo(MOT::Row* currRow)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        return txn->SendLockInfo(currRow);  // wzy：发送本地锁集，并本地和远端上锁
    } else {
        // Nothing to do in coordinator
        return MOT::RC_OK;
    }
}

void MOTAdaptor::UnlockInteractiveLockInfo(uint64_t csn, bool abort)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        txn->UnlockLockInfo(csn, abort);  // wzy：本地解锁，但csn会发生变化，所以后序可能采用其他标识符？
    } else {
        // Nothing to do in coordinator
    }
}

void MOTAdaptor::UnlockInteractiveLockInfoPlor(uint64_t csn, bool abort)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        txn->UnlockLockInfo_Plor(csn, abort);  // wzy：本地解锁，但csn会发生变化，所以后序可能采用其他标识符？
    } else {
        // Nothing to do in coordinator
    }
}

void MOTAdaptor::UnlockInteractiveLockInfoWoundWait(uint64_t csn, bool abort)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        txn->UnlockLockInfo_WoundWait(csn, abort);  // wzy：本地解锁，但csn会发生变化，所以后序可能采用其他标识符？
    } else {
        // Nothing to do in coordinator
    }
}

MOT::RC MOTAdaptor::ValidateCommit()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        if (cc_mode == 3) {                 // wzy: Silo
            return txn->ValidateCommit();   //ADDBY NEU change to Commit
        } else if (cc_mode == 4) {                 // wzy: Silo
            return txn->ValidateCommit();   //ADDBY NEU change to Commit
        }
        else return txn->Commit();      // CRDT + hybrid
    } else {
        // Nothing to do in coordinator
        return MOT::RC_OK;
    }
}

// wzy: 切换到Plor
MOT::RC MOTAdaptor::SwitchPlor()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        return txn->ReadLockForSwitch_Plor();
    } else {
        // Nothing to do in coordinator
        return MOT::RC_OK;
    }
}

// wzy: Plor
MOT::RC MOTAdaptor::ValidateCommitPlor()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        if (cc_mode == 4) return txn->Commit_Plor();        // 无epoch
        else return txn->Commit_Plor_Epoch();               // 分epoch验证
    } else {
        // Nothing to do in coordinator
        return MOT::RC_OK;
    }
}

// wzy: DL detect
MOT::RC MOTAdaptor::ValidateCommitDL()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        return txn->Commit_DL();               // 分epoch验证
    } else {
        // Nothing to do in coordinator
        return MOT::RC_OK;
    }
}

// wzy: Wound-wait
MOT::RC MOTAdaptor::ValidateCommitWoundWait()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        return txn->Commit_WoundWait();
    } else {
        // Nothing to do in coordinator
        return MOT::RC_OK;
    }
}


void MOTAdaptor::RecordCommit(uint64_t csn)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);

    // 记录下precsn
//    txn->pre_csn = txn->GetCommitSequenceNumber();
    txn->SetCommitSequenceNumber(csn);
    if (!IS_PGXC_COORDINATOR) {
        txn->RecordCommit(txn->pre_csn);
    } else {
        txn->LiteCommit();
    }
}

MOT::RC MOTAdaptor::Commit(uint64_t csn)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetCommitSequenceNumber(csn);
    if (!IS_PGXC_COORDINATOR) {
        return txn->Commit();
    } else {
        txn->LiteCommit();
        return MOT::RC_OK;
    }
}

void MOTAdaptor::EndTransaction()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    // Nothing to do in coordinator
    if (!IS_PGXC_COORDINATOR) {
        txn->EndTransaction();
    }
}

void MOTAdaptor::Rollback()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        txn->Rollback();
    } else {
        txn->LiteRollback();
    }
}

MOT::RC MOTAdaptor::Prepare()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        return txn->Prepare();
    } else {
        txn->LitePrepare();
        return MOT::RC_OK;
    }
}

void MOTAdaptor::CommitPrepared(uint64_t csn)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetCommitSequenceNumber(csn);
    if (!IS_PGXC_COORDINATOR) {
        txn->CommitPrepared();
    } else {
        txn->LiteCommitPrepared();
    }
}

void MOTAdaptor::RollbackPrepared()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        txn->RollbackPrepared();
    } else {
        txn->LiteRollbackPrepared();
    }
}

MOT::RC MOTAdaptor::InsertRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot)
{
    EnsureSafeThreadAccessInline();
    uint8_t* newRowData = nullptr;
    fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
    MOT::Table* table = fdwState->m_table;
    MOT::Row* row = table->CreateNewRow();
    if (row == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Insert Row", "Failed to create new row for table %s", table->GetLongTableName().c_str());
        return MOT::RC_MEMORY_ALLOCATION_ERROR;
    }
    newRowData = const_cast<uint8_t*>(row->GetData());
    PackRow(slot, table, fdwState->m_attrsUsed, newRowData);

    MOT::RC res = table->InsertRow(row, fdwState->m_currTxn);
    if ((res != MOT::RC_OK) && (res != MOT::RC_UNIQUE_VIOLATION)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Insert Row", "Failed to insert new row for table %s", table->GetLongTableName().c_str());
    }
    return res;
}

MOT::RC MOTAdaptor::UpdateRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot, MOT::Row* currRow)
{
    EnsureSafeThreadAccessInline();
    MOT::RC rc;

    do {
        fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
        rc = fdwState->m_currTxn->UpdateLastRowState(MOT::AccessType::WR);
        if (rc != MOT::RC::RC_OK) {
            break;
        }
        uint8_t* rowData = const_cast<uint8_t*>(currRow->GetData());
        PackUpdateRow(slot, fdwState->m_table, fdwState->m_attrsModified, rowData);
        MOT::BitmapSet modified_columns(fdwState->m_attrsModified, fdwState->m_table->GetFieldCount() - 1);

        rc = fdwState->m_currTxn->OverwriteRow(currRow, modified_columns);
    } while (0);

    return rc;
}

MOT::RC MOTAdaptor::DeleteRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot)
{
    EnsureSafeThreadAccessInline();
    fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
    MOT::RC rc = fdwState->m_currTxn->DeleteLastRow();
    return rc;
}

// NOTE: colId starts from 1
bool MOTAdaptor::SetMatchingExpr(
    MOTFdwStateSt* state, MatchIndexArr* marr, int16_t colId, KEY_OPER op, Expr* expr, Expr* parent, bool set_local)
{
    bool res = false;
    uint16_t numIx = state->m_table->GetNumIndexes();

    for (uint16_t i = 0; i < numIx; i++) {
        MOT::Index* ix = state->m_table->GetIndex(i);
        if (ix != nullptr && ix->IsFieldPresent(colId)) {
            if (marr->m_idx[i] == nullptr) {
                marr->m_idx[i] = (MatchIndex*)palloc0(sizeof(MatchIndex));
                marr->m_idx[i]->Init();
                marr->m_idx[i]->m_ix = ix;
            }

            res |= marr->m_idx[i]->SetIndexColumn(state, colId, op, expr, parent, set_local);
        }
    }

    return res;
}

MatchIndex* MOTAdaptor::GetBestMatchIndex(MOTFdwStateSt* festate, MatchIndexArr* marr, int numClauses, bool setLocal)
{
    MatchIndex* best = nullptr;
    double bestCost = INT_MAX;
    uint16_t numIx = festate->m_table->GetNumIndexes();
    uint16_t bestI = (uint16_t)-1;

    for (uint16_t i = 0; i < numIx; i++) {
        if (marr->m_idx[i] != nullptr && marr->m_idx[i]->IsUsable()) {
            double cost = marr->m_idx[i]->GetCost(numClauses);
            if (cost < bestCost) {
                if (bestI < MAX_NUM_INDEXES) {
                    if (marr->m_idx[i]->GetNumMatchedCols() < marr->m_idx[bestI]->GetNumMatchedCols())
                        continue;
                }
                bestCost = cost;
                bestI = i;
            }
        }
    }

    if (bestI < MAX_NUM_INDEXES) {
        best = marr->m_idx[bestI];
        for (int k = 0; k < 2; k++) {
            for (int j = 0; j < best->m_ix->GetNumFields(); j++) {
                if (best->m_colMatch[k][j]) {
                    if (best->m_opers[k][j] < KEY_OPER::READ_INVALID) {
                        best->m_params[k][j] = AddParam(&best->m_remoteConds, best->m_colMatch[k][j]);
                        if (!list_member(best->m_remoteCondsOrig, best->m_parentColMatch[k][j])) {
                            best->m_remoteCondsOrig = lappend(best->m_remoteCondsOrig, best->m_parentColMatch[k][j]);
                        }

                        if (j > 0 && best->m_opers[k][j - 1] != KEY_OPER::READ_KEY_EXACT &&
                            !list_member(festate->m_localConds, best->m_parentColMatch[k][j])) {
                            if (setLocal)
                                festate->m_localConds = lappend(festate->m_localConds, best->m_parentColMatch[k][j]);
                        }
                    } else if (!list_member(festate->m_localConds, best->m_parentColMatch[k][j]) &&
                               !list_member(best->m_remoteCondsOrig, best->m_parentColMatch[k][j])) {
                        if (setLocal)
                            festate->m_localConds = lappend(festate->m_localConds, best->m_parentColMatch[k][j]);
                        best->m_colMatch[k][j] = nullptr;
                        best->m_parentColMatch[k][j] = nullptr;
                    }
                }
            }
        }
    }

    for (uint16_t i = 0; i < numIx; i++) {
        if (marr->m_idx[i] != nullptr) {
            MatchIndex* mix = marr->m_idx[i];
            if (i != bestI) {
                if (setLocal) {
                    for (int k = 0; k < 2; k++) {
                        for (int j = 0; j < mix->m_ix->GetNumFields(); j++) {
                            if (mix->m_colMatch[k][j] &&
                                !list_member(festate->m_localConds, mix->m_parentColMatch[k][j]) &&
                                !(best != nullptr &&
                                    list_member(best->m_remoteCondsOrig, mix->m_parentColMatch[k][j]))) {
                                festate->m_localConds = lappend(festate->m_localConds, mix->m_parentColMatch[k][j]);
                            }
                        }
                    }
                }
                pfree(mix);
                marr->m_idx[i] = nullptr;
            }
        }
    }
    if (best != nullptr && best->m_ix != nullptr) {
        for (uint16_t i = 0; i < numIx; i++) {
            if (best->m_ix == festate->m_table->GetIndex(i)) {
                best->m_ixPosition = i;
                break;
            }
        }
    }

    return best;
}

void MOTAdaptor::OpenCursor(Relation rel, MOTFdwStateSt* festate)
{
    bool matchKey = true;
    bool forwardDirection = true;
    bool found = false;

    EnsureSafeThreadAccessInline();

    // GetTableByExternalId cannot return nullptr at this stage, because it is protected by envelope's table lock.
    festate->m_table = festate->m_currTxn->GetTableByExternalId(rel->rd_id);

    do {
        // this scan all keys case
        // we need to open both cursors on start and end to prevent
        // infinite scan in case "insert into table A ... as select * from table A ...
        if (festate->m_bestIx == nullptr) {
            int fIx, bIx;
            uint8_t* buf = nullptr;
            // assumption that primary index cannot be changed, can take it from
            // table and not look on ddl_access
            MOT::Index* ix = festate->m_table->GetPrimaryIndex();
            uint16_t keyLength = ix->GetKeyLength();

            if (festate->m_order == SORTDIR_ENUM::SORTDIR_ASC) {
                fIx = 0;
                bIx = 1;
                festate->m_forwardDirectionScan = true;
            } else {
                fIx = 1;
                bIx = 0;
                festate->m_forwardDirectionScan = false;
            }

            festate->m_cursor[fIx] = festate->m_table->Begin(festate->m_currTxn->GetThdId());

            festate->m_stateKey[bIx].InitKey(keyLength);
            buf = festate->m_stateKey[bIx].GetKeyBuf();
            errno_t erc = memset_s(buf, keyLength, 0xff, keyLength);
            securec_check(erc, "\0", "\0");
            festate->m_cursor[bIx] =
                ix->Search(&festate->m_stateKey[bIx], false, false, festate->m_currTxn->GetThdId(), found);
            break;
        }

        for (int i = 0; i < 2; i++) {
            if (i == 1 && festate->m_bestIx->m_end < 0) {
                if (festate->m_forwardDirectionScan) {
                    uint8_t* buf = nullptr;
                    MOT::Index* ix = festate->m_bestIx->m_ix;
                    uint16_t keyLength = ix->GetKeyLength();

                    festate->m_stateKey[1].InitKey(keyLength);
                    buf = festate->m_stateKey[1].GetKeyBuf();
                    errno_t erc = memset_s(buf, keyLength, 0xff, keyLength);
                    securec_check(erc, "\0", "\0");
                    festate->m_cursor[1] =
                        ix->Search(&festate->m_stateKey[1], false, false, festate->m_currTxn->GetThdId(), found);
                } else {
                    festate->m_cursor[1] = festate->m_bestIx->m_ix->Begin(festate->m_currTxn->GetThdId());
                }
                break;
            }

            KEY_OPER oper = (i == 0 ? festate->m_bestIx->m_ixOpers[0] : festate->m_bestIx->m_ixOpers[1]);

            forwardDirection = ((oper & ~KEY_OPER_PREFIX_BITMASK) < KEY_OPER::READ_KEY_OR_PREV);

            CreateKeyBuffer(rel, festate, i);

            if (i == 0) {
                festate->m_forwardDirectionScan = forwardDirection;
            }

            switch (oper) {
                case KEY_OPER::READ_KEY_EXACT:
                case KEY_OPER::READ_KEY_OR_NEXT:
                case KEY_OPER::READ_KEY_LIKE:
                case KEY_OPER::READ_PREFIX_LIKE:
                case KEY_OPER::READ_PREFIX:
                case KEY_OPER::READ_PREFIX_OR_NEXT:
                    matchKey = true;
                    forwardDirection = true;
                    break;

                case KEY_OPER::READ_KEY_AFTER:
                case KEY_OPER::READ_PREFIX_AFTER:
                    matchKey = false;
                    forwardDirection = true;
                    break;

                case KEY_OPER::READ_KEY_OR_PREV:
                case KEY_OPER::READ_PREFIX_OR_PREV:
                    matchKey = true;
                    forwardDirection = false;
                    break;

                case KEY_OPER::READ_KEY_BEFORE:
                case KEY_OPER::READ_PREFIX_BEFORE:
                    matchKey = false;
                    forwardDirection = false;
                    break;

                default:
                    elog(INFO, "Invalid key operation: %u", oper);
                    break;
            }

            festate->m_cursor[i] = festate->m_bestIx->m_ix->Search(
                &festate->m_stateKey[i], matchKey, forwardDirection, festate->m_currTxn->GetThdId(), found);

            if (!found && oper == KEY_OPER::READ_KEY_EXACT && festate->m_bestIx->m_ix->GetUnique()) {
                festate->m_cursor[i]->Invalidate();
                festate->m_cursor[i]->Destroy();
                delete festate->m_cursor[i];
                festate->m_cursor[i] = nullptr;
            }
        }
    } while (0);
}

static void VarLenFieldType(
    Form_pg_type typeDesc, Oid typoid, int32_t colLen, int16* typeLen, bool& isBlob, MOT::RC& res)
{
    isBlob = false;
    res = MOT::RC_OK;
    if (typeDesc->typlen < 0) {
        *typeLen = colLen;
        switch (typeDesc->typstorage) {
            case 'p':
                break;
            case 'x':
            case 'm':
                if (typoid == NUMERICOID) {
                    *typeLen = DECIMAL_MAX_SIZE;
                    break;
                }
                /* fall through */
            case 'e':
#ifdef USE_ASSERT_CHECKING
                if (typoid == TEXTOID)
                    *typeLen = colLen = MAX_VARCHAR_LEN;
#endif
                if (colLen > MAX_VARCHAR_LEN || colLen < 0) {
                    res = MOT::RC_COL_SIZE_INVALID;
                } else {
                    isBlob = true;
                }
                break;
            default:
                break;
        }
    }
}

static MOT::RC TableFieldType(
    const ColumnDef* colDef, MOT::MOT_CATALOG_FIELD_TYPES& type, int16* typeLen, Oid& typoid, bool& isBlob)
{
    MOT::RC res = MOT::RC_OK;
    Type tup;
    Form_pg_type typeDesc;
    int32_t colLen;

    if (colDef->typname->arrayBounds != nullptr) {
        return MOT::RC_UNSUPPORTED_COL_TYPE_ARR;
    }

    tup = typenameType(nullptr, colDef->typname, &colLen);
    typeDesc = ((Form_pg_type)GETSTRUCT(tup));
    typoid = HeapTupleGetOid(tup);
    *typeLen = typeDesc->typlen;

    // Get variable-length field length.
    VarLenFieldType(typeDesc, typoid, colLen, typeLen, isBlob, res);

    switch (typoid) {
        case CHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_CHAR;
            break;
        case INT1OID:
        case BOOLOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TINY;
            break;
        case INT2OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_SHORT;
            break;
        case INT4OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_INT;
            break;
        case INT8OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG;
            break;
        case DATEOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DATE;
            break;
        case TIMEOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIME;
            break;
        case TIMESTAMPOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMESTAMP;
            break;
        case TIMESTAMPTZOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMESTAMPTZ;
            break;
        case INTERVALOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_INTERVAL;
            break;
        case TINTERVALOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TINTERVAL;
            break;
        case TIMETZOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMETZ;
            break;
        case FLOAT4OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_FLOAT;
            break;
        case FLOAT8OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DOUBLE;
            break;
        case NUMERICOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL;
            break;
        case VARCHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case BPCHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case TEXTOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case CLOBOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB;
            break;
        case BYTEAOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        default:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_UNKNOWN;
            res = MOT::RC_UNSUPPORTED_COL_TYPE;
    }

    if (tup) {
        ReleaseSysCache(tup);
    }

    return res;
}

void MOTAdaptor::ValidateCreateIndex(IndexStmt* stmt, MOT::Table* table, MOT::TxnManager* txn)
{
    if (stmt->primary) {
        if (!table->IsTableEmpty(txn->GetThdId())) {
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_ERROR),
                    errmsg(
                        "Table %s is not empty, create primary index is not allowed", table->GetTableName().c_str())));
            return;
        }
    } else if (table->GetNumIndexes() == MAX_NUM_INDEXES) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(ERRCODE_FDW_TOO_MANY_INDEXES),
                errmsg("Can not create index, max number of indexes %u reached", MAX_NUM_INDEXES)));
        return;
    }

    if (strcmp(stmt->accessMethod, "btree") != 0) {
        ereport(ERROR, (errmodule(MOD_MOT), errmsg("MOT supports indexes of type BTREE only (btree or btree_art)")));
        return;
    }

    if (list_length(stmt->indexParams) > (int)MAX_KEY_COLUMNS) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(ERRCODE_FDW_TOO_MANY_INDEX_COLUMNS),
                errmsg("Can't create index"),
                errdetail(
                    "Number of columns exceeds %d max allowed %u", list_length(stmt->indexParams), MAX_KEY_COLUMNS)));
        return;
    }
}

MOT::RC MOTAdaptor::CreateIndex(IndexStmt* stmt, ::TransactionId tid)
{
    MOT::RC res;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);
    MOT::Table* table = txn->GetTableByExternalId(stmt->relation->foreignOid);

    if (table == nullptr) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("Table not found for oid %u", stmt->relation->foreignOid)));
        return MOT::RC_ERROR;
    }

    ValidateCreateIndex(stmt, table, txn);

    elog(LOG,
        "creating %s index %s (OID: %u), for table: %s",
        (stmt->primary ? "PRIMARY" : "SECONDARY"),
        stmt->idxname,
        stmt->indexOid,
        stmt->relation->relname);
    uint64_t keyLength = 0;
    MOT::Index* index = nullptr;
    MOT::IndexOrder index_order = MOT::IndexOrder::INDEX_ORDER_SECONDARY;

    // Use the default index tree flavor from configuration file
    MOT::IndexingMethod indexing_method = MOT::IndexingMethod::INDEXING_METHOD_TREE;
    MOT::IndexTreeFlavor flavor = MOT::GetGlobalConfiguration().m_indexTreeFlavor;

    // check if we have primary and delete previous definition
    if (stmt->primary) {
        index_order = MOT::IndexOrder::INDEX_ORDER_PRIMARY;
    }

    index = MOT::IndexFactory::CreateIndex(index_order, indexing_method, flavor);
    if (index == nullptr) {
        report_pg_error(MOT::RC_ABORT);
        return MOT::RC_ABORT;
    }
    index->SetExtId(stmt->indexOid);
    index->SetNumTableFields((uint32_t)table->GetFieldCount());
    int count = 0;

    ListCell* lc = nullptr;
    foreach (lc, stmt->indexParams) {
        IndexElem* ielem = (IndexElem*)lfirst(lc);

        uint64_t colid = table->GetFieldId((ielem->name != nullptr ? ielem->name : ielem->indexcolname));
        if (colid == (uint64_t)-1) {  // invalid column
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Can't create index on field"),
                    errdetail("Specified column not found in table definition")));
            return MOT::RC_ERROR;
        }

        MOT::Column* col = table->GetField(colid);

        // Temp solution for NULLs, do not allow index creation on column that does not carry not null flag
        if (!MOT::GetGlobalConfiguration().m_allowIndexOnNullableColumn && !col->m_isNotNull) {
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_INDEX_ON_NULLABLE_COLUMN_NOT_ALLOWED),
                    errmsg("Can't create index on nullable columns"),
                    errdetail("Column %s is nullable", col->m_name)));
            return MOT::RC_ERROR;
        }

        // Temp solution, we have to support DECIMAL and NUMERIC indexes as well
        if (col->m_type == MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL) {
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Can't create index on field"),
                    errdetail("INDEX on NUMERIC or DECIMAL fields not supported yet")));
            return MOT::RC_ERROR;
        }
        if (col->m_keySize > MAX_KEY_SIZE) {
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Can't create index on field"),
                    errdetail("Column size is greater than maximum index size")));
            return MOT::RC_ERROR;
        }
        keyLength += col->m_keySize;

        index->SetLenghtKeyFields(count, colid, col->m_keySize);
        count++;
    }

    index->SetNumIndexFields(count);

    if ((res = index->IndexInit(keyLength, stmt->unique, stmt->idxname, nullptr)) != MOT::RC_OK) {
        delete index;
        report_pg_error(res);
        return res;
    }

    res = txn->CreateIndex(table, index, stmt->primary);
    if (res != MOT::RC_OK) {
        delete index;
        if (res == MOT::RC_TABLE_EXCEEDS_MAX_INDEXES) {
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_TOO_MANY_INDEXES),
                    errmsg("Can not create index, max number of indexes %u reached", MAX_NUM_INDEXES)));
            return MOT::RC_TABLE_EXCEEDS_MAX_INDEXES;
        } else {
            report_pg_error(txn->m_err, stmt->idxname, txn->m_errMsgBuf);
            return MOT::RC_UNIQUE_VIOLATION;
        }
    }

    return MOT::RC_OK;
}

void MOTAdaptor::AddTableColumns(MOT::Table* table, List *tableElts, bool& hasBlob)
{
    hasBlob = false;
    ListCell* cell = nullptr;
    foreach (cell, tableElts) {
        int16 typeLen = 0;
        bool isBlob = false;
        MOT::MOT_CATALOG_FIELD_TYPES colType;
        ColumnDef* colDef = (ColumnDef*)lfirst(cell);

        if (colDef == nullptr || colDef->typname == nullptr) {
            delete table;
            table = nullptr;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Column definition is not complete"),
                    errdetail("target table is a foreign table")));
            break;
        }

        Oid typoid = InvalidOid;
        MOT::RC res = TableFieldType(colDef, colType, &typeLen, typoid, isBlob);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(res, colDef, (void*)(int64)typeLen);
            break;
        }
        hasBlob |= isBlob;

        if (colType == MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL) {
            if (list_length(colDef->typname->typmods) > 0) {
                bool canMakeShort = true;
                int precision = 0;
                int scale = 0;
                int count = 0;

                ListCell* c = nullptr;
                foreach (c, colDef->typname->typmods) {
                    Node* d = (Node*)lfirst(c);
                    if (!IsA(d, A_Const)) {
                        canMakeShort = false;
                        break;
                    }
                    A_Const* ac = (A_Const*)d;

                    if (ac->val.type != T_Integer) {
                        canMakeShort = false;
                        break;
                    }

                    if (count == 0) {
                        precision = ac->val.val.ival;
                    } else {
                        scale = ac->val.val.ival;
                    }

                    count++;
                }

                if (canMakeShort) {
                    int len = 0;

                    len += scale / DEC_DIGITS;
                    len += (scale % DEC_DIGITS > 0 ? 1 : 0);

                    precision -= scale;

                    len += precision / DEC_DIGITS;
                    len += (precision % DEC_DIGITS > 0 ? 1 : 0);

                    typeLen = sizeof(MOT::DecimalSt) + len * sizeof(NumericDigit);
                }
            }
        }

        res = table->AddColumn(colDef->colname, typeLen, colType, colDef->is_not_null, typoid);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(res, colDef, (void*)(int64)typeLen);
            break;
        }
    }
}

MOT::RC MOTAdaptor::CreateTable(CreateForeignTableStmt* stmt, ::TransactionId tid)
{
    bool hasBlob = false;
    MOT::Index* primaryIdx = nullptr;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__, tid);
    MOT::Table* table = nullptr;
    MOT::RC res = MOT::RC_ERROR;
    std::string tname("");
    char* dbname = NULL;

    do {
        table = new (std::nothrow) MOT::Table();
        if (table == nullptr) {
            ereport(ERROR,
                (errmodule(MOD_MOT), errcode(ERRCODE_OUT_OF_MEMORY), errmsg("Allocation of table metadata failed")));
            break;
        }

        uint32_t columnCount = list_length(stmt->base.tableElts);

        // once the columns have been counted, we add one more for the nullable columns
        ++columnCount;

        // prepare table name
        dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        if (dbname == nullptr) {
            delete table;
            table = nullptr;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database with OID %u does not exist", u_sess->proc_cxt.MyDatabaseId)));
            break;
        }
        tname.append(dbname);
        tname.append("_");
        if (stmt->base.relation->schemaname != nullptr) {
            tname.append(stmt->base.relation->schemaname);
        } else {
            tname.append("#");
        }

        tname.append("_");
        tname.append(stmt->base.relation->relname);

        if (!table->Init(
                stmt->base.relation->relname, tname.c_str(), columnCount, stmt->base.relation->foreignOid)) {
            delete table;
            table = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);
            break;
        }

        // the null fields are copied verbatim because we have to give them back at some point
        res = table->AddColumn(
            "null_bytes", BITMAPLEN(columnCount - 1), MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_NULLBYTES);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);
            break;
        }

        /*
         * Add all the columns.
         * NOTE: On failure, table object will be deleted and ereport will be done in AddTableColumns.
         */
        AddTableColumns(table, stmt->base.tableElts, hasBlob);

        table->SetFixedLengthRow(!hasBlob);

        uint32_t tupleSize = table->GetTupleSize();
        if (tupleSize > (unsigned int)MAX_TUPLE_SIZE) {
            delete table;
            table = nullptr;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("MOT: Table %s tuple size %u exceeds MAX_TUPLE_SIZE=%u !!!",
                        stmt->base.relation->relname,
                        tupleSize,
                        (unsigned int)MAX_TUPLE_SIZE)));
        }

        if (!table->InitRowPool()) {
            delete table;
            table = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);
            break;
        }

        elog(LOG,
            "creating table %s (OID: %u), num columns: %u, tuple: %u",
            table->GetLongTableName().c_str(),
            stmt->base.relation->foreignOid,
            columnCount,
            tupleSize);

        res = txn->CreateTable(table);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(res);
            break;
        }

        // add default PK index
        primaryIdx = MOT::IndexFactory::CreatePrimaryIndexEx(MOT::IndexingMethod::INDEXING_METHOD_TREE,
            DEFAULT_TREE_FLAVOR,
            8,
            table->GetLongTableName(),
            res,
            nullptr);
        if (res != MOT::RC_OK) {
            txn->DropTable(table);
            report_pg_error(res);
            break;
        }
        primaryIdx->SetExtId(stmt->base.relation->foreignOid + 1);
        primaryIdx->SetNumTableFields(columnCount);
        primaryIdx->SetNumIndexFields(1);
        primaryIdx->SetLenghtKeyFields(0, -1, 8);
        primaryIdx->SetFakePrimary(true);

        // Add default primary index
        res = txn->CreateIndex(table, primaryIdx, true);
    } while (0);

    if (res != MOT::RC_OK) {
        if (table != nullptr) {
            txn->DropTable(table);
        }
        if (primaryIdx != nullptr) {
            delete primaryIdx;
        }
    }

    return res;
}

MOT::RC MOTAdaptor::DropIndex(DropForeignStmt* stmt, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "dropping index %s, ixoid: %u, taboid: %u", stmt->name, stmt->indexoid, stmt->reloid);

    // get table
    do {
        MOT::Index* index = txn->GetIndexByExternalId(stmt->reloid, stmt->indexoid);
        if (index == nullptr) {
            elog(LOG,
                "Drop index %s error, index oid %u of table oid %u not found.",
                stmt->name,
                stmt->indexoid,
                stmt->reloid);
            res = MOT::RC_INDEX_NOT_FOUND;
        } else if (index->IsPrimaryKey()) {
            elog(LOG, "Drop primary index is not supported, failed to drop index: %s", stmt->name);
        } else {
            MOT::Table* table = index->GetTable();
            uint64_t table_relid = table->GetTableExId();
            JitExec::PurgeJitSourceCache(table_relid, false);
            table->WrLock();
            res = txn->DropIndex(index);
            table->Unlock();
        }
    } while (0);

    return res;
}

MOT::RC MOTAdaptor::DropTable(DropForeignStmt* stmt, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "dropping table %s, oid: %u", stmt->name, stmt->reloid);
    do {
        tab = txn->GetTableByExternalId(stmt->reloid);
        if (tab == nullptr) {
            res = MOT::RC_TABLE_NOT_FOUND;
            elog(LOG, "Drop table %s error, table oid %u not found.", stmt->name, stmt->reloid);
        } else {
            uint64_t table_relid = tab->GetTableExId();
            JitExec::PurgeJitSourceCache(table_relid, false);
            res = txn->DropTable(tab);
        }
    } while (0);

    return res;
}

MOT::RC MOTAdaptor::TruncateTable(Relation rel, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;

    EnsureSafeThreadAccessInline();

    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "truncating table %s, oid: %u", NameStr(rel->rd_rel->relname), rel->rd_id);
    do {
        tab = txn->GetTableByExternalId(rel->rd_id);
        if (tab == nullptr) {
            elog(LOG, "Truncate table %s error, table oid %u not found.", NameStr(rel->rd_rel->relname), rel->rd_id);
            break;
        }

        JitExec::PurgeJitSourceCache(rel->rd_id, true);
        tab->WrLock();
        res = txn->TruncateTable(tab);
        tab->Unlock();
    } while (0);

    return res;
}

MOT::RC MOTAdaptor::VacuumTable(Relation rel, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "vacuuming table %s, oid: %u", NameStr(rel->rd_rel->relname), rel->rd_id);
    do {
        tab = MOT::GetTableManager()->GetTableSafeByExId(rel->rd_id);
        if (tab == nullptr) {
            elog(LOG, "Vacuum table %s error, table oid %u not found.", NameStr(rel->rd_rel->relname), rel->rd_id);
            break;
        }

        tab->Compact(txn);
        tab->Unlock();
    } while (0);
    return res;
}

uint64_t MOTAdaptor::GetTableIndexSize(uint64_t tabId, uint64_t ixId)
{
    uint64_t res = 0;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    MOT::Table* tab = nullptr;
    MOT::Index* ix = nullptr;

    do {
        tab = txn->GetTableByExternalId(tabId);
        if (tab == nullptr) {
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
                    errmsg("Get table size error, table oid %lu not found.", tabId)));
            break;
        }

        if (ixId > 0) {
            ix = tab->GetIndexByExtId(ixId);
            if (ix == nullptr) {
                ereport(ERROR,
                    (errmodule(MOD_MOT),
                        errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
                        errmsg("Get index size error, index oid %lu for table oid %lu not found.", ixId, tabId)));
                break;
            }
            res = ix->GetIndexSize();
        } else
            res = tab->GetTableSize();
    } while (0);

    return res;
}

MotMemoryDetail* MOTAdaptor::GetMemSize(uint32_t* nodeCount, bool isGlobal)
{
    EnsureSafeThreadAccessInline();
    MotMemoryDetail* result = nullptr;
    *nodeCount = 0;

    /* We allocate an array of size (m_nodeCount + 1) to accommodate one aggregated entry of all global pools. */
    uint32_t statsArraySize = MOT::g_memGlobalCfg.m_nodeCount + 1;
    MOT::MemRawChunkPoolStats* chunkPoolStatsArray =
        (MOT::MemRawChunkPoolStats*)palloc(statsArraySize * sizeof(MOT::MemRawChunkPoolStats));
    if (chunkPoolStatsArray != nullptr) {
        errno_t erc = memset_s(chunkPoolStatsArray,
            statsArraySize * sizeof(MOT::MemRawChunkPoolStats),
            0,
            statsArraySize * sizeof(MOT::MemRawChunkPoolStats));
        securec_check(erc, "\0", "\0");

        uint32_t realStatsEntries;
        if (isGlobal) {
            realStatsEntries = MOT::MemRawChunkStoreGetGlobalStats(chunkPoolStatsArray, statsArraySize);
        } else {
            realStatsEntries = MOT::MemRawChunkStoreGetLocalStats(chunkPoolStatsArray, statsArraySize);
        }

        MOT_ASSERT(realStatsEntries <= statsArraySize);
        if (realStatsEntries > 0) {
            result = (MotMemoryDetail*)palloc(realStatsEntries * sizeof(MotMemoryDetail));
            if (result != nullptr) {
                for (uint32_t node = 0; node < realStatsEntries; ++node) {
                    result[node].numaNode = chunkPoolStatsArray[node].m_node;
                    result[node].reservedMemory = chunkPoolStatsArray[node].m_reservedBytes;
                    result[node].usedMemory = chunkPoolStatsArray[node].m_usedBytes;
                }
                *nodeCount = realStatsEntries;
            }
        }
        pfree(chunkPoolStatsArray);
    }

    return result;
}

MotSessionMemoryDetail* MOTAdaptor::GetSessionMemSize(uint32_t* sessionCount)
{
    EnsureSafeThreadAccessInline();
    MotSessionMemoryDetail* result = nullptr;
    *sessionCount = 0;

    uint32_t session_count = MOT::g_memGlobalCfg.m_maxThreadCount;
    MOT::MemSessionAllocatorStats* session_stats_array =
        (MOT::MemSessionAllocatorStats*)palloc(session_count * sizeof(MOT::MemSessionAllocatorStats));
    if (session_stats_array != nullptr) {
        uint32_t real_session_count = MOT::MemSessionGetAllStats(session_stats_array, session_count);
        if (real_session_count > 0) {
            result = (MotSessionMemoryDetail*)palloc(real_session_count * sizeof(MotSessionMemoryDetail));
            if (result != nullptr) {
                for (uint32_t session_index = 0; session_index < real_session_count; ++session_index) {
                    GetSessionDetails(session_stats_array[session_index].m_sessionId,
                        &result[session_index].threadid,
                        &result[session_index].threadStartTime);
                    result[session_index].totalSize = session_stats_array[session_index].m_reservedSize;
                    result[session_index].usedSize = session_stats_array[session_index].m_usedSize;
                    result[session_index].freeSize = result[session_index].totalSize - result[session_index].usedSize;
                }
                *sessionCount = real_session_count;
            }
        }
        pfree(session_stats_array);
    }

    return result;
}

void MOTAdaptor::CreateKeyBuffer(Relation rel, MOTFdwStateSt* festate, int start)
{
    uint8_t* buf = nullptr;
    uint8_t pattern = 0x00;
    EnsureSafeThreadAccessInline();
    int16_t num = festate->m_bestIx->m_ix->GetNumFields();
    const uint16_t* fieldLengths = festate->m_bestIx->m_ix->GetLengthKeyFields();
    const int16_t* orgCols = festate->m_bestIx->m_ix->GetColumnKeyFields();
    TupleDesc desc = rel->rd_att;
    uint16_t offset = 0;
    int32_t* exprs = nullptr;
    KEY_OPER* opers = nullptr;
    uint16_t keyLength;
    KEY_OPER oper;

    if (start == 0) {
        exprs = festate->m_bestIx->m_params[festate->m_bestIx->m_start];
        opers = festate->m_bestIx->m_opers[festate->m_bestIx->m_start];
        oper = festate->m_bestIx->m_ixOpers[0];
    } else {
        exprs = festate->m_bestIx->m_params[festate->m_bestIx->m_end];
        opers = festate->m_bestIx->m_opers[festate->m_bestIx->m_end];
        // end may be equal start but the operation maybe different, look at getCost
        oper = festate->m_bestIx->m_ixOpers[1];
    }

    keyLength = festate->m_bestIx->m_ix->GetKeyLength();
    festate->m_stateKey[start].InitKey(keyLength);
    buf = festate->m_stateKey[start].GetKeyBuf();

    switch (oper) {
        case KEY_OPER::READ_KEY_EXACT:
        case KEY_OPER::READ_KEY_OR_NEXT:
        case KEY_OPER::READ_KEY_BEFORE:
        case KEY_OPER::READ_KEY_LIKE:
        case KEY_OPER::READ_PREFIX:
        case KEY_OPER::READ_PREFIX_LIKE:
        case KEY_OPER::READ_PREFIX_OR_NEXT:
        case KEY_OPER::READ_PREFIX_BEFORE:
            pattern = 0x00;
            break;

        case KEY_OPER::READ_KEY_OR_PREV:
        case KEY_OPER::READ_PREFIX_AFTER:
        case KEY_OPER::READ_PREFIX_OR_PREV:
        case KEY_OPER::READ_KEY_AFTER:
            pattern = 0xff;
            break;

        default:
            elog(LOG, "Invalid key operation: %u", oper);
            break;
    }

    for (int i = 0; i < num; i++) {
        if (opers[i] < KEY_OPER::READ_INVALID) {
            bool is_null = false;
            ExprState* expr = (ExprState*)list_nth(festate->m_execExprs, exprs[i] - 1);
            Datum val = ExecEvalExpr((ExprState*)(expr), festate->m_econtext, &is_null, nullptr);
            if (is_null) {
                MOT_ASSERT((offset + fieldLengths[i]) <= keyLength);
                errno_t erc = memset_s(buf + offset, fieldLengths[i], 0x00, fieldLengths[i]);
                securec_check(erc, "\0", "\0");
            } else {
                MOT::Column* col = festate->m_table->GetField(orgCols[i]);
                uint8_t fill = 0x00;

                // in case of like fill rest of the key with appropriate to direction values
                if (opers[i] == KEY_OPER::READ_KEY_LIKE) {
                    switch (oper) {
                        case KEY_OPER::READ_KEY_LIKE:
                        case KEY_OPER::READ_KEY_OR_NEXT:
                        case KEY_OPER::READ_KEY_AFTER:
                        case KEY_OPER::READ_PREFIX:
                        case KEY_OPER::READ_PREFIX_LIKE:
                        case KEY_OPER::READ_PREFIX_OR_NEXT:
                        case KEY_OPER::READ_PREFIX_AFTER:
                            break;

                        case KEY_OPER::READ_PREFIX_BEFORE:
                        case KEY_OPER::READ_PREFIX_OR_PREV:
                        case KEY_OPER::READ_KEY_BEFORE:
                        case KEY_OPER::READ_KEY_OR_PREV:
                            fill = 0xff;
                            break;

                        case KEY_OPER::READ_KEY_EXACT:
                        default:
                            elog(LOG, "Invalid key operation: %u", oper);
                            break;
                    }
                }

                DatumToMOTKey(col,
                    expr->resultType,
                    val,
                    desc->attrs[orgCols[i] - 1]->atttypid,
                    buf + offset,
                    fieldLengths[i],
                    opers[i],
                    fill);
            }
        } else {
            MOT_ASSERT((offset + fieldLengths[i]) <= keyLength);
            festate->m_stateKey[start].FillPattern(pattern, fieldLengths[i], offset);
        }

        offset += fieldLengths[i];
    }

    festate->m_bestIx->m_ix->AdjustKey(&festate->m_stateKey[start], pattern);
}

bool MOTAdaptor::IsScanEnd(MOTFdwStateSt* festate)
{
    bool res = false;
    EnsureSafeThreadAccessInline();

    // festate->cursor[1] (end iterator) might be NULL (in case it is not in use). If this is the case, return false
    // (which means we have not reached the end yet)
    if (festate->m_cursor[1] == nullptr) {
        return false;
    }

    if (!festate->m_cursor[1]->IsValid()) {
        return true;
    } else {
        const MOT::Key* startKey = nullptr;
        const MOT::Key* endKey = nullptr;
        MOT::Index* ix = (festate->m_bestIx != nullptr ? festate->m_bestIx->m_ix : festate->m_table->GetPrimaryIndex());

        startKey = reinterpret_cast<const MOT::Key*>(festate->m_cursor[0]->GetKey());
        endKey = reinterpret_cast<const MOT::Key*>(festate->m_cursor[1]->GetKey());
        if (startKey != nullptr && endKey != nullptr) {
            int cmpRes = memcmp(startKey->GetKeyBuf(), endKey->GetKeyBuf(), ix->GetKeySizeNoSuffix());

            if (festate->m_forwardDirectionScan) {
                if (cmpRes > 0)
                    res = true;
            } else {
                if (cmpRes < 0)
                    res = true;
            }
        }
    }

    return res;
}

void MOTAdaptor::PackRow(TupleTableSlot* slot, MOT::Table* table, uint8_t* attrs_used, uint8_t* destRow)
{
    errno_t erc;
    EnsureSafeThreadAccessInline();
    HeapTuple srcData = (HeapTuple)slot->tts_tuple;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    bool hasnulls = HeapTupleHasNulls(srcData);
    uint64_t i = 0;
    uint64_t j = 1;
    uint64_t cols = table->GetFieldCount() - 1;  // column count includes null bits field

    // the null bytes are necessary and have to give them back
    if (!hasnulls) {
        erc = memset_s(destRow + table->GetFieldOffset(i), table->GetFieldSize(i), 0xff, table->GetFieldSize(i));
        securec_check(erc, "\0", "\0");
    } else {
        erc = memcpy_s(destRow + table->GetFieldOffset(i),
            table->GetFieldSize(i),
            &srcData->t_data->t_bits[0],
            table->GetFieldSize(i));
        securec_check(erc, "\0", "\0");
    }

    // we now copy the fields, for the time being the null ones will be copied as well
    for (; i < cols; i++, j++) {
        bool isnull = false;
        Datum value = heap_slot_getattr(slot, j, &isnull);

        if (!isnull) {
            DatumToMOT(table->GetField(j), value, tupdesc->attrs[i]->atttypid, destRow);
        }
    }
}

void MOTAdaptor::PackUpdateRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* destRow)
{
    EnsureSafeThreadAccessInline();
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    uint8_t* bits;
    uint64_t i = 0;
    uint64_t j = 1;

    // column count includes null bits field
    uint64_t cols = table->GetFieldCount() - 1;
    bits = destRow + table->GetFieldOffset(i);

    for (; i < cols; i++, j++) {
        if (BITMAP_GET(attrs_used, i)) {
            bool isnull = false;
            Datum value = heap_slot_getattr(slot, j, &isnull);

            if (!isnull) {
                DatumToMOT(table->GetField(j), value, tupdesc->attrs[i]->atttypid, destRow);
                BITMAP_SET(bits, i);
            } else {
                BITMAP_CLEAR(bits, i);
            }
        }
    }
}

void MOTAdaptor::UnpackRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* srcRow)
{
    EnsureSafeThreadAccessInline();
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    uint64_t i = 0;

    // column count includes null bits field
    uint64_t cols = table->GetFieldCount() - 1;

    for (; i < cols; i++) {
        if (BITMAP_GET(attrs_used, i))
            MOTToDatum(table, tupdesc->attrs[i], srcRow, &(slot->tts_values[i]), &(slot->tts_isnull[i]));
        else {
            slot->tts_isnull[i] = true;
            slot->tts_values[i] = PointerGetDatum(nullptr);
        }
    }
}

// useful functions for data conversion: utils/fmgr/gmgr.cpp
void MOTAdaptor::MOTToDatum(MOT::Table* table, const Form_pg_attribute attr, uint8_t* data, Datum* value, bool* is_null)
{
    EnsureSafeThreadAccessInline();
    if (!BITMAP_GET(data, (attr->attnum - 1))) {
        *is_null = true;
        *value = PointerGetDatum(nullptr);

        return;
    }

    size_t len = 0;
    MOT::Column* col = table->GetField(attr->attnum);

    *is_null = false;
    switch (attr->atttypid) {
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
        case CLOBOID:
        case BYTEAOID: {
            uintptr_t tmp;
            col->Unpack(data, &tmp, len);

            bytea* result = (bytea*)palloc(len + VARHDRSZ);
            errno_t erc = memcpy_s(VARDATA(result), len, (uint8_t*)tmp, len);
            securec_check(erc, "\0", "\0");
            SET_VARSIZE(result, len + VARHDRSZ);

            *value = PointerGetDatum(result);
            break;
        }
        case NUMERICOID: {
            MOT::DecimalSt* d;
            col->Unpack(data, (uintptr_t*)&d, len);

            *value = NumericGetDatum(MOTNumericToPG(d));
            break;
        }
        default:
            col->Unpack(data, value, len);
            break;
    }
}

void MOTAdaptor::DatumToMOT(MOT::Column* col, Datum datum, Oid type, uint8_t* data)
{
    EnsureSafeThreadAccessInline();
    switch (type) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID: {
            bytea* txt = DatumGetByteaP(datum);
            size_t size = VARSIZE(txt);  // includes header len VARHDRSZ
            char* src = VARDATA(txt);
            col->Pack(data, (uintptr_t)src, size - VARHDRSZ);

            if ((char*)datum != (char*)txt) {
                pfree(txt);
            }

            break;
        }
        case NUMERICOID: {
            Numeric n = DatumGetNumeric(datum);
            char buf[DECIMAL_MAX_SIZE];
            MOT::DecimalSt* d = (MOT::DecimalSt*)buf;

            if (NUMERIC_NDIGITS(n) > DECIMAL_MAX_DIGITS) {
                ereport(ERROR,
                    (errmodule(MOD_MOT),
                        errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("Value exceeds maximum precision: %d", NUMERIC_MAX_PRECISION)));
                break;
            }
            PGNumericToMOT(n, *d);
            col->Pack(data, (uintptr_t)d, DECIMAL_SIZE(d));

            break;
        }
        default:
            col->Pack(data, datum, col->m_size);
            break;
    }
}

inline void MOTAdaptor::VarcharToMOTKey(
    MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len, KEY_OPER oper, uint8_t fill)
{
    bool noValue = false;
    switch (datumType) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID:
            break;
        default:
            noValue = true;
            errno_t erc = memset_s(data, len, 0x00, len);
            securec_check(erc, "\0", "\0");
            break;
    }

    if (noValue) {
        return;
    }

    bytea* txt = DatumGetByteaP(datum);
    size_t size = VARSIZE(txt);  // includes header len VARHDRSZ
    char* src = VARDATA(txt);

    if (size > len) {
        size = len;
    }

    size -= VARHDRSZ;
    if (oper == KEY_OPER::READ_KEY_LIKE) {
        if (src[size - 1] == '%') {
            size -= 1;
        } else {
            // switch to equal
            if (colType == BPCHAROID) {
                fill = 0x20;  // space ' ' == 0x20
            } else {
                fill = 0x00;
            }
        }
    } else if (colType == BPCHAROID) {  // handle padding for blank-padded type
        fill = 0x20;
    }
    col->PackKey(data, (uintptr_t)src, size, fill);

    if ((char*)datum != (char*)txt) {
        pfree(txt);
    }
}

inline void MOTAdaptor::FloatToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == FLOAT8OID) {
        MOT::DoubleConvT dc;
        MOT::FloatConvT fc;
        dc.m_r = (uint64_t)datum;
        fc.m_v = (float)dc.m_v;
        uint64_t u = (uint64_t)fc.m_r;
        col->PackKey(data, u, col->m_size);
    } else {
        col->PackKey(data, datum, col->m_size);
    }
}

inline void MOTAdaptor::NumericToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    Numeric n = DatumGetNumeric(datum);
    char buf[DECIMAL_MAX_SIZE];
    MOT::DecimalSt* d = (MOT::DecimalSt*)buf;
    PGNumericToMOT(n, *d);
    col->PackKey(data, (uintptr_t)d, DECIMAL_SIZE(d));
}

inline void MOTAdaptor::TimestampToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == TIMESTAMPTZOID) {
        Timestamp result = DatumGetTimestamp(DirectFunctionCall1(timestamptz_timestamp, datum));
        col->PackKey(data, result, col->m_size);
    } else if (datumType == DATEOID) {
        Timestamp result = DatumGetTimestamp(DirectFunctionCall1(date_timestamp, datum));
        col->PackKey(data, result, col->m_size);
    } else {
        col->PackKey(data, datum, col->m_size);
    }
}

inline void MOTAdaptor::TimestampTzToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == TIMESTAMPOID) {
        TimestampTz result = DatumGetTimestampTz(DirectFunctionCall1(timestamp_timestamptz, datum));
        col->PackKey(data, result, col->m_size);
    } else if (datumType == DATEOID) {
        TimestampTz result = DatumGetTimestampTz(DirectFunctionCall1(date_timestamptz, datum));
        col->PackKey(data, result, col->m_size);
    } else {
        col->PackKey(data, datum, col->m_size);
    }
}

inline void MOTAdaptor::DateToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == TIMESTAMPOID) {
        DateADT result = DatumGetDateADT(DirectFunctionCall1(timestamp_date, datum));
        col->PackKey(data, result, col->m_size);
    } else if (datumType == TIMESTAMPTZOID) {
        DateADT result = DatumGetDateADT(DirectFunctionCall1(timestamptz_date, datum));
        col->PackKey(data, result, col->m_size);
    } else {
        col->PackKey(data, datum, col->m_size);
    }
}

void MOTAdaptor::DatumToMOTKey(
    MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len, KEY_OPER oper, uint8_t fill)
{
    EnsureSafeThreadAccessInline();
    switch (colType) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID:
            VarcharToMOTKey(col, datumType, datum, colType, data, len, oper, fill);
            break;
        case FLOAT4OID:
            FloatToMOTKey(col, datumType, datum, data);
            break;
        case NUMERICOID:
            NumericToMOTKey(col, datumType, datum, data);
            break;
        case TIMESTAMPOID:
            TimestampToMOTKey(col, datumType, datum, data);
            break;
        case TIMESTAMPTZOID:
            TimestampTzToMOTKey(col, datumType, datum, data);
            break;
        case DATEOID:
            DateToMOTKey(col, datumType, datum, data);
            break;
        default:
            col->PackKey(data, datum, col->m_size);
            break;
    }
}

MOTFdwStateSt* InitializeFdwState(void* fdwState, List** fdwExpr, uint64_t exTableID)
{
    MOTFdwStateSt* state = (MOTFdwStateSt*)palloc0(sizeof(MOTFdwStateSt));
    List* values = (List*)fdwState;

    state->m_allocInScan = true;
    state->m_foreignTableId = exTableID;
    if (list_length(values) > 0) {
        ListCell* cell = list_head(values);
        int type = ((Const*)lfirst(cell))->constvalue;
        if (type != FDW_LIST_STATE) {
            return state;
        }
        cell = lnext(cell);
        state->m_cmdOper = (CmdType)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_order = (SORTDIR_ENUM)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_hasForUpdate = (bool)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_foreignTableId = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_numAttrs = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_ctidNum = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_numExpr = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);

        int len = BITMAP_GETLEN(state->m_numAttrs);
        state->m_attrsUsed = (uint8_t*)palloc0(len);
        state->m_attrsModified = (uint8_t*)palloc0(len);
        BitmapDeSerialize(state->m_attrsUsed, len, &cell);

        if (cell != NULL) {
            state->m_bestIx = &state->m_bestIxBuf;
            state->m_bestIx->Deserialize(cell, exTableID);
        }

        if (fdwExpr != NULL && *fdwExpr != NULL) {
            ListCell* c = NULL;
            int i = 0;

            // divide fdw expr to param list and original expr
            state->m_remoteCondsOrig = NULL;

            foreach (c, *fdwExpr) {
                if (i < state->m_numExpr) {
                    i++;
                    continue;
                } else {
                    state->m_remoteCondsOrig = lappend(state->m_remoteCondsOrig, lfirst(c));
                }
            }

            *fdwExpr = list_truncate(*fdwExpr, state->m_numExpr);
        }
    }
    return state;
}

void* SerializeFdwState(MOTFdwStateSt* state)
{
    List* result = NULL;

    // set list type to FDW_LIST_STATE
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, FDW_LIST_STATE, false, true));
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_cmdOper), false, true));
    result = lappend(result, makeConst(INT1OID, -1, InvalidOid, 4, Int8GetDatum(state->m_order), false, true));
    result = lappend(result, makeConst(BOOLOID, -1, InvalidOid, 1, BoolGetDatum(state->m_hasForUpdate), false, true));
    result =
        lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_foreignTableId), false, true));
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_numAttrs), false, true));
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_ctidNum), false, true));
    result = lappend(result, makeConst(INT2OID, -1, InvalidOid, 2, Int16GetDatum(state->m_numExpr), false, true));
    int len = BITMAP_GETLEN(state->m_numAttrs);
    result = BitmapSerialize(result, state->m_attrsUsed, len);

    if (state->m_bestIx != nullptr) {
        state->m_bestIx->Serialize(&result);
    }
    ReleaseFdwState(state);
    return result;
}

void ReleaseFdwState(MOTFdwStateSt* state)
{
    CleanCursors(state);

    if (state->m_currTxn) {
        state->m_currTxn->m_queryState.erase((uint64_t)state);
    }

    if (state->m_bestIx && state->m_bestIx != &state->m_bestIxBuf)
        pfree(state->m_bestIx);

    if (state->m_remoteCondsOrig != nullptr)
        list_free(state->m_remoteCondsOrig);

    if (state->m_attrsUsed != NULL)
        pfree(state->m_attrsUsed);

    if (state->m_attrsModified != NULL)
        pfree(state->m_attrsModified);

    state->m_table = NULL;
    pfree(state);
}



























///#################################################################################


///                                 GeoGauss


///#################################################################################









static uint64_t max_length = 10000;//cache中容器预设大小

struct pack_params {
    std::string* str;
    uint64_t epoch, index;
    bool is_lockinfo_;      // wzy: 
    pack_params(std::string* s = nullptr,
        uint64_t e = 0, uint64_t i = 0, bool is_lockinfo = false):str(s), epoch(e), index(i), is_lockinfo_(is_lockinfo) {}
    pack_params(){}
};

struct commit_thread_params {
    std::unique_ptr<merge::Message> msg;
    std::unique_ptr<std::vector<MOT::Row*>> row_vector;
    commit_thread_params(std::unique_ptr<merge::Message> &&ptr1 = nullptr, 
        std::unique_ptr<std::vector<MOT::Row*>> &&ptr2 = nullptr): 
        msg(std::move(ptr1)), row_vector(std::move(ptr2)) {}
    commit_thread_params() {}
};

class MultiRaftState{
public:
    static uint64_t _length, _size, _num, is_server_leader;
    static std::vector<std::unique_ptr<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>> 
        leader_epoch_accept_state, leader_epoch_commit_state, ///current server as a leader
        fellower_epoch_accept_state, fellower_epoch_commit_state; ///current server as a fellower
    static std::vector<std::unique_ptr<std::atomic<uint64_t>>> 
        server_state, server_reply_time, server_reply_epoch;

    static void Init(uint64_t length, uint64_t size, uint64_t is_leader) {
        is_server_leader = is_leader;
        _length = length;
        _size = size; ///kServerNum
        _num = size;
        if(_num >= 3) {
            _num = ((_num / 3) * 2) - 1;
        }
        else if(_num == 2) {
            _num = 1;
        }
        else {
            _num = 0;
        }
        leader_epoch_accept_state.resize(_length);
        leader_epoch_commit_state.resize(_length);
        fellower_epoch_accept_state.resize(_length);
        fellower_epoch_commit_state.resize(_length);
        server_state.resize(_size);
        server_reply_time.resize(_size);
        server_reply_epoch.resize(_size);

        for(int i = 0; i < (int)_length; i ++) {
            leader_epoch_accept_state[i] = std::move(std::make_unique<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>());
            leader_epoch_commit_state[i] = std::move(std::make_unique<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>());
            fellower_epoch_accept_state[i] = std::move(std::make_unique<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>());
            fellower_epoch_commit_state[i] = std::move(std::make_unique<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>());

            leader_epoch_accept_state[i]->resize(_size);
            leader_epoch_commit_state[i]->resize(_size);
            fellower_epoch_accept_state[i]->resize(_size);
            fellower_epoch_commit_state[i]->resize(_size);

            for(int j = 0; j < (int)_size; j ++) {
                (*leader_epoch_accept_state[i])[j] = std::move(std::make_unique<std::atomic<uint64_t>>(0));
                (*leader_epoch_commit_state[i])[j] = std::move(std::make_unique<std::atomic<uint64_t>>(0));
                (*fellower_epoch_accept_state[i])[j] = std::move(std::make_unique<std::atomic<uint64_t>>(0));
                (*fellower_epoch_commit_state[i])[j] = std::move(std::make_unique<std::atomic<uint64_t>>(0));
            }
        }
        for(int j = 0; j < (int)_size; j ++) {
            server_state[j] = std::move(std::make_unique<std::atomic<uint64_t>>(1));
            server_reply_time[j] = std::move(std::make_unique<std::atomic<uint64_t>>(0));
            server_reply_epoch[j] = std::move(std::make_unique<std::atomic<uint64_t>>(0));
        }
    }

    static uint64_t GetShouldReceivePackNum() {
        uint64_t ans = 0;
        for(int i = 0; i < (int)_size; i++) {
            ans += server_state[i]->load();
        }
        ans -= 1; ///sub current server itself
        return ans;
    }

    static uint64_t GetOnLineServerNum() {
        uint64_t ans = 0;
        for(int i = 0; i < (int)_size; i++) {
            ans += server_state[i]->load();
        }
        return ans;
    }

    static void HandleRaftRequest(std::unique_ptr<merge::Message> && msg);
    static void HandleRaftResponse(std::unique_ptr<merge::Message> && msg);
    static void SendHeartBeat();

    static uint64_t AddLeaderAcceptEpochState(uint64_t epoch, uint64_t server_id) {
        return (*leader_epoch_accept_state[epoch % _length])[server_id % _size]->fetch_add(1);
    }

    static uint64_t GetLeaderAcceptEpochState(uint64_t epoch, uint64_t server_id) {
        return (*leader_epoch_accept_state[epoch % _length])[server_id % _size]->load();
    }

    static void SetLeaderAcceptEpochState(uint64_t epoch, uint64_t server_id, uint64_t value) {
        (*leader_epoch_accept_state[epoch % _length])[server_id % _size]->store(value);
    }

    static uint64_t GetLeaderAcceptEpochState(uint64_t epoch) {
        uint64_t ans = 0;
        for(int i = 0; i < (int)_size; i++) {
            ans += (*leader_epoch_accept_state[epoch % _length])[i]->load();
        }
        return ans;
    }

    static uint64_t AddLeaderCommitEpochState(uint64_t epoch, uint64_t server_id) {
        return (*leader_epoch_commit_state[epoch % _length])[server_id % _size]->fetch_add(1);
    }

    static uint64_t GetLeaderCommitEpochState(uint64_t epoch, uint64_t server_id) {
        return (*leader_epoch_commit_state[epoch % _length])[server_id % _size]->load();
    }

    static void SetLeaderCommitEpochState(uint64_t epoch, uint64_t server_id, uint64_t value) {
        (*leader_epoch_commit_state[epoch % _length])[server_id % _size]->store(value);
    }

    static uint64_t GetLeaderCommitEpochState(uint64_t epoch) {
        uint64_t ans = 0;
        for(int i = 0; i < (int)_size; i++) {
            ans += (*leader_epoch_commit_state[epoch % _length])[i]->load();
        }
        return ans;
    }


    static uint64_t AddFollowerAcceptEpochState(uint64_t epoch, uint64_t server_id) {
        return (*fellower_epoch_accept_state[epoch % _length])[server_id % _size]->fetch_add(1);
    }

    static uint64_t GetFollowerAcceptEpochState(uint64_t epoch, uint64_t server_id) {
        return (*fellower_epoch_accept_state[epoch % _length])[server_id % _size]->load();
    }

    static void SetFollowerAcceptEpochState(uint64_t epoch, uint64_t server_id, uint64_t value) {
        (*fellower_epoch_accept_state[epoch % _length])[server_id % _size]->store(value);
    }

    static uint64_t GetFollowerAcceptEpochState(uint64_t epoch) {
        uint64_t ans = 0;
        for(int i = 0; i < (int)_size; i++) {
            if((*fellower_epoch_accept_state[epoch % _length])[i]->load() > 0) {
                ans ++;
            }       
        }
        return ans;
    }

    static uint64_t AddFollowerCommitEpochState(uint64_t epoch, uint64_t server_id) {
        return (*fellower_epoch_commit_state[epoch % _length])[server_id % _size]->fetch_add(1);
    }

    static uint64_t GetFollowerCommitEpochState(uint64_t epoch, uint64_t server_id) {
        return (*fellower_epoch_commit_state[epoch % _length])[server_id % _size]->load();
    }

    static void SetFollowerCommitEpochState(uint64_t epoch, uint64_t server_id, uint64_t value) {
        (*fellower_epoch_commit_state[epoch % _length])[server_id % _size]->store(value);
    }

    static uint64_t GetFollowerCommitEpochState(uint64_t epoch) {
        uint64_t ans = 0;
        for(int i = 0; i < (int)_size; i++) {
            if((*fellower_epoch_commit_state[epoch % _length])[i]->load() > 0) {
                ans ++;
            }
        }
        ans -= (*fellower_epoch_commit_state[epoch % _length])[local_ip_index % _size]->load();
        return ans;
    }

    static void ClearRaftEpochState(uint64_t epoch) {
        epoch %= _length;
        for(int i = 0; i < (int)_size; i++) {
            (*leader_epoch_accept_state[epoch])[i]->store(0);
            (*leader_epoch_commit_state[epoch])[i]->store(0);
            (*fellower_epoch_accept_state[epoch])[i]->store(0);
            (*fellower_epoch_commit_state[epoch])[i]->store(0);
        }
    }
    
    static uint64_t CheckServerState(uint64_t& server_id, uint64_t& epoch_id, uint64_t& state);

    static uint64_t SetServerState(uint64_t server_id, uint64_t epoch_id, uint64_t state);

    static void SetServerState(uint64_t server_id, uint64_t value) {
        server_state[server_id % _size]->store(value);
    }

    static void SetServerReplyTime(uint64_t server_id, uint64_t value) {
        server_reply_time[server_id % _size]->store(value);
    }

    static void SetServerReplyEpoch(uint64_t server_id, uint64_t value) {
        server_reply_epoch[server_id % _size]->store(value);
    }

    static void SetServerPongState(uint64_t server_id, uint64_t time, uint64_t epoch_id);

    static uint64_t GetServerState() {
        uint64_t ans = 0;
        for(int i = 0; i < (int)_size; i++) {
            ans += server_state[i]->load();
        }
        return ans;
    }

    static uint64_t GetShouldReceiveAcceptNum() {
        return _num;
    }

    static bool IsEpochTransmitComplete(uint64_t epoch) {
        return (GetLeaderAcceptEpochState(epoch) >= _num);
    }

    static bool IsEpochReceiveComplete(uint64_t epoch) {
        epoch %= _length;
        return (GetFollowerCommitEpochState(epoch) >=  GetShouldReceivePackNum());
    }

};
//MultiRaftState Static
uint64_t MultiRaftState::_length = 0, MultiRaftState::_size = 0, MultiRaftState::_num = 0, MultiRaftState::is_server_leader = 0;
std::vector<std::unique_ptr<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>> 
    MultiRaftState::leader_epoch_accept_state, MultiRaftState::leader_epoch_commit_state; ///current server as a leader
std::vector<std::unique_ptr<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>> 
    MultiRaftState::fellower_epoch_accept_state, MultiRaftState::fellower_epoch_commit_state; ///current server as a fellower
std::vector<std::unique_ptr<std::atomic<uint64_t>>> MultiRaftState::server_state, MultiRaftState::server_reply_time, MultiRaftState::server_reply_epoch;

//MOTAdaptor Static
volatile bool MOTAdaptor::lock_granted = false, MOTAdaptor::lock_execed = false, MOTAdaptor::lock_committed = true, MOTAdaptor::deadlock_detectted = false;       // wzy:
std::atomic<uint64_t> MOTAdaptor::lock_grant_num{0}, MOTAdaptor::lock_should_grant_num{0};

std::set<TransactionId> MOTAdaptor::active_txn_list;             // wzy: 活跃事务
std::mutex MOTAdaptor::active_txn_list_mutex;
std::atomic<TransactionId> MOTAdaptor::min_active_txn_id{0};      // wzy: 最小活跃事务id

std::list<MOT::Row*> MOTAdaptor::clean_row_list;             /// wzy: 记录有多版本的row
std::unordered_map<std::string, std::list<MOT::Row*>::iterator> MOTAdaptor::clean_row_list_map;        // 去重
std::mutex MOTAdaptor::clean_row_list_mutex;

std::vector<std::shared_ptr<BlockingConcurrentQueue<std::shared_ptr<MOTAdaptor::LockRequestQueue>>>> MOTAdaptor::active_lock_queues;         // wzy: 用于多线程
std::vector<std::shared_ptr<std::unordered_set<std::string>>> MOTAdaptor::active_lock_queues_set;
std::vector<std::mutex> MOTAdaptor::active_lock_queues_mutex(2);




// wzy: 上锁线程数
uint64_t MOTAdaptor::lock_thread_num = kLockThreadNum;
std::vector<std::shared_ptr<std::list<std::shared_ptr<MOTAdaptor::LockRequestQueue>>>> MOTAdaptor::active_lock_list;         // wzy: 用于多线程
std::vector<std::shared_ptr<std::unordered_map<std::shared_ptr<MOTAdaptor::LockRequestQueue>, std::list<std::shared_ptr<MOTAdaptor::LockRequestQueue>>::iterator>>> MOTAdaptor::active_lock_list_set;
std::vector<std::mutex> MOTAdaptor::active_lock_list_mutex(10);
std::atomic<uint64_t> MOTAdaptor::active_lock_list_size{0};
std::vector<bool> MOTAdaptor::active_lock_list_exced;

std::list<std::shared_ptr<MOTAdaptor::LockRequestQueue>> MOTAdaptor::active_queue_list;      // wzy: 记录当前活跃的queue，单线程
std::unordered_map<std::shared_ptr<MOTAdaptor::LockRequestQueue>, std::list<std::shared_ptr<MOTAdaptor::LockRequestQueue>>::iterator> MOTAdaptor::active_queue_set;
std::mutex MOTAdaptor::active_queue_list_mutex;

MOTAdaptor::DynamicHotRow MOTAdaptor::dynamic_hot_rows;             // wzy: hot row


bool MOTAdaptor::timerStop = false;
volatile bool 
    MOTAdaptor::remote_execed = false, MOTAdaptor::record_committed = true, MOTAdaptor::remote_record_committed = true, 
    MOTAdaptor::is_current_epoch_abort = false;
volatile uint64_t 
    MOTAdaptor::logical_epoch = 1, MOTAdaptor::physical_epoch = 0, epoch_commit_time = 0;
std::default_random_engine MOTAdaptor::random_mot;
uint64_t MOTAdaptor::max_length = 10000, MOTAdaptor::pack_num = 10000;
std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> 
    MOTAdaptor::local_txn_counters, MOTAdaptor::local_txn_exc_counters, 
    MOTAdaptor::local_txn_execed_counters, MOTAdaptor::local_txn_committed_counters,
    MOTAdaptor::local_txn_index, 
    MOTAdaptor::remote_merged_txn_counters, MOTAdaptor::remote_commit_txn_counters,
    MOTAdaptor::remote_committed_txn_counters, MOTAdaptor::record_commit_txn_counters, MOTAdaptor::record_committed_txn_counters;
    
std::map<uint64_t, std::unique_ptr<std::vector<MOT::Row*>>> 
    MOTAdaptor::remote_row_ptr_map;
aum::concurrent_unordered_map<std::string, std::string, std::string> 
    MOTAdaptor::insertSet, MOTAdaptor::insertSetForCommit, MOTAdaptor::abort_transcation_csn_set;

//LocalWriteSet Static
uint64_t MOTAdaptor::_max_length = kCacheMaxLength, MOTAdaptor::_pack_num = kPackageNum;
std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> MOTAdaptor::txn_num_ptrs,
    MOTAdaptor::write_abort_before_send_txn_num,
    MOTAdaptor::write_abort_after_send_txn_num, MOTAdaptor::total_abort_txn_num, MOTAdaptor::read_abort_txn_num, 
    MOTAdaptor::read_committed_txn_num, MOTAdaptor::write_committed_txn_num, 
    MOTAdaptor::read_total_txn_num, MOTAdaptor::write_total_txn_num, MOTAdaptor::pack_txn_num, MOTAdaptor::limite_txn_num,
    MOTAdaptor::packd_txn_num_ptrs;

aum::concurrent_unordered_map<std::string, std::string, std::string> MOTAdaptor::epoch_lock_set;     // wzy: 优化

// RemoteCache Static
// uint64_t MOTAdaptor::_max_length = kCacheMaxLength, MOTAdaptor::_pack_num = kPackageNum;
std::unique_ptr<std::atomic<uint64_t>> MOTAdaptor::should_receive_pack_num, MOTAdaptor::online_server_num;
std::vector<std::unique_ptr<std::atomic<uint64_t>>> MOTAdaptor::is_server_online;
std::vector<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> 
        MOTAdaptor::received_pack_num, MOTAdaptor::received_txn_num, MOTAdaptor::should_receive_txn_num;
        
std::vector<std::unique_ptr<std::atomic<uint64_t>>> 
        MOTAdaptor::received_total_pack_num, MOTAdaptor::received_total_txn_num;

//RegionCacheServerState Static
// uint64_t MOTAdaptor::_max_length = kCacheMaxLength;
std::vector<std::unique_ptr<std::atomic<uint64_t>>>  MOTAdaptor::received_epoch;

// wzy: lockinfo
std::vector<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> MOTAdaptor::should_receive_lockinfo_num, MOTAdaptor::received_lockinfo_num;
std::vector<std::unique_ptr<std::atomic<uint64_t>>> MOTAdaptor::received_total_lockinfo_num;

// wzy: 添加lockinfo 统计
std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> MOTAdaptor::lockinfo_num_ptrs, MOTAdaptor::packd_lockinfo_num_ptrs, MOTAdaptor::pack_lockinfo_num; 
std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> MOTAdaptor::local_lockinfo_counters, MOTAdaptor::merge_lockinfo_counters, MOTAdaptor::local_lockinfo_execed_counters;


// wzy: 记录对应的tablename Rowid以及csn_server_id 队列和获取到锁的csn_server_id，用cv来唤醒
aum::concurrent_unordered_map<std::string, std::shared_ptr<MOTAdaptor::LockRequestQueue>, std::string>
    MOTAdaptor::row_lockrequest_map;

aum::concurrent_unordered_map<std::string, std::shared_ptr<std::vector<std::shared_ptr<MOTAdaptor::LockRequestQueue>>>, std::string>
    MOTAdaptor::csn_requests_map;  // csn + server id - lock request queue

aum::concurrent_unordered_map<std::string, std::shared_ptr<std::vector<std::string >>, std::string>
    MOTAdaptor::txn_rowid_map;

aum::concurrent_unordered_map<uint64_t, int, std::string> MOTAdaptor::txn_state_map_plor_;

// wzy: 记录已有LockRequest队列的rowid
std::set<std::string> MOTAdaptor::rowid_set;
std::vector<std::string> MOTAdaptor::rowid_vec;
std::mutex MOTAdaptor::rowid_set_mutex;

// wzy: 等待图，解决死锁，上锁参考
std::map<std::string, std::set<std::string>> MOTAdaptor::wait_for;
std::mutex MOTAdaptor::wait_for_mutex;
MOTAdaptor::WaitForGraph MOTAdaptor::wait_for_graph;
aum::concurrent_unordered_map<std::string, std::string, std::string> MOTAdaptor::deadlock_abort_set;        // 记录由死锁检测abort的事务tid

// wzy: recovery debug
std::atomic<bool> MOTAdaptor::isInited{false};
std::atomic<bool> MOTAdaptor::need_clean{false};

std::atomic<uint64_t> MOTAdaptor::txn_total_epoch{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_readCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_writeCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_hotCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_lockCnt{0};

// wzy: 30s statistics
std::atomic<uint64_t> MOTAdaptor::temp_commit_txn_num{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_time{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_readCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_writeCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_hotCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_epoch{0};

std::atomic<uint64_t> MOTAdaptor::txn_total_switchTime{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_read_lockTime{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_write_lockTime{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_validate_lockTime{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_validate_hotOccTime{0};

std::atomic<uint64_t> MOTAdaptor::txn_total_switchCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_read_lockCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_write_lockCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_validate_lockCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_validate_hotOccCnt{0};

std::atomic<uint64_t> MOTAdaptor::txn_total_switchTime_RLock{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_switchTime_WLock{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_switchTime_Sentinel{0};

std::atomic<uint64_t> MOTAdaptor::txn_total_switchTime_RLockCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_switchTime_WLockCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_total_switchTime_SentinelCnt{0};

// wzy: 30s statistics
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_switchTime{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_read_lockTime{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_write_lockTime{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_validate_lockTime{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_validate_hotOccTime{0};

std::atomic<uint64_t> MOTAdaptor::txn_temp_total_switchCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_read_lockCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_write_lockCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_validate_lockCnt{0};
std::atomic<uint64_t> MOTAdaptor::txn_temp_total_validate_hotOccCnt{0};

std::atomic<uint64_t> MOTAdaptor::start_num_start_txn{0};
std::atomic<uint64_t> MOTAdaptor::start_num_txn_construct{0};

std::atomic<uint64_t> MOTAdaptor::pessimisitic_txn_num{0};              // PCC交互型事务
std::atomic<uint64_t> MOTAdaptor::pessimisitic_priority_txn_num{0};         // 因为优先级而切换
std::atomic<uint64_t> MOTAdaptor::pessimisitic_hot_visits_txn_num{0};       // 因为热数据访问而切换

std::atomic<uint64_t> MOTAdaptor::start_txn_num{0};
std::atomic<uint64_t> MOTAdaptor::start_interactive_txn_num{0};
std::atomic<uint64_t> MOTAdaptor::commit_txn_num{0};                    // 本地非交互型事务
std::atomic<uint64_t> MOTAdaptor::commit_stored_txn_num{0};
std::atomic<uint64_t> MOTAdaptor::commit_interactive_txn_num{0};        // 本地交互型事务
std::atomic<uint64_t> MOTAdaptor::commit_pcc_interactive_txn_num{0};
std::atomic<uint64_t> MOTAdaptor::commit_occ_interactive_txn_num{0};

std::atomic<uint64_t> MOTAdaptor::txn_total_time{0};
std::atomic<uint64_t> MOTAdaptor::stored_txn_total_time{0};
std::atomic<uint64_t> MOTAdaptor::interactive_txn_total_time{0};
std::atomic<uint64_t> MOTAdaptor::interactive_pcc_txn_total_time{0};
std::atomic<uint64_t> MOTAdaptor::interactive_occ_txn_total_time{0};
std::atomic<uint64_t> MOTAdaptor::txn_abort_time{0};
std::atomic<uint64_t> MOTAdaptor::interactive_txn_abort_time{0};
std::atomic<uint64_t> MOTAdaptor::LockCheck_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::Commit_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::DeadLock_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::Abort_txn_num{0};
std::atomic<uint64_t> MOTAdaptor::Abort_interactive_txn_num{0};
std::atomic<uint64_t> MOTAdaptor::Abort_pcc_interactive_txn_num{0};
std::atomic<uint64_t> MOTAdaptor::Remote_Abort_interactive_txn_num{0};

std::atomic<uint64_t> MOTAdaptor::CommitPhase_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::CommitCheck_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::Abort_transcation_csn_set_abort_num{0};

std::atomic<uint64_t> MOTAdaptor::Remote_ValidateAndSetWriteForRemote_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::Remote_CommitCheck_abort_num{0};

std::atomic<uint64_t> MOTAdaptor::CommitPhase_ValidateAndSetWriteForCommit_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::CommitPhase_IsRowAvailable_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::CommitPhase_origSentinel_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::CommitLockCheck_IsRowAvailable_abort_num{0};

std::atomic<uint64_t> MOTAdaptor::Commit_abort_pcc_total_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::Commit_abort_occ_total_interactive_num{0};

std::atomic<uint64_t> MOTAdaptor::Commit_abort_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::CommitCheck_abort_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::CommitPhase_abort_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::CommitUpdate_abort_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::CommitCheck_deadlock_abort_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::ValidateReadInMerge_abort_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::InsertTxntoLocalChangeSet_abort_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::Abort_transcation_csn_set_abort_interactive_num{0};

std::atomic<uint64_t> MOTAdaptor::CommitPhase_ValidateAndSetWriteForCommit_abort_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::CommitPhase_IsRowAvailable_abort_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::CommitPhase_origSentinel_abort_interactive_num{0};

std::atomic<uint64_t> MOTAdaptor::Switch_validation_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::Switch_validation_occ_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::Switch_validation_pcc_abort_num{0};

std::atomic<uint64_t> MOTAdaptor::HotRow_quick_validation_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::HotRow_read_validation_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::HotRow_write_validation_abort_num{0};

std::atomic<uint64_t> MOTAdaptor::ReadLock_pcc_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::WriteLock_pcc_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::ReadLock_switch_pcc_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::WriteLock_switch_pcc_abort_num{0};

std::atomic<uint64_t> MOTAdaptor::Silo_validation_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::Silo_quick_validation_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::Silo_lockheader_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::Silo_lockheader_abort_by_interactive_num{0};
std::atomic<uint64_t> MOTAdaptor::Silo_write_validation_abort_num{0};
std::atomic<uint64_t> MOTAdaptor::Silo_read_validation_abort_num{0};

std::atomic<uint64_t> MOTAdaptor::local_lock_num{0};
std::atomic<uint64_t> MOTAdaptor::remote_lock_num{0};
std::atomic<uint64_t> MOTAdaptor::local_unlock_num{0};
std::atomic<uint64_t> MOTAdaptor::remote_unlock_num{0};
std::atomic<uint64_t> MOTAdaptor::send_lock_num{0};
std::atomic<uint64_t> MOTAdaptor::receive_lock_num{0};








uint64_t 
    start_time_ll, start_physical_epoch = 1, new_start_physical_epoch = 1, new_sleep_time = 10000, start_merge_time = 0, commit_time = 0;
struct timeval 
    start_time;
std::atomic<bool> 
    init_ok(false), is_epoch_advance_started(false);

BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>> 
    message_send_pool, message_receive_pool, listen_message_queue;
BlockingConcurrentQueue<std::unique_ptr<send_thread_params>> send_pool, raft_send_pool;
BlockingConcurrentQueue<std::unique_ptr<merge::Message>> raft_message_pool;
BlockingConcurrentQueue<std::unique_ptr<merge::Message>> merge_queue;

BlockingConcurrentQueue<std::unique_ptr<merge::Message>> lock_queue;    // wzy: 用于上锁queue

// TODO:
////////////////////////////// wzy: RL模型开启 //////////////////////////////////////
// RL模型predict queue，线程从该queue取消息发送过去
// RL模型predict reply，线程接收reply并存放到queue中，集中接收action
BlockingConcurrentQueue<std::unique_ptr<merge::Message>> predict_req_queue, predict_resp_queue;

// 对每个session uint32_t 存储? 前后state链表，定期发送train
aum::concurrent_unordered_map<std::string, std::unique_ptr<MOTAdaptor::RLState>, std::string> rl_state_map;
aum::concurrent_unordered_map<std::string, std::unique_ptr<merge::Message>, std::string> train_state_list;

////////////////////////////////////////////////////////////////////////////////////


BlockingConcurrentQueue<std::unique_ptr<commit_thread_params>> commit_txn_queue_struct;
std::vector<std::shared_ptr<BlockingConcurrentQueue<std::unique_ptr<pack_params>>>> txn_queue_ptrs;
std::vector<std::shared_ptr<BlockingMPMCQueue<std::unique_ptr<pack_params>>>> txn_queues;
std::vector<std::shared_ptr<BlockingConcurrentQueue<std::unique_ptr<merge::Message>>>> cache_txn_queues;

void SetCPU(){
    std::atomic<int> cpu_index(1);
    cpu_set_t logicalEpochSet;
    CPU_ZERO(&logicalEpochSet);
    CPU_SET(cpu_index.fetch_add(1), &logicalEpochSet); //2就是核心号
    int rc = sched_setaffinity(0, sizeof(cpu_set_t), &logicalEpochSet);
    if (rc == -1) {
        ereport(FATAL, (errmsg("绑核失败")));
    }
}
uint64_t now_to_us(){
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
uint64_t GetSleeptime(){
    uint64_t sleep_time;
    struct timeval current_time;
    uint64_t current_time_ll;
    gettimeofday(&current_time, NULL);
    current_time_ll = current_time.tv_sec * 1000000 + current_time.tv_usec;
    sleep_time = current_time_ll - (start_time_ll + (long)(MOTAdaptor::GetPhysicalEpoch() - start_physical_epoch) * kSleepTime);
    if(sleep_time >= kSleepTime){
        MOT_LOG_INFO("start time : %llu, current time : %llu, 差值 %llu ,sleep time : %d", start_time_ll, current_time_ll, sleep_time, 0);
        return 0;
    }
    else{
        // MOT_LOG_INFO("start time : %llu, current time : %llu, 差值 %llu, sleep time : %llu", start_time_ll, current_time_ll, sleep_time, ksleeptime - sleep_time);
        return kSleepTime - sleep_time;
    } 
}

void ChangeEpochInfo(){
}

void string_free(void *data, void *hint){
    delete static_cast<std::string*>(hint);
}

bool Enqueue(std::unique_ptr<pack_params> && ptr, std::unique_ptr<pack_params> && ptr1, uint64_t epoch_num, uint64_t index) {
    txn_queue_ptrs[0]->enqueue(std::move(ptr));
    txn_queue_ptrs[0]->enqueue(std::move(ptr1));
    return true;
}

bool TryDequeue(std::unique_ptr<pack_params> &v, uint64_t index) {
    if(txn_queue_ptrs[0]->try_dequeue(v)){
        return true;
    }
    return false;
}

std::string* Gzip(std::unique_ptr<merge::Message> && msg) {
    auto serialized_str_ptr = new std::string();
    google::protobuf::io::GzipOutputStream::Options options;
    options.format = google::protobuf::io::GzipOutputStream::GZIP;
    options.compression_level = 9;
    google::protobuf::io::StringOutputStream outputStream(&(*serialized_str_ptr));
    google::protobuf::io::GzipOutputStream gzipStream(&outputStream, options);
    msg->SerializeToZeroCopyStream(&gzipStream);
    gzipStream.Close();
    return serialized_str_ptr;
}


bool MOTAdaptor::TryIncLocalChangeSetNum(uint64_t epoch, uint64_t index_pack, uint64_t value){
    return MOTAdaptor::TryAddNum(epoch, index_pack, value);
}

bool MOTAdaptor::IncLocalChangeSetNum(uint64_t epoch, uint64_t index_pack, uint64_t value){
    return MOTAdaptor::AddNum(epoch, index_pack, value);
}




bool MOTAdaptor::InsertTxntoLocalChangeSet(MOT::TxnManager* txMan, const uint64_t& index_pack, const uint64_t& index_unique){
    auto msg = std::make_unique<merge::Message>();
    auto* txn = msg->mutable_txn();
    // auto txn = std::make_unique<merge::Transaction>();
    if(kServerNum > 1) {
        merge::Transaction_Row *row;
        merge::Transaction_Row_Column* col;
        MOT::Row* local_row = nullptr;
        MOT::Key* key = nullptr;
        void* buf = nullptr; 
        uint64_t fieldCnt;
        const MOT::Access* access = nullptr;
        MOT::BitmapSet* bmp;
        MOT::TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
        int op_type = 0; //  0-update, 1-insert, 2-delete
        for (const auto &raPair : orderedSet){
            access = raPair.second;
            if (access->m_type == MOT::RD) {
                continue;
            }
            row = txn->add_row();
            if (access->m_type == MOT::WR){
                op_type = 0;
                local_row = access->m_localRow;
            }
            else if (access->m_type == MOT::INS){
                op_type = 1;
                local_row = access->m_localInsertRow;
            }
            else if (access->m_type == MOT::DEL){
                op_type = 2;
                local_row = access->m_localRow;
            }
            if(local_row == nullptr || local_row->GetTable() == nullptr){
                return false;
            }
            key = local_row->GetTable()->BuildKeyByRow(local_row, txMan, buf);
            if (key == nullptr) {
                return false;
            }
            row->set_key(std::move(std::string(key->GetKeyBuf(), key->GetKeyBuf() + key->GetKeyLength() )));
            row->set_tablename(local_row->GetTable()->GetLongTableName());
            if (op_type == 0) {
                // row->set_data(local_row->GetData(), local_row->GetTable()->GetTupleSize());

                fieldCnt = local_row->GetTable()->GetFieldCount();
                bmp = const_cast<MOT::BitmapSet*>(&(access->m_modifiedColumns));
                for (uint16_t field_i = 0; field_i < fieldCnt - 1 ; field_i ++) { 
                    if (bmp->GetBit(field_i)) { //真正的列有一个单位的偏移量，这是经过试验得出的结论 
                        int real_field = field_i + 1;
                        col = row->add_column(); 
                        col->set_id(real_field); 
                        col->set_value(local_row->GetValue(real_field),local_row->GetTable()->GetFieldSize(real_field));
                    }
                }
            }
            else if (op_type == 1) {
                row->set_data(local_row->GetData(), local_row->GetTable()->GetTupleSize());
            }
            row->set_type(op_type);
        }
        
        txn->set_startepoch(txMan->GetStartEpoch());
        txn->set_server_id(local_ip_index);
        txn->set_txnid(local_ip_index * 100000000000000 + index_pack * 10000000000 + index_unique * 1000000);
    }

    if(is_sync_exec) {
        // txMan->SetCommitEpoch(txMan->GetStartEpoch());
        // 非交互型事务或csn = 0
        if (!txMan->IsInteractive() || txMan->GetCommitSequenceNumber() == 0)
            txMan->SetCommitSequenceNumber(now_to_us());
        (*MOTAdaptor::pack_txn_num[(txMan->GetStartEpoch() % MOTAdaptor::_max_length)])[txMan->GetIndexPack()]->fetch_add(1);
    }
    else {
        add_num_again:
        txMan->SetCommitEpoch(MOTAdaptor::GetPhysicalEpoch());
        if(!MOTAdaptor::TryAddNum(txMan->GetCommitEpoch(), index_pack, 1)){
            goto add_num_again;
        }

        if (!txMan->IsInteractive() || txMan->GetCommitSequenceNumber() == 0)
            txMan->SetCommitSequenceNumber(now_to_us());
        (*MOTAdaptor::pack_txn_num[(txMan->GetCommitEpoch() % MOTAdaptor::_max_length)])[txMan->GetIndexPack()]->fetch_add(1);
    }

    if(kServerNum > 1) {
        txn->set_commitepoch(txMan->GetCommitEpoch());
        txn->set_csn(txMan->GetCommitSequenceNumber());
        
        auto time1 = now_to_us();
        auto* serialized_txn_str_ptr = Gzip(std::move(msg));
        auto time2 = now_to_us();

        txMan->SetZipSize(serialized_txn_str_ptr->size());
        txMan->SetZipTime(time2 - time1);

        // MOT_LOG_INFO("=**= protobuf size %lu", serialized_txn_str_ptr->size());
        Enqueue(std::move(std::make_unique<pack_params>(serialized_txn_str_ptr, txMan->GetCommitEpoch(), index_pack)), 
            std::move(std::make_unique<pack_params>(nullptr, 0, 0)),
            txMan->GetCommitEpoch(), index_pack);
        txn = nullptr;
    }
    else {
        txn = nullptr;
    }
    return true;
}

// wzy: 添加对于交互型row的指定
bool MOTAdaptor::InsertTxntoLocalChangeSet2(MOT::TxnManager* txMan, const uint64_t& index_pack, const uint64_t& index_unique){
    auto msg = std::make_unique<merge::Message>();
    auto* txn = msg->mutable_txn();
    // auto txn = std::make_unique<merge::Transaction>();
    if(kServerNum > 1) {
        merge::Transaction_Row *row;
        merge::Transaction_Row_Column* col;
        MOT::Row* local_row = nullptr;
        MOT::Key* key = nullptr;
        void* buf = nullptr;
        uint64_t fieldCnt;
        const MOT::Access* access = nullptr;
        MOT::BitmapSet* bmp;
        MOT::TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
        int op_type = 0; //  0-update, 1-insert, 2-delete
        for (const auto &raPair : orderedSet){
            access = raPair.second;
            if (access->m_type == MOT::RD) {
                continue;
            }
            row = txn->add_row();
            if (access->m_type == MOT::WR){
                op_type = 0;
                local_row = access->m_localRow;
            }
            else if (access->m_type == MOT::INS){
                op_type = 1;
                local_row = access->m_localInsertRow;
            }
            else if (access->m_type == MOT::DEL){
                op_type = 2;
                local_row = access->m_localRow;
            }
            if(local_row == nullptr || local_row->GetTable() == nullptr){
                return false;
            }
            key = local_row->GetTable()->BuildKeyByRow(local_row, txMan, buf);
            if (key == nullptr) {
                return false;
            }
            row->set_key(std::move(std::string(key->GetKeyBuf(), key->GetKeyBuf() + key->GetKeyLength() )));
            row->set_tablename(local_row->GetTable()->GetLongTableName());
            if (op_type == 0) {
                // row->set_data(local_row->GetData(), local_row->GetTable()->GetTupleSize());

                fieldCnt = local_row->GetTable()->GetFieldCount();
                bmp = const_cast<MOT::BitmapSet*>(&(access->m_modifiedColumns));
                for (uint16_t field_i = 0; field_i < fieldCnt - 1 ; field_i ++) {
                    if (bmp->GetBit(field_i)) { //真正的列有一个单位的偏移量，这是经过试验得出的结论
                        int real_field = field_i + 1;
                        col = row->add_column();
                        col->set_id(real_field);
                        col->set_value(local_row->GetValue(real_field),local_row->GetTable()->GetFieldSize(real_field));
                    }
                }
                // wzy: 对写操作设置是否访问的hot row
                if (local_row->GetRowInteractive()) row->set_isinteractiverow(true);
                else row->set_isinteractiverow(false);
            }
            else if (op_type == 1) {
                row->set_data(local_row->GetData(), local_row->GetTable()->GetTupleSize());
            }
            row->set_type(op_type);
        }

        txn->set_startepoch(txMan->GetStartEpoch());
        txn->set_server_id(local_ip_index);
        txn->set_txnid(local_ip_index * 100000000000000 + index_pack * 10000000000 + index_unique * 1000000);

        if (txMan->IsInteractive()) txn->set_isinteractive(true);
        else txn->set_isinteractive(false);
    }

    if(is_sync_exec) {
        // txMan->SetCommitEpoch(txMan->GetStartEpoch());
        if (txMan->IsInteractive() == false || txMan->GetCommitSequenceNumber() == 0) {
            txMan->SetCommitSequenceNumber(now_to_us());
        }
        (*MOTAdaptor::pack_txn_num[(txMan->GetStartEpoch() % MOTAdaptor::_max_length)])[txMan->GetIndexPack()]->fetch_add(1);
    }
    else {
    add_num_again:
        txMan->SetCommitEpoch(MOTAdaptor::GetPhysicalEpoch());
        if(!MOTAdaptor::TryAddNum(txMan->GetCommitEpoch(), index_pack, 1)){
            goto add_num_again;
        }
        if (txMan->IsInteractive() == false || txMan->GetCommitSequenceNumber() == 0) {
            txMan->SetCommitSequenceNumber(now_to_us());
        }
        (*MOTAdaptor::pack_txn_num[(txMan->GetCommitEpoch() % MOTAdaptor::_max_length)])[txMan->GetIndexPack()]->fetch_add(1);
    }

    if(kServerNum > 1) {
        txn->set_commitepoch(txMan->GetCommitEpoch());
        txn->set_csn(txMan->GetCommitSequenceNumber());

        auto time1 = now_to_us();
        auto* serialized_txn_str_ptr = Gzip(std::move(msg));
        auto time2 = now_to_us();

        txMan->SetZipSize(serialized_txn_str_ptr->size());
        txMan->SetZipTime(time2 - time1);

        // MOT_LOG_INFO("=**= protobuf size %lu", serialized_txn_str_ptr->size());
        Enqueue(std::move(std::make_unique<pack_params>(serialized_txn_str_ptr, txMan->GetCommitEpoch(), index_pack)),
            std::move(std::make_unique<pack_params>(nullptr, 0, 0)),
            txMan->GetCommitEpoch(), index_pack);
        txn = nullptr;
    }
    else {
        txn = nullptr;
    }
    return true;
}


// wzy：生成LockInfo 发送到远端，发送单个lockinfo，而不是累计
bool MOTAdaptor::InsertTxntoLocalLockInfo(
    MOT::TxnManager* txMan, const uint64_t& index_pack, const uint64_t& index_unique, const bool& lock, MOT::Row* currRow)
{
    auto msg = std::make_unique<merge::Message>();
    auto* lock_info = msg->mutable_lockinfo();
    // auto txn = std::make_unique<merge::Transaction>();
    // 生成txn，添加update的row
    if (kServerNum > 1) {
        merge::LockInfo_Row* lock_row;    // wzy:
        MOT::Row* local_row = nullptr;
        MOT::Key* key = nullptr;
        void* buf = nullptr;
        uint64_t fieldCnt;
        const MOT::Access* access = nullptr;
        MOT::TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
        int op_type = 0;                         //  0-update, 1-insert, 2-delete

        // wzy : 当前row仅当WR时发送
        lock_row = lock_info->add_row();
        local_row = currRow;
        key = local_row->GetTable()->BuildKeyByRow(local_row, txMan, buf);
        lock_row->set_key(std::move(std::string(key->GetKeyBuf(), key->GetKeyBuf() + key->GetKeyLength())));
        lock_row->set_tablename(local_row->GetTable()->GetLongTableName());
        lock_row->set_type(op_type);

        lock_info->set_startepoch(txMan->GetStartEpoch());
        lock_info->set_server_id(local_ip_index);
        lock_info->set_txnid(local_ip_index * 100000000000000 + index_pack * 10000000000 + index_unique * 1000000);
        lock_info->set_islock(lock);  // 进行上锁或者解锁
    }

    if (is_sync_exec) {
        (*MOTAdaptor::pack_lockinfo_num[(txMan->GetStartEpoch() % MOTAdaptor::_max_length)])[txMan->GetIndexPack()]
            ->fetch_add(1);
    } else {
    add_num_again:
        txMan->SetCommitEpoch(MOTAdaptor::GetPhysicalEpoch());
        if (!MOTAdaptor::TryAddLockinfoNum(txMan->GetCommitEpoch(), index_pack, 1)) {
            goto add_num_again;
        }
//        if(!MOTAdaptor::TryAddNum(txMan->GetCommitEpoch(), index_pack, 1)){
//            goto add_num_again;
//        }
        (*MOTAdaptor::pack_lockinfo_num[(txMan->GetCommitEpoch() % MOTAdaptor::_max_length)])[txMan->GetIndexPack()]
            ->fetch_add(1);
    }

    if (kServerNum > 1) {
        // 发送LockInfo
        lock_info->set_commitepoch(txMan->GetCommitEpoch());
        lock_info->set_csn(txMan->GetCommitSequenceNumber());

        auto time1 = now_to_us();
        auto* serialized_txn_str_ptr = Gzip(std::move(msg));
        auto time2 = now_to_us();

        txMan->SetZipSize(serialized_txn_str_ptr->size());
        txMan->SetZipTime(time2 - time1);

        Enqueue(std::move(std::make_unique<pack_params>(serialized_txn_str_ptr, txMan->GetCommitEpoch(), index_pack, true)),
            std::move(std::make_unique<pack_params>(nullptr, 0, 0)),
            txMan->GetCommitEpoch(),
            index_pack);

        auto csn_temp = std::to_string(txMan->GetCommitSequenceNumber())+ ":" + std::to_string(local_ip_index);
        MOT_LOG_INFO("InsertTxntoLocalLockInfo() tmp_csn : %s, commitepoch = %llu", csn_temp.c_str(), lock_info->commitepoch());

        MOTAdaptor::send_lock_num.fetch_add(1);
        lock_info = nullptr;
    } else {
        lock_info = nullptr;
    }
    return true;
}


void MOTAdaptor::Merge(MOT::TxnManager* txMan, uint64_t& index_pack) {///delete
    
}

void MOTAdaptor::Commit(MOT::TxnManager* txMan, uint64_t& index_pack) {///delete
    
}




























void InitEpochTimerManager(){
    // txn_queue.init(3000000);
    // wzy: 初始化DynamicHotRow
    MOTAdaptor::dynamic_hot_rows.freq_ = kHotRowsFreq;
    MOTAdaptor::lock_thread_num = kLockThreadNum;

    //==========Cache===============
    MOTAdaptor::max_length = max_length = kCacheMaxLength;
    MOTAdaptor::pack_num = kPackageNum;
    MultiRaftState::Init(max_length, kServerNum, (local_ip_index == kRaftLeaderId ? 1 : 0));
    for(int i = 0; i < (int)kServerNum; i++) {
        MultiRaftState::SetServerState(i, 1);
    }
    MOTAdaptor::random_mot.seed(now_to_us());
    srand(time(0));
    txn_queue_ptrs.resize(1);
    txn_queues.resize(kPackageNum + 2);
    for(int i = 0; i < (int)1; i++) {
        txn_queues[i] = std::make_shared<BlockingMPMCQueue<std::unique_ptr<pack_params>>>();
        txn_queue_ptrs[i] = std::make_shared<BlockingConcurrentQueue<std::unique_ptr<pack_params>>>();
    }

    // wzy: 初始化上锁队列
    MOTAdaptor::active_lock_queues.resize(2);
    MOTAdaptor::active_lock_queues_set.resize(2);
    for(int i = 0; i < 2; i ++) {
        MOTAdaptor::active_lock_queues[i] = std::make_shared<BlockingConcurrentQueue<std::shared_ptr<MOTAdaptor::LockRequestQueue>>>();
        MOTAdaptor::active_lock_queues_set[i] = std::make_shared<std::unordered_set<std::string>>();
    }
    /////////// 多线程上锁list
    MOTAdaptor::active_lock_list.resize(10);
    MOTAdaptor::active_lock_list_set.resize(10);
    MOTAdaptor::active_lock_list_exced.resize(10);
    for(int i = 0; i < 10; i ++) {
        MOTAdaptor::active_lock_list[i] = std::make_shared<std::list<std::shared_ptr<MOTAdaptor::LockRequestQueue>>>();
        MOTAdaptor::active_lock_list_set[i] = std::make_shared<std::unordered_map<std::shared_ptr<MOTAdaptor::LockRequestQueue>, std::list<std::shared_ptr<MOTAdaptor::LockRequestQueue>>::iterator>>();
        MOTAdaptor::active_lock_list_exced[i] = false;
    }

    //==========Logical=============
    MOTAdaptor::local_txn_counters.resize(kCacheMaxLength + 2);
    MOTAdaptor::local_txn_exc_counters.resize(kCacheMaxLength + 2);
    MOTAdaptor::local_txn_execed_counters.resize(kCacheMaxLength + 2);
    MOTAdaptor::local_txn_committed_counters.resize(kCacheMaxLength + 2);
    MOTAdaptor::local_txn_index.resize(kCacheMaxLength + 2);
    MOTAdaptor::record_commit_txn_counters.resize(kCacheMaxLength + 2);
    MOTAdaptor::record_committed_txn_counters.resize(kCacheMaxLength + 2);
    MOTAdaptor::remote_merged_txn_counters.resize(kCacheMaxLength + 2);
    MOTAdaptor::remote_commit_txn_counters.resize(kCacheMaxLength + 2);
    MOTAdaptor::remote_committed_txn_counters.resize(kCacheMaxLength + 2);

    MOTAdaptor::_pack_num = kPackageNum;
    MOTAdaptor::_max_length = kCacheMaxLength;
    MOTAdaptor::txn_num_ptrs.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::packd_txn_num_ptrs.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::write_abort_before_send_txn_num.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::write_abort_after_send_txn_num.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::write_committed_txn_num.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::read_abort_txn_num.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::read_committed_txn_num.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::total_abort_txn_num.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::read_total_txn_num.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::write_total_txn_num.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::pack_txn_num.resize(MOTAdaptor::_max_length + 2);

    MOTAdaptor::limite_txn_num.resize(MOTAdaptor::_max_length + 2);

    // wzy: lockinfo init
    MOTAdaptor::local_lockinfo_counters.resize(kCacheMaxLength + 2);
    MOTAdaptor::lockinfo_num_ptrs.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::packd_lockinfo_num_ptrs.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::pack_lockinfo_num.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::merge_lockinfo_counters.resize(MOTAdaptor::_max_length + 2);
    MOTAdaptor::local_lockinfo_execed_counters.resize(kCacheMaxLength + 2);


    for(int i = 0; i < (int)kCacheMaxLength; i ++) {

        MOTAdaptor::local_txn_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::local_txn_exc_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::local_txn_execed_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::local_txn_committed_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::local_txn_index[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::record_commit_txn_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::record_committed_txn_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::remote_merged_txn_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::remote_commit_txn_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::remote_committed_txn_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        
        MOTAdaptor::txn_num_ptrs[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::packd_txn_num_ptrs[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::write_abort_before_send_txn_num[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::write_abort_after_send_txn_num[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::write_committed_txn_num[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::read_abort_txn_num[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::read_committed_txn_num[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::total_abort_txn_num[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::read_total_txn_num[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::write_total_txn_num[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::pack_txn_num[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();

        MOTAdaptor::limite_txn_num[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();

        /// wzy: lockinfo
        MOTAdaptor::local_lockinfo_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::lockinfo_num_ptrs[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::packd_lockinfo_num_ptrs[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::pack_lockinfo_num[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::merge_lockinfo_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        MOTAdaptor::local_lockinfo_execed_counters[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();

        MOTAdaptor::local_txn_counters[i]->resize(kPackageNum + 2);
        MOTAdaptor::local_txn_exc_counters[i]->resize(kPackageNum + 2);
        MOTAdaptor::local_txn_execed_counters[i]->resize(kPackageNum + 2);
        MOTAdaptor::local_txn_committed_counters[i]->resize(kPackageNum + 2);
        MOTAdaptor::local_txn_index[i]->resize(kPackageNum + 2);
        MOTAdaptor::record_commit_txn_counters[i]->resize(kPackageNum + 2);
        MOTAdaptor::record_committed_txn_counters[i]->resize(kPackageNum + 2);
        MOTAdaptor::remote_merged_txn_counters[i]->resize(kPackageNum + 2);
        MOTAdaptor::remote_commit_txn_counters[i]->resize(kPackageNum + 2);
        MOTAdaptor::remote_committed_txn_counters[i]->resize(kPackageNum + 2);


        MOTAdaptor::txn_num_ptrs[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::packd_txn_num_ptrs[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::write_abort_before_send_txn_num[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::write_abort_after_send_txn_num[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::write_committed_txn_num[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::read_abort_txn_num[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::read_committed_txn_num[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::total_abort_txn_num[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::read_total_txn_num[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::write_total_txn_num[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::pack_txn_num[i]->resize(MOTAdaptor::_pack_num + 2);

        MOTAdaptor::limite_txn_num[i]->resize(MOTAdaptor::_pack_num + 2);

        /// wzy: lockinfo
        MOTAdaptor::local_lockinfo_counters[i]->resize(kPackageNum + 2);
        MOTAdaptor::lockinfo_num_ptrs[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::packd_lockinfo_num_ptrs[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::pack_lockinfo_num[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::merge_lockinfo_counters[i]->resize(MOTAdaptor::_pack_num + 2);
        MOTAdaptor::local_lockinfo_execed_counters[i]->resize(kPackageNum + 2);


        for(int j = 0; j <= (int)kPackageNum; j++){
            (*MOTAdaptor::local_txn_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::local_txn_exc_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::local_txn_execed_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::local_txn_committed_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::local_txn_index[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::record_commit_txn_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::record_committed_txn_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::remote_merged_txn_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::remote_commit_txn_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::remote_committed_txn_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);

            (*MOTAdaptor::txn_num_ptrs[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::packd_txn_num_ptrs[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::write_abort_before_send_txn_num[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::write_abort_after_send_txn_num[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::write_committed_txn_num[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::read_abort_txn_num[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::read_committed_txn_num[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::total_abort_txn_num[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::read_total_txn_num[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::write_total_txn_num[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::pack_txn_num[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            
            (*MOTAdaptor::limite_txn_num[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);

            /// wzy:lockinfo
            (*MOTAdaptor::local_lockinfo_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::lockinfo_num_ptrs[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::packd_lockinfo_num_ptrs[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::pack_lockinfo_num[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::merge_lockinfo_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            (*MOTAdaptor::local_lockinfo_execed_counters[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
        }
    }

    MOTAdaptor::received_total_pack_num.reserve(MOTAdaptor::_max_length + 1); 
    MOTAdaptor::received_total_txn_num.reserve(MOTAdaptor::_max_length + 1);
    MOTAdaptor::received_total_lockinfo_num.reserve(MOTAdaptor::_max_length + 1);
    MOTAdaptor::received_pack_num.reserve(MOTAdaptor::_max_length + 1);
    MOTAdaptor::received_txn_num.reserve(MOTAdaptor::_max_length + 1);
    MOTAdaptor::should_receive_txn_num.reserve(MOTAdaptor::_max_length + 1);
    MOTAdaptor::is_server_online.reserve(kServerNum + 2);
    MOTAdaptor::online_server_num = std::make_unique<std::atomic<uint64_t>>(kServerNum);
    MOTAdaptor::should_receive_pack_num= std::make_unique<std::atomic<uint64_t>>(kServerNum - 1);

    /// wzy: lockinfo
    MOTAdaptor::received_lockinfo_num.reserve(MOTAdaptor::_max_length + 1);
    MOTAdaptor::should_receive_lockinfo_num.reserve(MOTAdaptor::_max_length + 1);
    MOTAdaptor::received_total_lockinfo_num.reserve(MOTAdaptor::_max_length + 1);

    for(int i = 0; i <= static_cast<int>(MOTAdaptor::_max_length); i ++){
        MOTAdaptor::received_total_pack_num.emplace_back(std::make_unique<std::atomic<uint64_t>>(0));
        MOTAdaptor::received_total_txn_num.emplace_back(std::make_unique<std::atomic<uint64_t>>(0));
        MOTAdaptor::received_pack_num.emplace_back(std::vector<std::unique_ptr<std::atomic<uint64_t>>>(0));
        MOTAdaptor::received_txn_num.emplace_back(std::vector<std::unique_ptr<std::atomic<uint64_t>>>(0));
        MOTAdaptor::should_receive_txn_num.emplace_back(std::vector<std::unique_ptr<std::atomic<uint64_t>>>(0));

        /// wzy: lockinfo
        MOTAdaptor::received_lockinfo_num.emplace_back(std::vector<std::unique_ptr<std::atomic<uint64_t>>>(0));
        MOTAdaptor::should_receive_lockinfo_num.emplace_back(std::vector<std::unique_ptr<std::atomic<uint64_t>>>(0));
        MOTAdaptor::received_total_lockinfo_num.emplace_back(std::make_unique<std::atomic<uint64_t>>(0));

        for(int j = 0; j < (int)kServerIp.size() + 2; j++){
            MOTAdaptor::received_pack_num[i].push_back(std::make_unique<std::atomic<uint64_t>>(0));
            MOTAdaptor::received_txn_num[i].push_back(std::make_unique<std::atomic<uint64_t>>(0));
            MOTAdaptor::should_receive_txn_num[i].push_back(std::make_unique<std::atomic<uint64_t>>(0));

            /// wzy: lockinfo
            MOTAdaptor::received_lockinfo_num[i].push_back(std::make_unique<std::atomic<uint64_t>>(0));
            MOTAdaptor::should_receive_lockinfo_num[i].push_back(std::make_unique<std::atomic<uint64_t>>(0));
        }
    }
    for(int j = 0; j < (int)kServerIp.size() + 2; j++) {
        MOTAdaptor::is_server_online.push_back(std::make_unique<std::atomic<uint64_t>>(0));
    }
    for(int j = 0; j < (int)kServerNum; j++) {
        MOTAdaptor::is_server_online[j]->store(1);
    }

    MOTAdaptor::received_epoch.reserve(MOTAdaptor::_max_length + 1);
    uint64_t val = 1;
    if(is_cache_server_available) {
        val = 0;
    }
    for(int i = 0; i <= static_cast<int>(MOTAdaptor::_max_length); i ++) {
        MOTAdaptor::received_epoch.emplace_back(std::make_unique<std::atomic<uint64_t>>(val));
    }

    if(is_fault_tolerance_enable) {
        cache_txn_queues.resize(MOTAdaptor::_max_length + 2);
        for(int i = 0; i < (int) MOTAdaptor::_max_length; i ++) {
            cache_txn_queues[i] == std::make_shared<BlockingConcurrentQueue<std::unique_ptr<merge::Message>>>();
        }
    }

    MOTAdaptor::AddPhysicalEpoch();
    MOTAdaptor::isInited.store(true);       // wzy:
    init_ok.store(true);
}
























// read 565 write 297 tot 504 pack 297
// log read 334 write 170 tot 504

// total 54 write 42
// 发送 41
// 41 42


void OUTPUTLOG(string s){
    auto epoch_mod = MOTAdaptor::GetLogicalEpoch() % MOTAdaptor::max_length;
    double txn_avg_time = 0, txn_avg_epoch = 0, txn_avg_readCnt = 0, txn_avg_writeCnt = 0, txn_avg_lockCnt = 0, txn_avg_hotCnt = 0;
    double txn_avg_switch_time = 0, txn_avg_read_lock_time = 0, txn_avg_write_lock_time = 0, txn_avg_validate_time = 0, txn_avg_validate_hotOcc_time = 0;
    double txn_avg_switch_rlock_time = 0, txn_avg_switch_wlock_time = 0, txn_avg_switch_sentinel_time = 0;


    if(MOTAdaptor::commit_txn_num.load() != 0) {
        txn_avg_time = MOTAdaptor::txn_total_time.load() / MOTAdaptor::commit_txn_num.load();
        txn_avg_epoch = MOTAdaptor::txn_total_epoch.load() / MOTAdaptor::commit_txn_num.load();
        txn_avg_readCnt = MOTAdaptor::txn_total_readCnt.load() / MOTAdaptor::commit_txn_num.load();
        txn_avg_writeCnt = MOTAdaptor::txn_total_writeCnt.load() / MOTAdaptor::commit_txn_num.load();
        txn_avg_hotCnt = MOTAdaptor::txn_total_hotCnt.load() / MOTAdaptor::commit_txn_num.load();
    }

    double interactive_txn_avg_time = 0, interactive_pcc_txn_avg_time = 0, interactive_occ_txn_avg_time = 0, stored_txn_avg_time = 0;
    if(MOTAdaptor::commit_interactive_txn_num.load() != 0) {
        txn_avg_lockCnt = MOTAdaptor::txn_total_lockCnt.load() / MOTAdaptor::commit_interactive_txn_num.load();
        interactive_txn_avg_time = MOTAdaptor::interactive_txn_total_time.load() / MOTAdaptor::commit_interactive_txn_num.load();
    }

    if(MOTAdaptor::commit_pcc_interactive_txn_num.load() != 0) interactive_pcc_txn_avg_time = MOTAdaptor::interactive_pcc_txn_total_time.load() / MOTAdaptor::commit_pcc_interactive_txn_num.load();
    if(MOTAdaptor::commit_occ_interactive_txn_num.load() != 0) interactive_occ_txn_avg_time = MOTAdaptor::interactive_occ_txn_total_time.load() / MOTAdaptor::commit_occ_interactive_txn_num.load();
    if(MOTAdaptor::commit_stored_txn_num.load() != 0) stored_txn_avg_time = MOTAdaptor::stored_txn_total_time.load() / MOTAdaptor::commit_stored_txn_num.load();



    double txn_avg_abort_time = 0, interactive_txn_abort_time = 0;
    if (MOTAdaptor::Abort_txn_num.load() != 0) txn_avg_abort_time = MOTAdaptor::txn_abort_time.load() / MOTAdaptor::Abort_txn_num.load();
    if (MOTAdaptor::Abort_interactive_txn_num.load() != 0) interactive_txn_abort_time = MOTAdaptor::interactive_txn_abort_time.load() / MOTAdaptor::Abort_interactive_txn_num.load();

    if (MOTAdaptor::txn_total_switchCnt.load() != 0) txn_avg_switch_time = MOTAdaptor::txn_total_switchTime.load() / MOTAdaptor::txn_total_switchCnt.load();
    if (MOTAdaptor::txn_total_read_lockCnt.load() != 0) txn_avg_read_lock_time = MOTAdaptor::txn_total_read_lockTime.load() / MOTAdaptor::txn_total_read_lockCnt.load();
    if (MOTAdaptor::txn_total_write_lockCnt.load() != 0) txn_avg_write_lock_time = MOTAdaptor::txn_total_write_lockTime.load() / MOTAdaptor::txn_total_write_lockCnt.load();
    if (MOTAdaptor::txn_total_validate_lockCnt.load() != 0) txn_avg_validate_time = MOTAdaptor::txn_total_validate_lockTime.load() / MOTAdaptor::txn_total_validate_lockCnt.load();
    if (MOTAdaptor::txn_total_validate_hotOccCnt.load() != 0) txn_avg_validate_hotOcc_time = MOTAdaptor::txn_total_validate_hotOccTime.load() / MOTAdaptor::txn_total_validate_hotOccCnt.load();

    if (MOTAdaptor::txn_total_switchTime_RLockCnt.load() != 0) txn_avg_switch_rlock_time = MOTAdaptor::txn_total_switchTime_RLock.load() / MOTAdaptor::txn_total_switchTime_RLockCnt.load();
    if (MOTAdaptor::txn_total_switchTime_WLockCnt.load() != 0) txn_avg_switch_wlock_time = MOTAdaptor::txn_total_switchTime_WLock.load() / MOTAdaptor::txn_total_switchTime_WLockCnt.load();
    if (MOTAdaptor::txn_total_switchTime_SentinelCnt.load() != 0) txn_avg_switch_sentinel_time = MOTAdaptor::txn_total_switchTime_Sentinel.load() / MOTAdaptor::txn_total_switchTime_SentinelCnt.load();

    // TODO: 30s内的avg
    double txn_temp_avg_time = 0, txn_temp_avg_epoch = 0, txn_temp_avg_readCnt = 0, txn_temp_avg_writeCnt = 0, txn_temp_avg_lockCnt = 0, txn_temp_avg_hotCnt = 0;
    double txn_temp_avg_switch_time = 0, txn_temp_avg_read_lock_time = 0, txn_temp_avg_write_lock_time = 0, txn_temp_avg_validate_time = 0, txn_temp_avg_validate_hotOcc_time = 0;

    uint64_t temp_commit_txn_num1 = MOTAdaptor::temp_commit_txn_num.load();
    if(temp_commit_txn_num1 != 0) {
        txn_temp_avg_time = MOTAdaptor::txn_temp_total_time.load() / temp_commit_txn_num1;
        txn_temp_avg_epoch = MOTAdaptor::txn_temp_total_epoch.load() / temp_commit_txn_num1;
        txn_temp_avg_readCnt = MOTAdaptor::txn_temp_total_readCnt.load() / temp_commit_txn_num1;
        txn_temp_avg_writeCnt = MOTAdaptor::txn_temp_total_writeCnt.load() / temp_commit_txn_num1;
        txn_temp_avg_hotCnt = MOTAdaptor::txn_temp_total_hotCnt.load() / temp_commit_txn_num1;
    }

    uint64_t txn_temp_total_switchCnt1 = MOTAdaptor::txn_temp_total_switchCnt.load(), txn_temp_total_read_lockCnt1 = MOTAdaptor::txn_temp_total_read_lockCnt.load(), txn_temp_total_write_lockCnt1 = MOTAdaptor::txn_temp_total_write_lockCnt.load(),
             txn_temp_total_validate_lockCnt1 = MOTAdaptor::txn_temp_total_validate_lockCnt.load(), txn_temp_total_validate_hotOccCnt1 = MOTAdaptor::txn_temp_total_validate_hotOccCnt.load();

    if (txn_temp_total_switchCnt1 != 0) txn_temp_avg_switch_time = MOTAdaptor::txn_temp_total_switchTime.load() / txn_temp_total_switchCnt1;
    if (txn_temp_total_read_lockCnt1 != 0) txn_temp_avg_read_lock_time = MOTAdaptor::txn_temp_total_read_lockTime.load() / txn_temp_total_read_lockCnt1;
    if (txn_temp_total_write_lockCnt1 != 0) txn_temp_avg_write_lock_time = MOTAdaptor::txn_temp_total_write_lockTime.load() / txn_temp_total_write_lockCnt1;
    if (txn_temp_total_validate_lockCnt1 != 0) txn_temp_avg_validate_time = MOTAdaptor::txn_temp_total_validate_lockTime.load() / txn_temp_total_validate_lockCnt1;
    if (txn_temp_total_validate_hotOccCnt1 != 0) txn_temp_avg_validate_hotOcc_time = MOTAdaptor::txn_temp_total_validate_hotOccTime.load() / txn_temp_total_validate_hotOccCnt1;

    double copy_graph_time = 0, detect_graph_time = 0;
    uint64_t detect_count = MOTAdaptor::wait_for_graph.detect_count.load();
    if (detect_count != 0) {
        copy_graph_time = MOTAdaptor::wait_for_graph.copy_time.load() / detect_count;
        detect_graph_time = MOTAdaptor::wait_for_graph.detect_time.load() / detect_count;
    }

    MOT_LOG_INFO("%s physical %llu logical %llu epoch_mod %llu \
    \n== txn ShouldExecTxnNum %llu LocalTxnExc %llu \
    ReceivedPackNum %llu ShouldReceivePackNum %llu \
    RemoteMergedTxn %llu ShouldReceiveTxnNum %llu \
    LocalTxn %llu LocalCommitTxn %llu RemoteCommitTxn %llu \
    RemoteCommittedTxn %llu \
    RaftGetLeaderAcceptEpochState %llu GetShouldReceiveAcceptNum %llu RaftIsEpochTransmitComplete %llu \
    RaftGetFollowerCommitEpochState %llu RaftServerState %llu \
    RaftIsEpochReceiveComplete %llu \
    \n== read_abort %llu read_committed %llu write_committed %llu \
    write_abort_before_send %llu write_abort_after_send %llu \
    total_abort %llu read_total %llu \
    write_total %llu total %llu \
    pack_txn %llu packed_txn %llu \
    record_commit %llu record_committed %llu \
    \n== lock_info LocalLockinfo %llu pack_lockinfo %llu packed_lockinfo %llu \
    ReceiveLockinfoNum %llu  ShouldReceiveLockinfoNum %llu \
    RemoteLockinfo %llu \
    GrantLockCount %llu ActiveQueueSize size %llu\
    \nDynamicHotRows size %llu  DynamicHotRows total visits %llu \
    \nWaitForGraph vertexNum %llu copy_graph_time %f detect_graph_time %f detect_count %llu \
    \n== [STATISTICS]  commit_txn_num %llu PCC_txn_num %llu commit_interactive_txn_num %llu start_txn_num %llu start_interactive_txn_num %llu \
    start_num_start_txn %llu start_num_txn_construct %llu \
    \nPCC_txn_num %llu PCC_priority_txn_num %llu PCC_hot_visits_txn_num %llu \
    \ntxn_avg_time %f interactive_avg_time %f interactive_pcc_avg_time %f interactive_occ_avg_time %f stored_txn_avg_time %f \
    txn_avg_epoch %f txn_avg_readCnt %f txn_avg_writeCnt %f txn_avg_lockCnt %f txn_avg_hotCnt %f txn_total_lockCnt %llu\
    \ntxn_total_switchTime %llu txn_total_read_lockTime %llu txn_total_write_lockTime %llu txn_total_validate_lockTime %llu txn_total_validate_hotOccTime %llu \
    \ntxn_total_switchCnt %llu txn_avg_read_lockCnt %llu txn_avg_write_lockCnt %llu txn_avg_validate_lockCnt %llu txn_avg_validate_hotOccCnt %llu \
    \ntxn_avg_switchTime %f txn_avg_read_lockTime %f txn_avg_write_lockTime %f txn_avg_validate_lockTime %f txn_avg_validate_hotOccTime %f \
    \txn_avg_switch_rlock_time %f txn_avg_switch_wlock_time %f txn_avg_switch_sentinel_time %f\
    \ntxn_avg_abort_time %f interactive_txn_abort_time %f \
    \n== [RECENT] txn_temp_avg_time %f txn_temp_avg_epoch %f txn_temp_avg_readCnt %f txn_temp_avg_writeCnt %f txn_temp_avg_hotCnt %f \
    \n txn_temp_avg_switch_time %f txn_temp_avg_read_lock_time %f txn_temp_avg_write_lock_time %f txn_temp_avg_validate_time %f txn_temp_avg_validate_hotOcc_time %f \
    \n== [LOCKINFO]  local_lock_num %llu local_unlock_num %llu remote_lock_num %llu remote_unlock_num %llu send_lock_num %llu receive_lock_num %llu \
    time %llu",

        s.c_str(),
        MOTAdaptor::GetPhysicalEpoch(), MOTAdaptor::GetLogicalEpoch(), epoch_mod,
        MOTAdaptor::LoadShouldExecTxnNum(epoch_mod)  , MOTAdaptor::GetLocalTxnExcCounters(epoch_mod),
        MOTAdaptor::GetReceivedPackNum(epoch_mod), MultiRaftState::GetShouldReceivePackNum(),
        MOTAdaptor::GetRemoteMergedTxnCounters(epoch_mod), MOTAdaptor::GetShouldReceiveTxnNum(epoch_mod),
        MOTAdaptor::GetLocalTxnCounters(epoch_mod), MOTAdaptor::GetLocalCommittedCounters(epoch_mod), MOTAdaptor::GetRemoteCommitTxnCounters(epoch_mod),
        MOTAdaptor::GetRemoteCommittedTxnCounters(epoch_mod),

        MultiRaftState::GetLeaderAcceptEpochState(epoch_mod), MultiRaftState::GetShouldReceiveAcceptNum(), MultiRaftState::IsEpochReceiveComplete(epoch_mod),
        MultiRaftState::GetFollowerCommitEpochState(epoch_mod), MultiRaftState::GetServerState(), MultiRaftState::IsEpochReceiveComplete(epoch_mod),

        MOTAdaptor::LoadReadAbortTxnNum(epoch_mod), MOTAdaptor::LoadReadCommittedTxnNum(epoch_mod), MOTAdaptor::LoadWriteCommittedTxnNum(epoch_mod),
        MOTAdaptor::LoadWriteAbortBeforeSendTxnNum(epoch_mod), MOTAdaptor::LoadWriteAbortAfterSendTxnNum(epoch_mod),
        MOTAdaptor::LoadTotalAbortTxnNum(epoch_mod), MOTAdaptor::LoadReadTotalTxnNum(epoch_mod),
        MOTAdaptor::LoadWriteTotalTxnNum(epoch_mod), MOTAdaptor::LoadChangeSet(epoch_mod),
        MOTAdaptor::LoadPackTxnNum(epoch_mod), MOTAdaptor::LoadPackedTxnNum(epoch_mod),
        MOTAdaptor::GetRecordCommitTxnCounters(epoch_mod), MOTAdaptor::GetRecordCommittedTxnCounters(epoch_mod),

        // lockinfo
        MOTAdaptor::GetLocalLockinfoCounters(epoch_mod), MOTAdaptor::LoadPackLockinfoNum(epoch_mod), MOTAdaptor::LoadPackedLockinfoNum(epoch_mod),
        MOTAdaptor::GetReceivedLockinfoNum(epoch_mod), MOTAdaptor::GetShouldReceiveLockinfoNum(epoch_mod),
        MOTAdaptor::GetMergeLockinfoCounters(epoch_mod),
        MOTAdaptor::GetLockGrantedNum(), MOTAdaptor::GetActiveQueueNum(MOTAdaptor::lock_thread_num),
        MOTAdaptor::dynamic_hot_rows.size(), MOTAdaptor::dynamic_hot_rows.visits_num,
        MOTAdaptor::wait_for_graph.getVertexNum(), copy_graph_time, detect_graph_time, detect_count,

        MOTAdaptor::commit_txn_num.load(), MOTAdaptor::pessimisitic_txn_num.load(), MOTAdaptor::commit_interactive_txn_num.load(), MOTAdaptor::start_txn_num.load(), MOTAdaptor::start_interactive_txn_num.load(),
        MOTAdaptor::start_num_start_txn.load(), MOTAdaptor::start_num_txn_construct.load(),
        MOTAdaptor::pessimisitic_txn_num.load(), MOTAdaptor::pessimisitic_priority_txn_num.load(), MOTAdaptor::pessimisitic_hot_visits_txn_num.load(),
        txn_avg_time, interactive_txn_avg_time, interactive_pcc_txn_avg_time, interactive_occ_txn_avg_time, stored_txn_avg_time,
        txn_avg_epoch, txn_avg_readCnt, txn_avg_writeCnt, txn_avg_lockCnt, txn_avg_hotCnt, MOTAdaptor::txn_total_lockCnt.load(),
        MOTAdaptor::txn_total_switchTime.load(), MOTAdaptor::txn_total_read_lockTime.load(), MOTAdaptor::txn_total_write_lockTime.load(), MOTAdaptor::txn_total_validate_lockTime.load(), MOTAdaptor::txn_total_validate_hotOccTime.load(),
        MOTAdaptor::txn_total_switchCnt.load(), MOTAdaptor::txn_total_read_lockCnt.load(), MOTAdaptor::txn_total_write_lockCnt.load(), MOTAdaptor::txn_total_validate_lockCnt.load(), MOTAdaptor::txn_total_validate_hotOccCnt.load(),
        txn_avg_switch_time, txn_avg_read_lock_time, txn_avg_write_lock_time, txn_avg_validate_time, txn_avg_validate_hotOcc_time,
        txn_avg_switch_rlock_time, txn_avg_switch_wlock_time, txn_avg_switch_sentinel_time,
        txn_avg_abort_time, interactive_txn_abort_time,

        txn_temp_avg_time, txn_temp_avg_epoch, txn_temp_avg_readCnt, txn_temp_avg_writeCnt, txn_temp_avg_hotCnt,
        txn_temp_avg_switch_time, txn_temp_avg_read_lock_time, txn_temp_avg_write_lock_time, txn_temp_avg_validate_time, txn_temp_avg_validate_hotOcc_time,

        MOTAdaptor::local_lock_num.load(), MOTAdaptor::local_unlock_num.load(), MOTAdaptor::remote_lock_num.load(), MOTAdaptor::remote_unlock_num.load(), MOTAdaptor::send_lock_num.load(), MOTAdaptor::receive_lock_num.load(),

        now_to_us());
}

void OUTPUTLOGAbort_txn() {
    MOT_LOG_INFO(" ====================== [ OUTPUTLOG Abort INFO ] size of abort_transcation_csn_set : %llu , deadlock_abort_set size : %llu, DeadLock_abort_num : %llu , LockCheck_abort_num : %llu , Commit_abort_num : %llu \
                   \n===== Total : Abort_num %llu , Abort_interactive_txn_num %llu , Abort_pcc_interactive_txn_num %llu ,Remote_Abort_interactive_txn_num %llu\
                   \n===== CommitAbort Interactive total: Commit_abort_pcc_total_interactive_num %llu, Commit_abort_occ_total_interactive_num %llu\
                    \n===== CommitCheck Interactive :  Commit_abort_interactive_num %llu , CommitCheck_abort_interactive_num %llu , CommitPhase_abort_interactive_num %llu , CommitUpdate_abort_interactive_num %llu , CommitCheck_deadlock_abort_interactive_num %llu , Abort_transcation_csn_set_abort_interactive_num %llu , ValidateReadInMergeForSnap_abort_interactive_num %llu , ValidateReadInMerge_abort_interactive_num %llu , InsertTxntoLocalChangeSet_abort_interactive_num %llu \
                    \n===== CommitPhase Interactive : ValidateAndSetWriteForCommit abort : %llu , IsRowAvailable abort : %llu , origSentinel abort : %llu \
                    \n===== CommitPhase : ValidateAndSetWriteForCommit abort : %llu, LockCheckIsRowAvailable abort : %llu , IsRowAvailable abort : %llu , origSentinel abort : %llu \
                    \n===== Local TxnTotal : CommitPhase_abort_num %llu , CommitCheck_abort_num %llu , Abort_transcation_csn_set_abort_num %llu \
                    \n===== Remote TxnTotal : CommitPhase_abort_num %llu , CommitCheck_abort_num %llu \
                    \n===== Switch to PCC : Switch_validation_abort_num %llu , Switch_validation_occ_abort_num %llu Switch_validation_pcc_abort_num %llu\
                    \n===== Lock in PCC : ReadLock_pcc_abort_num %llu , WriteLock_pcc_abort_num %llu ReadLock_switch_pcc_abort_num %llu , WriteLock_switch_pcc_abort_num %llu \
                    \n===== HotLock in PCC : HotRow_quick_validation_abort_num %llu , HotRow_read_validation_abort_num %llu , HotRow_write_validation_abort_num %llu\
                    \n===== Silo Abort: Silo_validation_abort %llu , Silo_quick_validation_abort_num %llu , Silo_lockheader_abort_num %llu, Silo_lockheader_abort_by_interactive_num %llu, write_validation_abort_num %llu, read_validation_abort_num %llu",


        MOTAdaptor::abort_transcation_csn_set.size(), MOTAdaptor::deadlock_abort_set.size(), MOTAdaptor::DeadLock_abort_num.load(), MOTAdaptor::LockCheck_abort_num.load(), MOTAdaptor::Commit_abort_num.load(),
        MOTAdaptor::Abort_txn_num.load(), MOTAdaptor::Abort_interactive_txn_num.load(), MOTAdaptor::Abort_pcc_interactive_txn_num.load(), MOTAdaptor::Remote_Abort_interactive_txn_num.load(),
        MOTAdaptor::Commit_abort_pcc_total_interactive_num.load(), MOTAdaptor::Commit_abort_occ_total_interactive_num.load(),
        MOTAdaptor::Commit_abort_interactive_num.load(), MOTAdaptor::CommitCheck_abort_interactive_num.load(), MOTAdaptor::CommitPhase_abort_interactive_num.load(), MOTAdaptor::CommitUpdate_abort_interactive_num.load(), MOTAdaptor::CommitCheck_deadlock_abort_interactive_num.load(), MOTAdaptor::Abort_transcation_csn_set_abort_interactive_num.load(), MOTAdaptor::ValidateReadInMergeForSnap_abort_interactive_num.load(), MOTAdaptor::ValidateReadInMerge_abort_interactive_num.load(), MOTAdaptor::InsertTxntoLocalChangeSet_abort_interactive_num.load(),
        MOTAdaptor::CommitPhase_ValidateAndSetWriteForCommit_abort_interactive_num.load(), MOTAdaptor::CommitPhase_IsRowAvailable_abort_interactive_num.load(), MOTAdaptor::CommitPhase_origSentinel_abort_interactive_num.load(),
        MOTAdaptor::CommitPhase_ValidateAndSetWriteForCommit_abort_num.load(), MOTAdaptor::CommitLockCheck_IsRowAvailable_abort_num.load(), MOTAdaptor::CommitPhase_IsRowAvailable_abort_num.load(), MOTAdaptor::CommitPhase_origSentinel_abort_num.load(),
        MOTAdaptor::CommitPhase_abort_num.load(), MOTAdaptor::CommitCheck_abort_num.load(), MOTAdaptor::Abort_transcation_csn_set_abort_num.load(),
        MOTAdaptor::Remote_ValidateAndSetWriteForRemote_abort_num.load(), MOTAdaptor::Remote_CommitCheck_abort_num.load(),
        MOTAdaptor::Switch_validation_abort_num.load(), MOTAdaptor::Switch_validation_occ_abort_num.load(), MOTAdaptor::Switch_validation_pcc_abort_num.load(),
        MOTAdaptor::ReadLock_pcc_abort_num.load(), MOTAdaptor::WriteLock_pcc_abort_num.load(), MOTAdaptor::ReadLock_switch_pcc_abort_num.load(), MOTAdaptor::WriteLock_switch_pcc_abort_num.load(),
        MOTAdaptor::HotRow_quick_validation_abort_num.load(), MOTAdaptor::HotRow_read_validation_abort_num.load(), MOTAdaptor::HotRow_write_validation_abort_num.load(),
        MOTAdaptor::Silo_validation_abort_num.load(), MOTAdaptor::Silo_quick_validation_abort_num.load(), MOTAdaptor::Silo_lockheader_abort_num.load(), MOTAdaptor::Silo_lockheader_abort_by_interactive_num.load(), MOTAdaptor::Silo_write_validation_abort_num.load(), MOTAdaptor::Silo_read_validation_abort_num.load());
}


void EpochLogicalTimerManagerThreadMain(uint64_t id){
    SetCPU();
    MOT_LOG_INFO("线程开始工作 EpochLogicalTimerManagerThreadMain ");
    uint64_t remote_merged_txn_num = 0, remote_commit_txn_num = 0, current_local_txn_num = 0, remote_received_txn_num = 0,
        cnt = 0, epoch = 1, epoch_mod = 1, last_epoch_mod = 0, cache_server_available = 1, total_commit_txn_num = 0;
    uint64_t remote_lockinfo_num = 0;
    while(!init_ok.load()) usleep(200);

    if(is_cache_server_available)
        cache_server_available = 0;

//    while(true) usleep(100000000000);

    if(is_full_async_exec) {
        last_epoch_mod = 1;
        for(;;) {
            total_commit_txn_num += MOTAdaptor::GetRecordCommitTxnCounters(last_epoch_mod);
            MOT_LOG_INFO("===事务写入完成 %llu, %llu %llu total %llu ", MOTAdaptor::GetRecordCommittedTxnCounters(last_epoch_mod), 
                MOTAdaptor::GetRecordCommitTxnCounters(last_epoch_mod), total_commit_txn_num, now_to_us());
            OUTPUTLOG("=等待本地事务进入commit完成");
            usleep(1000000);
            MOTAdaptor::ClearMergeEpochState();
        }
    }
    
    if(is_sync_exec) {
        for(;;) {
            //等所有上一个epoch 的事务写完 再开始下一个epoch
            cnt = 0;

            while(MOTAdaptor::GetPhysicalEpoch() <= MOTAdaptor::GetLogicalEpoch() + kDelayEpochNum) usleep(200);
            
            while((MOTAdaptor::LoadShouldExecTxnNum(epoch_mod)) 
                > MOTAdaptor::GetLocalTxnExcCounters(epoch_mod) ){
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待本地事务进入commit完成");
                }
                usleep(200);
            } 
            while(MOTAdaptor::GetReceivedPackNum(epoch_mod) < MOTAdaptor::GetShouldReceivePackNum() ||
                MOTAdaptor::GetReceivedTxnNum(epoch_mod) < MOTAdaptor::GetShouldReceiveTxnNum(epoch_mod) ||
                MOTAdaptor::GetReceivedLockinfoNum(epoch_mod) < MOTAdaptor::GetShouldReceiveLockinfoNum(epoch_mod)  // wzy:
                ) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待远端pack接收完成");
                }
                usleep(200);
            }
            while(is_raft_enable && !MultiRaftState::IsEpochTransmitComplete(epoch_mod)) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待远端pack接收完成 Raft Transmit");
                }
                usleep(200);
            }

            while(is_raft_enable && !MultiRaftState::IsEpochReceiveComplete(epoch_mod)) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待远端pack接收完成 Raft Commit");
                }
                usleep(200);
            }

            while(is_remote_cache_server_enable && MultiRaftState::GetLeaderAcceptEpochState(epoch) < 1) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待远端cache server 接受完成 RemoteCacheServer");
                }
                usleep(200);
            }

            // wzy: 接收到的所有lock info 都以进入lock request 队列
            if (cc_mode == 1) {
                remote_lockinfo_num = MOTAdaptor::GetShouldReceiveLockinfoNum(epoch_mod);
                while(MOTAdaptor::GetMergeLockinfoCounters(epoch_mod) < remote_lockinfo_num) {
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("=等待Lock info接收并执行完成");
                    }
                    usleep(200);
                }

                while(!MOTAdaptor::IsLocalLockinfoCountersExced(epoch_mod)){
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("=等待本地Lock info完成");
                    }
                    usleep(200);
                }
            } else {
                OUTPUTLOG("=cc_mode 2 无需等待Lock info");
            }

            // wzy: merge执行结束开始上锁
            MOTAdaptor::SetLockGrantedNum(0);
            MOTAdaptor::SetLockExeced(true);
            ////

            remote_merged_txn_num = MOTAdaptor::GetShouldReceiveTxnNum(epoch_mod);
            while(MOTAdaptor::GetRemoteMergedTxnCounters(epoch_mod) < remote_merged_txn_num) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待远端事务接收并执行完成");
                }
                usleep(200);
            }
            while(!MOTAdaptor::IsLocalTxnCountersExced(epoch_mod)){
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待本地事务完成");
                }
                usleep(200);
            }

            while(!MOTAdaptor::IsCacheServerStored(epoch_mod)) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待cache接收完成");
                }
                usleep(200);
            }


            // ======= Merge结束 开始Commit ============   
            MOTAdaptor::SetRecordCommitted(false);
            MOTAdaptor::SetRemoteRecordCommitted(false);

            MOTAdaptor::SetRemoteExeced(true);

            current_local_txn_num = static_cast<uint64_t>(MOTAdaptor::GetLocalTxnCounters(epoch_mod));
            remote_commit_txn_num = MOTAdaptor::GetRemoteCommitTxnCounters(epoch_mod);

            // wzy: 对row上锁和死锁检测完毕，之后再进行下一个epoch
            if (cc_mode == 1) {
                while (!MOTAdaptor::IsLockGranted()) {
                    usleep(200);
                }
                MOT_LOG_INFO("==一个Epoch锁处理完成");
            } else {
                OUTPUTLOG("=cc_mode 2 无需等待Epoch锁处理");
            }

            OUTPUTLOG("==进行一个Epoch的合并");
            while(MOTAdaptor::GetRemoteCommittedTxnCounters(epoch_mod) < remote_commit_txn_num) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("==进行一个Epoch的合并 1");
                }
                usleep(200);
            }
            MOTAdaptor::SetRemoteRecordCommitted(true);
            while(!MOTAdaptor::IsLocalTxnCountersCommitted(epoch_mod)) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("==进行一个Epoch的合并 2");
                }
                usleep(200);
            }

//            // wzy: 对row上锁和死锁检测完毕，之后再进行下一个epoch
//            MOTAdaptor::SetDeadLockDetected(false);
//            while (!MOTAdaptor::IsDeadLockDetected()) {
//                usleep(200);
//            }
//            MOT_LOG_INFO("==一个Epoch 死锁检测完成");

            // ============= 结束处理 ==================
            //远端事务已经写完，不写完无法开始下一个logical epoch
            while(MOTAdaptor::GetRecordCommittedTxnCounters(epoch_mod) != MOTAdaptor::GetRecordCommitTxnCounters(epoch_mod)) usleep(200);
            MOTAdaptor::SetRecordCommitted(true);

            total_commit_txn_num += MOTAdaptor::GetRecordCommitTxnCounters(epoch_mod);
            MOT_LOG_INFO("===epoch所有事务写入完成 %llu, %llu %llu total %llu ", MOTAdaptor::GetRecordCommittedTxnCounters(epoch_mod), 
                MOTAdaptor::GetRecordCommitTxnCounters(epoch_mod), total_commit_txn_num, now_to_us());

            OUTPUTLOG("==================完成一个Epoch的合并");
            OUTPUTLOGAbort_txn();

            if (kInteractive_Active && epoch_mod % kHotCntEpochLen == 0) {
                uint64_t time1 = now_to_us();
                uint64_t pre_num = MOTAdaptor::dynamic_hot_rows.size();
                MOTAdaptor::dynamic_hot_rows.get_hot_rows_by_freq();       // wzy: 每100epoch生成热门row
                uint64_t time2 = now_to_us();
                MOT_LOG_INFO("===完成hot rows选取, 共 %llu 个hot rows, 前 10 轮共 %llu 个hot rows, 总共访问 %llu 次, 对hot rows访问总共 %llu 次, 总共耗时 %llu ", MOTAdaptor::dynamic_hot_rows.size(), pre_num, MOTAdaptor::dynamic_hot_rows.visits_num, MOTAdaptor::dynamic_hot_rows.hot_rows_visit_num, time2 - time1);
                MOTAdaptor::dynamic_hot_rows.visits_num = 0;
                MOTAdaptor::dynamic_hot_rows.hot_rows_visit_num = 0;

                MOTAdaptor::temp_commit_txn_num.store(0);
                MOTAdaptor::txn_temp_total_time.store(0);
                MOTAdaptor::txn_temp_total_epoch.store(0);
                MOTAdaptor::txn_temp_total_readCnt.store(0);
                MOTAdaptor::txn_temp_total_writeCnt.store(0);
                MOTAdaptor::txn_temp_total_hotCnt.store(0);

                MOTAdaptor::txn_temp_total_switchCnt.store(0);
                MOTAdaptor::txn_temp_total_read_lockCnt.store(0);
                MOTAdaptor::txn_temp_total_write_lockCnt.store(0);
                MOTAdaptor::txn_temp_total_validate_lockCnt.store(0);
                MOTAdaptor::txn_temp_total_validate_hotOccCnt.store(0);

                MOTAdaptor::txn_temp_total_switchTime.store(0);
                MOTAdaptor::txn_temp_total_read_lockTime.store(0);
                MOTAdaptor::txn_temp_total_write_lockTime.store(0);
                MOTAdaptor::txn_temp_total_validate_lockTime.store(0);
                MOTAdaptor::txn_temp_total_validate_hotOccTime.store(0);
            }


            // MultiRaftState::ClearRaftEpochState(epoch_mod);

            MOTAdaptor::RemoteCacheClear(epoch_mod);
            MOTAdaptor::ClearMergeEpochState();
            MOTAdaptor::AddLogicalEpoch();
            last_epoch_mod = epoch_mod;
            epoch ++;
            epoch_mod = epoch % max_length;
            epoch_commit_time = commit_time = now_to_us();
        }
    }
    else {
        for(;;){
            //等所有上一个epoch 的事务写完 再开始下一个epoch
            cnt = 0;

            while(MOTAdaptor::GetPhysicalEpoch() <= MOTAdaptor::GetLogicalEpoch() + kDelayEpochNum) usleep(200);
            
            while(static_cast<uint64_t>(MOTAdaptor::LoadChangeSet(epoch_mod)) 
                > MOTAdaptor::GetLocalTxnExcCounters(epoch_mod)){
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待本地事务进入commit完成");
                }
                usleep(200);
            } 
            while(MOTAdaptor::GetReceivedPackNum(epoch_mod) < MOTAdaptor::GetShouldReceivePackNum() ||
                MOTAdaptor::GetReceivedTxnNum(epoch_mod) < MOTAdaptor::GetShouldReceiveTxnNum(epoch_mod) ||
                MOTAdaptor::GetReceivedLockinfoNum(epoch_mod) < MOTAdaptor::GetShouldReceiveLockinfoNum(epoch_mod)  // wzy:
                ) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待远端pack接收完成");
                }
                usleep(200);
            }
            while(is_raft_enable && !MultiRaftState::IsEpochTransmitComplete(epoch_mod)) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待远端pack接收完成 Raft Transmit");
                }
                usleep(200);
            }

            while(is_raft_enable && !MultiRaftState::IsEpochReceiveComplete(epoch_mod)) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待远端pack接收完成 Raft Commit");
                }
                usleep(200);
            }
            while(is_remote_cache_server_enable && MultiRaftState::GetLeaderAcceptEpochState(epoch) < 1) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待远端cache server 接受完成 RemoteCacheServer");
                }
                usleep(200);
            }

            // wzy:
            if (cc_mode == 1) {
                remote_lockinfo_num = MOTAdaptor::GetShouldReceiveLockinfoNum(epoch_mod);
                while (MOTAdaptor::GetMergeLockinfoCounters(epoch_mod) < remote_lockinfo_num) {
                    cnt++;
                    if (cnt % 100 == 0) {
                        OUTPUTLOG("=等待Lock info接收并执行完成");
                    }
                    usleep(200);
                }

                while (!MOTAdaptor::IsLocalLockinfoCountersExced(epoch_mod)) {
                    cnt++;
                    if (cnt % 100 == 0) {
                        OUTPUTLOG("=等待本地Lock info完成");
                    }
                    usleep(200);
                }
            } else {
                OUTPUTLOG("=cc_mode 2 无需等待Lock info完成");
            }
            // wzy: 停止接收lockinfo lock执行完可以开始上锁
//            MOTAdaptor::SetLockCommitted(false);
//
//            MOTAdaptor::SetLockGrantedNum(0);
//            MOTAdaptor::SetLockGranted(false);
//            MOTAdaptor::SetLockExeced(true);
//            MOT_LOG_INFO("==一个Epoch锁处理开始");
            ////

            remote_merged_txn_num = MOTAdaptor::GetShouldReceiveTxnNum(epoch_mod);
            while(MOTAdaptor::GetRemoteMergedTxnCounters(epoch_mod) < remote_merged_txn_num) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待远端事务接收并执行完成");
                }
                usleep(200);
            }

            while(!MOTAdaptor::IsLocalTxnCountersExced(epoch_mod)){
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待本地事务完成");
                }
                usleep(200);
            }

            while(!MOTAdaptor::IsCacheServerStored(epoch_mod)) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("=等待cache接收完成");
                }
                usleep(200);
            }

            // ======= Merge结束 开始Commit ============   
            MOTAdaptor::SetRecordCommitted(false);
            MOTAdaptor::SetRemoteRecordCommitted(false);

            MOTAdaptor::SetRemoteExeced(true);

            // wzy: 停止接收lockinfo lock执行完可以开始上锁
            MOTAdaptor::SetLockCommitted(false);
            MOTAdaptor::SetLockGrantedNum(0);
            MOTAdaptor::SetLockGranted(false);
            MOTAdaptor::SetLockExeced(true);
            MOT_LOG_INFO("==一个Epoch锁处理开始");
            ////


            current_local_txn_num = static_cast<uint64_t>(MOTAdaptor::GetLocalTxnCounters(epoch_mod));
            remote_commit_txn_num = MOTAdaptor::GetRemoteCommitTxnCounters(epoch_mod);

//            // wzy: 对row上锁和死锁检测完毕，之后再进行下一个epoch
//            while (!MOTAdaptor::IsLockGranted()) {
//                usleep(200);
//            }
//            MOT_LOG_INFO("==一个Epoch锁处理完成");

            OUTPUTLOG("==进行一个Epoch的合并");
            
            while(MOTAdaptor::GetRemoteCommittedTxnCounters(epoch_mod) < remote_commit_txn_num) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("==进行一个Epoch的合并 1");
                }
                usleep(200);
            }
            MOTAdaptor::SetRemoteRecordCommitted(true);
            while(!MOTAdaptor::IsLocalTxnCountersCommitted(epoch_mod)) {
                cnt++;
                if(cnt % 100 == 0){
                    OUTPUTLOG("==进行一个Epoch的合并 2");
                }
                usleep(200);
            }
//
//            // wzy: 停止接收lockinfo lock执行完可以开始上锁
//            MOTAdaptor::SetLockCommitted(false);
//
//            MOTAdaptor::SetLockGrantedNum(0);
//            MOTAdaptor::SetLockGranted(false);
//            MOTAdaptor::SetLockExeced(true);
//            MOT_LOG_INFO("==一个Epoch锁处理开始");

            // wzy: 对row上锁和死锁检测完毕，之后再进行下一个epoch
            if (cc_mode == 1) {
                while (!MOTAdaptor::IsLockGranted()) {
                    usleep(200);
                }
                MOT_LOG_INFO("==一个Epoch锁处理完成");
            } else {
                OUTPUTLOG("=cc_mode 2 无需等待Epoch锁处理完成");
            }

            // wzy: 对row上锁和死锁检测完毕，之后再进行下一个epoch
//            MOTAdaptor::SetDeadLockDetected(false);
//            while (!MOTAdaptor::IsDeadLockDetected()) {
//                usleep(200);
//            }
//            MOT_LOG_INFO("==一个Epoch 死锁检测完成");

            remote_received_txn_num = MOTAdaptor::GetReceivedTxnNum(epoch_mod);

            auto temp_record_committed = MOTAdaptor::GetRecordCommittedTxnCounters(epoch_mod);
            auto temp_record_commit = MOTAdaptor::GetRecordCommitTxnCounters(epoch_mod);
            while(MOTAdaptor::GetRecordCommittedTxnCounters(epoch_mod) != MOTAdaptor::GetRecordCommitTxnCounters(epoch_mod)) usleep(200);

            MOTAdaptor::SetRecordCommitted(true);
            total_commit_txn_num += MOTAdaptor::GetRecordCommitTxnCounters(epoch_mod);

            MOT_LOG_INFO("===epoch所有事务写入完成 %llu, %llu %llu total %llu ", MOTAdaptor::GetRecordCommittedTxnCounters(epoch_mod),
                MOTAdaptor::GetRecordCommitTxnCounters(epoch_mod), total_commit_txn_num, now_to_us());

            OUTPUTLOG("==================完成一个Epoch的合并");
            OUTPUTLOGAbort_txn();
            // ============= 结束处理 ==================
            //远端事务已经写完，不写完无法开始下一个logical epoch

            if (kInteractive_Active && epoch_mod % 100 == 0) {
                uint64_t time1 = now_to_us();
                uint64_t pre_num = MOTAdaptor::dynamic_hot_rows.size();
                MOTAdaptor::dynamic_hot_rows.get_hot_rows_by_freq();       // wzy: 生成热门row
                uint64_t time2 = now_to_us();
                MOT_LOG_INFO("===完成hot rows选取, 共 %llu 个hot rows, 前 10 轮共 %llu 个hot rows, 总共访问 %llu 次, 对hot rows访问总共 %llu 次, 总共耗时 %llu ", MOTAdaptor::dynamic_hot_rows.size(), pre_num, MOTAdaptor::dynamic_hot_rows.visits_num, MOTAdaptor::dynamic_hot_rows.hot_rows_visit_num, time2 - time1);
                MOTAdaptor::dynamic_hot_rows.visits_num = 0;
                MOTAdaptor::dynamic_hot_rows.hot_rows_visit_num = 0;

                MOTAdaptor::temp_commit_txn_num.store(0);
                MOTAdaptor::txn_temp_total_time.store(0);
                MOTAdaptor::txn_temp_total_epoch.store(0);
                MOTAdaptor::txn_temp_total_readCnt.store(0);
                MOTAdaptor::txn_temp_total_writeCnt.store(0);
                MOTAdaptor::txn_temp_total_hotCnt.store(0);

                MOTAdaptor::txn_temp_total_switchCnt.store(0);
                MOTAdaptor::txn_temp_total_read_lockCnt.store(0);
                MOTAdaptor::txn_temp_total_write_lockCnt.store(0);
                MOTAdaptor::txn_temp_total_validate_lockCnt.store(0);
                MOTAdaptor::txn_temp_total_validate_hotOccCnt.store(0);

                MOTAdaptor::txn_temp_total_switchTime.store(0);
                MOTAdaptor::txn_temp_total_read_lockTime.store(0);
                MOTAdaptor::txn_temp_total_write_lockTime.store(0);
                MOTAdaptor::txn_temp_total_validate_lockTime.store(0);
                MOTAdaptor::txn_temp_total_validate_hotOccTime.store(0);
            }

            // MultiRaftState::ClearRaftEpochState(epoch_mod);
            MOTAdaptor::RemoteCacheClear(epoch_mod);//清空当前epoch的remote信息 为后面腾出空间 远端已经发送过来，不能清空下一个epoch的信息
            MOTAdaptor::ClearMergeEpochState();
            MOTAdaptor::AddLogicalEpoch();
            last_epoch_mod = epoch_mod;
            epoch ++;
            epoch_mod = epoch % max_length;
            epoch_commit_time = commit_time = now_to_us();
        }
    }
}




















void Send(std::string* serialized_str_ptr, uint64_t server_id) {
    raft_send_pool.enqueue(std::make_unique<send_thread_params>(0, server_id, serialized_str_ptr));
    raft_send_pool.enqueue(std::make_unique<send_thread_params>(0, 0, std::move(nullptr)));
    // send_pool.enqueue(std::make_unique<send_thread_params>(0, server_id, serialized_str_ptr));
    // send_pool.enqueue(std::make_unique<send_thread_params>(0, 0, std::move(nullptr)));
}

void Send(std::string* serialized_str_ptr) {
    send_pool.enqueue(std::make_unique<send_thread_params>(0, 0, serialized_str_ptr));
    send_pool.enqueue(std::make_unique<send_thread_params>(0, 0, std::move(nullptr)));
}


void RaftSendPing() {
    auto new_msg = std::make_unique<merge::Message>();
    auto* rep = new_msg->mutable_request();
    auto* ping = rep->mutable_ping();
    ping->set_from(local_ip_index);
    Send(Gzip(std::move(new_msg)));
    MOT_LOG_INFO("发送一个Ping local_ip_index %llu", local_ip_index);
}

void EpochPhysicalTimerManagerThreadMain(uint64_t id){
    SetCPU();
    InitEpochTimerManager();
    int n = 0;

//    MOTAdaptor::SetPhysicalEpoch(1);
//    MOTAdaptor::SetLogicalEpoch(1);
//    while(true) usleep(100000000000);

    //==========同步============
    zmq::message_t message;
    zmq::context_t context(1);
    zmq::socket_t request_puller(context, ZMQ_PULL);
//    request_puller.bind("tcp://*:15546");        // 原 5546 -> 15546
//    MOT_LOG_INFO("EpochPhysicalTimer 等待接受同步消息");
//    request_puller.recv(&message);
    gettimeofday(&start_time, NULL);
    start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
    if(!is_epoch_advanced_by_message){
        uint64_t sleep_time = static_cast<uint64_t>((((start_time.tv_sec / 60) + 1) * 60) * 1000000);
        usleep(sleep_time - start_time_ll);
        gettimeofday(&start_time, NULL);
        start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
    }
    uint64_t cache_server_available = 1;
    if(is_cache_server_available)
        cache_server_available = 0;
    MOT_LOG_INFO("EpochTimerManager 同步完成，数据库开始正常运行 %d %d", is_stable_epoch_send, is_epoch_advanced_by_message);
    epoch_commit_time = commit_time = now_to_us();
    is_epoch_advance_started.store(true);
    uint64_t epoch = MOTAdaptor::GetPhysicalEpoch() + 1000, epoch_mod = MOTAdaptor::GetPhysicalEpoch();
    if(is_full_async_exec) {
        OUTPUTLOG("====开始 异步执行");
        for(;;) {
            usleep(1000000);
        }
    }

    if(is_epoch_advanced_by_message){
        while(!MOTAdaptor::IsTimerStop()){
            OUTPUTLOG("====开始一个Physiacal Epoch, physical info");
            request_puller.recv(&message);
            epoch ++;
            epoch_mod = epoch % max_length; 
            MultiRaftState::ClearRaftEpochState(epoch_mod);
            MOTAdaptor::ClearEpochState(epoch_mod);
            MOTAdaptor::SetCacheServerStored(epoch_mod, cache_server_available);
            MOTAdaptor::AddPhysicalEpoch();
            if(MultiRaftState::is_server_leader && MOTAdaptor::GetPhysicalEpoch() % 10 == 0) {
                RaftSendPing();
            }
        }
    }
    else{
        while(!MOTAdaptor::IsTimerStop()){
            OUTPUTLOG("====开始一个Physiacal Epoch, physical info");
            usleep(GetSleeptime());
//            usleep(2000);           // wzy:
            epoch ++;
            epoch_mod = epoch % max_length; 
            MultiRaftState::ClearRaftEpochState(epoch_mod);
            MOTAdaptor::ClearEpochState(epoch_mod);
            MOTAdaptor::SetCacheServerStored(epoch_mod, cache_server_available);
            MOTAdaptor::AddPhysicalEpoch();
            if(MultiRaftState::is_server_leader && MOTAdaptor::GetPhysicalEpoch() % 10 == 0) {
                RaftSendPing();
            }
        }
    }
}



























std::string* GengrateEpochEndMessage(uint64_t epoch) {
    auto epoch_end_msg = std::make_unique<merge::Message>();
    auto* txn_end = epoch_end_msg->mutable_txn();
    txn_end->set_startepoch(0);
    txn_end->set_commitepoch(0);
    txn_end->set_csn(0);
    txn_end->set_txnid(0);
    txn_end->set_server_id(local_ip_index);
    txn_end->set_commitepoch(epoch);
    txn_end->set_csn(static_cast<uint64_t>(MOTAdaptor::LoadPackedTxnNum(epoch)));   // 设置csn为txn个数
//    txn_end->set_csn(static_cast<uint64_t>(MOTAdaptor::LoadPackedTxnNum(epoch) - MOTAdaptor::LoadPackedLockinfoNum(epoch)));   // 设置csn为txn个数
    // wzy: 添加对于lockinfo 的发送统计
    txn_end->set_lockinfocount(static_cast<uint64_t>(MOTAdaptor::LoadPackedLockinfoNum(epoch)));
    return Gzip(std::move(epoch_end_msg));
}

void EpochPackThreadMain(uint64_t id){
    MOT_LOG_INFO("线程开始工作 Pack id: %llu %llu", id, kBatchNum);
    SetCPU();
    uint64_t current_epoch = 0, send_index = 0, index = id, cnt = 0, send_epoch = 1;
    std::shared_ptr<zmq::message_t> merge_request_ptr;
    std::unique_ptr<pack_params> pack_param;
    std::unique_ptr<zmq::message_t> msg;
    std::string *serialized_txn_str_ptr;
    bool sleep_flag = false;
    while(!init_ok.load()) usleep(200);
    if(kServerNum == 1) {
        while(true) txn_queue_ptrs[0]->wait_dequeue(pack_param);
    }
    while(true){
        sleep_flag = true;
        if(TryDequeue(pack_param, index)) {
            if(pack_param == nullptr || pack_param->str == nullptr) continue;
            if(kServerNum == 1) continue;
            current_epoch = pack_param->epoch;
            if((MOTAdaptor::GetLogicalEpoch() % max_length) ==  ((MOTAdaptor::GetPhysicalEpoch() + 101) % max_length) ) assert(false);
            sleep_flag = false;
            if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(current_epoch, 0, pack_param->str))) ) Assert(false);
            if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(0, 0, std::move(nullptr)))) ) Assert(false);

            // wzy: 添加lockinfo统计
            if (pack_param->is_lockinfo_) {
                MOTAdaptor::AddPackedLockinfoNum(current_epoch, pack_param->index, 1);
                MOT_LOG_INFO("EpochPackThreadMain() send lockinfo current epoch = %llu", current_epoch);
            }
            else MOTAdaptor::AddPackedNum(current_epoch, pack_param->index, 1);
//            MOTAdaptor::AddPackedNum(current_epoch, pack_param->index, 1);
        }
        if(id == 0) {
            // TODO: 发送等待所有完成，为什么没有发送过去？不区分lockinfo和txn，或者lockinfo加入txn的统计中
            while(MOTAdaptor::IsCurrentEpochFinishedInteractive(send_epoch) == true) {
                sleep_flag = false;

                auto* serialized_txn_str_ptr = GengrateEpochEndMessage(send_epoch);
                if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(send_epoch, 1, serialized_txn_str_ptr)))) Assert(false);
                if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(0, 0, std::move(nullptr)))) ) Assert(false);
                MOT_LOG_INFO("MergeRequest 到达发送时间，开始发送 第%llu个Pack线程, 第%llu个package, epoch:%llu txn共%llu lock共%llu : %llu",
                    id, send_index, send_epoch, MOTAdaptor::LoadPackedTxnNum(send_epoch), MOTAdaptor::LoadPackedLockinfoNum(send_epoch), MOTAdaptor::LoadChangeSet(send_epoch));
                
                auto new_msg = std::make_unique<merge::Message>();
                auto* rep = new_msg->mutable_request();
                auto* accept = rep->mutable_raft_accept();
                accept->set_from(local_ip_index);
                accept->set_to(0);
                accept->set_epoch_id(send_epoch);
                Send(Gzip(std::move(new_msg)));
                MOT_LOG_INFO("发送一个RaftAcceptRequest epoch: %llu, %llu", send_epoch, now_to_us());
                send_epoch ++;
            }
        }
        if(sleep_flag)
            usleep(200);
    }
}






























void EpochSendThreadMain(uint64_t id){
    SetCPU();
    zmq::context_t context(1);
    zmq::message_t reply(5);
    int queue_length = 0;
    zmq::socket_t socket_send(context, ZMQ_PUB);
    socket_send.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
    socket_send.bind("tcp://*:5557");//to server
    socket_send.bind("tcp://*:5558");//to az_cache_server
    // socket_send.bind("tcp://*:" + std::to_string(20000 + local_ip_index * 100));
    std::unique_ptr<send_thread_params> params;
    std::unique_ptr<zmq::message_t> msg;
    while(init_ok.load() == false) usleep(200);
    MOT_LOG_INFO("线程开始工作 break; Sned  %llu %s", id, 
        (kServerIp[local_ip_index] + ":5557/5558").c_str());
    while(true) {
        send_pool.wait_dequeue(params);
        if(params == nullptr || params->merge_request_ptr == nullptr) continue;
        msg = std::make_unique<zmq::message_t>(static_cast<void*>(const_cast<char*>(params->merge_request_ptr->data())),
                params->merge_request_ptr->size(), string_free, static_cast<void*>(params->merge_request_ptr));
        socket_send.send(*(msg));
        // MOT_LOG_INFO("发送一条消息");
    }
}

void EpochRaftSendThreadMain(uint64_t id) {
    SetCPU();
    MOT_LOG_INFO("MultiRaftSendthreadMain 开始工作");
    std::unique_ptr<send_thread_params> params;
    std::unique_ptr<zmq::message_t> msg;
    std::vector<std::unique_ptr<zmq::context_t>> contexts;
    std::vector<std::unique_ptr<zmq::socket_t>> sockets;
    for(int i = 0; i < (int) kServerNum; i++) {
        contexts.emplace_back(std::move(std::make_unique<zmq::context_t>(1)));
        sockets.emplace_back(std::move(std::make_unique<zmq::socket_t>(*contexts[i], ZMQ_PUSH)));
        sockets[i]->connect("tcp://" + kServerIp[i] + ":5556");
    }
    while(init_ok.load() == false) usleep(200);
    for(;;) {
        raft_send_pool.wait_dequeue(params);
        if(params == nullptr || params->merge_request_ptr == nullptr) continue;
        // MOT_LOG_INFO("发送一个Raft message Sned to server %llu %s", params->tot, 
        //     (kServerIp[params->tot] + ":5556").c_str());
        msg = std::make_unique<zmq::message_t>(static_cast<void*>(const_cast<char*>(params->merge_request_ptr->data())),
                params->merge_request_ptr->size(), string_free, static_cast<void*>(params->merge_request_ptr));
        sockets[params->tot]->send(*(msg));
    }
}



























void EpochRaftListenThreadMain(uint64_t id) {
    SetCPU();
    MOT_LOG_INFO("MultiRaftListenthreadMain 开始工作");
    zmq::context_t listen_context(1);
    zmq::socket_t socket_listen(listen_context, ZMQ_PULL);
    socket_listen.bind("tcp://*:5556");
    for (;;) {
        std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));//防止上次遗留消息造成message cache出现问题
        if(is_epoch_advance_started.load() == true){
            if(!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
            if(!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>()))) assert(false);
            // MOT_LOG_INFO("Raft break 接收一条消息");
            break;
        }
    }

    for(;;) {
        std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));
        if(!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
        if(!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>()))) assert(false);
        // MOT_LOG_INFO("Raft 接收一条消息");
    }
}


void EpochListenThreadMain(uint64_t id){
    SetCPU();
    MOT_LOG_INFO("线程开始工作 ListenThread");
	zmq::context_t listen_context(1);
    zmq::socket_t socket_listen(listen_context, ZMQ_SUB);
    int queue_length = 0;
    socket_listen.setsockopt(ZMQ_SUBSCRIBE, "", 0);
    socket_listen.setsockopt(ZMQ_RCVHWM, &queue_length, sizeof(queue_length));
    for(int i = 0; i < (int)kServerIp.size(); i ++){
        if(i == (int)local_ip_index) continue;
        socket_listen.connect("tcp://" +kServerIp[i] + ":5557");
        MOT_LOG_INFO("线程开始工作 ListenThread %s", ("tcp://" +kServerIp[i] + ":5557").c_str());
    }
    if(is_full_async_exec) {
        for (;;) {
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
            socket_listen.recv(&(*message_ptr));//防止上次遗留消息造成message cache出现问题
            if(init_ok.load() == true){
                if(!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
                if(!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>()))) assert(false);                
                // MOT_LOG_INFO("接收一条消息");
                break;
            }
        }

        for(;;) {
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
            socket_listen.recv(&(*message_ptr));
            if(!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
            if(!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>()))) assert(false);
            // MOT_LOG_INFO("接收一条消息");
        }
    }
    
    for (;;) {
        std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));//防止上次遗留消息造成message cache出现问题
        if(is_epoch_advance_started.load() == true){
            if(!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
            if(!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>()))) assert(false);
            // MOT_LOG_INFO("接收一条消息");
            break;
        }
    }

    for(;;) {
        std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));
        if(!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
        if(!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>()))) assert(false);
        // MOT_LOG_INFO("接收一条消息");
    }
}






















void CheckAndSendRaftAcceptResponse(uint64_t epoch, uint64_t server_id) {
    if( MOTAdaptor::GetReceivedPackNum(epoch, server_id) >= 1 &&
        MOTAdaptor::GetReceivedTxnNum(epoch, server_id) >= MOTAdaptor::GetShouldReceiveTxnNum(epoch, server_id)) {
        if(MultiRaftState::GetFollowerAcceptEpochState(epoch, server_id) == 0) {
            /// once received compelete, response to the server
            /// current server as a follower send receive compelete(accept response) to the leader(txn->server_id)
            MultiRaftState::AddFollowerAcceptEpochState(epoch, server_id);
            auto new_msg = std::make_unique<merge::Message>();
            auto* rep = new_msg->mutable_response();
            auto* accept = rep->mutable_raft_accept();
            accept->set_from(local_ip_index);
            accept->set_to(server_id);
            accept->set_epoch_id(epoch);
            accept->set_result(1);
            Send(Gzip(std::move(new_msg)), server_id);
            MOT_LOG_INFO("Check 发送一个RaftAcceptResponse to server: %llu, epoch: %llu, pack num %llu, txn num: %llu,  %llu", 
                server_id, epoch, MOTAdaptor::GetReceivedPackNum(epoch),
                MOTAdaptor::GetShouldReceiveTxnNum(epoch), now_to_us());
        }
    }
}

void HandleMessage(std::unique_ptr<zmq::message_t> &&message_ptr, uint64_t& id, std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::Message>>>>>& message_cache) {
    auto msg_ptr = std::make_unique<merge::Message>();
    auto message_string_ptr = std::make_unique<std::string>(static_cast<const char*>(message_ptr->data()), message_ptr->size());
    google::protobuf::io::ArrayInputStream inputStream(message_string_ptr->data(), message_string_ptr->size());
    google::protobuf::io::GzipInputStream gzipStream(&inputStream);
    msg_ptr->ParseFromZeroCopyStream(&gzipStream);

    if(msg_ptr->type_case() != merge::Message::TypeCase::kTxn) {
        raft_message_pool.enqueue(std::move(msg_ptr));
        raft_message_pool.enqueue(std::move(std::make_unique<merge::Message>()));
        return ;
    }

    auto message_epoch_id = msg_ptr->txn().commitepoch();
    auto server_id = msg_ptr->txn().server_id();
    auto message_epoch_mod = message_epoch_id % max_length;

    if(is_fault_tolerance_enable) {
        auto msg_ptr_tmp = std::make_unique<merge::Message>();
        (*msg_ptr_tmp) = (*msg_ptr);
        cache_txn_queues[message_epoch_mod]->enqueue(std::move(msg_ptr_tmp));
    }

    if(msg_ptr->txn().row_size() == 0 && msg_ptr->txn().startepoch() == 0 && msg_ptr->txn().txnid() == 0){
        if((MOTAdaptor::GetLogicalEpoch() % max_length) ==  ((message_epoch_id + 101) % max_length) ) assert(false);
        MOTAdaptor::StoreShouldReceiveTxnNum(message_epoch_mod, server_id, msg_ptr->txn().csn());
        MOTAdaptor::StoreReceivedPackNum(message_epoch_mod, server_id, 1);
        MOTAdaptor::AddReceivedPackNumTotal(message_epoch_mod, 1);
        // raft_message_pool.enqueue(std::move(std::make_unique<merge::Message>()));///notify raft handle thread
        MOT_LOG_INFO("收到一个pack server: %llu, epoch: %llu, pack num %llu, txn num: %llu,  %llu", 
            msg_ptr->txn().server_id(), message_epoch_id, MOTAdaptor::GetReceivedPackNum(message_epoch_mod),
            MOTAdaptor::GetShouldReceiveTxnNum(message_epoch_mod), now_to_us());
    }
    else{
        if(is_full_async_exec) {
            merge_queue.enqueue(std::move(msg_ptr));
            merge_queue.enqueue(std::move(std::make_unique<merge::Message>()));
        }
        else {
            message_cache[message_epoch_mod][server_id]->push(std::move(msg_ptr));
        }
        MOTAdaptor::AddReceivedTxnNum(message_epoch_mod, server_id, 1);
        MOTAdaptor::AddReceivedTxnNumTotal(message_epoch_mod, 1);
    }

    return ;
}

// wzy: 修改后的HandleMessage，处理Message并将lock info加入lock queue，对相关计数器修改
void HandleMessageWithLockInfo(std::unique_ptr<zmq::message_t>&& message_ptr, uint64_t& id,
    std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::Message>>>>>& message_cache,
    std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::Message>>>>>& lockinfo_cache)
{
    auto msg_ptr = std::make_unique<merge::Message>();
    auto message_string_ptr =
        std::make_unique<std::string>(static_cast<const char*>(message_ptr->data()), message_ptr->size());
    google::protobuf::io::ArrayInputStream inputStream(message_string_ptr->data(), message_string_ptr->size());
    google::protobuf::io::GzipInputStream gzipStream(&inputStream);
    msg_ptr->ParseFromZeroCopyStream(&gzipStream);

    auto message_epoch_id = msg_ptr->txn().commitepoch();
    auto server_id = msg_ptr->txn().server_id();

    if (msg_ptr->type_case() == merge::Message::TypeCase::kLockinfo) {
        MOT_LOG_INFO("HandleMessageWithLockInfo received lockinfo");
        message_epoch_id = msg_ptr->lockinfo().commitepoch();
        server_id = msg_ptr->lockinfo().server_id();
    }
    if (msg_ptr->type_case() == merge::Message::TypeCase::kTxn) {
        MOT_LOG_INFO("HandleMessageWithLockInfo received txn");
    }

    // 后序只处理kTxn和kLockInfo的msg
    if (msg_ptr->type_case() != merge::Message::TypeCase::kTxn &&
        msg_ptr->type_case() != merge::Message::TypeCase::kLockinfo) {
        raft_message_pool.enqueue(std::move(msg_ptr));  // ?这是?
        raft_message_pool.enqueue(std::move(std::make_unique<merge::Message>()));
        return;
    }

    auto message_epoch_mod = message_epoch_id % max_length;

    if (is_fault_tolerance_enable) {
        auto msg_ptr_tmp = std::make_unique<merge::Message>();
        (*msg_ptr_tmp) = (*msg_ptr);
        cache_txn_queues[message_epoch_mod]->enqueue(std::move(msg_ptr_tmp));
    }

    // 对于txn空包处理，发送结束后的统计
    if (msg_ptr->type_case() == merge::Message::TypeCase::kTxn && msg_ptr->txn().row_size() == 0 && msg_ptr->txn().startepoch() == 0 && msg_ptr->txn().txnid() == 0) {
        if ((MOTAdaptor::GetLogicalEpoch() % max_length) == ((message_epoch_id + 101) % max_length))
            assert(false);
        // wzy: 其中有txn和lock info，区分
        MOTAdaptor::StoreShouldReceiveTxnNum(message_epoch_mod, server_id, msg_ptr->txn().csn());  // txn count
        MOTAdaptor::StoreShouldReceiveLockinfoNum(message_epoch_mod, server_id, msg_ptr->txn().lockinfocount());  // lockinfo count
        MOTAdaptor::StoreReceivedPackNum(message_epoch_mod, server_id, 1);
        MOTAdaptor::AddReceivedPackNumTotal(message_epoch_mod, 1);
        // raft_message_pool.enqueue(std::move(std::make_unique<merge::Message>()));///notify raft handle thread
        MOT_LOG_INFO("收到一个pack server: %llu, epoch: %llu, pack num %llu, txn num: %llu, lockinfo num: %llu,  %llu",
            msg_ptr->txn().server_id(),
            message_epoch_id,
            MOTAdaptor::GetReceivedPackNum(message_epoch_mod),
            MOTAdaptor::GetShouldReceiveTxnNum(message_epoch_mod),
            MOTAdaptor::GetShouldReceiveLockinfoNum(message_epoch_mod),
            now_to_us());
    } else if (msg_ptr->type_case() == merge::Message::TypeCase::kTxn) {
        if (is_full_async_exec) {
            merge_queue.enqueue(std::move(msg_ptr));
            merge_queue.enqueue(std::move(std::make_unique<merge::Message>()));
        } else {
            message_cache[message_epoch_mod][server_id]->push(std::move(msg_ptr));
        }
        MOTAdaptor::AddReceivedTxnNum(message_epoch_mod, server_id, 1);
        MOTAdaptor::AddReceivedTxnNumTotal(message_epoch_mod, 1);
        MOT_LOG_INFO("AddReceivedTxnNum finished");
    } else if (msg_ptr->type_case() == merge::Message::TypeCase::kLockinfo) {
        if (is_full_async_exec) {
            lock_queue.enqueue(std::move(msg_ptr));
            lock_queue.enqueue(std::move(std::make_unique<merge::Message>()));
        } else {
            lockinfo_cache[message_epoch_mod][server_id]->push(std::move(msg_ptr));
        }
        // wzy: 添加lock info 统计
        MOTAdaptor::AddReceivedLockinfoNum(message_epoch_mod, server_id, 1);
        MOTAdaptor::AddReceivedLockinfoNumTotal(message_epoch_mod, 1);
        MOT_LOG_INFO("AddReceivedLockinfoNum finished, message_epoch_mod = %llu", message_epoch_mod);
    }
    return;
}

//////////// 统计远端交互性事务和非交互性事务的commit和abort数量
// wzy: 对收到的lockinfo，插入queue
void HandleMergeLockinfo(const merge::LockInfo& lockinfo)
{
    auto tmp_csn = to_string(lockinfo.csn()) + ":" + to_string(lockinfo.server_id());
    string table_name, key_str;
    uint32_t op_type;
    int KeyLength;
    MOT::Table* table = nullptr;
    MOT::Row* localRow = nullptr;
    MOT::Key* key;
    void* buf;
    string tmp_rowid = "";
    uint64_t rowId;
    for (int i = 0; i < lockinfo.row_size(); i++)  // 对于LockInfo中不同行操作插入队列等待上锁
    {
        op_type = lockinfo.row(i).type();
        if (op_type == 0)  // 仅对update操作上锁
        {
            table_name = lockinfo.row(i).tablename();
            table = MOTAdaptor::m_engine->GetTableManager()->GetTable(table_name);

            KeyLength = lockinfo.row(i).key().length();
            buf = MOT::MemSessionAlloc(KeyLength);
            if (buf == nullptr) Assert(false);

            key = new (buf) MOT::Key(KeyLength);
            key->CpKey((uint8_t*)lockinfo.row(i).key().c_str(), KeyLength);  // 获取row.key（用于之后获取row）
            key_str = key->GetKeyStr();
            if (table->FindRow(key, localRow, 0) != MOT::RC::RC_OK) {  // 获取到本地的row
                continue;
            }
            // 找到该行，进行上锁操作
            rowId = localRow->GetRowId();
            tmp_rowid = table_name + ":" + to_string(rowId);
            std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
            std::shared_ptr<MOTAdaptor::LockRequestQueue> new_queue = nullptr;
            if (!MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) || !tmp_queue){    // 未找到则创建新lock request
                new_queue = std::make_shared<MOTAdaptor::LockRequestQueue>(tmp_rowid);
            }

            if(MOTAdaptor::row_lockrequest_map.get_or_create_queue(tmp_rowid, tmp_queue, new_queue) && tmp_queue){
                auto res = tmp_queue->lock_row_remote(tmp_rowid, lockinfo.server_id(), lockinfo.csn(), lockinfo.startepoch(), lockinfo.commitepoch());
                MOTAdaptor::AddActiveQueue(tmp_queue, rowId);
                if (!res) {
                    MOT_LOG_INFO("HandleMergeLockinfo() lock_row_remote [failed] because of duplicate tmp_csn : %s, tmp_rowid ： %s ", tmp_csn.c_str(), tmp_rowid.c_str());
                } else {
                    // 插入csn + queue
                    if (tmp_queue->grant_csn != "") {
                        MOTAdaptor::wait_for_graph.addEdge(tmp_csn, tmp_queue->grant_csn);      // 添加边
                    }
                    MOTAdaptor::remote_lock_num.fetch_add(1);
                    MOTAdaptor::AddCsnRequestQueue(tmp_csn, tmp_queue);
                    MOT_LOG_INFO("HandleMergeLockinfo() lock_row_remote tmp_csn : %s , tmp_rowid : %s ", tmp_csn.c_str(), tmp_rowid.c_str());
                }
            }
            localRow->SetRowInteractive(true);          // 设置该行为交互型
            new_queue.reset();
            tmp_queue = nullptr;
        } else {
            continue;
        }
    }
}

// wzy: 对abort交互性事务操作
void HandleAbortLockinfo(const merge::Transaction& txn) {
    string table_name, key_str;
    uint32_t op_type;
    int KeyLength;
    MOT::Table* table = nullptr;
    MOT::Row* localRow = nullptr;
    MOT::Key* key;
    void* buf;
    string tmp_rowid  = "";
    string tmp_csn;
    uint64_t rowId;
    uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
    ///////////////
    std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
    auto csn_temp = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());
    for (int i = 0; i < txn.row_size(); i++) {
        op_type = txn.row(i).type();
        if (op_type == 0) { // update操作解锁
            table_name = txn.row(i).tablename();
            table = MOTAdaptor::m_engine->GetTableManager()->GetTable(table_name);
            KeyLength = txn.row(i).key().length();
            buf = MOT::MemSessionAlloc(KeyLength);
            if (buf == nullptr)
                Assert(false);
            key = new (buf) MOT::Key(KeyLength);
            key->CpKey((uint8_t*)txn.row(i).key().c_str(), KeyLength);  // 获取row.key（用于之后获取row）
            key_str = key->GetKeyStr();

            if (table->FindRow(key, localRow, 0) != MOT::RC::RC_OK) {  // 获取到本地的row
                continue;
            }
            if(txn.row(i).isinteractiverow()) {
                // 找到该行，进行解锁
                rowId = localRow->GetRowId();
                tmp_rowid = table_name + ":" + to_string(rowId);
                string res_csn = "";
                tmp_queue = nullptr;
                if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue) {
                    tmp_queue->unlock_row(tmp_rowid, csn_temp, res_csn);
                    MOTAdaptor::RemoveActiveQueue(tmp_queue, rowId);
                    MOT_LOG_INFO("HandleAbortLockinfo() unlock success tid = %s , epoch : %llu , row id = %s", csn_temp.c_str(), epoch_mod, tmp_rowid.c_str());
                    MOTAdaptor::remote_unlock_num.fetch_add(1);
                } else {
                    MOT_LOG_INFO("HandleAbortLockinfo() queue not found");
                }
            }
            tmp_queue = nullptr;
        } else {
            continue;
        }
    }
    MOTAdaptor::Remote_Abort_interactive_txn_num.fetch_add(1);
    MOTAdaptor::deadlock_abort_set.remove(csn_temp);
    MOTAdaptor::wait_for_graph.removeNode(csn_temp);        // 清除等待图
    // TODO: csn_requests_map.remove并发问题
//    MOTAdaptor::csn_requests_map.remove(csn_temp);
    MOTAdaptor::txn_rowid_map.remove(csn_temp);
}

// wzy: 对commit完毕的txn进行解锁
void HandleCommitLockinfo(const merge::Transaction& txn)
{
    string table_name, key_str;
    uint32_t op_type;
    int KeyLength;
    MOT::Table* table = nullptr;
    MOT::Row* localRow = nullptr;
    MOT::Key* key;
    void* buf;
    string tmp_rowid = "";
    string tmp_csn;
    uint64_t rowId;
    std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
    auto csn_temp = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());
    uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
    for (int i = 0; i < txn.row_size(); i++) {
        op_type = txn.row(i).type();
        if (op_type == 0){
            // update操作解锁
            table_name = txn.row(i).tablename();
            table = MOTAdaptor::m_engine->GetTableManager()->GetTable(table_name);
            KeyLength = txn.row(i).key().length();
            buf = MOT::MemSessionAlloc(KeyLength);
            if (buf == nullptr)
                Assert(false);
            key = new (buf) MOT::Key(KeyLength);
            key->CpKey((uint8_t*)txn.row(i).key().c_str(), KeyLength);  // 获取row.key（用于之后获取row）
            key_str = key->GetKeyStr();


            if (table->FindRow(key, localRow, 0) != MOT::RC::RC_OK) {  // 获取到本地的row
                continue;
            }
            if (txn.row(i).isinteractiverow()) {
                // 找到该行，进行解锁
                rowId = localRow->GetRowId();
                tmp_rowid = table_name + ":" + to_string(rowId);
                string res_csn = "";
                tmp_queue = nullptr;
                if(MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue) {
                    tmp_queue->unlock_row(tmp_rowid, csn_temp, res_csn);
                    MOTAdaptor::RemoveActiveQueue(tmp_queue, rowId);
                    MOT_LOG_INFO("HandleCommitLockinfo() unlock success tid = %s , epoch : %llu , row id = %s", csn_temp.c_str(), epoch_mod, tmp_rowid.c_str());
                    MOTAdaptor::remote_unlock_num.fetch_add(1);
                } else {
                    MOT_LOG_INFO("EpochAbortThread() queue not found");
                }
            }
            tmp_queue = nullptr;
        } else {
            continue;
        }
    }
    MOTAdaptor::deadlock_abort_set.remove(csn_temp);
    MOTAdaptor::wait_for_graph.removeNode(csn_temp);
//    MOTAdaptor::csn_requests_map.remove(csn_temp);
    MOTAdaptor::txn_rowid_map.remove(csn_temp);
}


void EpochMergeThreadMain(uint64_t id){
    bool result, sleep_flag = false;
    MOT::SessionContext* session_context = MOT::GetSessionManager()->
        CreateSessionContext(IS_PGXC_COORDINATOR, 0, nullptr, INVALID_CONNECTION_ID);
    MOT::Table* table = nullptr;
    MOT::Row* localRow = nullptr;
    MOT::Key* key;
    std::unique_ptr<zmq::message_t> message_ptr;
    std::unique_ptr<std::string> message_string_ptr;
    std::unique_ptr<merge::Message> msg_ptr;
    std::unique_ptr<merge::Message> lock_msg_ptr;  // wzy
    uint64_t received_lockinfo_num = 0, received_txn_num = 0;

    std::unique_ptr<std::vector<MOT::Key*>> key_vector_ptr;
    std::unique_ptr<std::vector<MOT::Row*>> row_vector_ptr;
    std::string csn_temp, key_temp, key_str, table_name, csn_result;
    uint64_t csn = 0, index_pack = id % kPackageNum, epoch_mod = 0, server_id = 0, clear_epoch = 0, loop_server_id = 0, epoch = 0, received_pack_num = 0;
    uint32_t op_type = 0;
    int KeyLength;
    void* buf;
    std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::Message>>>>> message_cache;
    std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::Message>>>>> lockinfo_cache;      // wzy
    message_cache.reserve(max_length + 1);
    lockinfo_cache.reserve(max_length + 1);
    for(int i = 0; i < (int)max_length; i ++) {
        message_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::Message>>>>());
        lockinfo_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::Message>>>>());
        for(int j = 0; j < (int)kServerIp.size() + 2; j++){
            message_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<merge::Message>>>());
            lockinfo_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<merge::Message>>>());
        }
    }
    
    while(!init_ok.load()) usleep(200);
    for(;;){    
        sleep_flag = true;
        if(listen_message_queue.try_dequeue(message_ptr) && message_ptr != nullptr && message_ptr->size() > 0) {
//            HandleMessage(std::move(message_ptr), id, message_cache);
            HandleMessageWithLockInfo(std::move(message_ptr), id, message_cache, lockinfo_cache);
            sleep_flag = false;
        }

        // epoch_mod = MOTAdaptor::GetLogicalEpoch() % max_length;
        // loop_server_id = (loop_server_id + 1) % kServerNum;
        // if(loop_server_id != local_ip_index && MOTAdaptor::IsServerOnLine(loop_server_id) &&
        //     MOTAdaptor::GetReceivedPackNum(epoch_mod, loop_server_id) >= 1 &&
        //     MOTAdaptor::GetReceivedTxnNum(epoch_mod, loop_server_id) >= MOTAdaptor::GetShouldReceiveTxnNum(epoch_mod, loop_server_id) &&
        //     !message_cache[epoch_mod][loop_server_id]->empty()) {

        //     clear_epoch = (epoch_mod - 1 + max_length) % max_length;
        //     while(!message_cache[epoch_mod][loop_server_id]->empty()){
        //         auto msg_ptr_tmp = std::move(message_cache[epoch_mod][loop_server_id]->front());
        //         message_cache[epoch_mod][loop_server_id]->pop();
        //         if(!merge_queue.enqueue(std::move(msg_ptr_tmp))) Assert(false); //防止moodycamel取不出
        //     }
        //     if(!merge_queue.enqueue(std::move(nullptr))) Assert(false); //防止moodycamel取不出
        //     while(!message_cache[clear_epoch][loop_server_id]->empty()) message_cache[clear_epoch][loop_server_id]->pop();
        //     sleep_flag = false;
        // }
//        if(received_pack_num != MOTAdaptor::GetReceivedPackNum(epoch_mod)) {
//            clear_epoch = (epoch_mod - 1 + max_length) % max_length;
//            for(int i = 0; i < (int)kServerNum; i++){
//                if(MOTAdaptor::IsServerOnLine(i) && MOTAdaptor::GetReceivedPackNum(epoch_mod, i) == 1 &&
//                    MOTAdaptor::GetReceivedTxnNum(epoch_mod, i) >= MOTAdaptor::GetShouldReceiveTxnNum(epoch_mod, i) &&
//                    !message_cache[epoch_mod][i]->empty()) {
//                    received_pack_num ++;
//                    while(!message_cache[epoch_mod][i]->empty()){
//                        auto txn_ptr_tmp = std::move(message_cache[epoch_mod][i]->front());
//                        message_cache[epoch_mod][i]->pop();
//                        if(!merge_queue.enqueue(std::move(txn_ptr_tmp))) Assert(false); //防止moodycamel取不出
//                    }
//                    if(!merge_queue.enqueue(nullptr)) Assert(false); //防止moodycamel取不出
//                }
//                while(!message_cache[clear_epoch][i]->empty()) message_cache[clear_epoch][i]->pop();
//            }
//        }


        epoch_mod = MOTAdaptor::GetLogicalEpoch() % max_length;
        if(epoch != epoch_mod) {
            epoch = epoch_mod;
            received_pack_num = 0;
            received_lockinfo_num = 0;
            received_txn_num = 0;
        }
        if(received_pack_num != MOTAdaptor::GetReceivedPackNum(epoch_mod)) {
            clear_epoch = (epoch_mod - 1 + max_length) % max_length;
            for(int server_id_t = 0; server_id_t < (int)kServerNum; server_id_t++){
                if(id == 0) {
                    CheckAndSendRaftAcceptResponse(epoch, server_id_t);
                }
                // wzy: 所有txn和lockinfo接收完毕，并已放入对应cache，从cache中取出放入queue
                if(MOTAdaptor::IsServerOnLine(server_id_t) && MOTAdaptor::GetReceivedPackNum(epoch_mod, server_id_t) == 1 &&
                    MOTAdaptor::GetReceivedTxnNum(epoch_mod, server_id_t) >= MOTAdaptor::GetShouldReceiveTxnNum(epoch_mod, server_id_t) &&
                    MOTAdaptor::GetReceivedLockinfoNum(epoch_mod, server_id_t) >= MOTAdaptor::GetShouldReceiveLockinfoNum(epoch_mod, server_id_t)) {

                    if (!message_cache[epoch_mod][server_id_t]->empty()) {
                        received_txn_num++;
                        while(!message_cache[epoch_mod][server_id_t]->empty()){
                            auto msg_ptr_tmp = std::move(message_cache[epoch_mod][server_id_t]->front());
                            message_cache[epoch_mod][server_id_t]->pop();
                            if(!merge_queue.enqueue(std::move(msg_ptr_tmp))) Assert(false); //防止moodycamel取不出
                        }
                        if(!merge_queue.enqueue(std::move(std::make_unique<merge::Message>()))) Assert(false); //防止moodycamel取不出
                    }

                    if (!lockinfo_cache[epoch_mod][server_id_t]->empty()) {
                        received_lockinfo_num++;
                        while(!lockinfo_cache[epoch_mod][server_id_t]->empty()){
                            auto msg_ptr_tmp = std::move(lockinfo_cache[epoch_mod][server_id_t]->front());
                            lockinfo_cache[epoch_mod][server_id_t]->pop();
                            if(!lock_queue.enqueue(std::move(msg_ptr_tmp))) Assert(false); //防止moodycamel取不出
                        }
                        if(!lock_queue.enqueue(std::move(std::make_unique<merge::Message>()))) Assert(false); //防止moodycamel取不出
                    }

                    received_pack_num++;
                    // MOT_LOG_INFO("处理完pack cache server: %llu, epoch: %llu, txn num %llu, lockinfo num: %llu ", server_id_t, epoch_mod, received_txn_num, received_lockinfo_num);
                }

                // 清空
                while(!message_cache[clear_epoch][server_id_t]->empty()) message_cache[clear_epoch][server_id_t]->pop();
                while(!lockinfo_cache[clear_epoch][server_id_t]->empty()) lockinfo_cache[clear_epoch][server_id_t]->pop();
            }
        }

        // wzy: 获取epoch中所有lock，并添加到等待queue中，或者进行解锁
        if (lock_queue.try_dequeue(lock_msg_ptr)) {
            sleep_flag = false;
            if (lock_msg_ptr == nullptr || lock_msg_ptr->type_case() != merge::Message::TypeCase::kLockinfo)
                continue;
            auto& lockinfo = lock_msg_ptr->lockinfo();
            epoch_mod = lockinfo.commitepoch() % MOTAdaptor::max_length;
            MOT_LOG_INFO("LockWriteSet() lock_row_remote lockinfo csn = %llu, server id = %llu , epoch_mode = %llu , index_pack = %llu", lockinfo.csn(), lockinfo.server_id(), epoch_mod, index_pack);
            HandleMergeLockinfo(lockinfo);  // 处理lock info
            MOTAdaptor::receive_lock_num.fetch_add(1);
            MOT_LOG_INFO("LockWriteSet() lock_row_remote 2");
            MOTAdaptor::IncMergeLockinfoCounters(epoch_mod, index_pack);
        }

        if(merge_queue.try_dequeue(msg_ptr)) {
            result = true;
            sleep_flag = false;
            if(msg_ptr == nullptr || msg_ptr->type_case() != merge::Message::TypeCase::kTxn) continue;
            auto& txn = msg_ptr->txn();
            csn = txn.csn();
            server_id = txn.server_id();
            csn_temp = std::to_string(csn) + ":" + std::to_string(server_id);
            row_vector_ptr = std::make_unique<std::vector<MOT::Row*>>();
            row_vector_ptr->reserve(txn.row_size());
            epoch_mod = txn.commitepoch() % MOTAdaptor::max_length;
            for(int j = 0; j < txn.row_size(); j++){
                table_name = txn.row(j).tablename();
                op_type = txn.row(j).type();
                table = MOTAdaptor::m_engine->GetTableManager()->GetTable(table_name);
                if(table == nullptr){
                    result = false;
                }
                KeyLength = txn.row(j).key().length();
                buf = MOT::MemSessionAlloc(KeyLength);
                if(buf == nullptr) Assert(false);
                key = new (buf) MOT::Key(KeyLength);
                key->CpKey((uint8_t*)txn.row(j).key().c_str(),KeyLength);
                key_str = key->GetKeyStr();
                if(op_type == 0 || op_type == 2){
                    if (table->FindRow(key,localRow, 0) != MOT::RC::RC_OK) {
                        result = false;
                        continue;
                    }

                    // wzy: 找到该行，确认是否上过锁
                    auto rowId = localRow->GetRowId();
                    auto tmp_rowid = table_name + ":" + to_string(rowId);

                    // 统计hot row
                    if (kHotRow_Active) {  // wzy: INS操作没有现存的row id
                        MOTAdaptor::dynamic_hot_rows.visit_row(tmp_rowid);      // wzy: 添加统计
                    }

                    string res_tid = "";
                    if (!MOTAdaptor::IsRowAvailable(tmp_rowid, csn_temp, res_tid)) {
                        MOT_LOG_INFO("IsRowAvailable() not available visited row : %s, cur csn : %s, locked csn : %s",  tmp_rowid.c_str(), csn_temp.c_str(), res_tid.c_str());
                        result = false;
                    }
                    ///////
                    if (result && !localRow->ValidateAndSetWriteForRemoteInteractive(csn, txn.startepoch(), txn.commitepoch(), server_id, txn.isinteractive())){
                        result = false;
                        if (txn.isinteractive()) {
                            MOT_LOG_INFO("[Abort] ValidateAndSetWriteSet() remote validate failed tmp_csn : %s , tmp_rowid : %s ", csn_temp.c_str(), tmp_rowid.c_str());
                            MOTAdaptor::Remote_ValidateAndSetWriteForRemote_abort_num.fetch_add(1);
                        }
                    }
//                    if (!localRow->ValidateAndSetWriteForRemote(csn, txn.startepoch(), txn.commitepoch(), server_id, txn.isinteractive())){
//                        result = false;
//                    }
                    row_vector_ptr->emplace_back(localRow);
                } 
                else {//insert 
                    if (table->FindRow(key, localRow, 0) == MOT::RC::RC_OK) {
                        result = false;
                    }
                    key_temp = table_name + key_str;
                    if(!MOTAdaptor::insertSetForCommit.insert(key_temp, csn_temp, &csn_result)){
                        result = false;
                    }
                    MOTAdaptor::abort_transcation_csn_set.insert(csn_result, csn_result);
                    row_vector_ptr->emplace_back(nullptr);
                }
                MOT::MemSessionFree(buf);
            }
            if(!result) {
                MOTAdaptor::abort_transcation_csn_set.insert(csn_temp, csn_temp);
                // wzy: 交互型事务失败则从queue中删除/解锁
                if (txn.isinteractive()) {
                    MOT_LOG_INFO("[Abort] CommitPhase() remote failed tmp_csn : %s ", csn_temp.c_str());
                    HandleAbortLockinfo(txn);
                    // 从LockRequestQueue中删除
//                    std::shared_ptr<std::vector<std::string>> row_id_set = nullptr;
//                    MOTAdaptor::csn_row_map.get_element(csn_temp, row_id_set);
//                    std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
//                    if (row_id_set) {
//                        for (auto tmp_rowid : *row_id_set) {
//                            if (MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue) {
//                                tmp_queue->remove_lock_request(csn_temp);
//                            }
//                        }
//                    } else {
//                        MOT_LOG_INFO("EpochMergeMainThread() row_id_set not found ");
//                    }
//                    tmp_queue = nullptr;
//                    row_id_set = nullptr;
                }
                //////////////////
            }
            else{
                if (txn.isinteractive()) HandleCommitLockinfo(txn);      // wzy: 先直接解锁
                if(commit_txn_queue_struct.enqueue(std::make_unique<commit_thread_params>(std::move(msg_ptr), std::move(row_vector_ptr)))){
                    MOTAdaptor::IncRemoteCommitTxnCounters(epoch_mod, index_pack);
                    if(!commit_txn_queue_struct.enqueue(std::make_unique<commit_thread_params>(nullptr, nullptr))) Assert(false);
                }
                else{
                    MOT_LOG_INFO("Merge 放入队列失败 ");
                    Assert(false);
                }
            }
            MOTAdaptor::IncRemoteMergedTxnCounters(epoch_mod, index_pack);
            continue;
        }

        if(sleep_flag) {
            usleep(200);
        }
    }
}

void EpochCommitThreadMain(uint64_t id){//validate
    MOT::SessionContext* session_context = MOT::GetSessionManager()->
        CreateSessionContext(IS_PGXC_COORDINATOR, 0, nullptr, INVALID_CONNECTION_ID);
    MOT::TxnManager* txn_manager = session_context->GetTxnManager();
    MOT::Table* table = nullptr;
    MOT::Row* localRow = nullptr, *new_row = nullptr;
    MOT::Key* key;
    MOT::RC res;
    std::unique_ptr<commit_thread_params> params;
    std::unique_ptr<std::vector<MOT::Row*>> row_vector_ptr;
    std::unique_ptr<merge::Message> msg_ptr;
    std::string csn_temp, key_temp, key_str, table_name;
    uint64_t csn = 0, index_pack = id % kPackageNum, epoch_mod;
    uint32_t op_type = 0;
    uint32_t server_id = 0;

    // wzy:
    uint64_t rowId = 0;
    std::string tmp_rowid;

    int KeyLength;
    void* buf;
    int flag = 0;
    // bool sleep_flag = false;
    while(!init_ok.load()) usleep(200);
    for(;;){
        if(commit_txn_queue_struct.try_dequeue(params)) {
            if(params == nullptr || params->msg == nullptr) continue;
            msg_ptr = std::move(params->msg);
            row_vector_ptr = std::move(params->row_vector);
            if(msg_ptr == nullptr) {
                continue;
            }
            if(is_full_async_exec == false)
                while(!MOTAdaptor::IsRemoteExeced()) {
                    usleep(200);
                }
            auto& txn = msg_ptr->txn();
            csn = txn.csn();
            // uint64_t csn_tmp = (((csn & STATUS_BITS) | (csn & CSN_BITS)) & CSN_BITS);
            server_id = txn.server_id();
            epoch_mod = txn.commitepoch() % MOTAdaptor::max_length;
            csn_temp = std::to_string(csn) + ":" + std::to_string(server_id);
            flag = 0;
            if(MOTAdaptor::abort_transcation_csn_set.contain(csn_temp, csn_temp)){
                flag ++;
                if (txn.isinteractive()) MOT_LOG_INFO("[Abort] abort_transcation_csn_set() remote validate failed tmp_csn : %s ", csn_temp.c_str());
            }
            if (MOTAdaptor::deadlock_abort_set.contain(csn_temp, csn_temp)) {
                flag ++;
                if (txn.isinteractive()) MOT_LOG_INFO("[Abort] deadlock_abort_set() remote validate failed tmp_csn : %s ", csn_temp.c_str());
            }
            // CRDT验证
            for(int j = 0; j < txn.row_size(); j++){
                const auto& row = txn.row(j);
                table = MOTAdaptor::m_engine->GetTableManager()->GetTable(row.tablename());
                MOT_ASSERT(table != nullptr);
                op_type = row.type();

                if(op_type == 0 || op_type == 2) {
                    localRow = (*row_vector_ptr)[j];
                    if(localRow->GetRowHeader()->GetCSN_1() != csn || localRow->GetRowHeader()->GetServerId() != server_id){
                        flag ++;
                        MOT_LOG_INFO("[Abort] CommitCheck() remote validate failed tmp_csn : %s ", csn_temp.c_str());
                        break;
                    }
                    /////////////
//                    if(localRow->GetRowHeader()->GetCSN_1() != csn || localRow->GetRowHeader()->GetServerId() != server_id){
//                        flag ++;
//                        break;
//                    }
                }
                else {
                    KeyLength = txn.row(j).key().length();
                    buf = MOT::MemSessionAlloc(KeyLength);
                    if(buf == nullptr) Assert(false);
                    key = new (buf) MOT::Key(KeyLength);
                    key->CpKey((uint8_t*)txn.row(j).key().c_str(),KeyLength);
                    key_str = key->GetKeyStr();

                    table_name = txn.row(j).tablename();
                    key_temp = "" + table_name + key_str;
                    MOT::MemSessionFree(buf);
                    if(MOTAdaptor::insertSetForCommit.contain(key_temp, csn_temp) == false){
                        flag ++;
                        break;
                    }
                }
            }

            // wzy: 判断commit 对交互型事务进行解锁操作
            if (txn.isinteractive()) {
                if (flag != 0) {
//                    HandleAbortLockinfo(txn);
                    MOTAdaptor::Remote_Abort_interactive_txn_num.fetch_add(1);      // 统计
                    MOTAdaptor::Remote_CommitCheck_abort_num.fetch_add(1);
                }
                else {
//                    HandleCommitLockinfo(txn);
                }
            }

            // 更新
            if(flag == 0){
                MOTAdaptor::IncRecordCommitTxnCounters(epoch_mod, index_pack);
                txn_manager->CleanTxn();
                std::map<MOT::Row*, bool> lock_map;
                lock_map.clear();
                for(int j = 0; j < txn.row_size(); j++){
                    const auto& row = txn.row(j);
                    op_type = row.type();
                    if(op_type == 0 || op_type == 2) {
                        localRow = (*row_vector_ptr)[j];
                        if(lock_map[localRow] == false) {//可能存在两次写入同一行
                            if(is_full_async_exec == true && localRow->GetRowHeader()->GetCSN() == csn 
                                && localRow->GetRowHeader()->GetServerId() == server_id) {   
                                localRow->GetRowHeader()->Lock();
                                localRow->GetRowHeader()->LockStable();
                            }
                            else {
                                localRow->GetRowHeader()->Lock();
                                localRow->GetRowHeader()->LockStable();
                            }
                            lock_map[localRow] = true;
                        }   
                    }
                }
                for(int j = 0; j < txn.row_size(); j++){
                    const auto& row = txn.row(j);
                    table = MOTAdaptor::m_engine->GetTableManager()->GetTable(row.tablename());
                    MOT_ASSERT(table != nullptr);
                    op_type = row.type();
                    if(op_type == 0 || op_type == 2) {
                        localRow = (*row_vector_ptr)[j];
                        if(is_full_async_exec) {

                            // wzy: 更新就不用检查上锁了，找到该行，确认是否上过锁
                            if(localRow->GetRowHeader()->GetCSN_1() == csn && localRow->GetRowHeader()->GetServerId() == server_id)
                                {
                                if(op_type == 2) {
                                    localRow->GetPrimarySentinel()->SetDirty();
                                    localRow->GetRowHeader()->m_csnWord = (csn | LOCK_BIT | ABSENT_BIT | LATEST_VER_BIT);
                                }
                                else{
                                    // localRow->CopyData((uint8_t*)row.data().c_str(),table->GetTupleSize());
                                    for (int k=0; k < row.column_size(); k++){
                                        const auto &col = row.column(k);
                                        localRow->SetValueVariable_1(col.id(),col.value().c_str(),col.value().length());
                                    }
                                    localRow->GetRowHeader()->m_csnWord = (csn | LOCK_BIT);
                                }
                            }
                        }
                        else {
                            if(op_type == 2) {
                                localRow->GetPrimarySentinel()->SetDirty();
                                localRow->GetRowHeader()->m_csnWord = (csn | LOCK_BIT | ABSENT_BIT | LATEST_VER_BIT);
                            }
                            else{           // 对于update
                                // localRow->CopyData((uint8_t*)row.data().c_str(),table->GetTupleSize());
                                for (int k=0; k < row.column_size(); k++){
                                    const auto &col = row.column(k);
                                    localRow->SetValueVariable_1(col.id(),col.value().c_str(),col.value().length());
                                    // TODO: wzy: 远端事务直接修改row本身，是否也需要设置xmax_xmin? 还是说xmax_xmin只针对本地？
                                }
                                localRow->GetRowHeader()->m_csnWord = (csn | LOCK_BIT);
                            }
                        }
                        localRow->GetRowHeader()->KeepStable();
                    }
                    else{
                        new_row = table->CreateNewRow();
                        new_row->CopyData((uint8_t*)row.data().c_str(),table->GetTupleSize());  //insert 
                        res = table->InsertRow(new_row, txn_manager);
                        if ((res != MOT::RC_OK) && (res != MOT::RC_UNIQUE_VIOLATION)) {
                            MOT_REPORT_ERROR(
                                MOT_ERROR_OOM, "Insert Row ", "Failed to insert new row for table %s", table->GetLongTableName().c_str());
                        }
                    }
                }

                txn_manager->CommitForRemote(server_id); //for insert     lock and release lock
                for(int j = 0; j < txn.row_size(); j++){
                    const auto& row = txn.row(j);
                    op_type = row.type();
                    if(op_type == 0 || op_type == 2) {
                        localRow = (*row_vector_ptr)[j];

                        if(lock_map[localRow] == true) {
                            if(is_full_async_exec == true && localRow->GetRowHeader()->GetCSN() == csn 
                                && localRow->GetRowHeader()->GetServerId() == server_id) {
                                localRow->GetRowHeader()->ReleaseStable();
                                localRow->GetRowHeader()->Release();
                            }
                            else {
                                localRow->GetRowHeader()->ReleaseStable();
                                localRow->GetRowHeader()->Release();
                            }
                            lock_map[localRow] = false;
                        }
                    }
                }

                // wzy: 判断commit 对交互型事务进行解锁操作
//                if (txn.isinteractive()){
//                    HandleCommitLockinfo(txn);
//                }

                MOTAdaptor::IncRecordCommittedTxnCounters(epoch_mod, index_pack);
            }
            else {

            }
            MOTAdaptor::IncRemoteCommittedTxnCounters(epoch_mod, index_pack);
        }
        else {
            usleep(200);
        }
    }
}

// wzy: 上锁和死锁检测
void DeadlockDetection();

void DeadlockDetection_CRLS();

// wzy: 单线程
void EpochLockThreadMain_1(uint64_t id)
{
    // 读取wait_for，判断是否有环，DFS
    while (true) {
        // 在merge结束后进行
        if (MOTAdaptor::IsLockExeced() && !MOTAdaptor::IsLockGranted()) {
            //            std::string row_id;
            //            std::unique_lock<std::mutex> rowid_set_lock(MOTAdaptor::rowid_set_mutex);
            //            std::unique_lock<std::mutex> wait_for_lock(MOTAdaptor::wait_for_mutex);
            std::unique_lock<std::mutex> queue_lock(MOTAdaptor::active_queue_list_mutex);
            // TODO: 多线程授予锁
            for (const auto& tmp_queue : MOTAdaptor::active_queue_list) {
                tmp_queue->grantLocks();
                tmp_queue->generateWaitFor();
                MOTAdaptor::AddLockGrantedNum();
            }
            queue_lock.unlock();
            DeadlockDetection_CRLS();

            MOTAdaptor::SetLockGranted(true);
        } else usleep(200);
    }
}

// wzy: 多线程队列实现，有问题
void EpochLockThreadMain_2(uint64_t id)
{
    // 读取wait_for，判断是否有环，DFS
    while (true) {
        // 在merge结束后进行
        if (MOTAdaptor::IsLockExeced() && !MOTAdaptor::IsLockGranted()) {
            uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % 2;
            uint64_t next_epoch = (epoch_mod + 1) % 2;
            std::shared_ptr<MOTAdaptor::LockRequestQueue> request = nullptr;
            while (MOTAdaptor::active_lock_queues[epoch_mod]->try_dequeue(request) && request != nullptr) {
                request->grantLocks();
                request->generateWaitFor();
                // 如果request 中还有其他请求，则加入下一轮
                MOTAdaptor::AddLockGrantedNum();
                MOTAdaptor::AddEpochActiveQueue(request, next_epoch);
                request = nullptr;
            }
            if (id == 0) {
                DeadlockDetection_CRLS();
                MOTAdaptor::SetLockGranted(true);
                MOTAdaptor::ClearEpochActiveQueue(epoch_mod);       // wzy: 清除当前epoch的queue和set
            }
        } else usleep(200);
    }
}

void EpochLockThreadMain(uint64_t id) {
    EpochLockThreadMain_Wait(id);
}


// wzy:多线程 wait
void EpochLockThreadMain_Wait(uint64_t id)
{
    // 读取wait_for，判断是否有环，DFS
    bool sleep_flag = true;
    if (cc_mode == 3 || cc_mode == 5) {
        MOTAdaptor::SetLockGranted(true);       // test
        DeadlockDetection_CRLS();
        usleep(200);
    }
    else {
        while (true) {
            sleep_flag = true;
            // 在merge结束后进行
            if (MOTAdaptor::IsLockExeced() && !MOTAdaptor::IsLockGranted()) {
                if (!MOTAdaptor::IsActiveLockListExced(id)) {
                    sleep_flag = false;
                    std::unique_lock<std::mutex> queue_lock(MOTAdaptor::active_lock_list_mutex[id]);
                    auto list = MOTAdaptor::active_lock_list[id];
                    uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
                    if (list) {
                        for (const auto& queue : *list) {
                            if (queue->grantLocks(epoch_mod)) {
                                queue->generateWaitFor();  // 选举出新锁才会重新生成等待图
                                MOT_LOG_INFO("[LockGranted] success EpochLockThreadMain(%llu) locked csn : %s , epoch : %llu , rowid : %s ,  request size : %llu ",
                                    id,
                                    queue->grant_csn.c_str(),
                                    queue->epoch_,
                                    queue->m_row_id.c_str(),
                                    queue->request_num.load());
                            } else {
                                // MOT_LOG_INFO("[LockGranted] EpochLockThreadMain() locked csn : %s , rowid : %s , epoch : %llu , request size : %llu ", queue->grant_csn.c_str(), queue->m_row_id.c_str(), queue->epoch_, queue->request_num.load());
                            }
                            MOTAdaptor::AddLockGrantedNum();
                        }
                    }
                    queue_lock.unlock();
                    MOTAdaptor::SetActiveLockListExced(id, true);
                } else if (id != 0)
                    usleep(200);
                else if (id == 0 && MOTAdaptor::IsAllActiveLockListExced()) {
                    DeadlockDetection_CRLS();
                    MOTAdaptor::SetLockGranted(true);
                }
            } else
                usleep(200);
        }
    }
}

// wzy:多线程 wound wait
void EpochLockThreadMain_WoundWait(uint64_t id)
{
    // 读取wait_for，判断是否有环，DFS
    bool sleep_flag = true;
    while (true) {
        sleep_flag = true;
        // 在merge结束后进行
        if (MOTAdaptor::IsLockExeced() && !MOTAdaptor::IsLockGranted()) {
            if (!MOTAdaptor::IsActiveLockListExced(id)) {
                sleep_flag = false;
                std::unique_lock<std::mutex> queue_lock(MOTAdaptor::active_lock_list_mutex[id]);
                auto list = MOTAdaptor::active_lock_list[id];
                uint64_t epoch_mod = MOTAdaptor::GetLogicalEpoch() % (UINT64_MAX - 1);
                std::string abort_csn = "";
                if (list) {
                    for (const auto& queue : *list) {
                        abort_csn = queue->grantLocks_woundWait(epoch_mod);
                        if (abort_csn != "") {
                            MOTAdaptor::deadlock_abort_set.insert(abort_csn, abort_csn);
                            MOT_LOG_INFO("[ Wound Wait ] tid = %s ABORT", abort_csn.c_str());
                        }
                        MOTAdaptor::AddLockGrantedNum();
                    }
                }
                queue_lock.unlock();
                MOTAdaptor::SetActiveLockListExced(id, true);
            }
            else if (id != 0) usleep(200);
            else if (id == 0 && MOTAdaptor::IsAllActiveLockListExced()) {
                MOTAdaptor::SetLockGranted(true);
            }
        }
        else usleep(200);
    }
}

void TryGetServerInfo() {
    zmq::context_t listen_context(1);
    zmq::socket_t socket_listen(listen_context, ZMQ_PULL);
    socket_listen.bind("tcp://*:1556");         // 端口
    for (;;) {
        std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));
        ReGetServerInfo();        // 更新配置信息
    }
}

void ReGetServerInfo() {
    tinyxml2::XMLDocument doc;
    doc.LoadFile("/home/zwx/ServerInfo.xml");
    tinyxml2::XMLElement *root=doc.RootElement();
    //////////////// wzy: 混合
    tinyxml2::XMLElement* cc_mode_ = root->FirstChildElement("CC_mode");
    cc_mode= std::stoull(cc_mode_->GetText());

    tinyxml2::XMLElement* lock_thread_num = root->FirstChildElement("lock_thread_num");
    kLockThreadNum= std::stoull(lock_thread_num->GetText());

    tinyxml2::XMLElement* hot_rows_freq = root->FirstChildElement("hot_rows_freq");
    kHotRowsFreq= std::stoull(hot_rows_freq->GetText());

    tinyxml2::XMLElement* interactive_perc = root->FirstChildElement("interactive_perc");
    kInteractivePerc= std::stoull(interactive_perc->GetText());

    tinyxml2::XMLElement* is_mvcc = root->FirstChildElement("is_mvcc");
    isMVCC_Active = std::stoi(is_mvcc->GetText()) == 0 ? false : true;

    tinyxml2::XMLElement* is_interactive = root->FirstChildElement("is_interactive");
    kInteractive_Active = std::stoi(is_interactive->GetText()) == 0 ? false : true;

    tinyxml2::XMLElement* is_priority = root->FirstChildElement("is_priority");
    kPriority_Active = std::stoi(is_priority->GetText()) == 0 ? false : true;

    tinyxml2::XMLElement* is_hot_row_lock = root->FirstChildElement("is_hot_row_lock");
    kHotRow_Active = std::stoi(is_hot_row_lock->GetText()) == 0 ? false : true;

    tinyxml2::XMLElement* is_all_pessimistic_lock = root->FirstChildElement("is_all_pessimistic_lock");
    kAllPessimisticLock = std::stoi(is_all_pessimistic_lock->GetText()) == 0 ? false : true;

    tinyxml2::XMLElement* is_pre_lock_check = root->FirstChildElement("is_pre_lock_check");
    kPreLockCheck_Active = std::stoi(is_pre_lock_check->GetText()) == 0 ? false : true;

    tinyxml2::XMLElement* is_wound_wait_check = root->FirstChildElement("is_wound_wait_enable");
    is_wound_wait_enable = std::stoi(is_wound_wait_check->GetText()) == 0 ? false : true;

    // is_debug_print_enable
    tinyxml2::XMLElement* is_debug_print_check = root->FirstChildElement("is_debug_print_enable");
    is_debug_print_enable = std::stoi(is_debug_print_check->GetText()) == 0 ? false : true;

    // 开启事务并发控制切换
    tinyxml2::XMLElement* is_CC_Switch_check = root->FirstChildElement("is_CC_Switch_enable");
    is_CC_Switch_enable = std::stoi(is_CC_Switch_check->GetText()) == 0 ? false : true;

    tinyxml2::XMLElement* epoch_limit_check = root->FirstChildElement("kEpochLimit");
    kEpochLimit = std::stoull(epoch_limit_check->GetText());

    tinyxml2::XMLElement* read_cnt_check = root->FirstChildElement("kReadCntLimit");
    kReadCntLimit = std::stoull(read_cnt_check->GetText());

    tinyxml2::XMLElement* write_cnt_check = root->FirstChildElement("kWriteCntLimit");
    kWriteCntLimit = std::stoull(write_cnt_check->GetText());

    tinyxml2::XMLElement* hot_cnt_epoch_check = root->FirstChildElement("kHotCntEpochLen");
    kHotCntEpochLen = std::stoull(hot_cnt_epoch_check->GetText());

    tinyxml2::XMLElement* hot_cnt_check = root->FirstChildElement("kHotCntLimit");
    kHotCntLimit = std::stoull(hot_cnt_check->GetText());

    tinyxml2::XMLElement* epoch_weight_check = root->FirstChildElement("kEpochWeight");
    kEpochWeight = std::stoull(epoch_weight_check->GetText());

    tinyxml2::XMLElement* read_weight_check = root->FirstChildElement("kReadCntWeight");
    kReadCntWeight = std::stoull(read_weight_check->GetText());

    tinyxml2::XMLElement* write_weight_check = root->FirstChildElement("kWriteCntWeight");
    kWriteCntWeight = std::stoull(write_weight_check->GetText());

    tinyxml2::XMLElement* switch_limit_check = root->FirstChildElement("kSwitchLimit");
    kSwitchLimit = std::stoull(switch_limit_check->GetText());

    // 修改配置信息
    MOTAdaptor::dynamic_hot_rows.freq_ = kHotRowsFreq;
}



/////////////// wzy: 死锁检测    // delete
bool HasCycle(std::string& target_tid)
{
    std::unordered_set<std::string> visited;  // 事务是否visited
    std::function<bool(std::string)> has_cycle = [&](std::string tid) -> bool {
        if (MOTAdaptor::wait_for.find(tid) == MOTAdaptor::wait_for.end()) {
            return false;
        }
        for (auto& wait_for_txn : MOTAdaptor::wait_for[tid]) {
            if (visited.count(wait_for_txn) == 1) {
                target_tid = wait_for_txn;
                for (const auto& visited_txn : visited) {
                    // 该visited是一个环
                    target_tid = std::max(target_tid, visited_txn);  // 找到最近才启动的事务
                }
                return true;
            }
            visited.insert(wait_for_txn);
            if (has_cycle(wait_for_txn)) {
                return true;
            }
            visited.erase(wait_for_txn);
        }
        return false;
    };
    for (auto txn : MOTAdaptor::wait_for){
        if (visited.find(txn.first) != visited.end())
            continue;
        if(has_cycle(txn.first)) return true;
    }
    return false;
}

// wzy: 所有lockinfo 都插入queue之后进行检测，及时abort   // delete
void DeadlockDetection()
{
    // 间隔多久检查一次，必须是在epoch commit阶段后且上锁全部完成，不然会出错
    if (MOTAdaptor::wait_for.empty()) {
        return;
    }
    std::string target_tid;
    while (HasCycle(target_tid)) {  // csn + server id
        // abort处理，添加到abort_transcation_csn_set
        MOTAdaptor::abort_transcation_csn_set.insert(target_tid, target_tid);
        // 事务从活跃表中移除 // 暂时不用
        MOT_LOG_INFO("[ DeadLock detection ] tid = %s ABORT", target_tid.c_str());

        // 从LockRequestQueue中删除
        std::shared_ptr<std::vector<std::string>> row_id_set;
        std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
        for (auto tmp_rowid : *row_id_set) {
            if (MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue) {
                tmp_queue->remove_lock_request(target_tid);
            }
        }
//        for (const auto& tmp_queue : MOTAdaptor::active_queue_list) {
//            tmp_queue->remove_lock_request(target_tid);
//        }
//        MOTAdaptor::csn_row_map.remove(target_tid);

        tmp_queue = nullptr;
        // 直接修改wait_for，一个epoch结束消除所有死锁
        for(auto &node : MOTAdaptor::wait_for) {
            node.second.erase(target_tid);
        }
        MOTAdaptor::wait_for.erase(target_tid);
    }
    MOTAdaptor::wait_for.clear();
}


// wzy: CRLS 环检测
void DeadlockDetection_CRLS()
{
    std::vector<std::string> target_tids;
    std::unordered_set<std::string> target_set;

    //    auto res = MOTAdaptor::wait_for_graph.CLRS_Cycles(target_tids, target_set);       // 无拷贝，直接加锁

//    auto res = MOTAdaptor::wait_for_graph.CLRS_Cycles1(target_tids, target_set);

    auto res = MOTAdaptor::wait_for_graph.JOHNSON_Cycles1(target_tids, target_set);
    if (!res)
        return;
    if (cc_mode == 1) {
        for (std::string& target_tid : target_tids) {
            // abort处理，添加到abort_transcation_csn_set
            MOTAdaptor::abort_transcation_csn_set.insert(target_tid, target_tid);
            MOTAdaptor::deadlock_abort_set.insert(target_tid, target_tid);
            MOTAdaptor::DeadLock_abort_num.fetch_add(1);

            // 移除queue中abort的事务
            //        std::shared_ptr<std::vector<std::shared_ptr<MOTAdaptor::LockRequestQueue>>> tmp_vec = nullptr;
            //        if(MOTAdaptor::csn_requests_map.get_element(target_tid, tmp_vec) && tmp_vec) {
            //            for (const auto& queue : *tmp_vec) {
            //                queue->remove_lock_request(target_tid);
            //            }
            //        }
            //        MOTAdaptor::csn_requests_map.remove(target_tid);

            std::shared_ptr<MOTAdaptor::LockRequestQueue> tmp_queue = nullptr;
            std::shared_ptr<std::vector<std::string>> tmp_vec_1 = nullptr;
            if (MOTAdaptor::txn_rowid_map.get_element(target_tid, tmp_vec_1) && tmp_vec_1) {
                for (auto& tmp_rowid : *tmp_vec_1) {
                    if (MOTAdaptor::row_lockrequest_map.get_element(tmp_rowid, tmp_queue) && tmp_queue) {
                        tmp_queue->remove_lock_request(target_tid);
                    }
                }
            }
            MOTAdaptor::txn_rowid_map.remove(target_tid);

            MOT_LOG_INFO("[Deadlock Detection] target txn =  %s", target_tid.c_str());
            // 从wait_for图中删除target_tid
            MOTAdaptor::wait_for_graph.removeNode(target_tid);
        }
    } else if (cc_mode == 3) {
        for (std::string& target_tid : target_tids) {
            // TODO: 找到环中优先级最低的进行中止
            // abort处理，添加到abort_transcation_csn_set
            MOTAdaptor::abort_transcation_csn_set.insert(target_tid, target_tid);
            MOTAdaptor::deadlock_abort_set.insert(target_tid, target_tid);
            MOTAdaptor::DeadLock_abort_num.fetch_add(1);

            MOT_LOG_INFO("[Deadlock Detection] target txn =  %s", target_tid.c_str());
            // 从wait_for图中删除target_tid
            MOTAdaptor::wait_for_graph.removeNode(target_tid);
        }

    }
}



// wzy: 定期清除过期版本
void EpochCleanVersionThreadMain(uint64_t id) {
    while(true) {
        if(MOTAdaptor::IsNeedClean()) {
            std::unique_lock<std::mutex> txn_lock(MOTAdaptor::active_txn_list_mutex);
            std::unique_lock<std::mutex> row_lock(MOTAdaptor::clean_row_list_mutex);
            TransactionId min_active_txn = INT64_MAX_VALUE;
            if (!MOTAdaptor::active_txn_list.empty()) {
                min_active_txn = *MOTAdaptor::active_txn_list.begin();
            }
            txn_lock.unlock();
            // 更新最小活跃txn id，并进行清除
            for (auto row : MOTAdaptor::clean_row_list) {
                row->GetRowHeader()->CleanupVersions(min_active_txn);
            }
            row_lock.unlock();
            MOTAdaptor::SetNeedClean(false);
        } else usleep(200);
    }
}


void EpochUnseriThreadMain(uint64_t id){//任务转移至merge线程工作
}

void EpochMessageCacheManagerThreadMain(uint64_t id){//任务转移至merge线程和Logical线程工作
}

void EpochUnpackThreadMain(uint64_t id){
}

void EpochRecordCommitThreadMain(uint64_t id) {//deleted
    
}



































///          part of Raft implementation


void SendChangeServerStateRequest(uint64_t server_id, uint64_t epoch_id, uint64_t state) {
    auto new_msg = std::make_unique<merge::Message>();
    auto* req = new_msg->mutable_request();
    auto* change_server_state = req->mutable_change_server_state();
    change_server_state->set_from(local_ip_index);
    change_server_state->set_target(server_id);
    change_server_state->set_epoch_id(epoch_id);
    change_server_state->set_state(state);
    Send(Gzip(std::move(new_msg)));
    MOT_LOG_INFO("发送一个ChangeServerStateRequest local_ip_index %llu target: %llu epoch: %llu state: %llu", local_ip_index, server_id, epoch_id, state);
}


void MultiRaftState::SetServerPongState(uint64_t server_id, uint64_t time, uint64_t epoch_id) {
    SetServerReplyTime(server_id, time);
    SetServerReplyEpoch(server_id, epoch_id);
}

uint64_t MultiRaftState::SetServerState(uint64_t server_id, uint64_t epoch_id, uint64_t state) {
    // change_server_state_epoch[server_id]->store(epoch_id);
    // change_server_state[server_id]->store(state);
    SetServerState(server_id, state);
    _num = GetOnLineServerNum();
    if(_num >= 3) {
        _num = ((_num / 3) * 2) - 1;
    }
    else if(_num == 2) {
        _num = 1;
    }
    else {
        _num = 0;
    }
}

uint64_t MultiRaftState::CheckServerState(uint64_t& server_id, uint64_t& epoch_id, uint64_t& state) {
    if(MOTAdaptor::GetPhysicalEpoch() < kRaftStartCheckEpoch) return 0;
    auto now_time = now_to_us();
    for(int i = 0; i < (int)_size; i ++) {
        if(i == (int)local_ip_index) continue;
        if(server_state[i]->load() == 1 && now_time - server_reply_time[i]->load() >= kServerTimeOut_us) {
            server_id = i;
            state = 0;
            return 1;
        }
        if(server_state[i]->load() == 0 && now_time - server_reply_time[i]->load() < kServerTimeOut_us) {
            server_id = i;
            state = 1;
            return 2;
        }
    }
    return 0;
}

void MultiRaftThreadMain(uint64_t id) {
    SetCPU();
    MOT_LOG_INFO("MultiRaftthreadMain 开始工作");
    std::unique_ptr<merge::Message> msg;
    while(!init_ok.load()) usleep(200);
    bool sleep_flag = false;
    uint64_t last_ping_time = 0;
    for(;;) {
        raft_message_pool.wait_dequeue(msg);
        if(msg != nullptr) {
            switch (msg->type_case()) {
                case merge::Message::TypeCase::kRequest :
                    MultiRaftState::HandleRaftRequest(std::move(msg));
                    break;

                case merge::Message::TypeCase::kResponse :
                    MultiRaftState::HandleRaftResponse(std::move(msg));
                    break;
                default:

                    break;
            }
        }
    }
}

void MultiRaftState::HandleRaftRequest(std::unique_ptr<merge::Message> && msg) {
    auto& req = msg->request();
    auto new_msg = std::make_unique<merge::Message>();
    auto* rep = new_msg->mutable_response();
    switch (req.type_case()) {
        case merge::Request::TypeCase::kPing : {
            MOT_LOG_INFO("收到一个RaftPingRequest from server: %llu %llu", 
                        req.ping().from(), now_to_us());
            if(local_ip_index == kRaftStopServerId && kRaftStopEpoch > 0 && 
                MOTAdaptor::GetPhysicalEpoch() > kRaftStopEpoch && MOTAdaptor::GetPhysicalEpoch() < kRaftRestrtEpoch) {
                MOT_LOG_INFO("宕机期间不处理ping request");
                break;
            }
            auto* pong = rep->mutable_pong();
            pong->set_from(local_ip_index);
            pong->set_to(req.ping().from());
            pong->set_epoch_id(MOTAdaptor::GetLogicalEpoch());
            pong->set_time(now_to_us());
            Send(Gzip(std::move(new_msg)), req.ping().from());
            MOT_LOG_INFO("发送一个RaftPongResponse to server: %llu %llu", 
                        req.ping().from(), now_to_us());
            break;
        }

        case merge::Request::TypeCase::kRaftAccept : {///one server(leader) to other servers(followers) , ask the receive state
            auto message_epoch_mod = req.raft_accept().epoch_id() % MOTAdaptor::max_length;
            auto server_id = req.raft_accept().from();
            if(MOTAdaptor::GetLogicalEpoch() > req.raft_accept().epoch_id()) {
                auto* accept = rep->mutable_raft_accept();
                accept->set_from(local_ip_index);
                accept->set_to(server_id);
                accept->set_epoch_id(req.raft_accept().epoch_id());
                accept->set_result(1);
                Send(Gzip(std::move(new_msg)), server_id);
            }
            else {
                if( (MOTAdaptor::GetReceivedPackNum(message_epoch_mod, server_id) < 1 ||
                    MOTAdaptor::GetReceivedTxnNum(message_epoch_mod, server_id) < MOTAdaptor::GetShouldReceiveTxnNum(message_epoch_mod, server_id))) {
                        // MOT_LOG_INFO("重新处理RaftAcceptRequest from server: %llu, epoch: %llu %llu", 
                        //     server_id, req.raft_accept().epoch_id(), now_to_us());
                        raft_message_pool.enqueue(std::move(msg));
                        raft_message_pool.enqueue(std::move(std::make_unique<merge::Message>()));
                        break;
                    }
                
                /// once received compelete, response to the server
                ///current server as a follower response to the leader receive complete
                MultiRaftState::SetFollowerAcceptEpochState(message_epoch_mod, server_id, 1);
                auto* accept = rep->mutable_raft_accept();
                accept->set_from(local_ip_index);
                accept->set_to(server_id);
                accept->set_epoch_id(req.raft_accept().epoch_id());
                accept->set_result(1);
                Send(Gzip(std::move(new_msg)), server_id);
                MOT_LOG_INFO("发送一个RaftAcceptResponse to server: %llu, epoch: %llu %llu", 
                        server_id, req.raft_accept().epoch_id(), now_to_us());
            }
            break;
        }

        case merge::Request::TypeCase::kRaftCommit : { ///one server to other servers , send commit command
            MOT_LOG_INFO("收到一个RaftCommitRequest from server: %llu, epoch: %llu, pack num %llu, txn num: %llu %llu,  %llu", 
                req.raft_commit().from(), req.raft_commit().epoch_id(), MOTAdaptor::GetReceivedPackNum(req.raft_commit().epoch_id(), req.raft_commit().from()),
                MOTAdaptor::GetReceivedTxnNum(req.raft_commit().epoch_id(), req.raft_commit().from()),
                MOTAdaptor::GetShouldReceiveTxnNum(req.raft_commit().epoch_id(), req.raft_commit().from()), now_to_us());
            MultiRaftState::SetFollowerCommitEpochState(req.raft_commit().epoch_id(), req.raft_commit().from(), 1);
            /// current server as a follower response to the leader commit
            auto* commit = rep->mutable_raft_commit();
            commit->set_from(local_ip_index);
            commit->set_to(req.raft_commit().from());
            commit->set_epoch_id(req.raft_commit().epoch_id());
            commit->set_result(1);
            Send(Gzip(std::move(new_msg)), req.raft_commit().from());
            // MOT_LOG_INFO("发送一个RaftCommiResponse to server: %llu, epoch: %llu, %llu", req.raft_commit().from(), req.raft_commit().epoch_id(), now_to_us());
            break;
        }

        case merge::Request::TypeCase::kChangeServerState : {
            MOT_LOG_INFO("收到一个ChangeServerStateRequest server_id %llu epoch_id %llu state %llu", req.change_server_state().target(), 
                req.change_server_state().epoch_id(), req.change_server_state().state());
            MultiRaftState::SetServerState(req.change_server_state().target(), req.change_server_state().epoch_id(), req.change_server_state().state());
        }

        default:
            break;
    }
}

void MultiRaftState::HandleRaftResponse(std::unique_ptr<merge::Message> && msg) {
    auto& rep = msg->response();
    auto new_msg = std::make_unique<merge::Message>(); 
    auto* req = new_msg->mutable_request();
    switch (rep.type_case()) {
        case merge::Response::TypeCase::kPong : {
            if(is_server_leader) {
                MOT_LOG_INFO("Server back to online from server_id %llu time %llu epoch %llu", 
                    rep.pong().from(), rep.pong().time(), rep.pong().epoch_id());
                MultiRaftState::SetServerPongState(rep.pong().from(), rep.pong().time(), rep.pong().epoch_id());
                uint64_t server_id, epoch_id, state;
                auto res = MultiRaftState::CheckServerState(server_id, epoch_id, state);
                if(res == 0) {//normal
                    // nothing to do
                }
                else if(res == 1) {// server offline
                    MOT_LOG_INFO("Server OFFLine server_id %llu epoch_id %llu", server_id, epoch_id);
                    SendChangeServerStateRequest(server_id, epoch_id, state);
                    MultiRaftState::SetServerState(server_id, epoch_id, state);
                }
                else if(res == 2) {// server back to online
                    MOT_LOG_INFO("Server back to online server_id %llu epoch_id %llu", server_id, epoch_id);
                    SendChangeServerStateRequest(server_id, epoch_id, state);
                    MultiRaftState::SetServerState(server_id, epoch_id, state);
                }
            }
            break;
        }

        case merge::Response::TypeCase::kRaftAccept : {///one server to one server  report the receive state
            if(rep.raft_accept().to() == local_ip_index) {
                MOT_LOG_INFO("收到一个RaftAcceptResponse local_ip_index %llu, from %llu to server: %llu, epoch: %llu, %llu", 
                    local_ip_index, rep.raft_accept().from(), rep.raft_accept().to(), rep.raft_accept().epoch_id(), now_to_us());
                MultiRaftState::SetLeaderAcceptEpochState(rep.raft_accept().epoch_id(), rep.raft_accept().from(), 1);
                if(rep.raft_accept().epoch_id() < MOTAdaptor::GetLogicalEpoch()) {
                    auto* commit = req->mutable_raft_commit();
                    commit->set_from(local_ip_index);
                    commit->set_to(0);
                    commit->set_epoch_id(rep.raft_accept().epoch_id());
                    Send(Gzip(std::move(new_msg)));
                }
                else {
                    if(local_ip_index == kRaftStopServerId && kRaftStopEpoch > 0 && 
                        MOTAdaptor::GetPhysicalEpoch() > kRaftStopEpoch && MOTAdaptor::GetPhysicalEpoch() < kRaftRestrtEpoch) {
                        MOT_LOG_INFO("宕机期间不处理Accept response 不发送CommitAccept");
                        break;
                    }
                    if(MultiRaftState::IsEpochTransmitComplete(rep.raft_accept().epoch_id())) {
                        /// if followers received complete, current server as a leader send commit command to followers
                        if(MultiRaftState::GetLeaderCommitEpochState(rep.raft_accept().epoch_id(), local_ip_index) == 0) {
                            MOT_LOG_INFO("发送一个RaftCommitRequest from server: %llu, to others epoch: %llu %llu", 
                                local_ip_index, rep.raft_accept().epoch_id(), now_to_us());
                            auto* commit = req->mutable_raft_commit();
                            commit->set_from(local_ip_index);
                            commit->set_to(0);
                            commit->set_epoch_id(rep.raft_accept().epoch_id());
                            Send(Gzip(std::move(new_msg)));
                            MultiRaftState::SetLeaderCommitEpochState(rep.raft_accept().epoch_id(), local_ip_index, 1);
                        }
                    }
                }
            }
            break;
        }

        case merge::Response::TypeCase::kRaftCommit : {///one server to one server  report the commit state
            // MOT_LOG_INFO("收到一个RaftCommitResponse local_ip_index %llu, from %llu to server: %llu, epoch: %llu, %llu", 
            //     local_ip_index, rep.raft_commit().from(), rep.raft_commit().to(), rep.raft_commit().epoch_id(), now_to_us());
            if(rep.raft_commit().to() == local_ip_index) {
                /// current server as a leader collect other servers' commit state
                MultiRaftState::SetLeaderCommitEpochState(rep.raft_commit().epoch_id(), rep.raft_commit().from(), 1);
            }
            break;
        }
        
        default:
            break;
    }
}

void MultiRaftState::SendHeartBeat() {
    auto new_msg = std::make_unique<merge::Message>();
    auto* rep = new_msg->mutable_response();
    auto* pong = rep->mutable_pong();
    pong->set_from(local_ip_index);
    pong->set_to(0);
    pong->set_time(now_to_us());
    Send(Gzip(std::move(new_msg)));
}


/*
server
39.101.128.98
39.99.152.57
cache
8.142.85.130
39.99.158.230
*/

























































































































//===================== Cache Server ============================

struct ListenParams {///delete
    uint64_t remote_ip_index, current_epoch;
    std::shared_ptr<zmq::socket_t> socket_listen;
    std::shared_ptr<std::atomic<uint64_t>> total_pack_num, message_received;
    std::shared_ptr<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> job_num;
    std::shared_ptr<BlockingConcurrentQueue<std::unique_ptr<merge::Message>>> queue;
    void Init(uint64_t value1, uint64_t value, std::shared_ptr<zmq::socket_t> socket, std::shared_ptr<std::atomic<uint64_t>> value2, 
        std::shared_ptr<std::atomic<uint64_t>> value3, std::shared_ptr<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> val,
        std::shared_ptr<BlockingConcurrentQueue<std::unique_ptr<merge::Message>>> value4) {
            remote_ip_index = value1 , current_epoch = value, socket_listen = socket, total_pack_num = value2, message_received = value3, job_num = val,
            queue = value4;
        }
    
};

void* ListenThread(void* param) {///delete
    ListenParams* params = (ListenParams*) param;
    auto port = 40000 + params->remote_ip_index * 100 + local_ip_index;
    auto index = params->remote_ip_index;
    auto txn_queue = params->queue;
    uint64_t received_total_pack = 0, received_total_txn_num = 0, should_received_total_txn_num = 0;
    zmq::context_t listen_context(1);
    zmq::socket_t socket_listen(listen_context, ZMQ_PULL);
    int queue_length = 0;
    socket_listen.setsockopt(ZMQ_RCVHWM, &queue_length, sizeof(queue_length));
    MOT_LOG_INFO("***Receive From CacheServer Start PULL %s", 
        ("tcp://" +kCacheServerIp[params->remote_ip_index] + ":" + std::to_string(port)).c_str());
    socket_listen.bind("tcp://*:" + std::to_string(port));
    while(true) {
        auto message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));
        auto msg_ptr = std::make_unique<merge::Message>();
        auto message_string_ptr = std::make_unique<std::string>(static_cast<const char*>(message_ptr->data()), message_ptr->size());
        msg_ptr->ParseFromString(*message_string_ptr);
        auto &txn = msg_ptr->txn();
        auto message_epoch_id = txn.commitepoch();
        auto server_id = txn.server_id();
        if(server_id != index) Assert(false);
        auto message_epoch_mod = message_epoch_id % max_length;
        if(txn.row_size() == 0 && txn.startepoch() == 0 && txn.txnid() == 0) {
            MOTAdaptor::StoreShouldReceiveTxnNum(message_epoch_id, index, txn.csn());
            MOTAdaptor::StoreReceivedPackNum(message_epoch_id, index, 1);
            should_received_total_txn_num += txn.csn();
            received_total_pack ++;
            MOT_LOG_INFO("Receive From Cache Server 收到一个pack server: %llu, epoch: %llu, index:%llu pack num %llu, txn num: %llu,  %llu", 
                    txn.server_id(), message_epoch_id, index, MOTAdaptor::GetReceivedPackNum(message_epoch_mod),
                    MOTAdaptor::GetShouldReceiveTxnNum(message_epoch_mod), now_to_us());
        }
        else{
            txn_queue->enqueue(std::move(msg_ptr));
            received_total_txn_num ++;
            txn_queue->enqueue(std::move(std::make_unique<merge::Message>()));//防止moodycamel取不出
        }
        //未接收到cache的消息之前和epoch txn接受完全为完成之前 不退出
        if(params->message_received->load(std::memory_order_acquire) == 1 && received_total_pack == params->total_pack_num->load(std::memory_order_acquire) &&  
            received_total_txn_num == should_received_total_txn_num) {
            break;
        }
    }
    MOT_LOG_INFO("***Receive From CacheServer End PULL %s", 
        ("tcp://" +kCacheServerIp[params->remote_ip_index] + ":" + std::to_string(port)).c_str());
}

void* MergeThread(void* param){///delete
    ListenParams* params = (ListenParams*) param;
    auto index = params->remote_ip_index;
    auto txn_queue = params->queue;
    bool sleep_flag = false;
    std::unique_ptr<merge::Message> msg_ptr;
    std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::Message>>>>> message_cache;
    message_cache.reserve(max_length + 1);
    for(int i = 0; i < (int)max_length; i ++) {
        message_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::Message>>>>());
        for(int j = 0; j < (int)kServerIp.size() + 2; j++) {
            message_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<merge::Message>>>());
        }
    }
    //未接收到cache的消息之前和epoch merge为完成之前 不退出
    while(true) {
        sleep_flag = true;
        if(params->message_received->load(std::memory_order_acquire) == 1 && 
            (params->total_pack_num->load(std::memory_order_acquire) + params->current_epoch) <= MOTAdaptor::GetLogicalEpoch()) {
                break;
        }
        if(txn_queue->try_dequeue(msg_ptr) && msg_ptr != nullptr) {
            auto &txn = msg_ptr->txn(); 
            auto message_epoch_mod = txn.commitepoch() % max_length;
            message_cache[message_epoch_mod][index]->push(std::move(msg_ptr));
            sleep_flag = false;
        }
        auto epoch_mod = MOTAdaptor::GetLogicalEpoch();
        if(MOTAdaptor::GetReceivedPackNum(epoch_mod, index) == 1 && 
            MOTAdaptor::GetReceivedTxnNum(epoch_mod, index) == MOTAdaptor::GetShouldReceiveTxnNum(epoch_mod, index)) {
            while(!message_cache[epoch_mod][index]->empty()){
                auto msg_ptr_tmp = std::move(message_cache[epoch_mod][index]->front());
                message_cache[epoch_mod][index]->pop();
                if(!merge_queue.enqueue(std::move(msg_ptr_tmp))) Assert(false); //防止moodycamel取不出
            }
            if(!merge_queue.enqueue(std::move(std::make_unique<merge::Message>()))) Assert(false); //防止moodycamel取不出
        }
        if(sleep_flag) {
            usleep(200);
        }
    }
    MOTAdaptor::SubShouldReceivePackNum(1);
    MOTAdaptor::SetServerOffLine(index);
    (*(params->job_num))[index]->store(0);
    MOT_LOG_INFO("***服务器退出集群 server_ip_index : %llu ip:%s", index, kServerIp[index].c_str() );
}

uint64_t StartThreads(ListenParams* ptr) {
    pthread_t thread_listen, thread_merge;
    if(pthread_create(&thread_listen, NULL, ListenThread, (void*)ptr)) {      
        return 1;
    }
    if(pthread_create(&thread_merge, NULL, MergeThread, (void*)ptr)) {      
        return 1;
    } 
    return 0;
}

uint64_t GetRaftServerNum() {///delete
    if(MOTAdaptor::GetShouldReceivePackNum() == 0) return MOTAdaptor::GetShouldReceivePackNum();
    return (MOTAdaptor::GetShouldReceivePackNum() - 1);
}

void SendCacheTxn(uint64_t epoch) {
    epoch %= MOTAdaptor::_max_length;
    auto msg_ptr = std::make_unique<merge::Message>();
    while(cache_txn_queues[epoch]->try_dequeue(msg_ptr)) {
        if(msg_ptr->type_case() == merge::Message::TypeCase::kTxn) {
            if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(0, 1, Gzip(std::move(msg_ptr)))))) Assert(false);
            if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(0, 0, std::move(nullptr)))) ) Assert(false);
        }
    }
}

void EpochMessageManagerThreadMain(uint64_t id) {///delete
    std::unique_ptr<zmq::message_t> message_ptr;
    std::string local_ip = kServerIp[local_ip_index];
    std::map<std::string, std::string> _map;
    std::map<uint64_t, std::shared_ptr<std::atomic<uint64_t>>> _map_message_received, _map_total_pack_num;
    std::unique_ptr<merge::ServerMessage> _msg;
    uint64_t remote_ip_index;
    bool sleep_flag = true;
    std::shared_ptr<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> job_num = std::make_shared<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>(0);
    for(int i = 0; i < (int)kServerIp.size() + 2; i ++) {
        job_num->push_back(std::make_unique<std::atomic<uint64_t>>(0));
        _map_message_received[i] = std::make_shared<std::atomic<uint64_t>>(0);
        _map_total_pack_num[i] = std::make_shared<std::atomic<uint64_t>>(0);
    }
    while(!is_epoch_advance_started.load()) usleep(200);
    for(;;) {
        sleep_flag = true;
        if(is_fault_tolerance_enable || is_cache_server_available) {
            while(message_receive_pool.try_dequeue(message_ptr)) {
                if(message_ptr == nullptr || message_ptr->size() > 0) continue;
                _map.clear();
                std::unique_ptr<std::string> message_string_ptr= 
                    std::make_unique<std::string>(static_cast<const char*>(message_ptr->data()), message_ptr->size());
                _msg = std::make_unique<merge::ServerMessage>();
                _msg->ParseFromString(*message_string_ptr);
                // MOT_LOG_INFO("MessageManager接收一条消息 %s  %s\n", _msg->receive_ip().c_str(), local_ip.c_str());
                if(_msg->receive_ip().compare(local_ip) == 0 || _msg->receive_ip().compare(kPrivateIp) == 0) {
                    for(int i = 0; i < _msg->msg_size(); i++) {
                            _map[_msg->msg(i).key()] = _msg->msg(i).value();
                    }
                    _map["send_ip"] = _msg->send_ip();
                    for(int i = 0; i < (int)kServerIp.size(); i++) {
                        if(kServerIp[i] == _map["send_ip"] || kCacheServerIp[i] == _map["send_ip"]) {
                            remote_ip_index = i;
                            break;
                        }
                    }
                    _map["port"] = _msg->port();
                    if(_map["message_type"] == "cache_reply") {
                        if(_map["receive_compelete"] == std::to_string(true)) {
                            MOTAdaptor::SetCacheServerStored(std::stoull(_map["epoch_num"]), 1);
                        }
                    }

                    if(_map["message_type"] == "cache_txn_reply" ) {
                        MOT_LOG_INFO("***CacheServer Txn Reply cache_index:%llu, cache_epoch_num:%llu", remote_ip_index, std::stoull(_map["cache_epoch_num"]))
                            _map_total_pack_num[remote_ip_index]->store(std::stoull(_map["cache_epoch_num"]));
                        _map_message_received[remote_ip_index]->store(1);
                    }

                    if(_map["message_type"] == "server_txn_request") {
                        SendCacheTxn(std::stoull(_map["epoch_num"]));
                    }
                    sleep_flag = false;

                }
                else {
                    continue;
                }
            }
            auto now_time = now_to_us();
            if(is_fault_tolerance_enable &&
                now_time - epoch_commit_time >= kServerTimeOut_us && MOTAdaptor::GetRecordCommittedTxnCounters(MOTAdaptor::GetLogicalEpoch()) >= kStartCheckStateNum) {
                //load数据是会出现短暂的线程调度问题，导致误判，需要度过这一段时间，默认load数量为100000
                uint64_t current_epoch = MOTAdaptor::GetLogicalEpoch();
                if(MOTAdaptor::GetReceivedPackNum(current_epoch) != MOTAdaptor::GetShouldReceivePackNum()){    
                    if(MOTAdaptor::GetShouldReceivePackNum() > GetRaftServerNum() ) {
                        remote_ip_index = local_ip_index;
                        for(int i = 0; i < (int)kServerIp.size(); i++) {
                            if(i != (int)local_ip_index &&  (*job_num)[i]->load(std::memory_order_acquire) != 1 && 
                                MOTAdaptor::IsServerOnLine(i) && MOTAdaptor::GetReceivedPackNum(current_epoch, i) != 1) {
                                    remote_ip_index = i;
                                    _map_total_pack_num[remote_ip_index]->store(kCacheMaxLength);
                                    _map_message_received[remote_ip_index]->store(0);
                                    auto param = new ListenParams();
                                    param->Init(remote_ip_index, current_epoch, nullptr/*socket_vector[remote_ip_index]*/, 
                                        _map_total_pack_num[remote_ip_index], _map_message_received[remote_ip_index], job_num, 
                                        std::make_shared<BlockingConcurrentQueue<std::unique_ptr<merge::Message>>>());
                                    if(StartThreads(param) == 1) {
                                            Assert(false);
                                    }
                                    (*job_num)[i]->store(1);
                                    usleep(1000);
                                    break;
                            }
                        }
                        if(remote_ip_index != local_ip_index) {
                            MOT_LOG_INFO("***出现服务器超时现象 server_ip_index :%llu %llu ip:%s",MOTAdaptor::GetLogicalEpoch(),
                                remote_ip_index, kServerIp[remote_ip_index].c_str() );
                            std::unique_ptr<merge::ServerMessage> send_server_message_ptr = std::make_unique<merge::ServerMessage>();
                            merge::ServerMessage_Msg* msg;
                            send_server_message_ptr->set_send_ip(kServerIp[local_ip_index]);
                            send_server_message_ptr->set_receive_ip(kCacheServerIp[remote_ip_index]);
                            msg = send_server_message_ptr->add_msg();
//                            msg->set_key("message_type");
                            msg->set_value("cache_txn_request");
                            msg = send_server_message_ptr->add_msg();
//                            msg->set_key("epoch_num");
                            msg->set_value(std::to_string(current_epoch));

                            auto* serialized_txn_str_ptr = new std::string();
                            send_server_message_ptr->SerializeToString(serialized_txn_str_ptr);
                            std::unique_ptr<zmq::message_t> send_message_ptr = std::make_unique<zmq::message_t>(serialized_txn_str_ptr->size());
                            memcpy(send_message_ptr->data(), serialized_txn_str_ptr->c_str(), serialized_txn_str_ptr->size());
                            message_send_pool.enqueue(std::move(send_message_ptr));
                            sleep_flag = false;
                            if(is_cache_server_available == false) {
                                _map_total_pack_num[remote_ip_index]->store(0);
                                _map_message_received[remote_ip_index]->store(1);
                            }
                        }
                    }
                    else {

                    }
                }

            }
            if(sleep_flag)
                usleep(200);
        }
        else {
            message_receive_pool.wait_dequeue(message_ptr);
        }
    }
}

void EpochMessageSendThreadMain(uint64_t id) {///delete
    zmq::context_t context(1);
    zmq::message_t reply(5);
    int queue_length = 0;
    zmq::socket_t socket_send(context, ZMQ_PUB);
    socket_send.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length)); 
    socket_send.bind("tcp://*:5547");//to server
    socket_send.bind("tcp://*:5548");//to cache
    MOT_LOG_INFO("线程开始工作 EpochMessageSendThreadMain");
    std::unique_ptr<send_thread_params> params;
    std::unique_ptr<zmq::message_t> msg;
    while(true) {
        message_send_pool.wait_dequeue(msg);
        if(msg == nullptr) continue;
        socket_send.send(*(msg));
        MOT_LOG_INFO("发送一条消息");
    }
}

void EpochMessageListenThreadMain(uint64_t id) {///delete
	zmq::context_t listen_context(1);
    zmq::socket_t socket_listen(listen_context, ZMQ_SUB);
    int queue_length = 0;
    socket_listen.setsockopt(ZMQ_SUBSCRIBE, "", 0);
    socket_listen.setsockopt(ZMQ_RCVHWM, &queue_length, sizeof(queue_length));
    for(int i = 0; i < (int)kServerIp.size(); i ++) {
        if(i == (int)local_ip_index) continue;
        socket_listen.connect("tcp://" +kServerIp[i] + ":5547");
        MOT_LOG_INFO("线程开始工作 EpochMessageListenThreadMain %s", ("tcp://" +kServerIp[i] + ":5547").c_str());
    }
    for(int i = 0; i < (int)kCacheServerIp.size(); i ++) {
        socket_listen.connect("tcp://" +kCacheServerIp[i] + ":5549");
        MOT_LOG_INFO("线程开始工作 EpochMessageListenThreadMain %s", ("tcp://" +kCacheServerIp[i] + ":5549").c_str());
    }
    for(;;) {
        std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));

        message_receive_pool.enqueue(std::move(message_ptr));
        message_receive_pool.enqueue(std::move(std::make_unique<zmq::message_t>()));
    }

}


///backup
//// begin
////message MergeRequest {
////  message Transaction{
////    message Row{
////      message Column {
////        uint64 id = 1; // column id
////        bytes value = 2; // column value/data
////      }
////      string tableName = 1;
////      bytes key = 2;
////      uint32 type = 3; /// 0-update, 1-insert, 2-delete
////      repeated Column column = 4;
////      bytes data = 5;
////    }
////    repeated Row row = 2;
////    uint64 StartEpoch = 3;
////    uint64 CommitEpoch = 4;
////    uint64 CSN = 5;
////    uint64 server_id = 6;
////    uint64 txnid = 7;
////  }
////  repeated Transaction Txn = 1
////  uint64 server_id = 2;
////  uint64 epoch = 3;
////  uint64 pack_id = 4;
////}
//
//
//message MergeRequest {
//    message Transaction{
//        message Row{
//            message Column {
//                uint64 id = 1; // column id
//                bytes value = 2; // column value/data
//            }
//            string tableName = 1;
//            bytes key = 2;
//            uint32 type = 3; /// 0-update, 1-insert, 2-delete
//            repeated Column column = 4;
//            bytes data = 5;
//        }
//        repeated Row row = 2;
//        uint64 StartEpoch = 3;
//        uint64 CommitEpoch = 4;
//        uint64 CSN = 5;
//        uint64 server_id = 6;
//        uint64 txnid = 7;
//        uint64 lockinfoCount = 8;
//    }
//    // 对于交互型事务的update和read操作
//    message LockInfo{
//        message Row{
//            string tableName = 1;
//            bytes key = 2;
//            uint32 type = 3; /// 0-update(interactive), 1-insert, 2-delete, 3-read(interactive)
//            bytes data = 4;
//        }
//        repeated Row row = 2;
//        uint64 StartEpoch = 3;
//        uint64 CommitEpoch = 4;
//        uint64 CSN = 5;
//        uint64 server_id = 6;
//        uint64 txnid = 7;
//        bool isLock = 8;        //1-lock 0-unlock
//    }
//    repeated Transaction Txn = 1;
//    uint64 server_id = 2;
//    uint64 epoch = 3;
//    uint64 pack_id = 4;
//    repeated LockInfo lock_info = 5;
//}
//
//message ServerMessage{
//    message Msg{
//        string key = 1;
//        string value = 2;
//    }
//    string send_ip = 1;
//    string receive_ip = 2;
//    string port = 3;
//    repeated Msg msg= 4;
//
//    //uint32 type = 1;//server to master 消息类型 0:新增节点以及新增节点开始时间？ 1:server节点通报自己的情况 运行到了哪个epoch
//    //2: server节点反馈 epoch size更新信息 3:server节点返回新增节点信息 4:cache server反馈存储信息相关
//    //5：某节点请求相关epoch的写集，cache反馈有没有完整的epoch写集，有则发
//    //master to server 消息类型 0:新增节点 1:adjust epoch size 2:
//    //uint32 server_id = 2;
//    //string ip = 3;
//    //uint64 physical_epoch = 4;//新epochsize开始时间epoch 发送给server进行修改自己的sleeptime或修改发送线程sleeptime
//    //uint64 logical_epoch = 5;
//    //uint64 epoch_size = 6;//新epochsize 或现有的epochsize大小
//    //uint64 commit_time = 7;//提交耗时
//    //uint64 send_time = 8; //用以确定网络时延，决定新epoch size开启时间
//    //uint64 new_server_id = 9;
//    //string new_server_ip = 10;
//}
//
//// end


