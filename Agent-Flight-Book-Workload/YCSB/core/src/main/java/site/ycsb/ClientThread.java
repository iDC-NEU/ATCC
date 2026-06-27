/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb;

import site.ycsb.measurements.Measurements;
import site.ycsb.agent.AgentResp;

import java.sql.SQLException;
import java.util.*;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

/**
 * A thread for executing transactions or data inserts to the database.
 */
public class ClientThread implements Runnable {
  // Counts down each of the clients completing.
  private final CountDownLatch completeLatch;

  private static boolean spinSleep;
  private DB db;
  private boolean dotransactions;
  private Workload workload;
  private int opcount;
  private double targetOpsPerMs;

  private int opsdone;

  // add by Cui at 20220531
  private int opserror;
  private long totalTransExecTime;

  private int threadid;
  private int threadcount;
  private Object workloadstate;
  private Properties props;
  private long targetOpsTickNs;
  private final Measurements measurements;

  // add by wzy
  public static AtomicInteger thread_id = new AtomicInteger(0);
  public static AtomicInteger tid = new AtomicInteger(0);
  public static final String INTERACTIVE_TRANSACTION_RATES = "interactive_rates";
  public static final String RETRY_SLEEP_TIME = "retry_sleep_time";
  public static final String MIN_SLEEP_TIME = "min_sleep_time";
  public static final String RETRY_TRANSACTION_ACTIVE = "retry_active";
  public static final String MAX_RETRY_COUNT = "max_retry_count";
  public static final String MAX_PRIORITY = "max_priority";
  public static final String LEVEL_RETRY_COUNT = "level_retry_count";
  public static final String UNIFORM_RATES = "uniform_rates";
  public static final String ALL_OCC = "all_occ";
  public static final String ALL_PCC = "all_pcc";
  public static final String RETRY_SLEEP_GAP = "retry_sleep_gap";

  public static final String STORED_MAX_SLEEP_TIME = "stored_max_sleep_time";
  public static final String STORED_MIN_SLEEP_TIME = "stored_min_sleep_time";

  public static final String SWITCH_PROPERTY_ACTIVE = "switch_active";
  public static final String SWITCH_INTERNAL = "switch_internal_second";

  public static final Random random = new Random();
  public double interactiveRates;   // 0 - 1
  public boolean retryActive;
  public double uniformRates;
  public boolean allOCC;
  public boolean allPCC;

  // wzy: 负载动态变化
  public boolean switchActive;
  public int switchInternal;

  private int retrySleepTime;
  private int minSleepTime;
  private int maxRetryCount;
  private int maxPriority;
  private int levelRetryCount;
  private int retrySleepGap;

  private int storedMaxSleepTime;
  private int storedMinSleepTime;

  private int stored_process_opsdone;
  private int interactive_opsdone;
  private int stored_process_retry;
  private int interactive_retry;
  private long stored_process_totalTransExecTime;
  private long interactive_totalTransExecTime;
  private int interactive_opserror;
  private int stored_process_opserror;

  // penalty sleep time
  private int penalty_sleep_time;
  private int interactive_penalty_sleep_time;
  private int stored_penalty_sleep_time;

  // uniform + zipfian
  private int uniform_opsdone;
  private int zipfian_opsdone;
  private int uniform_retry;
  private int zipfian_retry;
  private long uniform_totalTransExecTime;
  private long zipfian_totalTransExecTime;
  private int uniform_opserror;
  private int zipfian_opserror;

  private List<Integer> zipfian_retry_list;
  private List<Integer> uniform_retry_list;
  private List<Integer> interactive_retry_list;
  private List<Integer> stored_retry_list;

  private List<Long> interactive_latency_list;
  private List<Long> agent_sql_cnt_list;
  private List<Long> agent_sql_latency_list;
  private List<Long> agent_token_cost_list;
  private List<AgentResp> agent_resp_list;

  // add by Cui
  public static final String OPERATIONS_PER_TRANSACTION = "opspertrans";

  private int opsPerTrans = 1;


    //add by PQ
  public static final String CONTENTION_LEVEL = "contention_level";

  //add by PQ
  private double contention_level = 0.99;

  /**
   * Constructor.
   *
   * @param db                   the DB implementation to use
   * @param dotransactions       true to do transactions, false to insert data
   * @param workload             the workload to use
   * @param props                the properties defining the experiment
   * @param opcount              the number of operations (transactions or inserts) to do
   * @param targetperthreadperms target number of operations per thread per ms
   * @param completeLatch        The latch tracking the completion of all clients.
   */
  public ClientThread(DB db, boolean dotransactions, Workload workload, Properties props, int opcount,
                      double targetperthreadperms, CountDownLatch completeLatch) {

    // add by Cui
    this.opsPerTrans = Integer.parseInt(props.getOrDefault(OPERATIONS_PER_TRANSACTION, "1").toString());

    //add by PQ
    this.contention_level = Double.parseDouble(props.getOrDefault(CONTENTION_LEVEL, "0.99").toString());

    // add by wzy
    this.interactiveRates = Double.parseDouble(props.getOrDefault(INTERACTIVE_TRANSACTION_RATES, "0.5").toString());
    this.retryActive = Boolean.parseBoolean(props.getOrDefault(RETRY_TRANSACTION_ACTIVE, false).toString());

    // 加入切换
    this.switchActive = Boolean.parseBoolean(props.getOrDefault(SWITCH_PROPERTY_ACTIVE, false).toString());
    this.switchInternal = Integer.parseInt(props.getOrDefault(SWITCH_INTERNAL, "30").toString());

    this.retrySleepTime = Integer.parseInt(props.getOrDefault(RETRY_SLEEP_TIME, "15").toString());
    this.minSleepTime = Integer.parseInt(props.getOrDefault(MIN_SLEEP_TIME, "3").toString());
    this.maxRetryCount = Integer.parseInt(props.getOrDefault(MAX_RETRY_COUNT, "99").toString());
    this.maxPriority = Integer.parseInt(props.getOrDefault(MAX_PRIORITY, "5").toString());
    this.levelRetryCount = Integer.parseInt(props.getOrDefault(LEVEL_RETRY_COUNT, "5").toString());
    this.retrySleepGap = Integer.parseInt(props.getOrDefault(RETRY_SLEEP_GAP, "5").toString());

    this.storedMaxSleepTime = Integer.parseInt(props.getOrDefault(STORED_MAX_SLEEP_TIME, "15").toString());
    this.storedMinSleepTime = Integer.parseInt(props.getOrDefault(STORED_MIN_SLEEP_TIME, "3").toString());

    this.uniformRates = Double.parseDouble(props.getOrDefault(UNIFORM_RATES, "0.5").toString());
    this.allOCC = Boolean.parseBoolean(props.getOrDefault(ALL_OCC, false).toString());
    this.allPCC = Boolean.parseBoolean(props.getOrDefault(ALL_PCC, false).toString());


    this.zipfian_retry_list = new ArrayList<>();
    this.uniform_retry_list = new ArrayList<>();
    this.interactive_retry_list = new ArrayList<>();
    this.stored_retry_list = new ArrayList<>();
    this.interactive_latency_list = new ArrayList<>();

    this.agent_sql_cnt_list = new ArrayList<>();
    this.agent_sql_latency_list = new ArrayList<>();
    this.agent_token_cost_list = new ArrayList<>();
    this.agent_resp_list = new ArrayList<>();

   System.out.println("clientThread is initing, opsPerTrans = " + this.opsPerTrans + " contention level = " + this.contention_level + " interactive rates = " + this.interactiveRates);

    this.db = db;
    this.dotransactions = dotransactions;
    this.workload = workload;
    this.opcount = opcount;
    opsdone = 0;
    opserror = 0;
    if (targetperthreadperms > 0) {
      targetOpsPerMs = targetperthreadperms;
      targetOpsTickNs = (long) (1000000 / targetOpsPerMs);
    }
    this.props = props;
    measurements = Measurements.getMeasurements();
    spinSleep = Boolean.valueOf(this.props.getProperty("spin.sleep", "false"));
    this.completeLatch = completeLatch;
  }

  public void setThreadId(final int threadId) {
    threadid = threadId;
  }

  public void setThreadCount(final int threadCount) {
    threadcount = threadCount;
  }

  public int getOpsDone() {
    return opsdone;
  }

  public int getOpsError() {
    return this.opserror;
  }

  public int getStoredOpsError() {
    return this.stored_process_opserror;
  }

  public int getInteractiveOpsError() {
    return this.interactive_opserror;
  }

  public long getTotalTransExecTime() {
    return this.totalTransExecTime;
  }

  // wzy
  public int getStoredOpsDone() {
    return this.stored_process_opsdone;
  }

  public int getStoredOpsRetry() {
    return this.stored_process_retry;
  }

  public long getStoredTotalTransExecTime() {
    return this.stored_process_totalTransExecTime;
  }

  public int getInteractiveOpsDone() {
    return this.interactive_opsdone;
  }

  public int getInteractiveOpsRetry() {
    return this.interactive_retry;
  }

  public long getInteractiveTotalTransExecTime() {
    return this.interactive_totalTransExecTime;
  }

  public int getPenaltySleepTime() {
    return this.penalty_sleep_time;
  }

  public int getInteractivePenaltySleepTime() {
    return this.interactive_penalty_sleep_time;
  }

  public int getStoredPenaltySleepTime() {
    return this.stored_penalty_sleep_time;
  }

  // wzy
  public int getUniformOpsDone() {
    return this.uniform_opsdone;
  }

  public int getUniformOpsRetry() {
    return this.uniform_retry;
  }

  public long getUniformTotalTransExecTime() {
    return this.uniform_totalTransExecTime;
  }

  public int getZipfianOpsDone() {
    return this.zipfian_opsdone;
  }

  public int getZipfianOpsRetry() {
    return this.zipfian_retry;
  }

  public long getZipfianTotalTransExecTime() {
    return this.zipfian_totalTransExecTime;
  }

  public int getUniformOpsError() {
    return this.uniform_opserror;
  }

  public int getZipfianOpsError() {
    return this.zipfian_opserror;
  }

  // wzy 重做次数统计
  public List<Integer> getZipfianRetryList() {
    return this.zipfian_retry_list;
  }

  public List<Integer> getUniformRetryList() {
    return this.uniform_retry_list;
  }

  public List<Integer> getInteractiveRetryList() {
    return this.interactive_retry_list;
  }

  public List<Integer> getStoredRetryList() {
    return this.stored_retry_list;
  }

  public List<Long> getInteractiveLatencyList() {
    return this.interactive_latency_list;
  }
  public List<Long> getAgentSqlCntList() {
    return this.agent_sql_cnt_list;
  }
  public List<Long> getAgentSqlLatencyList() {
    return this.agent_sql_latency_list;
  }
  public List<Long> getAgentTokenCostList() {
    return this.agent_token_cost_list;
  }
  public List<AgentResp> getAgentRespList() {
    return this.agent_resp_list;
  }


  @Override
  public void run() {
    try {
//      if (false) db.init();   // test
      db.init();
    } catch (DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }

    try {
      workloadstate = workload.initThread(props, threadid, threadcount);
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }

    //NOTE: Switching to using nanoTime and parkNanos for time management here such that the measurements
    // and the client thread have the same view on time.

    //spread the thread operations out so they don't all hit the DB at the same time
    // GH issue 4 - throws exception if _target>1 because random.nextInt argument must be >0
    // and the sleep() doesn't make sense for granularities < 1 ms anyway
    if ((targetOpsPerMs > 0) && (targetOpsPerMs <= 1.0)) {
      long randomMinorDelay = ThreadLocalRandom.current().nextInt((int) targetOpsTickNs);
      sleepUntil(System.nanoTime() + randomMinorDelay);
    }

    // wzy: 创建存储过程
    workload.setMaxPriority(maxPriority);
    workload.setMaxRetryCnt(maxRetryCount);
    workload.setLevelRetryCnt(levelRetryCount);

    // TODO: 记录总开始时间
    // 每 X s 进行切换
    long lastSwitchTimeNanos = System.nanoTime();
    long switchIntervalNanos = switchInternal * 1000000000L;
    int type = 0;

    if (dotransactions) {
      long startTimeNanos = System.nanoTime();
      // 固定线程做交互型事务
//      boolean interactive_ = (ThreadLocalRandom.current().nextInt(100) < interactiveRates * 100) ;    // 设置为交互性事务(OCC, 2PL)
      boolean interactive_ = threadid < (int) Math.floor(threadcount * interactiveRates) ;
      System.err.println("[INFO] interactive rates = " + interactiveRates);
      System.err.println("[INFO] thread: " + threadid + " process interactive: " + interactive_);

      while (Client.running && (Client.runningByMs || (opcount == 0) || (opsdone + opserror < opcount)) && !workload.isStopRequested()) {
        if (switchActive && threadid == threadcount - 1) {
          long curTimeNanos = System.nanoTime();
          if (curTimeNanos - lastSwitchTimeNanos >= switchIntervalNanos) {
            lastSwitchTimeNanos = curTimeNanos;
            // 切换到下一个type，确保在0, 1, 2之间循环
            type = (type + 1) % 3;
            workload.switchProperty(type);
            System.err.println("[INFO] worker id = " + threadid + " switch to property type = " + type);
          }
        }

        int cur_tid = tid.incrementAndGet();
//        boolean interactive_ = (cur_tid % 10) < interactiveRates * 10;    // 设置为交互性事务
//        boolean interactive_ = (ThreadLocalRandom.current().nextInt(100) < interactiveRates * 100) ;    // 设置为交互性事务(OCC, 2PL)
        boolean uniform_flag = ThreadLocalRandom.current().nextInt(100) < uniformRates * 100;

        int retryCount = 0;
        Date start = new Date();
        try {
          boolean res = true;
          if (!retryActive) {
            res = workload.doTransaction(db, workloadstate, interactive_);
          }
          else {
//            if (allOCC) res = workload.redoTransactionResult(db, workloadstate, false, cur_tid, false, retryCount, uniform_flag);
            res = workload.redoTransactionResult(db, workloadstate, false, cur_tid, interactive_, retryCount, uniform_flag, allOCC, allPCC);

            while (!res) {
              retryCount++;
              try {
                long sleepTime = 0L;
                if (interactive_) {
                  sleepTime = ThreadLocalRandom.current().nextInt(retrySleepTime - minSleepTime - 1) + minSleepTime;
                  if (retrySleepGap != 0) sleepTime += (long) Math.pow(2, Math.min(retryCount / retrySleepGap, 10));
                  Thread.sleep(sleepTime);
                  interactive_penalty_sleep_time += sleepTime;
                } else {
                  // wzy: 非交互型事务，设置随机penalty
                  sleepTime = ThreadLocalRandom.current().nextInt(storedMaxSleepTime - storedMinSleepTime - 1) + storedMinSleepTime;
                  Thread.sleep(sleepTime);
                  stored_penalty_sleep_time += sleepTime;
                }
                penalty_sleep_time += sleepTime;
              } catch (Exception e) {
                // do nothing
              }
              if (interactive_) interactive_retry++;
              else stored_process_retry++;
              if (uniform_flag) uniform_retry++;
              else zipfian_retry++;
//            res = workload.redoTransaction(db, workloadstate, true, cur_tid, interactive_);
//            res = workload.redoTransactionResult(db, workloadstate, true, cur_tid, interactive_, retryCount, uniform_flag);
//              if (allOCC) res = workload.redoTransactionResult(db, workloadstate, true, cur_tid, false, retryCount, uniform_flag);
              res = workload.redoTransactionResult(db, workloadstate, true, cur_tid, interactive_, retryCount, uniform_flag, allOCC, allPCC);
              if (!Client.running) {
                workload.requestStop();
                break;
              }
            }
          }

          if (!res) {
            opserror += this.opsPerTrans;
            if (interactive_) interactive_opserror += this.opsPerTrans;
            else stored_process_opserror += this.opsPerTrans;
            if (uniform_flag) uniform_opserror += this.opsPerTrans;
            else zipfian_opserror += this.opsPerTrans;
          } else {
            Date end = new Date();
            totalTransExecTime += (end.getTime() - start.getTime());
            opsdone += this.opsPerTrans;
            // wzy: 此处统计
            if (interactive_) {
              interactive_opsdone += this.opsPerTrans;
              interactive_totalTransExecTime += (end.getTime() - start.getTime());
              interactive_retry_list.add(retryCount);
              interactive_latency_list.add(end.getTime() - start.getTime());

              // wzy: 统计agent延迟等等
              agent_sql_cnt_list.add(workload.getSqlCnt(cur_tid));
              agent_sql_latency_list.add(workload.getSqlLatency(cur_tid));
              agent_token_cost_list.add(workload.getTokenCost(cur_tid));
              agent_resp_list.addAll(workload.getAgentResps(cur_tid));
            } else {
              stored_process_opsdone += this.opsPerTrans;
              stored_process_totalTransExecTime += (end.getTime() - start.getTime());
              stored_retry_list.add(retryCount);
            }

            if (uniform_flag) {
              uniform_opsdone += this.opsPerTrans;
              uniform_totalTransExecTime += (end.getTime() - start.getTime());
              uniform_retry_list.add(retryCount);
            } else {
              zipfian_opsdone += this.opsPerTrans;
              zipfian_totalTransExecTime += (end.getTime() - start.getTime());
              if (interactive_) zipfian_retry_list.add(retryCount);
            }
            if (!Client.running) {
              break;
            }
          }
        } catch (SQLException e) {
          opserror += this.opsPerTrans;
          if (interactive_) interactive_opserror += this.opsPerTrans;
          else stored_process_opserror += this.opsPerTrans;
          if (uniform_flag) uniform_opserror += this.opsPerTrans;
          else zipfian_opserror += this.opsPerTrans;
        }

        throttleNanos(startTimeNanos);
      }
      System.err.println("[INFO] thread: " + threadid + " end interactive: " + interactive_ +" Client running:" + Client.running + " StopRequest:" + workload.isStopRequested());
    } else {    // insert
      try {
        long startTimeNanos = System.nanoTime();

        while (((opcount == 0) || (opsdone < opcount)) && !workload.isStopRequested()) {

          if (!workload.doInsert(db, workloadstate)) {
            break;
          }

          opsdone++;
          throttleNanos(startTimeNanos);
        }
      } catch (Exception e) {
        // e.printStackTrace();
        // e.printStackTrace(System.out);
        System.exit(0);
      }
    }
    try {
      measurements.setIntendedStartTimeNs(0);
      db.cleanup();
    } catch (DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
    } finally {
      completeLatch.countDown();
    }
  }

  private static void sleepUntil(long deadline) {
    while (System.nanoTime() < deadline) {
      if (!spinSleep) {
        LockSupport.parkNanos(deadline - System.nanoTime());
      }
    }
  }

  private void throttleNanos(long startTimeNanos) {
    //throttle the operations
    if (targetOpsPerMs > 0) {
      // delay until next tick
      long deadline = startTimeNanos + opsdone * targetOpsTickNs;
      sleepUntil(deadline);
      measurements.setIntendedStartTimeNs(deadline);
    }
  }

  /**
   * The total amount of work this thread is still expected to do.
   */
  int getOpsTodo() {
    int todo = opcount - opsdone - opserror;
    return todo < 0 ? 0 : todo;
  }
}
