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
import site.ycsb.measurements.exporter.MeasurementsExporter;
import site.ycsb.measurements.exporter.TextMeasurementsExporter;
import site.ycsb.agent.AgentResp;

import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Turn seconds remaining into more useful units.
 * i.e. if there are hours or days worth of seconds, use them.
 */
final class RemainingFormatter {
  private RemainingFormatter() {
    // not used
  }

  public static StringBuilder format(long seconds) {
    StringBuilder time = new StringBuilder();
    long days = TimeUnit.SECONDS.toDays(seconds);
    if (days > 0) {
      time.append(days).append(days == 1 ? " day " : " days ");
      seconds -= TimeUnit.DAYS.toSeconds(days);
    }
    long hours = TimeUnit.SECONDS.toHours(seconds);
    if (hours > 0) {
      time.append(hours).append(hours == 1 ? " hour " : " hours ");
      seconds -= TimeUnit.HOURS.toSeconds(hours);
    }
    /* Only include minute granularity if we're < 1 day. */
    if (days < 1) {
      long minutes = TimeUnit.SECONDS.toMinutes(seconds);
      if (minutes > 0) {
        time.append(minutes).append(minutes == 1 ? " minute " : " minutes ");
        seconds -= TimeUnit.MINUTES.toSeconds(seconds);
      }
    }
    /* Only bother to include seconds if we're < 1 minute */
    if (time.length() == 0) {
      time.append(seconds).append(time.length() == 1 ? " second " : " seconds ");
    }
    return time;
  }
}

/**
 * Main class for executing YCSB.
 */
public final class Client {
  private Client() {
    //not used
  }

  public static final String DEFAULT_RECORD_COUNT = "0";

  /**
   * The target number of operations to perform.
   */
  public static final String OPERATION_COUNT_PROPERTY = "operationcount";

  /**
   * The number of records to load into the database initially.
   */
  public static final String RECORD_COUNT_PROPERTY = "recordcount";

  /**
   * The workload class to be loaded.
   */
  public static final String WORKLOAD_PROPERTY = "workload";

  /**
   * The database class to be used.
   */
  public static final String DB_PROPERTY = "db";

  /**
   * The exporter class to be used. The default is
   * site.ycsb.measurements.exporter.TextMeasurementsExporter.
   */
  public static final String EXPORTER_PROPERTY = "exporter";

  /**
   * If set to the path of a file, YCSB will write all output to this file
   * instead of STDOUT.
   */
  public static final String EXPORT_FILE_PROPERTY = "exportfile";

  /**
   * The number of YCSB client threads to run.
   */
  public static final String THREAD_COUNT_PROPERTY = "threadcount";

  /**
   * Indicates how many inserts to do if less than recordcount.
   * Useful for partitioning the load among multiple servers if the client is the bottleneck.
   * Additionally workloads should support the "insertstart" property which tells them which record to start at.
   */
  public static final String INSERT_COUNT_PROPERTY = "insertcount";

  /**
   * Target number of operations per second.
   */
  public static final String TARGET_PROPERTY = "target";

  /**
   * The maximum amount of time (in seconds) for which the benchmark will be run.
   */
  public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

  /**
   * Whether or not this is the transaction phase (run) or not (load).
   */
  public static final String DO_TRANSACTIONS_PROPERTY = "dotransactions";

  /**
   * Whether or not to show status during run.
   */
  public static final String STATUS_PROPERTY = "status";

  /**
   * Use label for status (e.g. to label one experiment out of a whole batch).
   */
  public static final String LABEL_PROPERTY = "label";

  /**
   * An optional thread used to track progress and measure JVM stats.
   */
  private static StatusThread statusthread = null;

  public static volatile boolean running = true;
  public static volatile boolean runningByMs = false;
  // HTrace integration related constants.

  /**
   * All keys for configuring the tracing system start with this prefix.
   */
  private static final String HTRACE_KEY_PREFIX = "htrace.";
  private static final String CLIENT_WORKLOAD_INIT_SPAN = "Client#workload_init";
  private static final String CLIENT_INIT_SPAN = "Client#init";
  private static final String CLIENT_WORKLOAD_SPAN = "Client#workload";
  private static final String CLIENT_CLEANUP_SPAN = "Client#cleanup";
  private static final String CLIENT_EXPORT_MEASUREMENTS_SPAN = "Client#export_measurements";

  public static void usageMessage() {
    System.out.println("Usage: java site.ycsb.Client [options]");
    System.out.println("Options:");
    System.out.println("  -threads n: execute using n threads (default: 1) - can also be specified as the \n" +
        "        \"threadcount\" property using -p");
    System.out.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n" +
        "       be specified as the \"target\" property using -p");
    System.out.println("  -load:  run the loading phase of the workload");
    System.out.println("  -t:  run the transactions phase of the workload (default)");
    System.out.println("  -db dbname: specify the name of the DB to use (default: site.ycsb.BasicDB) - \n" +
        "        can also be specified as the \"db\" property using -p");
    System.out.println("  -P propertyfile: load properties from the given file. Multiple files can");
    System.out.println("           be specified, and will be processed in the order specified");
    System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
    System.out.println("          multiple properties can be specified, and override any");
    System.out.println("          values in the propertyfile");
    System.out.println("  -s:  show status during run (default: no status)");
    System.out.println("  -l label:  use label for status (e.g. to label one experiment out of a whole batch)");
    System.out.println("");
    System.out.println("Required properties:");
    System.out.println("  " + WORKLOAD_PROPERTY + ": the name of the workload class to use (e.g. " +
        "site.ycsb.workloads.CoreWorkload)");
    System.out.println("");
    System.out.println("To run the transaction phase from multiple servers, start a separate client on each.");
    System.out.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
    System.out.println("use the \"insertcount\" and \"insertstart\" properties to divide up the records " +
        "to be inserted");
  }

  public static boolean checkRequiredProperties(Properties props) {
    if (props.getProperty(WORKLOAD_PROPERTY) == null) {
      System.out.println("Missing property: " + WORKLOAD_PROPERTY);
      return false;
    }

    return true;
  }

  /**
   * Exports the measurements to either sysout or a file using the exporter
   * loaded from conf.
   *
   * @throws IOException Either failed to write to output stream or failed to close it.
   */
  private static void exportMeasurements(Properties props, int opcount, int opserror,
                                         long totalTransExecTime,
                                         int opsPerTran,
                                         long runtime)
      throws IOException {
    MeasurementsExporter exporter = null;
    try {
      // if no destination file is provided the results will be written to stdout
      OutputStream out;
      String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
      if (exportFile == null) {
        out = System.out;
      } else {
        out = new FileOutputStream(exportFile);
      }

      // if no exporter is provided the default text one will be used
      String exporterStr = props.getProperty(EXPORTER_PROPERTY,
          "site.ycsb.measurements.exporter.TextMeasurementsExporter");
      try {
        exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class)
            .newInstance(out);
      } catch (Exception e) {
        System.err.println("Could not find exporter " + exporterStr
            + ", will use default text reporter.");
        e.printStackTrace();
        exporter = new TextMeasurementsExporter(out);
      }

      exporter.write("OVERALL", "RunTime(ms)", runtime);
      double throughput = 1000.0 * (opcount) / (runtime);
      exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

      exporter.write("ERROR_OPS", "Count", opserror);
      exporter.write("SUCCESS_OPS", "Count", opcount);
      exporter.write("AVG_TRANS_LATENCY", "ms", (totalTransExecTime) * 1.0 / (opcount / opsPerTran));

      exporter.write("[============>AVG_TRANS_LATENCY_TEST]", "ms", (totalTransExecTime) * 1.0 / (opcount / opsPerTran));     // wzy: 测试


      final Map<String, Long[]> gcs = Utils.getGCStatst();
      long totalGCCount = 0;
      long totalGCTime = 0;
      for (final Entry<String, Long[]> entry : gcs.entrySet()) {
        exporter.write("TOTAL_GCS_" + entry.getKey(), "Count", entry.getValue()[0]);
        exporter.write("TOTAL_GC_TIME_" + entry.getKey(), "Time(ms)", entry.getValue()[1]);
        exporter.write("TOTAL_GC_TIME_%_" + entry.getKey(), "Time(%)",
            ((double) entry.getValue()[1] / runtime) * (double) 100);
        totalGCCount += entry.getValue()[0];
        totalGCTime += entry.getValue()[1];
      }
      exporter.write("TOTAL_GCs", "Count", totalGCCount);

      exporter.write("TOTAL_GC_TIME", "Time(ms)", totalGCTime);
      exporter.write("TOTAL_GC_TIME_%", "Time(%)", ((double) totalGCTime / runtime) * (double) 100);

      if (statusthread != null && statusthread.trackJVMStats()) {
        exporter.write("MAX_MEM_USED", "MBs", statusthread.getMaxUsedMem());
        exporter.write("MIN_MEM_USED", "MBs", statusthread.getMinUsedMem());
        exporter.write("MAX_THREADS", "Count", statusthread.getMaxThreads());
        exporter.write("MIN_THREADS", "Count", statusthread.getMinThreads());
        exporter.write("MAX_SYS_LOAD_AVG", "Load", statusthread.getMaxLoadAvg());
        exporter.write("MIN_SYS_LOAD_AVG", "Load", statusthread.getMinLoadAvg());
      }

//      System.out.println("Measurements.getMeasurements().getClass() = " + Measurements.getMeasurements().getClass());
      Measurements.getMeasurements().exportMeasurements(exporter);
    } finally {
      if (exporter != null) {
        exporter.close();
      }
    }
  }

  private static void exportTailList(MeasurementsExporter exporter, String str, List<Long> totalLatencyRecord) {
    Collections.sort(totalLatencyRecord);
    int txnCnt = totalLatencyRecord.size();
    try {
      if (txnCnt > 0) exporter.write("[" + str + "]interactive txn_cnt", "Count", txnCnt);
      if (txnCnt >= 2) {
        exporter.write(str + " [p50]", "ms", totalLatencyRecord.get((int) (txnCnt * 50 / 100.0)));
      }
      if (txnCnt >= 10) {
        exporter.write(str + " [p90]", "ms", totalLatencyRecord.get((int) (txnCnt * 90 / 100.0)));
        exporter.write(str + " [p95]", "ms", totalLatencyRecord.get((int) (txnCnt * 95 / 100.0)));
      }
      if (txnCnt >= 100) {
        exporter.write(str + " [p99]", "ms", totalLatencyRecord.get((int) (txnCnt * 99 / 100.0)));
      }
      if (txnCnt >= 1000) {
        exporter.write(str + " [p999]", "ms", totalLatencyRecord.get((int) (txnCnt * 999 / 1000.0)));
      }
      if (txnCnt >= 10000) {
        exporter.write(str + " [p9999]", "ms", totalLatencyRecord.get((int) (txnCnt * 9999 / 10000.0)));
      }
    } catch (Exception e) {
      // do nothing
    }
  }

  private static String toCsvRow(AgentResp resp) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    String timestamp = sdf.format(new Date());

    // 处理llmResp：转义引号、换行符和逗号
    String escapedLlmResp = escapeForCsv(resp.llmResp != null ? resp.llmResp : "");

    // 处理query：转义引号、换行符和逗号
    String escapedQuery = escapeForCsv(resp.query != null ? resp.query : "");

    // 清理query用于显示（移除换行和多余空格）
    String cleanQuery = (resp.query != null) ?
        resp.query.replace("\n", " ").replace("\r", " ").trim() : "";

    // 计算query长度
    int queryLength = (resp.query != null) ? resp.query.length() : 0;

    return String.format("%d,\"%s\",\"%s\",%s,\"%s\",%d,%d,%s,%d,%b,\"%s\"",
        resp.tid,
        "TXN_" + resp.tid,  // transaction_id
        escapedLlmResp,
        resp.action != null ? resp.action : "UNKNOWN",
        escapedQuery,
        resp.timeCost,
        resp.tokenCost,
        timestamp,
        queryLength,
        (resp.query != null && !resp.query.isEmpty()),
        cleanQuery
    );
  }

  // CSV转义函数
  private static String escapeForCsv(String text) {
    if (text == null) {
      return "";
    }
    return text.replace("\"", "\"\"")
        .replace("\n", " ")
        .replace("\r", " ");
  }

  // 获取CSV表头
  private static String getCsvHeader() {
    return "tid,transaction_id,llm_response,action,query,time_cost_ms,token_cost," +
        "timestamp,query_length,has_query,clean_query";
  }

  private static void writeAgentRespToCsv(List<AgentResp> agentRespList, String filePath) throws IOException {
    try (PrintWriter writer = new PrintWriter(new FileWriter(filePath, false))) {
      // 写入UTF-8 BOM（可选，确保Excel正确显示中文）
      writer.write("\uFEFF");

      // 写入CSV表头
      writer.println(getCsvHeader());

      // 写入数据行
      for (AgentResp resp : agentRespList) {
        writer.println(toCsvRow(resp));
      }

      // 写入汇总信息
      writer.println();
      writer.println("# Summary");
      writer.println("Total records," + agentRespList.size());
      writer.println("Export timestamp," + new Date().toString());

      // 统计不同类型的action
      long totalTimeCost = 0;
      long totalTokenCost = 0;
      int[] actionCounts = new int[4]; // 假设有4种action类型

      for (AgentResp resp : agentRespList) {
        totalTimeCost += resp.timeCost;
        totalTokenCost += resp.tokenCost;

        // 统计action类型
        if ("SCAN".equals(resp.action)) actionCounts[0]++;
        else if ("JOIN".equals(resp.action)) actionCounts[1]++;
        else if ("CHECK_SEATS".equals(resp.action)) actionCounts[2]++;
        else if ("UPDATE_AND_COMMIT".equals(resp.action)) actionCounts[3]++;
      }

      writer.println("Total time cost (ms)," + totalTimeCost);
      writer.println("Total token cost," + totalTokenCost);
      writer.println("Average time per response (ms)," +
          (agentRespList.size() > 0 ? totalTimeCost / agentRespList.size() : 0));
      writer.println("Average tokens per response," +
          (agentRespList.size() > 0 ? totalTokenCost / agentRespList.size() : 0));
      writer.println("SCAN operations," + actionCounts[0]);
      writer.println("JOIN operations," + actionCounts[1]);
      writer.println("CHECK_SEATS operations," + actionCounts[2]);
      writer.println("UPDATE_AND_COMMIT operations," + actionCounts[3]);
    }
  }

  private static void exportAgentMeasurements(MeasurementsExporter exporter, List<Long> agent_sql_cnt_list, List<Long> agent_sql_latency_list,List<Long> agent_token_cost_list) {
    exportTailList(exporter, "Agent Sql Count", agent_sql_cnt_list);
    exportTailList(exporter, "Agent Sql Latency", agent_sql_latency_list);
    exportTailList(exporter, "Agent Token Cost", agent_token_cost_list);
  }

  // wzy: 输出交互性事务和存储事务的统计结果
  private static void exportMeasurementsInteractive(Properties props, int opcount, int opserror, long totalTransExecTime,
                                                    int stored_process_opsdone, int stored_process_retry, int stored_opserror, long stored_process_totalTransExecTime,
                                                    int interactive_opsdone, int interactive_retry, int interactive_opserror, long interactive_totalTransExecTime,
                                                    int uniform_opsdone, int uniform_retry, int uniform_opserror, long uniform_totalTransExecTime,
                                                    int zipfian_opsdone, int zipfian_retry, int zipfian_opserror, long zipfian_totalTransExecTime,
                                                    int max_zipfian_retry, double median_zipfian_retry,
                                                    int max_uniform_retry, double median_uniform_retry,
                                                    int max_interactive_retry, double median_interactive_retry,
                                                    int max_stored_retry, double median_stored_retry,
                                         int opsPerTran,
                                         long runtime, int penalty_sleep_time, int interactive_penalty_sleep_time, int stored_penalty_sleep_time,
                                         List<Long> totalLatencyRecord,
                                                    List<Long> agent_sql_cnt_list, List<Long> agent_sql_latency_list,List<Long> agent_token_cost_list, List<AgentResp> agent_sql_list)
      throws IOException {
    MeasurementsExporter exporter = null;
    try {
      // if no destination file is provided the results will be written to stdout
      OutputStream out;
      String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
      if (exportFile == null) {
        out = System.out;
      } else {
        out = new FileOutputStream(exportFile);
      }

      // if no exporter is provided the default text one will be used
      String exporterStr = props.getProperty(EXPORTER_PROPERTY,
          "site.ycsb.measurements.exporter.TextMeasurementsExporter");
      try {
        exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class)
            .newInstance(out);
      } catch (Exception e) {
        System.err.println("Could not find exporter " + exporterStr
            + ", will use default text reporter.");
        e.printStackTrace();
        exporter = new TextMeasurementsExporter(out);
      }

      exporter.write("OVERALL", "RunTime(ms)", runtime);

      double throughput = 1000.0 * (opcount) / (runtime);
      exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

      exporter.write("PENALTY", "RunTime(ms)", penalty_sleep_time);
      exporter.write("INTERACTIVE_PENALTY", "RunTime(ms)", interactive_penalty_sleep_time);
      exporter.write("STORED_PENALTY", "RunTime(ms)", stored_penalty_sleep_time);

      // wzy: throughput
      double stored_throughput = 1000.0 * (stored_process_opsdone) / (runtime);
      exporter.write("STORED_PROCESS", "Throughput(ops/sec)", stored_throughput);
      double interactive_throughput = 1000.0 * (interactive_opsdone) / (runtime);
      exporter.write("INTERACTIVE", "Throughput(ops/sec)", interactive_throughput);

      // wzy: uniform zipfian throughput
      double uniform_throughput = 1000.0 * (uniform_opsdone) / (runtime);
      exporter.write("UNIFORM", "Throughput(ops/sec)", uniform_throughput);
      double zipfian_throughput = 1000.0 * (zipfian_opsdone) / (runtime);
      exporter.write("ZIPFIAN", "Throughput(ops/sec)", zipfian_throughput);

      // wzy: retry throughput
      double retry_throughput = 1000.0 * (interactive_retry + stored_process_retry) / (runtime);
      exporter.write("RETRY", "Throughput(ops/sec)", retry_throughput);

      exporter.write("ERROR_OPS", "Count", opserror);
      exporter.write("SUCCESS_OPS", "Count", opcount);
      exporter.write("AVG_TRANS_LATENCY", "ms", (totalTransExecTime) * 1.0 / (opcount / opsPerTran));

      // wzy: interactive ops retrys
      exporter.write("STORED_RETRY", "Count", stored_process_retry);
      exporter.write("STORED_ERROR_OPS", "Count", stored_opserror);
      exporter.write("STORED_SUCCESS_OPS", "Count", stored_process_opsdone);
      exporter.write("STORED_AVG_TRANS_LATENCY", "ms", (stored_process_totalTransExecTime) * 1.0 / (stored_process_opsdone / opsPerTran));

      exporter.write("INTERACTIVE_RETRY", "Count", interactive_retry);
      exporter.write("INTERACTIVE_ERROR_OPS1", "Count", opserror - stored_opserror);
      exporter.write("INTERACTIVE_ERROR_OPS2", "Count", interactive_opserror);
      exporter.write("INTERACTIVE_SUCCESS_OPS", "Count", interactive_opsdone);
      exporter.write("INTERACTIVE_AVG_TRANS_LATENCY", "ms", (interactive_totalTransExecTime) * 1.0 / (interactive_opsdone / opsPerTran));

      // wzy: uniform ops zipfian
      exporter.write("UNIFORM_RETRY", "Count", uniform_retry);
      exporter.write("UNIFORM_ERROR_OPS", "Count", uniform_opserror);
      exporter.write("UNIFORM_SUCCESS_OPS", "Count", uniform_opsdone);
      exporter.write("UNIFORM_AVG_TRANS_LATENCY", "ms", (uniform_totalTransExecTime) * 1.0 / (uniform_opsdone / opsPerTran));

      exporter.write("ZIPFIAN_RETRY", "Count", zipfian_retry);
      exporter.write("ZIPFIAN_ERROR_OPS", "Count", zipfian_opserror);
      exporter.write("ZIPFIAN_SUCCESS_OPS", "Count", zipfian_opsdone);
      exporter.write("ZIPFIAN_AVG_TRANS_LATENCY", "ms", (zipfian_totalTransExecTime) * 1.0 / (zipfian_opsdone / opsPerTran));

      // wzy: max and median
      exporter.write("MAX_ZIPFIAN_RETRY", "Count", max_zipfian_retry);
      exporter.write("MEDIAN_ZIPFIAN_RETRY", "Count", median_zipfian_retry);
      exporter.write("MAX_UNIFORM_RETRY", "Count", max_uniform_retry);
      exporter.write("MEDIAN_UNIFORM_RETRY", "Count", median_uniform_retry);
      exporter.write("MAX_INTERACTIVE_RETRY", "Count", max_interactive_retry);
      exporter.write("MEDIAN_INTERACTIVE_RETRY", "Count", median_interactive_retry);
      exporter.write("MAX_STORED_RETRY", "Count", max_stored_retry);
      exporter.write("MEDIAN_STORED_RETRY", "Count", median_stored_retry);

      // 尾延迟
      Collections.sort(totalLatencyRecord);
      int txnCnt = totalLatencyRecord.size();
      if (txnCnt > 0) exporter.write("[tail latency]interactive txn_cnt", "Count", txnCnt);
      if (txnCnt >= 2) {
        exporter.write("[p50]", "ms", totalLatencyRecord.get((int)(txnCnt * 50 / 100.0)));
      }
      if (txnCnt >= 10) {
        exporter.write("[p90]", "ms", totalLatencyRecord.get((int)(txnCnt * 90 / 100.0)));
        exporter.write("[p95]", "ms", totalLatencyRecord.get((int)(txnCnt * 95 / 100.0)));
      }
      if (txnCnt >= 100) {
        exporter.write("[p99]", "ms", totalLatencyRecord.get((int)(txnCnt * 99 / 100.0)));
      }
      if (txnCnt >= 1000) {
        exporter.write("[p999]", "ms", totalLatencyRecord.get((int)(txnCnt * 999 / 1000.0)));
      }
      if (txnCnt >= 10000) {
        exporter.write("[p9999]", "ms", totalLatencyRecord.get((int)(txnCnt * 9999 / 10000.0)));
      }


      final Map<String, Long[]> gcs = Utils.getGCStatst();
      long totalGCCount = 0;
      long totalGCTime = 0;
      for (final Entry<String, Long[]> entry : gcs.entrySet()) {
        exporter.write("TOTAL_GCS_" + entry.getKey(), "Count", entry.getValue()[0]);
        exporter.write("TOTAL_GC_TIME_" + entry.getKey(), "Time(ms)", entry.getValue()[1]);
        exporter.write("TOTAL_GC_TIME_%_" + entry.getKey(), "Time(%)",
            ((double) entry.getValue()[1] / runtime) * (double) 100);
        totalGCCount += entry.getValue()[0];
        totalGCTime += entry.getValue()[1];
      }
      exporter.write("TOTAL_GCs", "Count", totalGCCount);

      exporter.write("TOTAL_GC_TIME", "Time(ms)", totalGCTime);
      exporter.write("TOTAL_GC_TIME_%", "Time(%)", ((double) totalGCTime / runtime) * (double) 100);

      if (statusthread != null && statusthread.trackJVMStats()) {
        exporter.write("MAX_MEM_USED", "MBs", statusthread.getMaxUsedMem());
        exporter.write("MIN_MEM_USED", "MBs", statusthread.getMinUsedMem());
        exporter.write("MAX_THREADS", "Count", statusthread.getMaxThreads());
        exporter.write("MIN_THREADS", "Count", statusthread.getMinThreads());
        exporter.write("MAX_SYS_LOAD_AVG", "Load", statusthread.getMaxLoadAvg());
        exporter.write("MIN_SYS_LOAD_AVG", "Load", statusthread.getMinLoadAvg());
      }

//      System.out.println("Measurements.getMeasurements().getClass() = " + Measurements.getMeasurements().getClass());
      Measurements.getMeasurements().exportMeasurements(exporter);

      // wzy: 输出每个事务延迟
      if (totalLatencyRecord != null && !totalLatencyRecord.isEmpty()) {
        try {
          String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
          String filePath = "latency_" + timestamp + ".csv";

          try (PrintWriter writer = new PrintWriter(new FileWriter(filePath))) {
            // 简单写入，只有一列时间
            writer.println("latency_ms");
            for (Long latency : totalLatencyRecord) {
              writer.println(latency);
            }
          }

          System.out.println("\n✓ Latency data saved to: " + filePath);

        } catch (Exception e) {
          // 静默失败，不影响主流程
        }
      }


      // wzy: 输出Agentic
      exportAgentMeasurements(exporter, agent_sql_cnt_list, agent_sql_latency_list, agent_token_cost_list);
      if (agent_sql_list != null && !agent_sql_list.isEmpty()) {
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS").format(new Date());
        String fileName = "agent_resp_" + timestamp + ".csv";
        String filePath = "./" + fileName;
        writeAgentRespToCsv(agent_sql_list, filePath);

        if (filePath != null) {
          System.out.println("\n✓ Agent responses exported to: " + filePath);
        }
      }
    } finally {
      if (exporter != null) {
        exporter.close();
      }
    }
  }


  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    Properties props = parseArguments(args);

    boolean status = Boolean.valueOf(props.getProperty(STATUS_PROPERTY, String.valueOf(false)));
    String label = props.getProperty(LABEL_PROPERTY, "");

    long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

    //get number of threads, target and db
    int threadcount = Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, "1"));
    String dbname = props.getProperty(DB_PROPERTY, "site.ycsb.BasicDB");
    int target = Integer.parseInt(props.getProperty(TARGET_PROPERTY, "0"));

    //compute the target throughput
    double targetperthreadperms = -1;
    if (target > 0) {
      double targetperthread = ((double) target) / ((double) threadcount);
      targetperthreadperms = targetperthread / 1000.0;
    }

    Thread warningthread = setupWarningThread();
    warningthread.start();

    Measurements.setProperties(props);

    Workload workload = getWorkload(props);

    final Tracer tracer = getTracer(props, workload);

    initWorkload(props, warningthread, workload, tracer);

    System.err.println("Starting test.");
    final CountDownLatch completeLatch = new CountDownLatch(threadcount);

    final List<ClientThread> clients = initDb(dbname, props, threadcount, targetperthreadperms,
        workload, tracer, completeLatch);


    if (status) {
      boolean standardstatus = false;
      if (props.getProperty(Measurements.MEASUREMENT_TYPE_PROPERTY, "").compareTo("timeseries") == 0) {
        standardstatus = true;
      }
      int statusIntervalSeconds = Integer.parseInt(props.getProperty("status.interval", "10"));
      boolean trackJVMStats = props.getProperty(Measurements.MEASUREMENT_TRACK_JVM_PROPERTY,
          Measurements.MEASUREMENT_TRACK_JVM_PROPERTY_DEFAULT).equals("true");
      statusthread = new StatusThread(completeLatch, clients, label, standardstatus, statusIntervalSeconds,
          trackJVMStats);
      statusthread.start();
    }

    Thread terminator = null;
    long st;
    long en;
    int opsDone;
    int opsError;
    long totalTransExecTime;
    // add by wzy
    int stored_process_opsdone;
    int stored_process_opsError;
    int stored_process_retry;
    long stored_process_totalTransExecTime;

    int interactive_process_opsError;
    int interactive_opsdone;
    int interactive_retry;
    long interactive_totalTransExecTime;

    int penalty_sleep_time;
    int interactive_penalty_sleep_time;
    int stored_penalty_sleep_time;

    int uniform_process_opsdone;
    int uniform_process_opsError;
    int uniform_process_retry;
    long uniform_totalTransExecTime;

    int zipfian_process_opsdone;
    int zipfian_process_opsError;
    int zipfian_process_retry;
    long zipfian_totalTransExecTime;

    int max_zipfian_retry = 0;
    double median_zipfian_retry = 0.0;

    int max_uniform_retry = 0;
    double median_uniform_retry = 0.0;

    int max_interactive_retry = 0;
    double median_interactive_retry = 0.0;

    int max_stored_retry = 0;
    double median_stored_retry = 0.0;

    List<Long> interactive_latency_list = new ArrayList<>();

    List<Long> agent_sql_cnt_list = new ArrayList<>();
    List<Long> agent_sql_latency_list = new ArrayList<>();
    List<Long> agent_token_cost_list = new ArrayList<>();
    List<AgentResp> agent_sql_list = new ArrayList<>();

    int opsPerTrans = Integer.parseInt(props.getOrDefault("opspertrans", "1").toString());
    // wzy: 读取是否按照时间执行，执行时间长度
    runningByMs = Boolean.parseBoolean(props.getOrDefault("run_by_ms", "true").toString());
    int runningMs = Integer.parseInt(props.getOrDefault("run_ms", "60000").toString());
//    System.out.println("opsPerTrans in Client.java = " + opsPerTrans);

    try (final TraceScope span = tracer.newScope(CLIENT_WORKLOAD_SPAN)) {
      final Map<Thread, ClientThread> threads = new HashMap<>(threadcount);
      for (ClientThread client : clients) {
        threads.put(new Thread(tracer.wrap(client, "ClientThread")), client);
      }

      st = System.currentTimeMillis();

      for (Thread t : threads.keySet()) {
        t.start();
      }

      if (maxExecutionTime > 0) {
        terminator = new TerminatorThread(maxExecutionTime, threads.keySet(), workload);
        terminator.start();
      }

      // wzy: 主线程监控时间
      if (runningByMs && running) {
        try {
          Thread.sleep(runningMs);
        } catch (Exception e) {

        } finally {
          running = false;
        }
      }

      en = System.currentTimeMillis();

      opsDone = 0;
      opsError = 0;
      totalTransExecTime = 0L;

      stored_process_opsdone = 0;
      stored_process_opsError = 0;
      stored_process_retry = 0;
      stored_process_totalTransExecTime = 0L;

      interactive_opsdone = 0;
      interactive_process_opsError = 0;
      interactive_retry = 0;
      interactive_totalTransExecTime = 0L;

      penalty_sleep_time = 0;
      interactive_penalty_sleep_time = 0;
      stored_penalty_sleep_time = 0;

      uniform_process_opsdone = 0;
      uniform_process_opsError = 0;
      uniform_process_retry = 0;
      uniform_totalTransExecTime = 0L;

      zipfian_process_opsdone = 0;
      zipfian_process_opsError = 0;
      zipfian_process_retry = 0;
      zipfian_totalTransExecTime = 0L;

      List<Integer> zipfian_retry_list = new ArrayList<>();
      List<Integer> uniform_retry_list = new ArrayList<>();
      List<Integer> interactive_retry_list = new ArrayList<>();
      List<Integer> stored_retry_list = new ArrayList<>();

      interactive_latency_list = new ArrayList<>();

      agent_sql_cnt_list = new ArrayList<>();
      agent_sql_latency_list = new ArrayList<>();
      agent_token_cost_list = new ArrayList<>();
      agent_sql_list = new ArrayList<>();

      for (Map.Entry<Thread, ClientThread> entry : threads.entrySet()) {
        try {
          entry.getKey().join();
          opsDone += entry.getValue().getOpsDone();
          opsError += entry.getValue().getOpsError();
          totalTransExecTime += entry.getValue().getTotalTransExecTime();
        } catch (InterruptedException ignored) {
          // ignored
        }
      }


      // wzy: 统计结果
      for (Map.Entry<Thread, ClientThread> entry : threads.entrySet()) {
          // stored
          stored_process_opsdone += entry.getValue().getStoredOpsDone();
          stored_process_retry += entry.getValue().getStoredOpsRetry();
          stored_process_opsError += entry.getValue().getStoredOpsError();
          stored_process_totalTransExecTime += entry.getValue().getStoredTotalTransExecTime();

          // interactive
          interactive_opsdone += entry.getValue().getInteractiveOpsDone();
          interactive_retry += entry.getValue().getInteractiveOpsRetry();
          interactive_process_opsError += entry.getValue().getInteractiveOpsError();
          interactive_totalTransExecTime += entry.getValue().getInteractiveTotalTransExecTime();

          penalty_sleep_time += entry.getValue().getPenaltySleepTime();
          interactive_penalty_sleep_time += entry.getValue().getInteractivePenaltySleepTime();
          stored_penalty_sleep_time += entry.getValue().getStoredPenaltySleepTime();

          // uniform
          uniform_process_opsdone += entry.getValue().getUniformOpsDone();
          uniform_process_retry += entry.getValue().getUniformOpsRetry();
          uniform_process_opsError += entry.getValue().getUniformOpsError();
          uniform_totalTransExecTime += entry.getValue().getUniformTotalTransExecTime();

          // zipfian
          zipfian_process_opsdone += entry.getValue().getZipfianOpsDone();
          zipfian_process_retry += entry.getValue().getZipfianOpsRetry();
          zipfian_process_opsError += entry.getValue().getZipfianOpsError();
          zipfian_totalTransExecTime += entry.getValue().getZipfianTotalTransExecTime();

          // retry list
          zipfian_retry_list.addAll(entry.getValue().getZipfianRetryList());
          uniform_retry_list.addAll(entry.getValue().getUniformRetryList());
          interactive_retry_list.addAll(entry.getValue().getInteractiveRetryList());
          stored_retry_list.addAll(entry.getValue().getStoredRetryList());
          interactive_latency_list.addAll(entry.getValue().getInteractiveLatencyList());

          agent_sql_cnt_list.addAll(entry.getValue().getAgentSqlCntList());
          agent_sql_latency_list.addAll(entry.getValue().getAgentSqlLatencyList());
          agent_token_cost_list.addAll(entry.getValue().getAgentTokenCostList());
          agent_sql_list.addAll(entry.getValue().getAgentRespList());
      }

      // 统计 retry 最大值，中位数
      if (!zipfian_retry_list.isEmpty()) {
        Collections.sort(zipfian_retry_list);
        max_zipfian_retry = zipfian_retry_list.get(zipfian_retry_list.size() - 1);
        median_zipfian_retry = zipfian_retry_list.size() % 2 == 1 ? zipfian_retry_list.get(zipfian_retry_list.size() /2 ) :
            (zipfian_retry_list.get(zipfian_retry_list.size() / 2 - 1) + zipfian_retry_list.get(zipfian_retry_list.size() / 2)) / 2.0;
      }

      if (!uniform_retry_list.isEmpty()) {
        Collections.sort(uniform_retry_list);
        // 统计 retry 最大值，中位数
        max_uniform_retry = uniform_retry_list.get(uniform_retry_list.size() - 1);
        median_uniform_retry = uniform_retry_list.size() % 2 == 1 ? uniform_retry_list.get(uniform_retry_list.size() / 2) :
            (uniform_retry_list.get(uniform_retry_list.size() / 2 - 1) + uniform_retry_list.get(uniform_retry_list.size() / 2)) / 2.0;
      }

      if (!interactive_retry_list.isEmpty()) {
        Collections.sort(interactive_retry_list);
        // 统计 retry 最大值，中位数
        max_interactive_retry = interactive_retry_list.get(interactive_retry_list.size() - 1);
        median_interactive_retry = interactive_retry_list.size() % 2 == 1 ? interactive_retry_list.get(interactive_retry_list.size() / 2) :
            (interactive_retry_list.get(interactive_retry_list.size() / 2 - 1) + interactive_retry_list.get(interactive_retry_list.size() / 2)) / 2.0;

      }
      if (!stored_retry_list.isEmpty()) {
        Collections.sort(stored_retry_list);
        // 统计 retry 最大值，中位数
        max_stored_retry = stored_retry_list.get(stored_retry_list.size() - 1);
        median_stored_retry = stored_retry_list.size() % 2 == 1 ? stored_retry_list.get(stored_retry_list.size() / 2) :
            (stored_retry_list.get(stored_retry_list.size() / 2 - 1) + stored_retry_list.get(stored_retry_list.size() / 2)) / 2.0;
      }
    }

    try {
      try (final TraceScope span = tracer.newScope(CLIENT_CLEANUP_SPAN)) {

        if (terminator != null && !terminator.isInterrupted()) {
          terminator.interrupt();
        }

        if (status) {
          // wake up status thread if it's asleep
          statusthread.interrupt();
          // at this point we assume all the monitored threads are already gone as per above join loop.
          try {
            statusthread.join();
          } catch (InterruptedException ignored) {
            // ignored
          }
        }

        workload.cleanup();
      }
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      try (final TraceScope span = tracer.newScope(CLIENT_EXPORT_MEASUREMENTS_SPAN)) {
//        exportMeasurements(props, opsDone, opsError, totalTransExecTime, opsPerTrans, en - st);
        // wzy: 输出有交互性事务的统计
//        exportMeasurementsInteractive(props, opsDone, opsError, totalTransExecTime,
//            stored_process_opsdone, stored_process_retry, stored_process_opsError, stored_process_totalTransExecTime,
//            interactive_opsdone, interactive_retry, interactive_process_opsError, interactive_totalTransExecTime,
//            opsPerTrans, en - st);

        exportMeasurementsInteractive(props, opsDone, opsError, totalTransExecTime,
            stored_process_opsdone, stored_process_retry, stored_process_opsError, stored_process_totalTransExecTime,
            interactive_opsdone, interactive_retry, interactive_process_opsError, interactive_totalTransExecTime,
            uniform_process_opsdone, uniform_process_retry, uniform_process_opsError, uniform_totalTransExecTime,
            zipfian_process_opsdone, zipfian_process_retry, zipfian_process_opsError, zipfian_totalTransExecTime,
            max_zipfian_retry,  median_zipfian_retry,
            max_uniform_retry,  median_uniform_retry,
            max_interactive_retry,  median_interactive_retry,
            max_stored_retry,  median_stored_retry,
            opsPerTrans, en - st,
            penalty_sleep_time, interactive_penalty_sleep_time, stored_penalty_sleep_time,
            interactive_latency_list,
            agent_sql_cnt_list, agent_sql_latency_list, agent_token_cost_list, agent_sql_list);
      }
    } catch (IOException e) {
      System.err.println("Could not export measurements, error: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }

    System.exit(0);
  }

  private static List<ClientThread> initDb(String dbname, Properties props, int threadcount,
                                           double targetperthreadperms, Workload workload, Tracer tracer,
                                           CountDownLatch completeLatch) {
    boolean initFailed = false;
    boolean dotransactions = Boolean.valueOf(props.getProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true)));

    final List<ClientThread> clients = new ArrayList<>(threadcount);
    try (final TraceScope span = tracer.newScope(CLIENT_INIT_SPAN)) {
      int opcount;
      if (dotransactions) {
        opcount = Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY, "0"));
      } else {
        if (props.containsKey(INSERT_COUNT_PROPERTY)) {
          opcount = Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY, "0"));
        } else {
          opcount = Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT));
        }
      }
      if (threadcount > opcount && opcount > 0) {
        threadcount = opcount;
        System.out.println("Warning: the threadcount is bigger than recordcount, the threadcount will be recordcount!");
      }
      for (int threadid = 0; threadid < threadcount; threadid++) {
        DB db;
        try {
          db = DBFactory.newDB(dbname, props, tracer);
        } catch (UnknownDBException e) {
          System.out.println("Unknown DB " + dbname);
          initFailed = true;
          break;
        }

        int threadopcount = opcount / threadcount;

        // ensure correct number of operations, in case opcount is not a multiple of threadcount
        if (threadid < opcount % threadcount) {
          ++threadopcount;
        }

        // threadopcount 是单线程执行的数量
        ClientThread t = new ClientThread(db, dotransactions, workload, props, threadopcount, targetperthreadperms,
            completeLatch);
        t.setThreadId(threadid);
        t.setThreadCount(threadcount);
        clients.add(t);
      }

      if (initFailed) {
        System.err.println("Error initializing datastore bindings.");
        System.exit(0);
      }
    }
    return clients;
  }

  private static Tracer getTracer(Properties props, Workload workload) {
    return new Tracer.Builder("YCSB " + workload.getClass().getSimpleName())
        .conf(getHTraceConfiguration(props))
        .build();
  }

  private static void initWorkload(Properties props, Thread warningthread, Workload workload, Tracer tracer) {
    try {
      try (final TraceScope span = tracer.newScope(CLIENT_WORKLOAD_INIT_SPAN)) {
        workload.init(props);
        warningthread.interrupt();
      }
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }
  }

  private static HTraceConfiguration getHTraceConfiguration(Properties props) {
    final Map<String, String> filteredProperties = new HashMap<>();
    for (String key : props.stringPropertyNames()) {
      if (key.startsWith(HTRACE_KEY_PREFIX)) {
        filteredProperties.put(key.substring(HTRACE_KEY_PREFIX.length()), props.getProperty(key));
      }
    }
    return HTraceConfiguration.fromMap(filteredProperties);
  }

  private static Thread setupWarningThread() {
    //show a warning message that creating the workload is taking a while
    //but only do so if it is taking longer than 2 seconds
    //(showing the message right away if the setup wasn't taking very long was confusing people)
    return new Thread() {
      @Override
      public void run() {
        try {
          sleep(2000);
        } catch (InterruptedException e) {
          return;
        }
        System.err.println(" (might take a few minutes for large data sets)");
      }
    };
  }

  private static Workload getWorkload(Properties props) {
    ClassLoader classLoader = Client.class.getClassLoader();

    try {
      Properties projectProp = new Properties();
      projectProp.load(classLoader.getResourceAsStream("project.properties"));
      System.err.println("YCSB Client " + projectProp.getProperty("version"));
    } catch (IOException e) {
      System.err.println("Unable to retrieve client version.");
    }

    System.err.println();
    System.err.println("Loading workload...");
    try {
      Class workloadclass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));

      return (Workload) workloadclass.newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    return null;
  }

  private static Properties parseArguments(String[] args) {
    Properties props = new Properties();
    System.err.print("Command line:");
    for (String arg : args) {
      System.err.print(" " + arg);
    }
    System.err.println();

    Properties fileprops = new Properties();
    int argindex = 0;

    if (args.length == 0) {
      usageMessage();
      System.out.println("At least one argument specifying a workload is required.");
      System.exit(0);
    }

    while (args[argindex].startsWith("-")) {
      if (args[argindex].compareTo("-threads") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -threads.");
          System.exit(0);
        }
        int tcount = Integer.parseInt(args[argindex]);
        props.setProperty(THREAD_COUNT_PROPERTY, String.valueOf(tcount));
        argindex++;
      } else if (args[argindex].compareTo("-target") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -target.");
          System.exit(0);
        }
        int ttarget = Integer.parseInt(args[argindex]);
        props.setProperty(TARGET_PROPERTY, String.valueOf(ttarget));
        argindex++;
      } else if (args[argindex].compareTo("-load") == 0) {
        props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(false));
        argindex++;
      } else if (args[argindex].compareTo("-t") == 0) {
        props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true));
        argindex++;
      } else if (args[argindex].compareTo("-s") == 0) {
        props.setProperty(STATUS_PROPERTY, String.valueOf(true));
        argindex++;
      } else if (args[argindex].compareTo("-db") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -db.");
          System.exit(0);
        }
        props.setProperty(DB_PROPERTY, args[argindex]);
        argindex++;
      } else if (args[argindex].compareTo("-l") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -l.");
          System.exit(0);
        }
        props.setProperty(LABEL_PROPERTY, args[argindex]);
        argindex++;
      } else if (args[argindex].compareTo("-P") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -P.");
          System.exit(0);
        }
        String propfile = args[argindex];
        argindex++;

        Properties myfileprops = new Properties();
        try {
          myfileprops.load(new FileInputStream(propfile));
        } catch (IOException e) {
          System.out.println("Unable to open the properties file " + propfile);
          System.out.println(e.getMessage());
          System.exit(0);
        }

        //Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
        for (Enumeration e = myfileprops.propertyNames(); e.hasMoreElements(); ) {
          String prop = (String) e.nextElement();

          fileprops.setProperty(prop, myfileprops.getProperty(prop));
        }

      } else if (args[argindex].compareTo("-p") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -p");
          System.exit(0);
        }
        int eq = args[argindex].indexOf('=');
        if (eq < 0) {
          usageMessage();
          System.out.println("Argument '-p' expected to be in key=value format (e.g., -p operationcount=99999)");
          System.exit(0);
        }

        String name = args[argindex].substring(0, eq);
        String value = args[argindex].substring(eq + 1);
        props.put(name, value);
        argindex++;
      } else {
        usageMessage();
        System.out.println("Unknown option " + args[argindex]);
        System.exit(0);
      }

      if (argindex >= args.length) {
        break;
      }
    }

    if (argindex != args.length) {
      usageMessage();
      if (argindex < args.length) {
        System.out.println("An argument value without corresponding argument specifier (e.g., -p, -s) was found. "
            + "We expected an argument specifier and instead found " + args[argindex]);
      } else {
        System.out.println("An argument specifier without corresponding value was found at the end of the supplied " +
            "command line arguments.");
      }
      System.exit(0);
    }

    //overwrite file properties with properties from the command line

    //Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
    for (Enumeration e = props.propertyNames(); e.hasMoreElements(); ) {
      String prop = (String) e.nextElement();

      fileprops.setProperty(prop, props.getProperty(prop));
    }

    props = fileprops;

    if (!checkRequiredProperties(props)) {
      System.out.println("Failed check required properties.");
      System.exit(0);
    }

    return props;
  }
}
