package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.generator.*;
import site.ycsb.workloads.CoreWorkload;
import site.ycsb.agent.AgentResp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import java.util.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// 测试import
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * AgentWorkload: 机票预订场景 LLM Agent 压测负载
 * CREATE TABLE flights (
 *     id VARCHAR(255) PRIMARY KEY,  -- 对应 FLIGHT_SHA_PEK_...
 *     seats_total INT,
 *     seats_booked INT,
 *     price DECIMAL(10, 2),
 *     status VARCHAR(50),
 *     -- YCSB 可能还需要 field0, field1... 如果混合 Workload 运行，
 *     -- 但对于纯 AgentWorkload，以上字段足够。
 *     -- 为了兼容 YCSB 默认读取逻辑，建议保留 text 类型的 data 字段作为备用
 *     data TEXT
 * );
 *
 * 特性：
 * 1. 20城市/380航线，支持 Zipfian 访问倾斜
 * 2. 初始化插入 100w 行真实语义数据
 * 3. 混合负载：查询(Read) vs 预订(Write)
 */
public class AgentWorkloadV1 extends CoreWorkload {

  // 1. 扩大的城市列表 (20个国内主要机场)
  private static final String[] CITIES = {
      "PEK", "SHA", "CAN", "CTU", "SZX", "KMG", "XIY", "CKG", "HGH", "XMN",
      "NKG", "WUH", "CSX", "HAK", "TAO", "SHE", "TSN", "URC", "HRB", "DKR"
  };

  private static final String[] resps = {
      "CHECK for flights. Origin: PEK. Destination: SHA. Date: 2025-12-20. Return up to 3 options (each option must contain a single flight object with id, seats_total, seats_booked, price, status) and include id and description. After I confirm an option id, perform the UPDATE as described.",
      "[{\"id\": \"FLT_PEK_SHA_2025-12-20_CZ3101\", \"seats_total\": 150, \"seats_booked\": 100, \"price\": 1200.50, \"status\": \"Active\"}, {\"id\": \"FLIGHT_PEK_SHA_2025-12-20_CA1502\", \"seats_total\": 180, \"seats_booked\": 180, \"price\": 1050.00, \"status\": \"Full\"}]",
      "I select flight FLT_PEK_SHA_2025-12-20_CZ3101. Please book it.",
      "System Notification: UPDATE successful for key FLIGHT_PEK_SHA_2025-12-20_CZ3101."
  };

  private static final String[] req = {
      "{\n    \"action\": \"SCAN\",\n    \"key\": \"FLT_PEK_SHA_20251006\",\n    \"count\": 4\n}",
      "{\"action\":\"DISPLAY_OPTIONS\",\"options\":[{\"flight_id\":\"FLT_PEK_SHA_20251006_104500\",\"info\":\"Price: 1200.5, Seats Available: 50/150, Status: Active\"},{\"flight_id\":\"FLT_PEK_SHA_20251006_114014\",\"info\":\"Price: 1050.0, Seats Available: 0/180, Status: Full\"}]}",
      "{\"action\":\"UPDATE\",\"key\":\"FLT_PEK_SHA_20251006_104500-12-20_CZ3101\",\"fields\":{\"seats_booked\":\"101\"}}",
      "{\"action\":\"COMMIT\",\"reason\":\"Booking confirmed for FLT_PEK_SHA_20251006_104500\"}",
      "{\"action\":\"ABORT\",\"reason\":\"No flights available for route PEK to SHA on 2025-10-06.\"}"
  };

  private static final String[] AIRLINES = {"CZ","CA","MU","HU","ZV","MF"};

  // 预计算所有可能的航线 (380条)
  private static final List<String> ROUTES = new ArrayList<>();
  static {
    for (String org : CITIES) {
      for (String dst : CITIES) {
        if (!org.equals(dst)) {
          ROUTES.add(org + "_" + dst);
        }
      }
    }
  }

  // 基础配置
  private static final String START_DATE_STR = "20251001";
  private static final int DATE_RANGE_DAYS = 30;

  // HTTP & JSON
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Pattern JSON_BLOCK_PATTERN = Pattern.compile("```json\\s*(\\{.*?\\})\\s*```", Pattern.DOTALL);

  private String apiKey;
  private String apiProject;
  private String model;
  private String endpoint;
  private long recordcount;
  public double contention_level = 0.99;
  public int minMs = 1000;
  public int maxMs = 2000;
  // 负载生成器
  private ZipfianFast routeZipfGenerator; // 用于控制航线访问热度
  private UniformLongGenerator dateGenerator; // 日期通常是均匀访问的

  public static final String SYSTEM_PROMPT =
      "You are a Flight Booking Agent. Protocol: JSON ONLY.\n" +
          "### DATABASE SCHEMA\n" +
          "- Table: flights\n" +
          "- Key (ycsb_key): FLT_<ORIGIN>_<DEST>_<DATE>_<ID>\n" +
          "- Fields: 'seats_total' (int), 'seats_booked' (int), 'price' (double), 'status' (string)\n\n" +
          "### TOOLS\n" +
          "1. SCAN: {\"action\": \"SCAN\", \"key\": \"FLT_<ORG>_<DST>_<DATE>\", \"count\": 5}\n" +
          "2. DISPLAY_OPTIONS: {\"action\": \"DISPLAY_OPTIONS\", \"options\": [{\"flight_id\": \"...\", \"info\": \"...\"}]}\n" +
          "   (Use this to show up to 5 options to the user. Wait for user selection.)\n" +
          "3. UPDATE: {\"action\": \"UPDATE\", \"key\": \"<exact_flight_id>\", \"fields\": {\"seats_booked\": \"<val>\"}}\n" +
          "   (Only update if user confirmed selection AND seats < total)\n" +
          "4. COMMIT: {\"action\": \"COMMIT\", \"reason\": \"Done\"}\n" +
          "5. ABORT: {\"action\": \"ABORT\", \"reason\": \"...\"}\n" +
          "### WORKFLOW\n" +
          "- Search: SCAN -> find available flights -> COMMIT\n" +
          "- Book: SCAN -> DISPLAY_OPTIONS -> (User picks one) -> UPDATE -> COMMIT";

  static {
//    System.setProperty("proxySet", "true");
//    System.setProperty("http.proxyHost", "2409:8a14:659:3fc0::1000");
//    System.setProperty("http.proxyPort", "7890");
//    System.setProperty("https.proxyHost", "2409:8a14:659:3fc0::1000");
//    System.setProperty("https.proxyPort", "7890");

//    System.setProperty("javax.net.debug", "all");
//    System.setProperty("java.net.useSystemProxies", "true");
    System.setProperty("java.net.preferIPv4Stack", "false");
    System.setProperty("java.net.preferIPv6Addresses", "true");
    System.setProperty("http.maxConnections", "60");
    System.setProperty("https.maxConnections", "60");
    System.setProperty("http.keepAlive", "true");
    System.setProperty("https.protocols", "TLSv1.2");
  }

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    // 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
    this.apiKey = p.getProperty("agent_apikey");
    this.apiProject = p.getProperty("agent_apiProject");
    this.model = p.getProperty("agent_model", "deepseek-chat");
    this.endpoint = p.getProperty("agent_endpoint", "https://api.deepseek.com/chat/completions");
    this.contention_level = Double.parseDouble(p.getOrDefault(CONTENTION_LEVEL, "0.99").toString());
    this.minMs = Integer.parseInt(p.getOrDefault("min_sleep_per_ops", "1000").toString());
    this.maxMs = Integer.parseInt(p.getOrDefault("max_sleep_per_ops", "2000").toString());

    table = p.getProperty("table_name", "flights");

    if (this.apiKey == null || this.apiProject == null) {
      System.err.println("WARN: agent.apikey or agent.apiProject is missing.");
    }

    this.recordcount = Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (this.recordcount == 0) {
      this.recordcount = Integer.MAX_VALUE;
    }

    // 核心改动：初始化 Zipfian 生成器，范围是航线索引
    // constant=0.7 (模拟温和的热点)
    this.routeZipfGenerator = new ZipfianFast(this.contention_level);
    this.routeZipfGenerator.calculateDenom(this.recordcount);
    this.dateGenerator = new UniformLongGenerator(0, DATE_RANGE_DAYS - 1);
  }

  // =================================================================
  // Load 阶段: 均匀铺底数据 (Round-Robin)
  // =================================================================
  /**
   * 目标：确保 100w 行数据均匀分布在 380 条航线 * 30 天中。
   * 这样无论 Agent 随机到哪一天，都能查到数据，避免空查询。
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keyNum = keysequence.nextValue().intValue();

    // 1. 确定航线 (使用取模，保证均匀分布)
    int routeIdx = (int) (keyNum % ROUTES.size());
    String routeKey = ROUTES.get(routeIdx);
    String[] parts = routeKey.split("_");
    String origin = parts[0];
    String dest = parts[1];

    // 2. 确定日期 (KeyNum / Routes) % 30 -> 均匀分布在30天内
    int dayOffset = (int) ((keyNum / ROUTES.size()) % DATE_RANGE_DAYS);
    String date = getDateString(dayOffset);

    // 3. 生成 ID
    String dbKey = String.format("FLT_%s_%s_%s_%d", origin, dest, date, keyNum);

    // 4. 构建数据
    Map<String, ByteIterator> values = new HashMap<>();
    Random r = new Random(keyNum); // 确定性随机
    int total = 100 + r.nextInt(100);
    int booked = r.nextInt(total / 2); // 初始一半空位

    // id 为 YCSB_KEY
//    values.put("id", new StringByteIterator(dbKey));
    values.put("seats_total", new StringByteIterator(String.valueOf(total)));
    values.put("seats_booked", new StringByteIterator(String.valueOf(booked)));
    values.put("price", new StringByteIterator(String.valueOf(500 + r.nextInt(1000))));
    values.put("status", new StringByteIterator("SCHEDULED"));

    Status status = db.insert(table, dbKey, values);
    return null!= status && status.isOk();
  }

  // =================================================================
  // Run 阶段: 模拟 Zipfian 访问
  // =================================================================
  public long reqSize = 0;
  public static ConcurrentHashMap<Integer, String> tid_request_map = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<Integer, String> tid_key_map = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<Integer, Map<String,Object>> tid_pref_map = new ConcurrentHashMap<>();

  @Override
  public boolean redoTransactionResult(DB db, Object threadstate, boolean retry, int tid, boolean interactive_, int retryCnt_, boolean uniform_flag, boolean all_occ, boolean all_pcc) throws SQLException {
    // 1. 使用 Zipfian 选择航线 (模拟热门航线高并发)
    String userRequest;
    String key;
    if (retry) {
      userRequest = tid_request_map.get(tid);
      key = tid_key_map.get(tid);
    } else {
      int routeIdx = (int)nextKeynum();     // zipf分布
      // 保护性边界检查
      if (routeIdx >= ROUTES.size()) routeIdx = 0;

      String routeKey = ROUTES.get(routeIdx);
      String[] parts = routeKey.split("_");
      String origin = parts[0];
      String dest = parts[1];

      // 2. 随机选择日期
      int dayOffset = (int)uniformNextKeynum();   // uniform 分布
      String date = getDateString(dayOffset);

      // 3. 生成 User Intent (90% 查询, 10% 预订)
      // 通过 Agent 的行为来控制读写比
      boolean isBooking = ThreadLocalRandom.current().nextDouble() < 0.15; // 稍微设高一点，因为预订包含读+写

      if (isBooking) {
        userRequest = String.format("Book a flight from %s to %s on %s.", origin, dest, date);
        key =  String.format("FLT_%s_%s_%s", origin, dest, date);
      } else {
        userRequest = String.format("Check available flights from %s to %s on %s.", origin, dest, date);
        key =  String.format("FLT_%s_%s_%s", origin, dest, date);
      }
      tid_request_map.put(tid, userRequest);
      tid_key_map.put(tid, key);
    }

    if (retryCnt_ >= maxRetryCnt) {
      System.err.println("[Abort over " + maxRetryCnt + " ] tid = " + tid + " interactive : " + interactive_ + " : doTransactionCommit write_set_size = " + write_set_size_map.get(tid) + " retryCnt:" + retryCnt_);
      tid_request_map.remove(tid);
      tid_key_map.remove(tid);
      throw new SQLException("Abort over maxRetryCnt");
    }

    // 4. 执行 Agent Loop
//    boolean res = executeAgentLoop(db, userRequest, interactive_ , tid, retryCnt_, all_occ, all_pcc);
    boolean res = executeAgentLoopSimulation(db, key, interactive_ , tid, retryCnt_, all_occ, all_pcc);
//    boolean res = executeTestScan();

    if (res) {
      tid_request_map.remove(tid);
      tid_key_map.remove(tid);
    }
    return res;
  }

  public boolean executeTestScan(){
    String url = "jdbc:postgresql://127.0.0.1:16000/postgres";
    Properties props = new Properties();
    props.setProperty("user", "jack");
    props.setProperty("password", "Test@123");

    try (Connection conn = DriverManager.getConnection(url, props)) {
      System.out.println("autocommit = " + conn.getAutoCommit());

      String sql = "SELECT * FROM flights WHERE ycsb_key >= 'FLT_PEK_SHA_20251019' LIMIT '4'";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        for (int i = 0; i < 5; i++) {
          long t1 = System.nanoTime();
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              // 不做任何复杂处理
            }
          }
          long t2 = System.nanoTime();
          System.out.println("flights query cost(ns): " + (t2 - t1));
        }
      }catch (Exception e) {
        System.err.println("Unexpected 1 error:");
        e.printStackTrace();
      }

      try (PreparedStatement ps = conn.prepareStatement("SELECT 1")) {
        for (int i = 0; i < 5; i++) {
          long t1 = System.nanoTime();
          try (ResultSet rs = ps.executeQuery()) {
            rs.next();
          }
          long t2 = System.nanoTime();
          System.out.println("SELECT 1 cost(ns): " + (t2 - t1));
        }
      }catch (Exception e) {
        System.err.println("Unexpected 2 error:");
        e.printStackTrace();
      }
    }catch (Exception e) {
      System.err.println("Unexpected 3 error:");
      e.printStackTrace();
    }

    return true;
  }


  public boolean executeAgentLoopSimulation(DB db, String key, boolean interactive_, int tid, int retryCnt_, boolean all_occ, boolean all_pcc) {
    try {
      if (interactive_ && !all_occ) doInteractiveTransactionBegin(db, retryCnt_);
      else doTransactionBegin(db);
      // -------------------------------------------------------
      // 步骤 1: 收到 User Request 后，直接调用 db.scan 获取航班列表
      // -------------------------------------------------------
      Vector<HashMap<String, ByteIterator>> scanRes = new Vector<>();
//      System.out.println("start transaction scan key: " + key);
      // key 在这里是 prefix, e.g., FLT_PEK_SHA_20251001
      HashSet<String> fields = new HashSet<String>();
      fields.add("ycsb_key");
      fields.add("seats_total");
      fields.add("seats_booked");
      fields.add("price");
      fields.add("status");
      fields.add("data");
      int targetCount = 3 + ThreadLocalRandom.current().nextInt(3); // 随机目标数量 3-5 个
      long startTimeNano = System.nanoTime();
      Status s = db.scan(table, key, targetCount, fields, scanRes);
      long endTimeNano = System.nanoTime();
//      System.out.println("scan spend(ns): " + (endTimeNano - startTimeNano));
      if (!s.isOk() ||scanRes.isEmpty()) {
        if (interactive_ && !all_occ) doInteractiveTransactionCommit(db);
        else doTransactionCommit(db);
//        System.out.println("scan error no results");
        return true;
      }
      List<Flight> flightList = parseFlights(scanRes);
//      System.out.println("scan results: " + flightList);
      // -------------------------------------------------------
      // 步骤 2: 在 Java 代码中模拟 分析 过程
      // -------------------------------------------------------
      // (seats_booked < seats_total)
      List<Flight> availableFlights = new ArrayList<>();
      for (Flight flight : flightList) {
        // 检查是否有空位
        availableFlights.add(flight);
//        if (flight.seats_booked < flight.seats_total) {
//          availableFlights.add(flight);
//        }
        // 优化：一旦找到足够的候选航班，可以提前终止筛选，
        // 模拟Agent找到满意结果即停止搜索的行为
        if (availableFlights.size() >= targetCount) {
          break;
        }
      }

      // 边界处理：如果扫描后没有可用航班，则本次仿真结束或回滚
      if (availableFlights.isEmpty()) {
        // System.out.println("No available flights found for prefix: " + req.getKeyPrefix());
        if (interactive_ && !all_occ) doInteractiveTransactionCommit(db);
        else doTransactionCommit(db);
        return true;
      }

//      System.out.println("available results: " + availableFlights);

      // -------------------------------------------------------
      // 步骤 3: 模拟用户选择
      // -------------------------------------------------------
      if (interactive_) {
        try {
          Thread.sleep(ThreadLocalRandom.current().nextInt(this.minMs, this.maxMs + 1));
        } catch (Exception e) {
          // .. do nothing
        }
      }
      int selectedIndex = ThreadLocalRandom.current().nextInt(availableFlights.size());
      Flight selectedFlight = availableFlights.get(selectedIndex);
//      System.out.println("selected flight_id: " + selectedFlight.key);
      // -------------------------------------------------------
      // 步骤 4: 执行 db.update 对选中航班进行预订
      // -------------------------------------------------------
      // 逻辑：seats_booked + 1
      HashMap<String, ByteIterator> vals = new HashMap<>();
      vals.put(FIELD_SEATS_BOOKED, new StringByteIterator(Integer.toString(selectedFlight.seats_booked + 1)));
      db.update(table, selectedFlight.key, vals);

      // -------------------------------------------------------
      // 步骤 5: 执行 db.commit
      // -------------------------------------------------------
      if (interactive_ && !all_occ) doInteractiveTransactionCommit(db);
      else doTransactionCommit(db);
    } catch (Exception e) {
      try {
//        System.err.println("[SQLException e] tid = " + tid + " rollback" + e.toString());
        if (interactive_ && !all_occ) doInteractiveTransactionRollback(db);
        else doTransactionRollback(db);
      } catch (SQLException e2) {
        // do nothing
      }
      return false;
    }
    return true;
  }


  private boolean executeAgentLoop(DB db, String userRequest, boolean interactive_, int tid, int retryCnt_, boolean all_occ, boolean all_pcc) {
    List<ObjectNode> messages = new ArrayList<>();
    messages.add(createMessage("system", SYSTEM_PROMPT));
    messages.add(createMessage("user", userRequest));

    boolean active = true;
    int turns = 0;
    int ops = 0;

    while (active && turns < 10) {
      turns++;
      String llmResp = callLLM(messages);
      if (llmResp == null) return true; // Fail safe

      AgentCommand cmd = parseAgentResponse(llmResp);
      messages.add(createMessage("assistant", llmResp));
//      System.out.println("action:" + cmd.action + " key:" + cmd.key + " fields:" + cmd.fields + " options:"+ cmd.options + " reason:" + cmd.reason);

      if (cmd == null || cmd.action == null) return true;

      try {
        if (ops == 0) {
          if (interactive_ && !all_occ) doInteractiveTransactionBegin(db, retryCnt_);
          else doTransactionBegin(db);
        }
        ops++;
        switch (cmd.action.toUpperCase()) {
          case "SCAN":
//            if (true) {
//              messages.add(createMessage("user", "SCAN_RESULTS: " + resps[1]));
//              break;
//            }
            // 关键：SCAN 操作是读取的主要来源
            Vector<HashMap<String, ByteIterator>> scanRes = new Vector<>();
            // key 在这里是 prefix, e.g., FLT_PEK_SHA_20251001
            HashSet<String> fields = new HashSet<String>();
            fields.add("ycsb_key");
            fields.add("seats_total");
            fields.add("seats_booked");
            fields.add("price");
            fields.add("status");
            fields.add("data");

            Status s = db.scan(table, cmd.key, cmd.count, fields, scanRes);
            if (s.isOk() &&!scanRes.isEmpty()) {
              messages.add(createMessage("user", "SCAN_RESULTS: " + scanRes.toString()));
            } else {
              messages.add(createMessage("user", "SCAN_RESULTS: None found."));
            }
            break;
          case "DISPLAY_OPTIONS":
//            if (true) {
//              messages.add(createMessage("user", resps[2]));
//              break;
//            }
            // 模拟用户行为：从 Agent 提供的选项中随机选一个
            if (cmd.options!= null &&!cmd.options.isEmpty()) {
              int choiceIndex = ThreadLocalRandom.current().nextInt(cmd.options.size());
              Map<String, String> selectedOption = cmd.options.get(choiceIndex);
              String selectedFlightId = selectedOption.getOrDefault("flight_id", "Unknown");

              // 构造用户回复
              String userReply = String.format("I choose option %d (Flight ID: %s). Please book it now.",
                  choiceIndex + 1, selectedFlightId);
              messages.add(createMessage("user", userReply));

              System.out.println("User selected: " + selectedFlightId + " from " + cmd.options.size() + " options.");
            } else {
              messages.add(createMessage("user", "You didn't provide any options! Please check again."));
            }
            break;
          case "UPDATE":
//            if (true) {
//              messages.add(createMessage("user", "UPDATE_STATUS: " + resps[3]));
//              break;
//            }
            // 只有在 Booking 意图下，Agent 才会走到这里
            HashMap<String, ByteIterator> vals = new HashMap<>();
            if (cmd.fields!= null) {
              cmd.fields.forEach((k,v) -> vals.put(k, new StringByteIterator(v)));
            }
            Status upS = db.update(table, cmd.key, vals);
            messages.add(createMessage("user", "UPDATE_STATUS: " + upS.getName()));
            break;

          case "READ": // 备用
            HashMap<String, ByteIterator> rRes = new HashMap<>();
            db.read(table, cmd.key, null, rRes);
            messages.add(createMessage("user", "RESULT: " + rRes.toString()));
            break;

          case "COMMIT":
//            if (true) {
//              active = false;
//              break;
//            }
            if (interactive_ && !all_occ) doInteractiveTransactionCommit(db);
            else doTransactionCommit(db);
            active = false;
            break;
          case "ABORT":
//            if (true) {
//              active = false;
//              break;
//            }
            if (interactive_ && !all_occ) doInteractiveTransactionRollback(db);
            else doTransactionRollback(db);
            active = false;
            break;
          default:
            System.out.println("out of plan action:" + cmd.action + " key:" + cmd.key + " fields:" + cmd.fields + " options:"+ cmd.options + " reason:" + cmd.reason);
            if (interactive_ && !all_occ) doInteractiveTransactionRollback(db);
            else doTransactionRollback(db);
            return true;
        }
      } catch (SQLException e) {
        try {
          System.err.println("[SQLException e] tid = " + tid + " rollback" + e.toString());
          if (interactive_ && !all_occ) doInteractiveTransactionRollback(db);
          else doTransactionRollback(db);
        } catch (SQLException e2) {
          // do nothing
        }
        return false;
      }
    }
    return true;
  }

  // 辅助方法：生成日期字符串 202510XX
  private String getDateString(int dayOffset) {
    int day = 1 + dayOffset;
    return "202510" + (day < 10? "0" + day : day);
  }

  // --------------------------------------------------------
  // 以下是 JSON 解析和 HTTP 通信的 boilerplate (保持不变)
  // --------------------------------------------------------

  private ObjectNode createMessage(String role, String content) {
    ObjectNode node = mapper.createObjectNode();
    node.put("role", role);
    node.put("content", content);
    return node;
  }

  private String callLLM(List<ObjectNode> messages) {
    HttpURLConnection conn = null;
    InputStream stream = null; // 定义在外面方便 finally 关闭
    try {
      ObjectNode requestBody = mapper.createObjectNode();
      requestBody.put("model", this.model);
      requestBody.put("temperature", 0.0);
      ArrayNode arr = requestBody.putArray("messages");
      for (ObjectNode msg : messages) {
        arr.add(msg);
      }

      String jsonInputString = mapper.writeValueAsString(requestBody);
      // TODO: 统计json长度
//      reqSize += jsonInputString.length();
//       System.out.println("Req: " + jsonInputString);

       // test
//       if (true) {
//        int idx = ThreadLocalRandom.current().nextInt(5);
//        return req[0];
//      }

      URL url = new URL(this.endpoint);
      conn = (HttpURLConnection) url.openConnection();

      conn.setRequestMethod("POST");
      // 显式开启 Keep-Alive
      conn.setRequestProperty("Connection", "Keep-Alive");
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setRequestProperty("Accept", "application/json");
      conn.setRequestProperty("Authorization", "Bearer " + this.apiKey);
      conn.setDoOutput(true);

      // 在高并发 SSL 握手时，10秒很容易耗尽
      conn.setConnectTimeout(10000);
      conn.setReadTimeout(60000);

      // 发送请求体
      try (OutputStream os = conn.getOutputStream()) {
        byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
        os.write(input, 0, input.length);
        os.flush();
      }

      int code = conn.getResponseCode();

      if (code >= 200 && code < 300) {
        stream = conn.getInputStream();
      } else {
        stream = conn.getErrorStream();
      }

      if (stream == null) {
        // 极少数情况 stream 为空，直接返回
        return null;
      }

      // 读取内容
      try (BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
        StringBuilder response = new StringBuilder();
        String responseLine;
        while ((responseLine = br.readLine()) != null) {
          response.append(responseLine.trim());
        }

        if (code >= 200 && code < 300) {
          return extractContent(response.toString());
        } else {
          System.err.println("LLM API Error: " + code + " " + response.toString());
          return null;
        }
      }

    } catch (java.net.SocketTimeoutException e) {
      System.err.println("Timeout calling LLM: " + e.getMessage());
      return null;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    } finally {
      // 只关闭流，不调用 conn.disconnect()
      // 只要流被关闭（上面的 try-with-resources 里的 BufferedReader 关闭时会自动关闭 stream），
      // 连接就会被放回池中复用。
      if (stream != null) {
        try { stream.close(); } catch (Exception ignored) {}
      }
    }
  }


  private String callLLM1(List<ObjectNode> messages) {
    HttpURLConnection conn = null;
    InputStream stream = null;
    try {
      ObjectNode requestBody = mapper.createObjectNode();
      requestBody.put("model", this.model);
      requestBody.put("temperature", 0.0);
      ArrayNode arr = requestBody.putArray("messages");
      for (ObjectNode msg : messages) {
        arr.add(msg);
      }

      String jsonInputString = mapper.writeValueAsString(requestBody);
      System.out.println("Req: " + jsonInputString);
//      if (true) {
//        int idx = ThreadLocalRandom.current().nextInt(5);
//        return req[idx];
//      }

      URL url = new URL(this.endpoint);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Connection", "Keep-Alive");
      conn.setRequestProperty("Content-Type", "application/json; utf-8");
      conn.setRequestProperty("Accept", "application/json");
      conn.setRequestProperty("Authorization", "Bearer " + this.apiKey);
//      conn.setRequestProperty("OpenAI-Project", this.apiProject);
      conn.setDoOutput(true);
      conn.setConnectTimeout(10000);
      conn.setReadTimeout(60000);

      try (OutputStream os = conn.getOutputStream()) {
        byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
        os.write(input, 0, input.length);
        os.flush(); // 显式刷新
      }

      int code = conn.getResponseCode();
//      InputStream stream = (code >= 200 && code < 300)? conn.getInputStream() : conn.getErrorStream();
      if (code >= 200 && code < 300) {
        stream = conn.getInputStream();
      } else {
        stream = conn.getErrorStream();
      }

      if (stream == null) return null;

      try (BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
        StringBuilder response = new StringBuilder();
        String responseLine;
        while ((responseLine = br.readLine())!= null) {
          response.append(responseLine.trim());
        }
        if (code >= 200 && code < 300) {
          return extractContent(response.toString());
        } else {
          System.err.println("LLM API Error: " + code + " " + response.toString());
          return null;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    } finally {
//      if (conn!= null) conn.disconnect();
      if (stream != null) {
        try { stream.close(); } catch (Exception ignored) {}
      }
    }
  }

  private String extractContent(String jsonResponse) {
    try {
      return mapper.readTree(jsonResponse)
          .path("choices").get(0).path("message").path("content").asText();
    } catch (Exception e) { return null; }
  }

  private AgentCommand parseAgentResponse(String raw) {
    try {
      String json = raw.trim();
      Matcher m = JSON_BLOCK_PATTERN.matcher(json);
      if (m.find()) json = m.group(1);
      else {
        int s = json.indexOf('{'), e = json.lastIndexOf('}');
        if (s!= -1 && e!= -1) json = json.substring(s, e + 1);
      }
      return mapper.readValue(json, AgentCommand.class);
    } catch (Exception e) { return null; }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class AgentCommand {
    @JsonProperty("action") public String action;
    @JsonProperty("key") public String key;
    @JsonProperty("count") public int count;
    @JsonProperty("reason") public String reason;
    @JsonProperty("fields") public Map<String, String> fields;
    @JsonProperty("options") public List<Map<String, String>> options;
  }

  // ================= Helper ==================
  public void setMaxPriority(int maxPriority1) {
    this.maxPriority = maxPriority1;
  }

  public void setMaxRetryCnt(int maxRetryCnt1) {
    this.maxRetryCnt = maxRetryCnt1;
  }

  public void setLevelRetryCnt(int levelRetryCount1) {
    this.levelRetryCount = levelRetryCount1;
  }

  public void doTransactionBegin(DB db) throws SQLException {
    // do transaction begin
    db.beginTransaction();
  }

  public void doInteractiveTransactionBegin(DB db, int retryCnt_) throws SQLException {
    // do transaction begin
    db.beginInteractiveTransaction(retryCnt_);
  }

  public void doTransactionCommit(DB db) throws SQLException {
    // do transaction commit
    db.commitTransaction();
  }

  // wzy 提交交互性事务
  public void doInteractiveTransactionCommit(DB db) throws SQLException {
    // do transaction commit
    db.commitInteractiveTransaction();
  }

  public void doTransactionRollback(DB db) throws SQLException {
    // do transaction abort/rollback
    db.rollbackTransaction();
  }

  public void doInteractiveTransactionRollback(DB db) throws SQLException {
    // do transaction abort/rollback
    db.rollbackInteractiveTransaction();
  }

  // key generator helper
  long uniformNextKeynum() {
    return dateGenerator.nextValue().intValue();
  }

  long nextKeynum() {
    return routeZipfGenerator.zipf();
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) throws SQLException {
    System.err.println("Agent workload does not support none-retry txn");
    return false;
  }

  public void switchProperty(int type) {
    System.err.println("Agent workload does not support switching workload");
  }

  public Long getSqlCnt(int tid) {
    return 0L;
  }
  public Long getSqlLatency(int tid) {
    return 0L;
  }
  public Long getTokenCost(int tid) {
    return 0L;
  }
  public List<AgentResp> getAgentResps(int tid) {
    return new ArrayList<>();
  }

  private static final String FIELD_FLIGHT_ID = "ycsb_key";
  private static final String FIELD_SEATS_BOOKED = "seats_booked";
  private static final String FIELD_SEATS_TOTAL = "seats_total";
  private static final String FIELD_PRICE = "price";
  private static final String FIELD_STATUS = "status";
  private static final String FIELD_DATA = "data";


  public class Flight{

    public String key;
    public int seats_total;
    public int seats_booked;
    public double price;
    public String status;
    public String data;
    public Flight(HashMap<String, ByteIterator> row) {
      ByteIterator idIter = row.get(FIELD_FLIGHT_ID);
      ByteIterator bookedIter = row.get(FIELD_SEATS_BOOKED);
      ByteIterator totalIter = row.get(FIELD_SEATS_TOTAL);
      ByteIterator priceIter = row.get(FIELD_PRICE);
      ByteIterator statusIter = row.get(FIELD_STATUS);
      ByteIterator dataIter = row.get(FIELD_DATA);

      String bookedStr = bookedIter.toString();
      String totalStr = totalIter.toString();
      String priceStr = priceIter.toString();

      // 格式清洗与类型转换
      key = idIter.toString();
      seats_booked = Integer.parseInt(bookedStr.trim());
      seats_total = Integer.parseInt(totalStr.trim());
      price = Double.parseDouble(priceStr.trim());
      status = statusIter.toString();
      data = dataIter.toString();

      if (key.equals("")) {
        System.out.println("key not exist");
      }
    }
  }

  public List<Flight> parseFlights(Vector<HashMap<String, ByteIterator>> scanResult) {
    // 使用 ArrayList 存储结果，避免 Vector 的同步开销
    List<Flight> availableFlights = new ArrayList<>();

    if (scanResult == null || scanResult.isEmpty()) {
      return availableFlights;
    }

    //  遍历每一行记录（代表一个航班）
    for (HashMap<String, ByteIterator> row : scanResult) {
      try {
        // 消耗 Iterator 并转换为 String
        // 构建结果对象
        ByteIterator idIter = row.get(FIELD_FLIGHT_ID);
        if (idIter!= null) {
          Flight flightData = new Flight(row);
          availableFlights.add(flightData);
        } else {
          System.out.println("idIter not exist");
        }
      } catch (NumberFormatException e) {
        System.err.println("数据格式错误，无法解析航班座位信息: " + e.getMessage());
      } catch (Exception e) {
        System.err.println("解析行数据时发生未知错误: " + e.getMessage());
      }
    }

    return availableFlights;
  }

  private Map<String, Object> generatePreference(int tid) {
    Map<String, Object> pref = new HashMap<>();
    List<Map<String, Object>> preferences = new ArrayList<>();
    // 生成2-4组偏好，每组针对不同airline
    int numPrefs = 2 + ThreadLocalRandom.current().nextInt(3); // 2-4
    Set<String> usedAirlines = new HashSet<>();
    for (int i = 0; i < numPrefs; i++) {
      String airline;
      do {
        airline = AIRLINES[ThreadLocalRandom.current().nextInt(AIRLINES.length)];
      } while (usedAirlines.contains(airline));
      usedAirlines.add(airline);

      Map<String, Object> airlinePref = new HashMap<>();
      airlinePref.put("airline", airline);
      airlinePref.put("max_price", 600 + ThreadLocalRandom.current().nextDouble() * 1200); // 600-1800
      airlinePref.put("min_price", ThreadLocalRandom.current().nextDouble() * 500); // 0-500
      preferences.add(airlinePref);
    }
    pref.put("preferences", preferences);
    return pref;
  }
}