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
import java.util.Collections;

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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

/**
 * AgentWorkloadV1: 机票预订场景 LLM Agent 压测负载 (版本2: 4表JOIN架构)
 *
 * 建表语句：
 * -- Routes表（航班基本信息）
 * CREATE TABLE flight_routes (
 *     ycsb_key VARCHAR(250) PRIMARY KEY,  -- FLT_<ORIGIN>_<DEST>_<DATE>_<ID>
 *     origin VARCHAR(10),
 *     dest VARCHAR(10),
 *     date VARCHAR(10),  -- e.g., 20251001
 *     dep_time VARCHAR(10)  -- e.g., 10:00
 * );
 *
 * -- Prices表（价格信息）
 * CREATE TABLE flight_prices (
 *     ycsb_key VARCHAR(250) PRIMARY KEY,
 *     price DECIMAL(10, 2),
 *     airline VARCHAR(10)  -- e.g., CZ, CA
 * );
 *
 * -- Aircraft表（飞机信息）
 * CREATE TABLE flight_aircraft (
 *     ycsb_key VARCHAR(250) PRIMARY KEY,
 *     model VARCHAR(50),  -- e.g., Boeing737
 *     has_first_class VARCHAR(20),
 *     first_class_seats INT,
 *     total_seats INT
 * );
 *
 * -- Seats表（读写，存储座位动态信息）
 * CREATE TABLE flight_seat (
 *     ycsb_key VARCHAR(250) PRIMARY KEY,  -- 同上
 *     seats_total INT,    -- 总座位数
 *     seats_booked INT,    -- 已预订座位数
 *     data TEXT
 * );
 *
 * 特性：
 * 1. 20城市/380航线，支持 Zipfian 访问倾斜
 * 2. 初始化插入 100w 行数据到4张表（routes/prices/aircraft预填，seats按total_seats插入）
 * 3. LLM Agent驱动JOIN查询：SCAN routes -> JOIN with preference -> DISPLAY_OPTIONS -> UPDATE seats -> COMMIT
 * 4. 用户偏好过滤：Agent构造JOIN SQL过滤airline/price
 * 5. 4表分离：模拟复杂JOIN性能测试，Serializable隔离
 * 6. 并发控制：热点航线，think-time，abort统计
 */
public class AgentWorkloadV3 extends CoreWorkload {

  // 1. 扩大的城市列表 (20个国内主要机场)
  private static final String[] CITIES = {
      "PEK", "SHA", "CAN", "CTU", "SZX", "KMG", "XIY", "CKG", "HGH", "XMN",
      "NKG", "WUH", "CSX", "HAK", "TAO", "SHE", "TSN", "URC", "HRB", "DKR"
  };

  private static final String[] AIRCRAFT_MODELS = {
      "Boeing737", "Airbus320", "Boeing787", "Airbus350", "Boeing777", "Airbus330"
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
  public static ConcurrentHashMap<Integer, Map<String,Object>> tid_pref_map = new ConcurrentHashMap<>();
  private static final String ROUTES_TABLE = "flight_routes"; // 航班基本信息
  private static final String PRICES_TABLE = "flight_prices"; // 价格信息
  private static final String AIRCRAFT_TABLE = "flight_aircraft"; // 飞机信息
  private static final String SEATS_TABLE = "flight_seats"; // 座位状态

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
  private String dbUrl;
  private String dbUser;
  private String dbPass;
  public double contention_level = 0.99;
  public int minMs = 1000;
  public int maxMs = 2000;
  // 负载生成器
  private ZipfianFast routeZipfGenerator; // 用于控制航线访问热度
  private UniformLongGenerator dateGenerator; // 日期通常是均匀访问的

  private static final String FEW_SHOT_HISTORY =
      "User: Book flight SHA to CAN on 20251010.\n" +
          "Assistant: {\"action\": \"SCAN\", \"table\": \"flight_routes\", \"key\": \"FLT_SHA_CAN_20251010\", \"count\": 5}\n" +
          "User: SCAN_RESULTS: [{\"id\":\"FLT_1\"}]\n" + // 数据极度简化
          "Assistant: {\"action\": \"JOIN\", \"query\": \"SELECT * FROM routes r JOIN prices p ON r.key=p.key ... LIMIT 5\"}\n" +
          "User: JOIN_RESULTS: [{\"id\":\"FLT_1\", \"price\":100}]\n" + // 数据极度简化
          "Assistant: {\"action\": \"CHECK_SEATS\", \"keys\": [\"FLT_1\"]}\n" +
          "User: CHECK_SEATS_RESULT: [{\"id\":\"FLT_1\", \"seats\":100}]\n" + // 数据极度简化
          "Assistant: {\"action\": \"UPDATE_AND_COMMIT\", \"key\": \"FLT_1\", \"fields\": {\"seats\": 1}}";

  public static final String SYSTEM_PROMPT =
      "You are a Flight Booking Agent. Protocol: JSON ONLY. DO NOT THINK, ANSWER IN 2 SECONDS.\n" +
          "### DATABASE SCHEMA\n" +
          "- Routes Table: flight_routes (ycsb_key VARCHAR(255), origin VARCHAR(10), dest VARCHAR(10), date VARCHAR(10), dep_time VARCHAR(10))\n" +
          "- Prices Table: flight_prices (ycsb_key VARCHAR(255), price DECIMAL(10, 2), airline VARCHAR(10))\n" +
          "- Aircraft Table: flight_aircraft (ycsb_key VARCHAR(255), model VARCHAR(50), has_first_class VARCHAR(20), first_class_seats INT, total_seats INT)\n" +
          "- Seats Table: flight_seats (ycsb_key VARCHAR(255), seats_total INT, seats_booked INT, data VARCHAR(100))\n" +
          "- Key (ycsb_key): FLT_<ORIGIN>_<DEST>_<DATE>_<ID>\n\n" +
          "### TOOLS\n" +
          "1. SCAN: {\"action\": \"SCAN\", \"table\": \"flight_routes\", \"key\": \"FLT_<ORG>_<DST>_<DATE>\", \"count\": 15}\n" +
          "2. JOIN: {\"action\": \"JOIN\", \"query\": \"SELECT r.ycsb_key, ... FROM flight_routes r JOIN ... WHERE r.ycsb_key LIKE 'FLT_PEK_TAO_20251007%' AND ...\"}\n" +
          "3. CHECK_SEATS: {\"action\": \"CHECK_SEATS\", \"table\": \"flight_seats\", \"keys\": [\"<flight_id>\", \"<flight_id>\"]}\n" +
          "4. ABORT: {\"action\": \"ABORT\", \"reason\": \"...\"}\n\n" +
          "5. UPDATE_AND_COMMIT: {\"action\": \"UPDATE_AND_COMMIT\", \"table\": \"flight_seats\", \"key\": \"<flight_key>\", \"fields\": {\"seats_booked\": \"<val>\"}}\n" +
          "### WORKFLOW\n" +
          "- User requests include preferences (e.g., airline, price range, aircraft model, dep_time range, etc.). Use them to filter in JOIN WHERE clauses.\n" +
          "- Book: SCAN flight_routes -> construct JOIN query with user preferences -> JOIN (use LIMIT to control result size, e.g., LIMIT 10) -> if preference filtered results are empty, SCAN more records (increase count or use a different key prefix) -> CHECK_SEATS for each flight (limit 10) -> Select the SINGLE BEST flight (prioritize user preferences if possible) -> UPDATE_AND_COMMIT flight_seats\n" +
          "- Always check table flight_seats seats_booked < seats_total before booking.\n If all flights are not suit preferences, choose one flight as fallback desicion.\n" +
          "- HINT STRICT SEARCH (Performance Critical): Construct a JOIN query using RANGE SCAN.\n" +
          "   - Calculate the NEXT DATE based on user's input date (e.g., if date is '20251007', next date is '20251008').\n" +
          "   - Use the syntax: WHERE r.ycsb_key >= 'FLT_<ORG>_<DST>_<DATE>' AND r.ycsb_key < 'FLT_<ORG>_<DST>_<NEXT_DATE>'\n" +
          "   - Combine with user preferences: AND p.airline = 'CZ' ...\n" +
          "   - Use LIMIT to control result size, e.g., LIMIT 5";

//  public static final String SYSTEM_PROMPT =
//      "You are a Transactional Flight Booking Agent. Protocol: JSON ONLY.\n" +
//      "### DATABASE SCHEMA\n" +
//      "- Routes Table: flight_routes (ycsb_key VARCHAR(255), origin VARCHAR(10), dest VARCHAR(10), date VARCHAR(10), dep_time VARCHAR(10))\n" +
//      "- Prices Table: flight_prices (ycsb_key VARCHAR(255), price DECIMAL(10, 2), airline VARCHAR(10))\n" +
//      "- Aircraft Table: flight_aircraft (ycsb_key VARCHAR(255), model VARCHAR(50), has_first_class VARCHAR(20), first_class_seats INT, total_seats INT)\n" +
//      "- Seats Table: flight_seats (ycsb_key VARCHAR(255), seats_total INT, seats_booked INT, data VARCHAR(100))\n" +
//      "- Key (ycsb_key): FLT_<ORIGIN>_<DEST>_<DATE>_<ID>\n\n" +
//
//      "### TOOLS\n" +
//      "1. SCAN: {\"action\": \"SCAN\", \"table\": \"flight_routes\", \"key\": \"FLT_<ORG>_<DST>_<DATE>\", \"count\": 5}\n" +
//      "2. JOIN: {\"action\": \"JOIN\", \"query\": \"SELECT ... FROM flight_routes r ... WHERE r.ycsb_key >= 'FLT_PEK_TAO_20251007' AND r.ycsb_key < 'FLT_PEK_TAO_20251008' ... LIMIT 20 \"}\n" +
//      "3. CHECK_SEATS: {\"action\": \"CHECK_SEATS\", \"table\": \"flight_seats\", \"keys\": [\"<key1>\", \"<key2>\"]}\n" +
//      "4. UPDATE_AND_COMMIT: {\"action\": \"UPDATE_AND_COMMIT\", \"table\": \"flight_seats\", \"key\": \"<flight_key>\", \"fields\": {\"seats_booked\": \"<val>\"}, \"reason\": \"Booked best option\"}\n" +
//      "5. ABORT: {\"action\": \"ABORT\", \"reason\": \"No flights available\"}\n\n" +
//
//      "### WORKFLOW\n" +
//      "1. ANALYZE REQUEST: Extract origin, dest, date, and user preferences (airline, price, etc.).\n" +
//      "2. STRICT SEARCH (Performance Critical): Construct a JOIN query using RANGE SCAN.\n" +
//      "   - Calculate the NEXT DATE based on user's input date (e.g., if date is '20251007', next date is '20251008').\n" +
//      "   - Use the syntax: WHERE r.ycsb_key >= 'FLT_<ORG>_<DST>_<DATE>' AND r.ycsb_key < 'FLT_<ORG>_<DST>_<NEXT_DATE>'\n" +
//      "   - This range query forces the database to use the primary key index, which is 300x faster than normal filtering.\n" +
//      "   - Combine with user preferences: AND p.airline = 'CZ' ...\n" +
//      "   - Use LIMIT to control result size, e.g., LIMIT 20" +
//      "3. FALLBACK DECISION: If STRICT SEARCH returns 0 results, re-run the JOIN query removing user preference constraints (keep only the 'r.ycsb_key' range filter) to find ANY flight.\n" +
//      "4. CHECK INVENTORY: Call CHECK_SEATS on the candidate flights LIMIT 10.\n" +
//      "5. EXECUTE: Select the SINGLE BEST flight where (seats_total > seats_booked).\n" +
//      "   - Prioritize user preferences. If falling back, choose the cheapest.\n" +
//      "   - DO NOT ask the user. DO NOT display options.\n" +
//      "   - Call UPDATE_AND_COMMIT immediately to book the seat.\n";

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
    this.dbUrl = p.getProperty("db_url", "jdbc:postgresql://127.0.0.1:16000/postgres");
    this.dbUser = p.getProperty("db_user", "jack");
    this.dbPass = p.getProperty("db_pass", "Test@123");
    this.contention_level = Double.parseDouble(p.getOrDefault(CONTENTION_LEVEL, "0.99").toString());
    this.minMs = Integer.parseInt(p.getOrDefault("min_sleep_per_ops", "1000").toString());
    this.maxMs = Integer.parseInt(p.getOrDefault("max_sleep_per_ops", "2000").toString());

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
    int total = 50000 + ThreadLocalRandom.current().nextInt(20000); // 100-300 seats per flight
    int booked = 0; // 初始未预订
    double price = 500 + ThreadLocalRandom.current().nextInt(1000);
    String airline = AIRLINES[ThreadLocalRandom.current().nextInt(AIRLINES.length)];
    String depTime = String.format("%02d:00", 6 + ThreadLocalRandom.current().nextInt(17)); // 06:00 to 22:00
    String model = AIRCRAFT_MODELS[ThreadLocalRandom.current().nextInt(AIRCRAFT_MODELS.length)];
    boolean hasFirst = ThreadLocalRandom.current().nextBoolean();
    int firstSeats = hasFirst ? 1 : 0;

    // 插入routes表
    Map<String, ByteIterator> routesValues = new HashMap<>();
    routesValues.put("origin", new StringByteIterator(origin));
    routesValues.put("dest", new StringByteIterator(dest));
    routesValues.put("date", new StringByteIterator(date));
    routesValues.put("dep_time", new StringByteIterator(depTime));
    Status routesStatus = db.insert(ROUTES_TABLE, dbKey, routesValues);
    if (!routesStatus.isOk()) return false;

    // 插入prices表
    Map<String, ByteIterator> pricesValues = new HashMap<>();
    pricesValues.put("price", new StringByteIterator(String.valueOf(price)));
    pricesValues.put("airline", new StringByteIterator(airline));
    Status pricesStatus = db.insert(PRICES_TABLE, dbKey, pricesValues);
    if (!pricesStatus.isOk()) return false;

    // 插入aircraft表
    Map<String, ByteIterator> aircraftValues = new HashMap<>();
    aircraftValues.put("model", new StringByteIterator(model));
    aircraftValues.put("has_first_class", new StringByteIterator(String.valueOf(hasFirst)));
    aircraftValues.put("first_class_seats", new StringByteIterator(String.valueOf(firstSeats)));
    aircraftValues.put("total_seats", new StringByteIterator(String.valueOf(total)));
    Status aircraftStatus = db.insert(AIRCRAFT_TABLE, dbKey, aircraftValues);
    if (!aircraftStatus.isOk()) return false;

    // 插入seats表
    Map<String, ByteIterator> seatValues = new HashMap<>();
    seatValues.put("seats_total", new StringByteIterator(String.valueOf(total)));
    seatValues.put("seats_booked", new StringByteIterator("0"));
    seatValues.put("data", new StringByteIterator(""));
    Status seatStatus = db.insert(SEATS_TABLE, dbKey, seatValues);
    if (!seatStatus.isOk()) return false;

    return true;
  }

  // =================================================================
  // Run 阶段: 模拟 Zipfian 访问
  // =================================================================
  public static ConcurrentHashMap<Integer, String> tid_request_map = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<Integer, String> tid_key_map = new ConcurrentHashMap<>();
  @Override
  public boolean redoTransactionResult(DB db, Object threadstate, boolean retry, int tid, boolean interactive_, int retryCnt_, boolean uniform_flag, boolean all_occ, boolean all_pcc) throws SQLException {
    // 1. 使用 Zipfian 选择航线 (模拟热门航线高并发)
    String userRequest = "";
    String key = "";
    if (!retry){
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
      boolean isBooking = ThreadLocalRandom.current().nextDouble() < 0.15 || true; // 稍微设高一点，因为预订包含读+写

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
    Map<String, Object> pref;
    if (retry) {
      userRequest = tid_request_map.get(tid);
      key = tid_key_map.get(tid);
      pref = tid_pref_map.get(tid);
    } else {
      pref = generatePreference(tid);
      tid_pref_map.put(tid, pref);
    }
    boolean res = false;
    // 交互式走agent，其他走普通后端逻辑
    if (!interactive_) res = executeAgentLoopSimulation(db, key, interactive_ , tid, retryCnt_, all_occ, all_pcc);
    else res = executeAgentLoop(db, userRequest, pref, interactive_ , tid, retryCnt_, all_occ, all_pcc);

    if (res) {
      tid_request_map.remove(tid);
      tid_key_map.remove(tid);
    }
    return res;
  }



  public boolean executeAgentLoopSimulation(DB db, String key, boolean interactive_, int tid, int retryCnt_, boolean all_occ, boolean all_pcc) {
    try {
      if (interactive_ && !all_occ) doInteractiveTransactionBegin(db, retryCnt_);
      else doTransactionBegin(db);
      // -------------------------------------------------------
      // 步骤 1: 收到 User Request 后，直接调用 db.scan 获取航班列表
      // -------------------------------------------------------
      String pre_key = key;
      int turns = 0;
      List<Flight> availableFlights = new ArrayList<>();
      HashSet<String> fields = new HashSet<String>();
      fields.add("ycsb_key");
      fields.add("seats_total");
      fields.add("seats_booked");
      fields.add("data");
      while (turns < 10) {
        if (isStopRequested()) return true;
        turns++;
        Vector<HashMap<String, ByteIterator>> scanRes = new Vector<>();
        // key 在这里是 prefix, e.g., FLT_PEK_SHA_20251001
        int targetCount = 3 + ThreadLocalRandom.current().nextInt(3); // 随机目标数量 3-5 个
        Status s = db.scan(SEATS_TABLE, pre_key, targetCount, fields, scanRes);
        if (!s.isOk()) {
          if (interactive_ && !all_occ) doInteractiveTransactionCommit(db);
          else doTransactionCommit(db);
          return true;
        }
        List<Flight> flightList = parseFlights(scanRes);
        // -------------------------------------------------------
        // 步骤 2: 在 Java 代码中模拟 分析 过程
        // -------------------------------------------------------
        // (seats_booked < seats_total)
        for (Flight flight : flightList) {
          pre_key = flight.key;
          if (flight.seats_booked <= flight.seats_total) availableFlights.add(flight);
          if (availableFlights.size() >= 3) {
            break;
          }
        }
        if (availableFlights.size() >= 3) {
          break;
        }
      }

      // 边界处理：如果扫描后没有可用航班，则本次仿真结束或回滚
      if (availableFlights.isEmpty()) {
        if (interactive_ && !all_occ) doInteractiveTransactionCommit(db);
        else doTransactionCommit(db);
        return true;
      }
      int selectedIndex = ThreadLocalRandom.current().nextInt(availableFlights.size());
      Flight selectedFlight = availableFlights.get(selectedIndex);
      // -------------------------------------------------------
      // 步骤 3: 执行 db.update 对选中航班进行预订
      // -------------------------------------------------------
      // 逻辑：seats_booked + 1
      HashMap<String, ByteIterator> vals = new HashMap<>();
      vals.put(FIELD_SEATS_BOOKED, new StringByteIterator(Integer.toString(selectedFlight.seats_booked + 1)));
      db.update(SEATS_TABLE, selectedFlight.key, vals);

      // -------------------------------------------------------
      // 步骤 4: 执行 db.commit
      // -------------------------------------------------------
      if (interactive_ && !all_occ) doInteractiveTransactionCommit(db);
      else doTransactionCommit(db);
    } catch (Exception e) {
      try {
        if (interactive_ && !all_occ) doInteractiveTransactionRollback(db);
        else doTransactionRollback(db);
      } catch (SQLException e2) {
        // do nothing
      }
      return false;
    }
    return true;
  }


  private boolean executeAgentLoop(DB db, String userRequest, Map<String, Object> pref, boolean interactive_, int tid, int retryCnt_, boolean all_occ, boolean all_pcc) {
    List<ObjectNode> messages = new ArrayList<>();
    long startTimeNano = 0;
    long endTimeNano = 0;
    boolean debug = false;
//    startTimeNano = System.nanoTime();
//    messages.add(createMessage("user", "Answer hi immediately"));
//    String testResp = callLLM(messages);
//    endTimeNano = System.nanoTime();
//
//    System.out.println("TID " + tid + " TEST LLM Response: " + testResp + " time cost(ms): " + 1.0 * (endTimeNano - startTimeNano) / 1000000);
//    try {
//      Thread.sleep(10000);
//    } catch (Exception e) {
//      // .. do nothing
//    }
//    if (true) return true;

//    messages.add(createMessage("system", SYSTEM_PROMPT));
    messages.add(createMessage("system", SYSTEM_PROMPT + "\n\nEXAMPLES:\n" + FEW_SHOT_HISTORY));
    messages.add(createMessage("user", userRequest));
    try {
      messages.add(createMessage("user", "PREFERENCE: " + mapper.writeValueAsString(pref)));
      if (debug) System.out.println("[New round] TID " + tid + " req: " + userRequest + " preference: " + mapper.writeValueAsString(pref));
    } catch (Exception e) {
      messages.add(createMessage("user", "PREFERENCE: " + pref.toString()));
    }

    boolean active = true;
    int turns = 0;
    int ops = 0;


    while (active && turns < 20) {
      if (isStopRequested()) {
        if (ops != 0) {
          try {
            if (interactive_ && !all_occ) doInteractiveTransactionRollback(db);
            else doTransactionRollback(db);
          } catch (Exception e) {
            // do nothing
          }
          active = false;
        }
        return true;
      }
      turns++;
      compactHistoryMessages(messages);
      startTimeNano = System.nanoTime();
      String llmResp = "";
      if (turns == 1) {
        // 模拟命中cache
        llmResp = tryGetLocalCacheResponse(userRequest);
      } else {
        llmResp = callLLM(messages);
      }
      if (isStopRequested()) {
        if (ops != 0) {
          try {
            if (interactive_ && !all_occ) doInteractiveTransactionRollback(db);
            else doTransactionRollback(db);
          } catch (Exception e) {
            // do nothing
          }
          active = false;
        }
        return true;
      }
      endTimeNano = System.nanoTime();

      // 统计数据
      tid_sql_cnt.put(tid, tid_sql_cnt.getOrDefault(tid, 0L) + 1);
      long time_cost = endTimeNano - startTimeNano;
      tid_sql_latency.put(tid, tid_sql_latency.getOrDefault(tid, 0L) + time_cost);
      long tokens = estimateTokens(messages);
      tid_token_cost.put(tid, tid_token_cost.getOrDefault(tid, 0L) + tokens);

      if (llmResp == null) {
        System.out.println("TID " + tid + " Turn " + turns + " LLM response is null");
        return true; // Fail safe
      }
      AgentCommand cmd = parseAgentResponse(llmResp);
      if (cmd == null) {
        System.out.println("TID " + tid + " Turn " + turns + " Failed to parse cmd from: " + llmResp);
        return true;
      }
      messages.add(createMessage("assistant", llmResp));


      if (debug) System.out.println("TID " + tid + " Turn " + turns + " LLM Response: " + llmResp + " time cost(ms): " + 1.0 * (endTimeNano - startTimeNano) / 1000000);

      if (cmd.action == null) return true;

      List<AgentResp> respList = tid_agent_resp.getOrDefault(tid, new ArrayList<>());
      respList.add(new AgentResp(tid, llmResp, cmd.action, cmd.query, time_cost, tokens));
      tid_agent_resp.put(tid, respList);

      try {
        if (ops == 0) {
          if (interactive_ && !all_occ) doInteractiveTransactionBegin(db, retryCnt_);
          else doTransactionBegin(db);
        }
        ops++;
        switch (cmd.action.toUpperCase()) {
          case "SCAN":{
            // SCAN routes表
            String scanTable = cmd.table != null ? cmd.table : ROUTES_TABLE;
            Vector<HashMap<String, ByteIterator>> scanRes = new Vector<>();
            HashSet<String> fields = new HashSet<>();
            fields.add("ycsb_key");
            fields.add("date");
            fields.add("dep_time");
            startTimeNano = System.nanoTime();
            Status s = db.scan(scanTable, cmd.key, cmd.count, fields, scanRes);
            endTimeNano = System.nanoTime();
            if (s.isOk() && !scanRes.isEmpty()) {
              try {
                ArrayNode arr = mapper.createArrayNode();
                for (HashMap<String, ByteIterator> row : scanRes) {
                  ObjectNode o = mapper.createObjectNode();
                  o.put("flight_id", row.get("ycsb_key").toString());
                  o.put("date", row.get("date").toString());
                  o.put("dep_time", row.get("dep_time").toString());
                  arr.add(o);
                }
                messages.add(createMessage("user", "SCAN_RESULTS: " + mapper.writeValueAsString(arr)));
                if (debug) System.out.println("TID " + tid + " SCAN Results: " + mapper.writeValueAsString(arr) + " time cost(ms): " + 1.0 * (endTimeNano - startTimeNano) / 1000000);
              } catch (Exception e) {
                messages.add(createMessage("user", "SCAN_RESULTS: [scan results]"));
                System.out.println("TID " + tid + " SCAN Results: [scan results]");
              }
            } else {
              messages.add(createMessage("user", "SCAN_RESULTS: None found."));
              System.out.println("TID " + tid + " SCAN Results: None found.");
            }
            break;
          }

          case "JOIN": {
            // 执行JOIN查询
            String query = cmd.query;
            try {
              startTimeNano = System.nanoTime();
              Vector<HashMap<String, ByteIterator>> results = executeQuery(query);
              endTimeNano = System.nanoTime();
              // 转为JSON
              ArrayNode arr = mapper.createArrayNode();
              for (HashMap<String, ByteIterator> row : results) {
                ObjectNode o = mapper.createObjectNode();
                for (Map.Entry<String, ByteIterator> entry : row.entrySet()) {
                  o.put(entry.getKey(), entry.getValue().toString());
                }
                arr.add(o);
              }
              messages.add(createMessage("user", "JOIN_RESULTS: " + mapper.writeValueAsString(arr)));
              if (debug) System.out.println("TID " + tid + " JOIN_RESULTS: " + mapper.writeValueAsString(arr) + " time cost(ms): " + 1.0 * (endTimeNano - startTimeNano) / 1000000);
            } catch (Exception e) {
              messages.add(createMessage("user", "JOIN_RESULTS: Error executing JOIN"));
              System.out.println("TID " + tid + " JOIN_RESULTS: Error executing JOIN");
            }
            break;
          }
          case "CHECK_SEATS": {
            // 执行CHECK_SEATS查询
            String readTable = cmd.table != null ? cmd.table : SEATS_TABLE;
            HashSet<String> fields = new HashSet<>();
            fields.add("ycsb_key");
            fields.add("seats_total");
            fields.add("seats_booked");

            // 确定要查询的目标Key列表
            List<String> targetKeys = new ArrayList<>();
            if (cmd.keys != null && !cmd.keys.isEmpty()) {
              targetKeys.addAll(cmd.keys);
            } else if (cmd.key != null && !cmd.key.isEmpty()) {
              targetKeys.add(cmd.key);
            }

            // 创建返回的 JSON 数组节点
            startTimeNano = System.nanoTime();
            try {
              ArrayNode resultsArray = mapper.createArrayNode();
              for (String currentKey : targetKeys) {
                HashMap<String, ByteIterator> rRes = new HashMap<>();
                Status status = db.read(readTable, currentKey, fields, rRes);
                ObjectNode itemNode = mapper.createObjectNode();
                itemNode.put("flight_key", currentKey);
                if (status.isOk() && !rRes.isEmpty()) {
                  try {
                    // 尝试解析字段，注意：这里需处理空指针或格式错误，防止一条失败影响整体
                    String sTotal = rRes.containsKey("seats_total") ? rRes.get("seats_total").toString() : "0";
                    String sBooked = rRes.containsKey("seats_booked") ? rRes.get("seats_booked").toString() : "0";
                    itemNode.put("found", true);
                    itemNode.put("seats_total", Integer.parseInt(sTotal));
                    itemNode.put("seats_booked", Integer.parseInt(sBooked));
                    // 计算剩余座位方便Agent直接决策
                    itemNode.put("seats_available", Integer.parseInt(sTotal) - Integer.parseInt(sBooked));
                  } catch (Exception e) {
                    itemNode.put("error", "Data parsing error: " + e.getMessage());
                  }
                } else {
                  itemNode.put("found", false);
                  itemNode.put("error", "Flight not found or DB read failed");
                }
                resultsArray.add(itemNode);
              }
              endTimeNano = System.nanoTime();
              // 返回整个数组给 Agent
              if (debug) System.out.println("TID " + tid + " CHECK_SEATS_RESULT: " + mapper.writeValueAsString(resultsArray) + " time cost(ms): " + 1.0 * (endTimeNano - startTimeNano) / 1000000);
              messages.add(createMessage("user", "CHECK_SEATS_RESULT: " + mapper.writeValueAsString(resultsArray)));
            } catch (Exception e) {
              messages.add(createMessage("user", "CHECK_SEATS_RESULT: Error executing CHECK_SEATS"));
              System.out.println("TID " + tid + " CHECK_SEATS_RESULT: Error executing CHECK_SEATS" + " error: " + e.getMessage());
            }
            break;
          }
          case "DISPLAY_OPTIONS": {
            // 模拟用户行为：从 Agent 提供的选项中随机选一个
            if (cmd.options != null && !cmd.options.isEmpty()) {
              int choiceIndex = ThreadLocalRandom.current().nextInt(cmd.options.size());
              Map<String, String> selectedOption = cmd.options.get(choiceIndex);
              String selectedFlightId = selectedOption.getOrDefault("flight_id", "Unknown");
              String selectedSeatId = selectedOption.getOrDefault("seat_id", "1");

              // 构造用户回复
              String userReply = String.format("I choose option %d (Flight ID: %s, Seat: %s). Please book it now.",
                  choiceIndex + 1, selectedFlightId, selectedSeatId);
              messages.add(createMessage("user", userReply));

              if (debug) System.out.println("User selected: " + selectedFlightId + " Seat: " + selectedSeatId + " from " + cmd.options.size() + " options.");
            } else {
              messages.add(createMessage("user", "You didn't provide any options! Please check again."));
            }
            break;
          }
          case "UPDATE": {
            // UPDATE seats表
            String updateTable = cmd.table != null ? cmd.table : SEATS_TABLE;
            HashMap<String, ByteIterator> vals = new HashMap<>();
            if (cmd.fields != null) {
              cmd.fields.forEach((k, v) -> vals.put(k, new StringByteIterator(v)));
            }
            Status upS = db.update(updateTable, cmd.key, vals);
            messages.add(createMessage("user", "UPDATE_STATUS: " + upS.getName()));
            if (debug) System.out.println("TID " + tid + " UPDATE " + (upS.isOk() ? "succeeded" : "failed") + " for key: " + cmd.key);
            break;
          }
          case "UPDATE_AND_COMMIT": {
            // UPDATE seats表并且提交
            String updateTable = cmd.table != null ? cmd.table : SEATS_TABLE;
            HashMap<String, ByteIterator> vals = new HashMap<>();
            if (cmd.fields != null) {
              cmd.fields.forEach((k, v) -> vals.put(k, new StringByteIterator(v)));
            }
            startTimeNano = System.nanoTime();
            Status upS = db.update(updateTable, cmd.key, vals);
            endTimeNano = System.nanoTime();
            messages.add(createMessage("user", "UPDATE_STATUS: " + upS.getName()));
            if (debug) System.out.println("TID " + tid + " UPDATE " + (upS.isOk() ? "succeeded" : "failed") + " for key: " + cmd.key + " time cost(ms): " + 1.0 * (endTimeNano - startTimeNano) / 1000000);

            startTimeNano = System.nanoTime();
            if (interactive_ && !all_occ) doInteractiveTransactionCommit(db);
            else doTransactionCommit(db);
            endTimeNano = System.nanoTime();
            if (debug) System.out.println("TID " + tid + " COMMIT time cost(ms): " + 1.0 * (endTimeNano - startTimeNano) / 1000000);
            active = false;
            break;
          }
          case "COMMIT":
            if (interactive_ && !all_occ) doInteractiveTransactionCommit(db);
            else doTransactionCommit(db);
            active = false;
            break;
          case "ABORT":
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

  private void compactHistoryMessages(List<ObjectNode> messages) {
    // 保留最新的 N 条消息不压缩 (比如最新的 User 输入 和 Assistant 回复)
    // 同时也保留最开始的 System Prompt 和 User Request
    int protectLastN = 2;
    // 从第 2 条开始遍历（跳过 System Prompt [0] and User Request [1]）
    // 到 倒数第 protectLastN 条为止
    for (int i = 2; i < messages.size() - protectLastN; i++) {
      ObjectNode msg = messages.get(i);
      String role = msg.get("role").asText();
      String content = msg.get("content").asText();

      // 如果是 User 发送给 Agent 的数据库结果，且长度很长，则进行截断
      if ("user".equals(role) && content.length() > 200) { // 阈值可调整
        if (content.contains("SCAN_RESULTS") ||
            content.contains("JOIN_RESULTS") ||
            content.contains("CHECK_SEATS_RESULT")) {

          // 替换为占位符
          msg.put("content", "[History Data: Previous DB Results processed and hidden to save tokens]");
        }
      }
    }
  }

  // 定义正则表达式，匹配 "from AAA to BBB on 202xxxx"
  private static final Pattern REQ_PATTERN = Pattern.compile(
      "from\\s+(\\w+)\\s+to\\s+(\\w+)\\s+on\\s+(\\d+)",
      Pattern.CASE_INSENSITIVE
  );
  private String tryGetLocalCacheResponse(String userRequest) {
    Matcher m = REQ_PATTERN.matcher(userRequest);
    if (m.find()) {
      // 1. 提取关键参数
      String origin = m.group(1).toUpperCase(); // e.g., SHA
      String dest = m.group(2).toUpperCase();   // e.g., CAN
      String date = m.group(3);                 // e.g., 20251010

      // 2. 拼装 Key
      String scanKey = String.format("FLT_%s_%s_%s", origin, dest, date);

      // 3. 拼装完整的 JSON Response
      // 这里的 count: 5 是为了配合之前说的 Limit 优化
      return String.format(
          "{\"action\": \"SCAN\", \"table\": \"flight_routes\", \"key\": \"%s\", \"count\": 15}",
          scanKey
      );
    }
    return null; // 没匹配上，还是去调 API
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


  // token估算方法
  private long estimateTokens(List<ObjectNode> messages) {
    long totalTokens = 0;
    totalTokens += 4; // 每个消息的元数据开销

    for (ObjectNode msg : messages) {
      String role = msg.has("role") ? msg.get("role").asText() : "user";
      String content = msg.has("content") ? msg.get("content").asText() : "";
      totalTokens += 2;
      totalTokens += estimateTokensFromText(content);
    }

    // 添加JSON格式开销（每对大括号、引号等）
    totalTokens += messages.size() * 5;
    return totalTokens;
  }

  private int estimateTokensFromText(String text) {
    if (text == null || text.isEmpty()) return 0;
    // DeepSeek tokenization经验公式：
    // - 中文：平均1.5-2个token/字符
    // - 英文单词：平均0.75个token/字符
    // - 标点和空格：约1个token/字符
    // 简单模式：总字符数 * 系数
    // 简单快速估算（对于实验统计足够）
    // 基于DeepSeek官方文档：约0.7-1.2 tokens/字符，这里取中值
    return  (int)(text.length() * 0.9);
  }

  private String callLLM(List<ObjectNode> messages) {
    HttpURLConnection conn = null;
    InputStream stream = null; // 定义在外面方便 finally 关闭
    try {
      ObjectNode requestBody = mapper.createObjectNode();
      requestBody.put("model", this.model);
//      requestBody.put("max_tokens", 250);
      requestBody.put("temperature", 0.0);
      ArrayNode arr = requestBody.putArray("messages");
      for (ObjectNode msg : messages) {
        arr.add(msg);
      }

      String jsonInputString = mapper.writeValueAsString(requestBody);
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
    } catch (Exception e) {
      System.out.println("Parse error: " + e.getMessage());
      return null;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class AgentCommand {
    @JsonProperty("action") public String action;
    @JsonProperty("table") public String table;
    @JsonProperty("key") public String key;
    @JsonProperty("keys") public List<String> keys; // 新增：支持批量传递key
    @JsonProperty("query") public String query;
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

  private static final String FIELD_FLIGHT_ID = "ycsb_key";
  private static final String FIELD_SEATS_BOOKED = "seats_booked";
  private static final String FIELD_SEATS_TOTAL = "seats_total";
  private static final String FIELD_PRICE = "price";
  private static final String FIELD_STATUS = "status";
  private static final String FIELD_DATA = "data";

  public static ConcurrentHashMap<Integer, Long> tid_sql_cnt = new ConcurrentHashMap<>();     // 统计sql次数
  public static ConcurrentHashMap<Integer, Long> tid_sql_latency = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<Integer, Long> tid_token_cost = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<Integer, List<AgentResp>> tid_agent_resp = new ConcurrentHashMap<>();
  public Long getSqlCnt(int tid) {
    return tid_sql_cnt.getOrDefault(tid, 0L);
  }
  public Long getSqlLatency(int tid) {
    return tid_sql_latency.getOrDefault(tid, 0L);
  }
  public Long getTokenCost(int tid) {
    return tid_token_cost.getOrDefault(tid, 0L);
  }
  public List<AgentResp> getAgentResps(int tid) {
    return tid_agent_resp.getOrDefault(tid, new ArrayList<>());
  }


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
      ByteIterator dataIter = row.get(FIELD_DATA);

      String bookedStr = bookedIter.toString();
      String totalStr = totalIter.toString();

      // 格式清洗与类型转换
      key = idIter.toString();
      seats_booked = Integer.parseInt(bookedStr.trim());
      seats_total = Integer.parseInt(totalStr.trim());
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
    // 生成1组偏好，每组随机包含部分字段
    int numPrefs = 1; //
    for (int i = 0; i < numPrefs; i++) {
      Map<String, Object> p = new HashMap<>();
      // 随机添加airline
      if (ThreadLocalRandom.current().nextBoolean()) {
        p.put("airline", AIRLINES[ThreadLocalRandom.current().nextInt(AIRLINES.length)]);
      }
      // 随机添加aircraft_model
      if (ThreadLocalRandom.current().nextBoolean()) {
        p.put("aircraft_model", AIRCRAFT_MODELS[ThreadLocalRandom.current().nextInt(AIRCRAFT_MODELS.length)]);
      }
      // 随机添加price range
      if (ThreadLocalRandom.current().nextBoolean()) {
        p.put("max_price", 600 + ThreadLocalRandom.current().nextDouble() * 1200);
        p.put("min_price", ThreadLocalRandom.current().nextDouble() * 500);
      }
      // 随机添加dep_time range
//      if (ThreadLocalRandom.current().nextBoolean()) {
//        int startHour = 6 + ThreadLocalRandom.current().nextInt(10);
//        int endHour = startHour + 6 + ThreadLocalRandom.current().nextInt(6);
//        if (endHour > 24) endHour = endHour - 24;
//        p.put("dep_time_start", String.format("%02d:00", startHour));
//        p.put("dep_time_end", String.format("%02d:00", endHour));
//      }
      // 随机添加has_first_class
      if (ThreadLocalRandom.current().nextBoolean()) {
        p.put("has_first_class", ThreadLocalRandom.current().nextBoolean());
      }
      // 如果p不空，添加
      if (!p.isEmpty()) {
        preferences.add(p);
      }
    }
    // 确保至少一个preference
//    if (preferences.isEmpty()) {
//      Map<String, Object> defaultP = new HashMap<>();
//      defaultP.put("airline", AIRLINES[0]);
//      preferences.add(defaultP);
//    }
    pref.put("preferences", preferences);
    return pref;
  }

  private Vector<HashMap<String, ByteIterator>> executeQuery(String query) {
    Vector<HashMap<String, ByteIterator>> results = new Vector<>();
    Properties props = new Properties();
    props.setProperty("user", this.dbUser);
    props.setProperty("password", this.dbPass);
    try (Connection conn = DriverManager.getConnection(this.dbUrl, props);
         PreparedStatement ps = conn.prepareStatement(query);
         ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData meta = rs.getMetaData();
      int colCount = meta.getColumnCount();
      while (rs.next()) {
        HashMap<String, ByteIterator> row = new HashMap<>();
        for (int i = 1; i <= colCount; i++) {
          String colName = meta.getColumnName(i);
          String value = rs.getString(i);
          row.put(colName, new StringByteIterator(value != null ? value : ""));
        }
        results.add(row);
      }
    } catch (SQLException e) {
      System.err.println("Error executing query: " + e.getMessage());
    }
    return results;
  }
}