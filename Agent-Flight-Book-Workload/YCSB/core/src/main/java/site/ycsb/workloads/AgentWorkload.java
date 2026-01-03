package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.generator.*;
import site.ycsb.workloads.CoreWorkload;

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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * AgentWorkload: 集成 LLM 的交互式工作负载
 *
 * 启动命令示例:
 *./bin/ycsb run jdbc -P workloads/workloada -p workload=site.ycsb.workloads.AgentWorkload -p agent.apikey=sk-xxx -threads 4
 */
public class AgentWorkload extends CoreWorkload {

  private static final String[] CITIES = {"NYC", "LON", "SHA", "PEK", "TYO", "PAR", "SIN", "DXB"};
  private static final String[] AIRLINES = {"UA", "BA", "MU", "CA", "JL"};

  // Jackson ObjectMapper (线程安全)
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Pattern JSON_BLOCK_PATTERN = Pattern.compile("```json\\s*(\\{.*?\\})\\s*```", Pattern.DOTALL);

  private String apiKey;
  private String model;
  private String endpoint;

  // 确定性随机数生成器 (用于重现数据)
  private Random random = new Random();

  public static final String SYSTEM_PROMPT =
      "You are a Transaction Orchestrator for a high-concurrency Flight Reservation System.\n" +
          "Your goal is to execute a flight booking transaction safely based on the user's request.\n\n" +
          "### DATABASE SCHEMA\n" +
          "- Table: flights\n" +
          "- Key (id): FLIGHT_<ORIGIN>_<DEST>_<DATE>_<ID>\n" +
          "- Fields: 'seats_total' (int), 'seats_booked' (int), 'price' (double), 'status' (string)\n\n" +
          "### PROTOCOL\n" +
          "Output ONLY a JSON object. No markdown. No conversational text.\n\n" +
          "### TOOLS\n" +
          "1. READ: {\"action\": \"READ\", \"key\": \"...\"}\n" +
          "2. UPDATE: {\"action\": \"UPDATE\", \"key\": \"...\", \"fields\": {\"seats_booked\": \"<new_val>\"}}\n" +
          "   (Only update if seats_booked < seats_total)\n" +
          "3. COMMIT: {\"action\": \"COMMIT\", \"reason\": \"Success\"}\n" +
          "4. ABORT: {\"action\": \"ABORT\", \"reason\": \"Full\"}\n";

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    this.apiKey = p.getProperty("agent.apikey");
    this.model = p.getProperty("agent.model", "gpt-3.5-turbo");
    this.endpoint = p.getProperty("agent.endpoint", "https://api.openai.com/v1/chat/completions");

    if (this.apiKey == null) {
      System.err.println("WARNING: 'agent.apikey' is missing. LLM calls will fail.");
    }
  }

  // =================================================================
  // Run 阶段: Agent 协同事务
  // =================================================================
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    // 1. 选择一个目标航班
    // 使用 CoreWorkload 的 transactioninsertkeysequence 或 keychooser 来选择一个已存在的 ID
    int keyNum = (int)nextKeynum(); // YCSB 核心方法，选择一个符合 Zipfian 分布的 ID
    String targetKey = buildSemanticKey(keyNum);

    // 2. 准备上下文
    List<ObjectNode> messages = new ArrayList<>();
    messages.add(createMessage("system", SYSTEM_PROMPT));

    String userRequest = "Request: Please book one seat on flight " + targetKey + ".";
    messages.add(createMessage("user", userRequest));

    boolean transactionActive = true;
    int turnCount = 0;
    int retryCnt = 0;

    // 3. 开启交互循环
    while (transactionActive && turnCount < 10) {
      turnCount++;

      // 调用 LLM
      String agentResponseRaw = callLLM(messages);
      if (agentResponseRaw == null) return true; // 网络/API错误，跳过但不报错

      // [Parse] 解析指令
      AgentCommand cmd = parseAgentResponse(agentResponseRaw);
      messages.add(createMessage("assistant", agentResponseRaw)); // 保持记忆

      if (cmd == null || cmd.action == null) {
        return true; // 格式错误，跳过
      }

      // if (interactive_ && !uniform_flag && !all_occ) doInteractiveTransactionBegin(db, retryCnt);
      // else doTransactionBegin(db);

      // [Act] 执行数据库操作
      switch (cmd.action.toUpperCase()) {
        case "READ":
          HashMap<String, ByteIterator> result = new HashMap<>();
          Status readStatus = db.read(table, cmd.key, null, result);
          String obs = readStatus.isOk()? "DB_RESULT: " + result.toString() : "DB_ERROR: " + readStatus.getName();
          messages.add(createMessage("user", obs));
          break;

        case "UPDATE":
          HashMap<String, ByteIterator> updateVals = new HashMap<>();
          if (cmd.fields!= null) {
            for (Map.Entry<String, String> entry : cmd.fields.entrySet()) {
              updateVals.put(entry.getKey(), new StringByteIterator(entry.getValue()));
            }
          }
          Status upStatus = db.update(table, cmd.key, updateVals);
          messages.add(createMessage("user", "UPDATE_STATUS: " + upStatus.getName()));
          break;

        case "COMMIT":
        case "ABORT":
          transactionActive = false;
          break;

        default:
          return true;
      }
    }
    return true;
  }

  // ===================== 插入数据 ==========================
  // 1. 定义有限的日期范围 (例如未来 30 天)
    private static final int DAYS_RANGE = 30;
    private static final String START_DATE_STR = "20251001";

    // 2. 优化的 doInsert: 保证数据在时间和航线上是“聚簇”的
    @Override
    public boolean doInsert(DB db, Object threadstate) {
        // YCSB Load 阶段生成的序列号: 0, 1, 2... 1,000,000
        int keyNum = (int)keysequence.nextValue();

        // 生成语义 Key
        String dbKey = buildSemanticKey(keyNum);

        // 生成语义 Value
        Map<String, ByteIterator> values = buildSemanticValues(keyNum);

        // 执行插入
        Status status = db.insert(table, dbKey, values);
        return null!= status && status.isOk();
    }

    /**
     * 关键算法：将线性 ID 映射为语义 Key
     * 逻辑：FLIGHT_<ORG>_<DEST>_<DATE>_<SEQ>
     * 这样设计后，相同航线、相同日期的航班在 B+树 上是物理连续的，SCAN 极快。
     */
    private String buildSemanticKey(int keyNum) {
        // 1. 确定航线 (Origin -> Dest)
        // CITIES.length = 8, 共有 8 * 7 = 56 条可能的航线
        int totalRoutes = CITIES.length * (CITIES.length - 1);
        int routeIdx = (int) (keyNum % totalRoutes);

        String origin = CITIES[0];
        List<String> validDests = new ArrayList<>();
        for(String c : CITIES) { if(!c.equals(origin)) validDests.add(c); }
        String dest = validDests.get(routeIdx % (CITIES.length - 1));

        // 2. 确定日期 (KeyNum / 56) % 30
        int dayOffset = (int) ((keyNum / totalRoutes) % DAYS_RANGE);
        String date = getDateString(dayOffset);

        // 3. 确定航班序列号 (处理 ID 唯一性)
        // 每天该航线的第几班
        long flightSeq = keyNum;

        // 最终 Key: FLIGHT_SHA_PEK_20251005_10045
        return String.format("FLIGHT_%s_%s_%s_%d", origin, dest, date, flightSeq);
    }

    // 简单的日期计算辅助方法
    private String getDateString(int dayOffset) {
        // 简单模拟
        int day = 1 + dayOffset;
        return "202510" + (day < 10? "0" + day : day);
    }

  // =================================================================
  // 辅助逻辑: 语义数据生成
  // =================================================================

  private Map<String, ByteIterator> buildSemanticValues(long keyNum) {
    Random r = new Random(keyNum); // 确定性内容
    Map<String, ByteIterator> values = new HashMap<>();

    int total = 100 + r.nextInt(100); // 100-200 座位
    int booked = r.nextInt(total);    // 0-total 已订
    double price = 500 + r.nextInt(1000);

    values.put("seats_total", new StringByteIterator(String.valueOf(total)));
    values.put("seats_booked", new StringByteIterator(String.valueOf(booked)));
    values.put("price", new StringByteIterator(String.format("%.2f", price)));
    values.put("status", new StringByteIterator("SCHEDULED"));
    return values;
  }

  // =================================================================
  // 辅助逻辑: HTTP & JSON
  // =================================================================

  private ObjectNode createMessage(String role, String content) {
    ObjectNode node = mapper.createObjectNode();
    node.put("role", role);
    node.put("content", content);
    return node;
  }

  private String callLLM(List<ObjectNode> messages) {
    HttpURLConnection conn = null;
    try {
      ObjectNode requestBody = mapper.createObjectNode();
      requestBody.put("model", this.model);
      requestBody.put("temperature", 0.0);
      ArrayNode arr = requestBody.putArray("messages");
      // 修复：Jackson ArrayNode 没有 addAll(Collection) 方法，需循环添加
      for (ObjectNode msg : messages) {
        arr.add(msg);
      }

      String jsonInputString = mapper.writeValueAsString(requestBody);
      URL url = new URL(this.endpoint);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json; utf-8");
      conn.setRequestProperty("Accept", "application/json");
      conn.setRequestProperty("Authorization", "Bearer " + this.apiKey);
      conn.setDoOutput(true);
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(60000);

      try (OutputStream os = conn.getOutputStream()) {
        byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
        os.write(input, 0, input.length);
      }

      int code = conn.getResponseCode();
      InputStream stream = (code >= 200 && code < 300)? conn.getInputStream() : conn.getErrorStream();

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
      if (conn!= null) conn.disconnect();
    }
  }

  private String extractContent(String jsonResponse) {
    try {
      JsonNode root = mapper.readTree(jsonResponse);
      return root.path("choices").get(0).path("message").path("content").asText();
    } catch (Exception e) {
      return null;
    }
  }

  private AgentCommand parseAgentResponse(String llmRawResponse) {
    try {
      String cleanJson = llmRawResponse.trim();
      Matcher matcher = JSON_BLOCK_PATTERN.matcher(cleanJson);
      if (matcher.find()) {
        cleanJson = matcher.group(1);
      } else {
        int firstBrace = cleanJson.indexOf('{');
        int lastBrace = cleanJson.lastIndexOf('}');
        if (firstBrace!= -1 && lastBrace!= -1) {
          cleanJson = cleanJson.substring(firstBrace, lastBrace + 1);
        }
      }
      return mapper.readValue(cleanJson, AgentCommand.class);
    } catch (Exception e) {
      return null;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class AgentCommand {
    @JsonProperty("action") public String action;
    @JsonProperty("key") public String key;
    @JsonProperty("fields") public Map<String, String> fields;
    @JsonProperty("reasoning") public String reasoning;
  }
}