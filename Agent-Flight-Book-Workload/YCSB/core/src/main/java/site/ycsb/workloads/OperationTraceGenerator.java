package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.measurements.Measurements;
import site.ycsb.agent.AgentResp;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import java.util.ArrayList;
import java.util.List;

/**
 * A workload that generates operation traces for reproducible experiments.
 * This workload generates a fixed sequence of operations and saves them to a trace file.
 */
public class OperationTraceGenerator extends Workload {

    private String traceFile;
    private PrintWriter traceWriter;
    private int operationCount;
    private int currentOperation;
    private Random random;
    private String tableName;
    private int recordCount;
    private List<String> availableKeys;
    
    @Override
    public void init(Properties p) throws WorkloadException {
        super.init(p);

        traceFile = p.getProperty("trace.file", "/tmp/operation_trace.txt");
        operationCount = Integer.parseInt(p.getProperty("operationcount", "1000"));
        tableName = p.getProperty("table", "usertable");
        recordCount = Integer.parseInt(p.getProperty("recordcount", "1000000"));

        try {
            traceWriter = new PrintWriter(new FileWriter(traceFile));
            System.err.println("OperationTraceGenerator: Writing to " + traceFile);
        } catch (IOException e) {
            throw new WorkloadException("Failed to create trace file: " + traceFile, e);
        }

        currentOperation = 0;
        random = new Random(12345); // Fixed seed for reproducibility
        availableKeys = new ArrayList<>();

        // Load existing keys from database for realistic key selection
        loadAvailableKeys();
    }
    
    @Override
    public boolean doTransaction(DB db, Object threadstate) throws SQLException {
        if (currentOperation >= operationCount) {
            return false;
        }
        
        // Generate operation based on workloadb pattern (95% read, 5% update)
        String operation;
        if (random.nextDouble() < 0.95) {
            // READ operation
            String key = generateKey();
            operation = "READ " + tableName + " " + key + " [<all fields>]";
        } else {
            // UPDATE operation
            String key = generateKey();
            String field = "field" + random.nextInt(10);
            String value = generateValue();
            operation = "UPDATE " + tableName + " " + key + " [" + field + "=" + value + "]";
        }
        
        // Write to trace file
        traceWriter.println(operation);
        traceWriter.flush();
        
        currentOperation++;
        
        // Also execute the operation on the database
        return executeOperation(db, operation);
    }
    
    @Override
    public boolean doTransaction(DB db, Object threadstate, boolean interactive_) throws SQLException {
        return doTransaction(db, threadstate);
    }
    
    @Override
    public boolean redoTransaction(DB db, Object threadstate, boolean retry, int tid, boolean interactive_) throws SQLException {
        return doTransaction(db, threadstate);
    }
    
    @Override
    public boolean redoTransactionResult(DB db, Object threadstate, boolean retry, int tid, boolean interactive_, int retryCnt_, boolean uniform_flag, boolean all_occ, boolean all_pcc) throws SQLException {
        return doTransaction(db, threadstate);
    }

  @Override
  public void switchProperty(int type) {
    return;
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
    
    @Override
    public boolean doInsert(DB db, Object threadstate) {
        return false;
    }

    private void loadAvailableKeys() throws WorkloadException {
        System.err.println("Loading available keys from database...");

        // Get database connection properties
        Properties dbProps = new Properties();
        dbProps.setProperty("db.driver", "org.postgresql.Driver");
        dbProps.setProperty("db.url", "jdbc:postgresql://127.0.0.1:16000/postgres");
        dbProps.setProperty("db.user", "jack");
        dbProps.setProperty("db.passwd", "Test@123");

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            // Load PostgreSQL driver
            Class.forName("org.postgresql.Driver");

            // Connect to database
            conn = DriverManager.getConnection(
                dbProps.getProperty("db.url"),
                dbProps.getProperty("db.user"),
                dbProps.getProperty("db.passwd")
            );

            // Query actual keys from database
            stmt = conn.createStatement();
            String query = "SELECT ycsb_key FROM usertable ORDER BY RANDOM() LIMIT " +
                          Math.min(recordCount, 10000);
            rs = stmt.executeQuery(query);

            while (rs.next()) {
                availableKeys.add(rs.getString("ycsb_key"));
            }

            System.err.println("Loaded " + availableKeys.size() + " available keys from database");

        } catch (ClassNotFoundException e) {
            throw new WorkloadException("PostgreSQL driver not found", e);
        } catch (SQLException e) {
            throw new WorkloadException("Failed to query database for keys", e);
        } catch (Exception e) {
            System.err.println("Error loading keys from database: " + e.getMessage());
            // Fallback to generated keys if database loading fails
            System.err.println("Falling back to generated keys...");
            for (int i = 0; i < 10000; i++) {
                long keyNum = 1000000000000000000L + (long)(random.nextDouble() * 9000000000000000000L);
                availableKeys.add("user" + keyNum);
            }
        } finally {
            // Clean up resources
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                System.err.println("Error closing database resources: " + e.getMessage());
            }
        }
    }

    private String generateKey() {
        // Select a random key from the available keys list
        if (availableKeys.isEmpty()) {
            // Fallback to generated key if no keys available
            long keyNum = 1000000000000000000L + (long)(random.nextDouble() * 9000000000000000000L);
            return "user" + keyNum;
        }
        return availableKeys.get(random.nextInt(availableKeys.size()));
    }
    
    private String generateValue() {
        // Generate a random string value
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append((char)('A' + random.nextInt(26)));
        }
        return sb.toString();
    }
    
    private boolean executeOperation(DB db, String operation) {
        try {
            String[] parts = operation.split("\\s+", 4);
            if (parts.length < 3) {
                return false;
            }
            
            String opType = parts[0];
            String table = parts[1];
            String key = parts[2];
            
            HashMap<String, ByteIterator> values = new HashMap<>();
            
            // Parse field values if present
            if (parts.length > 3) {
                String fieldsStr = parts[3];
                if (fieldsStr.startsWith("[") && fieldsStr.endsWith("]")) {
                    fieldsStr = fieldsStr.substring(1, fieldsStr.length() - 1);
                    if (!fieldsStr.equals("<all fields>")) {
                        String[] fieldPairs = fieldsStr.split("\\s+");
                        for (String pair : fieldPairs) {
                            if (pair.contains("=")) {
                                String[] kv = pair.split("=", 2);
                                if (kv.length == 2) {
                                    values.put(kv[0], new StringByteIterator(kv[1]));
                                }
                            }
                        }
                    }
                }
            }
            
            Status status;
            switch (opType.toUpperCase()) {
                case "READ":
                    status = db.read(table, key, null, values);
                    break;
                case "UPDATE":
                    status = db.update(table, key, values);
                    break;
                default:
                    return false;
            }
            
            return status == Status.OK;
            
        } catch (Exception e) {
            System.err.println("Error executing operation: " + operation + ", " + e.getMessage());
            return false;
        }
    }
    
    @Override
    public void cleanup() throws WorkloadException {
        if (traceWriter != null) {
            traceWriter.close();
            System.err.println("OperationTraceGenerator: Generated " + currentOperation + " operations in " + traceFile);
        }
    }
}
