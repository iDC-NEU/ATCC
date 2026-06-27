/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
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

package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.generator.*;
import site.ycsb.generator.UniformLongGenerator;
import site.ycsb.generator.ZipfianFast;
import site.ycsb.measurements.Measurements;
import site.ycsb.agent.AgentResp;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The
 * relative proportion of different kinds of operations, and other properties of the workload,
 * are controlled by parameters specified at runtime.
 * <p>
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>minfieldlength</b>: the minimum size of each field (default: 1)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just
 * one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record,
 * modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate
 * on - uniform, zipfian, hotspot, sequential, exponential or latest (default: uniform)
 * <LI><b>minscanlength</b>: for scans, what is the minimum number of records to scan (default: 1)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the
 * number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertstart</b>: for parallel loads and runs, defines the starting record for this
 * YCSB instance (default: 0)
 * <LI><b>insertcount</b>: for parallel loads and runs, defines the number of records for this
 * YCSB instance (default: recordcount)
 * <LI><b>zeropadding</b>: for generating a record sequence compatible with string sort order by
 * 0 padding the record number. Controls the number of 0s to use for padding. (default: 1)
 * For example for row 5, with zeropadding=1 you get 'user5' key and with zeropading=8 you get
 * 'user00000005' key. In order to see its impact, zeropadding needs to be bigger than number of
 * digits in the record number.
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed
 * order ("hashed") (default: hashed)
 * <LI><b>fieldnameprefix</b>: what should be a prefix for field names, the shorter may decrease the
 * required storage size (default: "field")
 * </ul>
 */
public class CoreWorkload extends Workload {
  /**
   * The name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY = "table";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

  protected String table;

  /**
   * The name of the property for the number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /**
   * Default number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  private List<String> fieldnames;

  /**
   * The name of the property for the field length distribution. Options are "uniform", "zipfian"
   * (favouring short records), "constant", and "histogram".
   * <p>
   * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the
   * fieldlength property. If "histogram", then the histogram will be read from the filename
   * specified in the "fieldlengthhistogram" property.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

  /**
   * The default field length distribution.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

  /**
   * The name of the property for the length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY = "fieldlength";

  /**
   * The default maximum length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

  /**
   * The name of the property for the minimum length of a field in bytes.
   */
  public static final String MIN_FIELD_LENGTH_PROPERTY = "minfieldlength";

  /**
   * The default minimum length of a field in bytes.
   */
  public static final String MIN_FIELD_LENGTH_PROPERTY_DEFAULT = "1";

  /**
   * The name of a property that specifies the filename containing the field length histogram (only
   * used if fieldlengthdistribution is "histogram").
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";

  /**
   * The default filename containing a field length histogram.
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

  // add by Cui
  public static final String OPERATIONS_PER_TRANSACTION = "opspertrans";

  // add by PQ
  public static final String CONTENTION_LEVEL = "contention_level";

  public static final String SLEEP_PER_OPS = "sleep_per_ops";
  public static final String MIN_SLEEP_PER_OPS = "min_sleep_per_ops";
  public static final String MAX_SLEEP_PER_OPS = "max_sleep_per_ops";
  // add by wzy
  public static int total_counter = 0;

  public static ConcurrentHashMap<Integer, Integer> write_set_size_map = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<Integer, List<Integer>> ops_set_map = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<Integer, List<String>> record_table = new ConcurrentHashMap<>();    // no use
  public static ConcurrentHashMap<Integer, List<String>> record_key = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<Integer, List<HashMap<String, ByteIterator>>> record_values = new ConcurrentHashMap<>();

  public static ConcurrentHashMap<Integer, List<String>> read_key = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<Integer, List<HashSet<String>>> record_fieds = new ConcurrentHashMap<>();


  // wzy
  public int ops_set_size_ = 0;
  public List<Integer> ops_container = new ArrayList<>();   // 1 update, 0 read
  public List<String> write_record_key = new ArrayList<>();
  public List<HashMap<String, ByteIterator>> write_record_values = new ArrayList<>();

  public List<String> read_record_key_name = new ArrayList<>();
  public List<HashSet<String>> read_fields = new ArrayList<>();

  /**
   * Generator object that produces field lengths.  The value of this depends on the properties that
   * start with "FIELD_LENGTH_".
   */
  protected NumberGenerator fieldlengthgenerator;

  /**
   * The name of the property for deciding whether to read one field (false) or all fields (true) of
   * a record.
   */
  public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

  /**
   * The default value for the readallfields property.
   */
  public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

  protected boolean readallfields;

  /**
   * The name of the property for deciding whether to write one field (false) or all fields (true)
   * of a record.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

  /**
   * The default value for the writeallfields property.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

  protected boolean writeallfields;

  /**
   * The name of the property for deciding whether to check all returned
   * data against the formation template to ensure data integrity.
   */
  public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";

  /**
   * The default value for the dataintegrity property.
   */
  public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "false";

  /**
   * Set to true if want to check correctness of reads. Must also
   * be set to true during loading phase to function.
   */
  private boolean dataintegrity;

  /**
   * The name of the property for the proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY = "readproportion";

  /**
   * The default proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

  /**
   * The name of the property for the proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

  /**
   * The default proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

  /**
   * The name of the property for the proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

  /**
   * The default proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

  /**
   * The default proportion of transactions that are scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are read-modify-write.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

  /**
   * The default proportion of transactions that are scans.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the the distribution of requests across the keyspace. Options are
   * "uniform", "zipfian" and "latest"
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

  /**
   * The default distribution of requests across the keyspace.
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for adding zero padding to record numbers in order to match
   * string sort order. Controls the number of 0s to left pad with.
   */
  public static final String ZERO_PADDING_PROPERTY = "zeropadding";

  /**
   * The default zero padding value. Matches integer sort order
   */
  public static final String ZERO_PADDING_PROPERTY_DEFAULT = "1";


  /**
   * The name of the property for the min scan length (number of records).
   */
  public static final String MIN_SCAN_LENGTH_PROPERTY = "minscanlength";

  /**
   * The default min scan length.
   */
  public static final String MIN_SCAN_LENGTH_PROPERTY_DEFAULT = "1";

  /**
   * The name of the property for the max scan length (number of records).
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

  /**
   * The default max scan length.
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";

  /**
   * The name of the property for the scan length distribution. Options are "uniform" and "zipfian"
   * (favoring short scans)
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

  /**
   * The default max scan length.
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for the order to insert records. Options are "ordered" or "hashed"
   */
  public static final String INSERT_ORDER_PROPERTY = "insertorder";

  /**
   * Default insert order.
   */
  public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

  /**
   * Percentage data items that constitute the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

  /**
   * Default value of the size of the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

  /**
   * Percentage operations that access the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

  /**
   * Default value of the percentage operations accessing the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

  /**
   * How many times to retry when insertion of a single item to a DB fails.
   */
  public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
  public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";

  /**
   * On average, how long to wait between the retries, in seconds.
   */
  public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
  public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";

  /**
   * Field name prefix.
   */
  public static final String FIELD_NAME_PREFIX = "fieldnameprefix";

  /**
   * Default value of the field name prefix.
   */
  public static final String FIELD_NAME_PREFIX_DEFAULT = "field";

  protected NumberGenerator keysequence;
  protected DiscreteGenerator operationchooser;
  protected NumberGenerator keychooser;
  protected NumberGenerator uniform_keychooser;       // wzy: uniform
  protected ZipfianFast zipfian_chooser;
  protected NumberGenerator fieldchooser;
  protected AcknowledgedCounterGenerator transactioninsertkeysequence;
  protected NumberGenerator scanlength;
  protected boolean orderedinserts;
  protected long fieldcount;
  protected long recordcount;
  protected int zeropadding;
  protected int insertionRetryLimit;
  protected int insertionRetryInterval;

  protected int txnRetryCnt = 0;
  protected int retryCnt = 0;

  public int maxPriority = 3;
  public int maxRetryCnt = 100;
  public int levelRetryCount = 5;


  /// add by Cui
  protected int opsPerTrans = 1;

  // wzy: 记录已有操作次数
  protected int counter1 = 0;
  protected int sleepPerOps = 10;
  protected int minMs = 0;
  protected int maxMs = 10;

    // add by PQ 
  public static  double contention_level = 0.99;

  private Measurements measurements = Measurements.getMeasurements();

  protected static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
    NumberGenerator fieldlengthgenerator;
    String fieldlengthdistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
    int fieldlength = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
    int minfieldlength = Integer.parseInt(p.getProperty(MIN_FIELD_LENGTH_PROPERTY, MIN_FIELD_LENGTH_PROPERTY_DEFAULT));
    String fieldlengthhistogram = p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
    if (fieldlengthdistribution.compareTo("constant") == 0) {
      fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
    } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
      fieldlengthgenerator = new UniformLongGenerator(minfieldlength, fieldlength);
    } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
            //add by PQ
      // System.out.println("1  coreWorkload is initing, opsPerTrans = " + this->opsPerTrans + " contention level = " + this->contention_level);
      fieldlengthgenerator = new ZipfianGenerator(minfieldlength, fieldlength, contention_level);
    } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
      try {
        fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
      } catch (IOException e) {
        throw new WorkloadException("Couldn't read field length histogram file: " + fieldlengthhistogram, e);
      }
    } else {
      throw new WorkloadException("Unknown field length distribution \"" + fieldlengthdistribution + "\"");
    }
    return fieldlengthgenerator;
  }

  // wzy: 初始化元信息
  public void init_map() {
    // add by wzy
    System.out.println("1 coreWorkload map is initing");
    txnRetryCnt = 0;
    retryCnt = 0;
    counter1 = 0;

    write_set_size_map = new ConcurrentHashMap<>();
    record_table = new ConcurrentHashMap<>();
    record_key = new ConcurrentHashMap<>();
    read_key = new ConcurrentHashMap<>();
    record_values = new ConcurrentHashMap<>();

  }

  // wzy: 初始化存储过程
  public void init_stored_procedure(DB db, int ops) {
    try {
      db.createStoredProcedure(ops);
    } catch (SQLException e) {
      System.err.println("Create stored procedure failed because " + e.getMessage());
    }
  }

  public void setMaxPriority(int maxPriority1) {
    this.maxPriority = maxPriority1;
  }

  public void setMaxRetryCnt(int maxRetryCnt1) {
    this.maxRetryCnt = maxRetryCnt1;
  }

  public void setLevelRetryCnt(int levelRetryCount1) {
    this.levelRetryCount = levelRetryCount1;
  }

  public void switchProperty(int type) {
    double contention_level = 0.7;
    double read_proportion = 0.9;
    double write_proportion = 0.1;
    switch (type) {
      case 0:
        contention_level = 0.3;
        read_proportion = 0.95;
        write_proportion = 0.05;
        break;
      case 1:
        contention_level = 0.7;
        read_proportion = 0.9;
        write_proportion = 0.1;
        break;
      case 2:
        contention_level = 0.99;
        read_proportion = 0.5;
        write_proportion = 0.5;
        break;
      default:
        contention_level = 0.7;
    }
    // 重新设置zipfian 读写比例
    ZipfianFast zipfian_chooser1 = new ZipfianFast(contention_level);
    zipfian_chooser1.calculateDenom(recordcount);
    operationchooser = regenerateOperationGenerator(read_proportion, write_proportion);
    zipfian_chooser = zipfian_chooser1;
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
  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {

    // add by Cui
    this.opsPerTrans = Integer.parseInt(p.getOrDefault(OPERATIONS_PER_TRANSACTION, "1").toString());

    // add by wzy
    this.minMs = Integer.parseInt(p.getOrDefault(MIN_SLEEP_PER_OPS, "10").toString());
    this.maxMs = Integer.parseInt(p.getOrDefault(MAX_SLEEP_PER_OPS, "10").toString());

    // add by PQ
    contention_level = Double.parseDouble(p.getOrDefault(CONTENTION_LEVEL, "0.99").toString());

    System.out.println("2  coreWorkload is initing, opsPerTrans = " + this.opsPerTrans + " contention level = " + contention_level);


    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    fieldcount = Long.parseLong(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    final String fieldnameprefix = p.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
    fieldnames = new ArrayList<>();
    for (int i = 0; i < fieldcount; i++) {
      fieldnames.add(fieldnameprefix + i);
    }
    fieldlengthgenerator = CoreWorkload.getFieldLengthGenerator(p);

    recordcount = Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
    int minscanlength = Integer.parseInt(p.getProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_PROPERTY_DEFAULT));
    int maxscanlength = Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
    String scanlengthdistrib = p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    long insertstart = Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    long insertcount = Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
    // Confirm valid values for insertstart and insertcount in relation to recordcount
    if (recordcount < (insertstart + insertcount)) {
      System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
      System.err.println("recordcount must be bigger than insertstart + insertcount.");
      System.exit(-1);
    }
    zeropadding = Integer.parseInt(p.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));

    readallfields = Boolean.parseBoolean(p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
    writeallfields = Boolean.parseBoolean(p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

    dataintegrity = Boolean.parseBoolean(p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));
    // Confirm that fieldlengthgenerator returns a constant if data
    // integrity check requested.
    if (dataintegrity && !(p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant")) {
      System.err.println("Must have constant field size to check data integrity.");
      System.exit(-1);
    }

    if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
      orderedinserts = false;
    } else {
      orderedinserts = true;
    }

    keysequence = new CounterGenerator(insertstart);
    operationchooser = createOperationGenerator(p);

    transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount);
    if (requestdistrib.compareTo("uniform") == 0) {
      keychooser = new UniformLongGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY, ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY, ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keychooser = new ExponentialGenerator(percentile, recordcount * frac);
    } else if (requestdistrib.compareTo("sequential") == 0) {
      keychooser = new SequentialGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("zipfian") == 0) {
      // it does this by generating a random "next key" in part by taking the modulus over the
      // number of keys.
      // If the number of keys changes, this would shift the modulus, and we don't want that to
      // change which keys are popular so we'll actually construct the scrambled zipfian generator
      // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
      // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
      // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
      // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
      // the keyspace doesn't change from the perspective of the scrambled zipfian generator
      final double insertproportion = Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
      int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
      int expectednewkeys = (int) ((opcount) * insertproportion * 2.0); // 2 is fudge factor

      keychooser = new ScrambledZipfianGenerator(insertstart, insertstart + insertcount + expectednewkeys);
    } else if (requestdistrib.compareTo("latest") == 0) {
      keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
    } else if (requestdistrib.equals("hotspot")) {
      double hotsetfraction = Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction = Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(insertstart, insertstart + insertcount - 1, hotsetfraction, hotopnfraction);
    } else {
      throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
    }

    // wzy:
    uniform_keychooser = new UniformLongGenerator(insertstart, insertstart + insertcount - 1);
    zipfian_chooser = new ZipfianFast(contention_level);
    zipfian_chooser.calculateDenom(recordcount);

    fieldchooser = new UniformLongGenerator(0, fieldcount - 1);

    if (scanlengthdistrib.compareTo("uniform") == 0) {
      scanlength = new UniformLongGenerator(minscanlength, maxscanlength);
    } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
      scanlength = new ZipfianGenerator(minscanlength, maxscanlength);
    } else {
      throw new WorkloadException("Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
    }

    insertionRetryLimit = Integer.parseInt(p.getProperty(INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));
  }

  protected String buildKeyName(long keynum) {
    if (!orderedinserts) {
      keynum = Utils.hash(keynum);
    }
    String value = Long.toString(keynum);
    int fill = zeropadding - value.length();
    String prekey = "user";
    for (int i = 0; i < fill; i++) {
      prekey += '0';
    }
    return prekey + value;
  }

  /**
   * Builds a value for a randomly chosen field.
   */
  private HashMap<String, ByteIterator> buildSingleValue(String key) {
    HashMap<String, ByteIterator> value = new HashMap<>();

    String fieldkey = fieldnames.get(fieldchooser.nextValue().intValue());
    ByteIterator data;
    if (dataintegrity) {
      data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
    } else {
      // fill with random data
      data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
    }
    value.put(fieldkey, data);

    return value;
  }

  /**
   * Builds values for all fields.
   */
  private HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<>();

    for (String fieldkey : fieldnames) {
      ByteIterator data;
      if (dataintegrity) {
        data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
      } else {
        // fill with random data
        data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
      }
      values.put(fieldkey, data);
    }
    return values;
  }

  /**
   * Build a deterministic value given the key information.
   */
  private String buildDeterministicValue(String key, String fieldkey) {
    int size = fieldlengthgenerator.nextValue().intValue();
    StringBuilder sb = new StringBuilder(size);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);

    return sb.toString();
  }

  /**
   * Do one insert operation. Because it will be called concurrently from multiple client threads,
   * this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    String dbkey = buildKeyName(keynum);
    HashMap<String, ByteIterator> values = buildValues(dbkey);

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, dbkey, values);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting status: " + status  +", not retrying any more. number of attempts: " + numOfRetries + "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return null != status && status.isOk();
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) throws SQLException {

    int counter = this.opsPerTrans;

    try {
      while (counter > 0) {

        // begin transaction
        if (counter == this.opsPerTrans && this.opsPerTrans > 1) {
          // TODO begin transaction
          doTransactionBegin(db);
        }

        String operation = operationchooser.nextString();
        if (operation == null) {
          return false;
        }

        switch (operation) {
          case "READ":
            doTransactionRead(db);
            break;
          case "UPDATE":
            doTransactionUpdate(db);
            break;
          case "INSERT":
            doTransactionInsert(db);
            break;
          case "SCAN":
            doTransactionScan(db);
            break;
          default:
            doTransactionReadModifyWrite(db);
        }

        counter--;
      }
    } catch (SQLException e) {
      if (this.opsPerTrans > 1) {
        // TODO rollback transaction
        doTransactionRollback(db);
      }
      System.err.println("[Abort] and doTransactionRollback");            // wzy
      return false;
    }

    if (this.opsPerTrans > 1) {
      // TODO commit transaction
//      total_counter++;
      doTransactionCommit(db);
    }
    return true;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate, boolean interactive_) throws SQLException {

    int counter = this.opsPerTrans;

    try {
      while (counter > 0) {

        // begin transaction
        if (counter == this.opsPerTrans && this.opsPerTrans > 1) {
          // TODO begin transaction
          if (interactive_) doInteractiveTransactionBegin(db);
          else doTransactionBegin(db);
        }

        String operation = operationchooser.nextString();
        if (operation == null) {
          return false;
        }

        switch (operation) {
          case "READ":
            doTransactionRead(db, 0);
            break;
          case "UPDATE":
            doTransactionUpdate(db, 0);
            break;
          case "INSERT":
            doTransactionInsert(db);
            break;
          case "SCAN":
            doTransactionScan(db);
            break;
          default:
            doTransactionReadModifyWrite(db);
        }

        counter--;
      }
    } catch (SQLException e) {
      if (this.opsPerTrans > 1) {
        // TODO rollback transaction
//        doTransactionRollback(db);
        if (interactive_) doInteractiveTransactionRollback(db);      // wzy: 是否需要自己写rollback?
        else doTransactionRollback(db);
      }
//      System.err.println("[Abort] and doTransactionRollback");            // wzy
      throw new SQLException("abort while executing");
    }

    if (this.opsPerTrans > 1) {
      // TODO commit transaction
      if (interactive_) doInteractiveTransactionCommit(db);
      else doTransactionCommit(db);
    }
    return true;
  }



    // add by wzy
    @Override
    public boolean redoTransaction(DB db, Object threadstate, boolean retry, int tid, boolean interactive_) throws SQLException{
      int counter = this.opsPerTrans;
      boolean result = true;

      // 测试！rollback
//      try {
//        doInteractiveTransactionBegin(db);
//        result = doTransactionReadResult(db, tid, result);
//        Thread.sleep(5000);
//        result = doTransactionUpdateResult(db, tid, result);
//        result = doTransactionUpdateResult(db, tid, result);
//        result = doTransactionUpdateResult(db, tid, result);
//      } catch (SQLException e) {
//        result = false;
//        System.err.println("[SQLException e] tid = " + tid + " rollback" + e.toString());
//        doInteractiveTransactionRollback(db);
//      } catch (Exception e) {
//        System.err.println("[Exception e] tid = " + tid + " rollback" + e.toString());
//      }
//
//      try {
//        if (result) {
//          System.err.println("[Interactive commit] tid = " + tid + " commit" + " doTransactionCommit write_set_size = " + write_set_size_map.get(tid) );
//          doInteractiveTransactionCommit(db);
//        } else {
//          System.err.println("[Interactive rollback] tid = " + tid + " rollback" + " doTransactionCommit write_set_size = " + write_set_size_map.get(tid) );
//          doInteractiveTransactionRollback(db);
//        }
//      } catch (SQLException e) {
//
//      }
//      if (true) return true;

      /////////////////////////////////////////

      // 初次运行
      if (!retry) {
        try {
          while (counter > 0) {
            // begin transaction
            if (counter == this.opsPerTrans && this.opsPerTrans > 1) {
              if (interactive_) {
//                System.err.println("[Interactive] tid = " + tid + " begin counter = " + counter);
                doInteractiveTransactionBegin(db);
              } else doTransactionBegin(db);
            }
            String operation = operationchooser.nextString();
            if (operation == null) {
              return false;
            }

            switch (operation) {
              case "READ":
//                System.err.println("read counter = " + counter);
                doTransactionRead(db, tid);
                break;
              case "UPDATE":
//                System.err.println("update counter = " + counter);
                doTransactionUpdate(db, tid);
                break;
              case "INSERT":
                doTransactionInsert(db);
                break;
              case "SCAN":
                doTransactionScan(db);
                break;
              default:
                doTransactionReadModifyWrite(db);
            }
            counter--;
          }
        } catch (SQLException e) {
//          System.err.println("[Rollback] tid = " + tid + " : " + e + " counter = " + counter + " txnRetryCnt = " + txnRetryCnt + " doTransactionCommit write_set_size = " + write_set_size_map.get(tid));
          try {
            if (this.opsPerTrans > 1) {
              if (interactive_) doInteractiveTransactionRollback(db);      // wzy: 是否需要自己写rollback
              else doTransactionRollback(db);
            }
          } catch (SQLException e2) {
            // do nothing
          }
          counter--;
          counter1 = counter;     // 记录已生成的 counter
          return false;
        }
      } else {
        // retry logic 重做超过 99次则直接 failed

        txnRetryCnt++;
        if (txnRetryCnt > 0 && txnRetryCnt % 20 == 0)
          System.err.println("[Abort] tid = " + tid + " interactive : " + interactive_ + " : txnRetryCnt = " + txnRetryCnt + " doTransactionCommit write_set_size = " + write_set_size_map.get(tid) + " counter1 = " + counter1);
        if (txnRetryCnt > 99) {
          System.err.println("[Abort over 99] tid = " + tid + " interactive : " + interactive_ + " : doTransactionCommit write_set_size = " + write_set_size_map.get(tid) + " counter1 = " + counter1);
          txnRetryCnt = 0;
          write_set_size_map.remove(tid);
          record_table.remove(tid);
          record_key.remove(tid);
          record_values.remove(tid);
          read_key.remove(tid);
          throw new SQLException("Abort over 99");
        }

        try {
          if (interactive_) {
//            System.err.println("[Interactive] tid = " + tid + " begin counter = " + counter);
            doInteractiveTransactionBegin(db);
          }
          else doTransactionBegin(db);
          redoTransactionUpdate(db, tid);
//          if (interactive_) doRemainedOps(db, threadstate, retry, tid, interactive_);
        } catch (SQLException e) {
//          System.err.println("[Abort] tid = " + tid + " interactive : " + interactive_ + " : " + e +  " : txnRetryCnt = " + txnRetryCnt + " doTransactionException write_set_size = " + write_set_size_map.get(tid));
//          System.err.println("[Rollback] tid = " + tid + " : " + e + " counter = " + counter + " txnRetryCnt = " + txnRetryCnt + " doTransactionCommit write_set_size = " + write_set_size_map.get(tid));
          try {
            if (this.opsPerTrans > 1) {
              if (interactive_) doInteractiveTransactionRollback(db);      // wzy: 是否需要自己写rollback?
              else doTransactionRollback(db);
            }
          } catch (SQLException e2) {
            // do nothing
          }
          counter1--;
          return false;
        }
      }

      if (this.opsPerTrans > 1) {
        // commit transaction
        total_counter++;
        try {
//          System.err.println("[Commit] tid = " + tid + " interactive : " + interactive_ + " : txnRetryCnt = " + txnRetryCnt + " doTransactionCommit write_set_size = " + write_set_size_map.get(tid));
          if (interactive_) doInteractiveTransactionCommit(db);
          else doTransactionCommit(db);
        } catch (SQLException e) {
//          if (interactive_)
//            System.err.println("[Interactive rollback] tid = " + tid + " rollback txnRetryCnt = " + txnRetryCnt);
//          else
//            System.err.println("[StoredProcesedure rollback] tid = " + tid + " rollback txnRetryCnt = " + txnRetryCnt);
//          if (txnRetryCnt > 0 && txnRetryCnt % 100 == 0 && interactive_)
//          if (interactive_) System.err.println("[Interactive Abort] tid = " + tid + " interactive : " + interactive_ + " : " + e +  " : txnRetryCnt = " + txnRetryCnt + " doTransactionCommit write_set_size = " + write_set_size_map.get(tid));
          if (txnRetryCnt > 0 && txnRetryCnt % 10 == 0)
            System.err.println("[Abort] tid = " + tid + " interactive : " + interactive_ + " : " + e +  " : txnRetryCnt = " + txnRetryCnt + " doTransactionCommit write_set_size = " + write_set_size_map.get(tid));
          if (txnRetryCnt > 99) {
            System.err.println("[Abort over 99] tid = " + tid + " interactive : " + interactive_ + " : " + e + " doTransactionCommit write_set_size = " + write_set_size_map.get(tid));
            txnRetryCnt = 0;
            counter1 = 0;
            write_set_size_map.remove(tid);
            record_table.remove(tid);
            record_key.remove(tid);
            record_values.remove(tid);
            read_key.remove(tid);
            throw new SQLException("Abort over 99");
          }
          return false;
        }
      }

//      System.err.println("[Commit success] tid = " + tid + " interactive : " + interactive_ + " : txnRetryCnt = " + txnRetryCnt + " doTransactionCommit write_set_size = " + write_set_size_map.get(tid));
      txnRetryCnt = 0;
      counter1 = 0;
      write_set_size_map.remove(tid);
      record_table.remove(tid);
      record_key.remove(tid);
      record_values.remove(tid);
      read_key.remove(tid);
      return true;
  }

  private void cleanTxnStates(int tid) {
    txnRetryCnt = 0;
    retryCnt = 0;
    write_set_size_map.remove(tid);
    ops_set_map.remove(tid);
    record_table.remove(tid);
    record_key.remove(tid);
    record_values.remove(tid);
    read_key.remove(tid);
    record_fieds.remove(tid);


    ops_set_size_ = 0;
    ops_container.clear();
    write_record_key.clear();
    write_record_values.clear();
    read_record_key_name.clear();
    read_fields.clear();
  }

  // add by wzy 生成读写集后统一
  @Override
  public boolean redoTransactionResult(DB db, Object threadstate, boolean retry, int tid, boolean interactive_, int retryCnt_, boolean uniform_flag, boolean all_occ, boolean all_pcc) throws SQLException{
    int counter = this.opsPerTrans;
    boolean result = true;
    // TODO: 设置重做优先级上限？
    // txnRetryCnt是共享的，coreWorkload是无状态的
    int retryCnt1 = Math.min(retryCnt_ / levelRetryCount, maxPriority);
    long st = 0, ed = 0;
    // 初次运行
    if (!retry) {
      try {
        while (counter > 0) {
          // begin transaction
          if (counter == this.opsPerTrans && this.opsPerTrans > 1) {
            if (interactive_ && !uniform_flag && !all_occ) doInteractiveTransactionBegin(db, retryCnt1);
            else doTransactionBegin(db);
          }
          String operation = operationchooser.nextString();
          if (operation == null) {
            return false;
          }
          if (interactive_) {
            try {
              Thread.sleep(ThreadLocalRandom.current().nextInt(this.minMs, this.maxMs + 1));
            } catch (Exception e) {
              // .. do nothing
            }
          }
          st = System.nanoTime();
          switch (operation) {
            case "READ":
              result = doTransactionReadResult(db, tid, result, uniform_flag);
              break;
            case "UPDATE":
              result = doTransactionUpdateResult(db, tid, result, uniform_flag);
              break;
            case "INSERT":
              doTransactionInsert(db);
              break;
            case "SCAN":
              doTransactionScan(db);
              break;
            default:
              doTransactionReadModifyWrite(db);
          }
          ed = System.nanoTime();
          //System.err.println("[DEBUG] tid = " + tid + " operation [" + counter + "] = " + operation + " cost = " + (ed - st) / 1000);
          counter--;
        }
      } catch (SQLException e) {
        result = false;
        System.err.println("[SQLException e] tid = " + tid + " rollback" + e.toString());
      }
      catch (Exception e) {
        System.err.println("[Exception e] tid = " + tid + " rollback" + e.toString());
      }
    } else {
      // retry logic 重做超过 maxRetryCnt 次则直接 failed
      if (retryCnt_ >= maxRetryCnt) {
        System.err.println("[Abort over " + maxRetryCnt + " ] tid = " + tid + " interactive : " + interactive_ + " : doTransactionCommit write_set_size = " + write_set_size_map.get(tid) + " retryCnt:" + retryCnt_);
        cleanTxnStates(tid);
        throw new SQLException("Abort over maxRetryCnt");
      }

      try {
        if (interactive_ && !uniform_flag && !all_occ) doInteractiveTransactionBegin(db, retryCnt1);
        else doTransactionBegin(db);
        result = redoTransactionUpdateResult(db, tid, interactive_);
//        result = redoTransactionOpsResult(db, tid);
      } catch (SQLException e) {
        result = false;
        System.err.println("[SQLException e] tid = " + tid + " rollback" + e.toString());
        doInteractiveTransactionRollback(db);
        return false;
      }
    }

    if (this.opsPerTrans > 1) {
      // commit transaction
      total_counter++;
      try {
        if (result) {
          st = System.nanoTime();
          if (interactive_ && !uniform_flag && !all_occ) doInteractiveTransactionCommit(db);
          else doTransactionCommit(db);
          ed = System.nanoTime();
          //System.err.println("[DEBUG] tid = " + tid + " commit cost = " + (ed - st) / 1000);
        } else {
          if (interactive_ && !uniform_flag && !all_occ) doInteractiveTransactionRollback(db);
          else doTransactionRollback(db);
          return false;
        }
      } catch (SQLException e) {
        return false;
      }
    }

    cleanTxnStates(tid);
    return true;
  }


  /**
   * Results are reported in the first three buckets of the histogram under
   * the label "VERIFY".
   * Bucket 0 means the expected data was returned.
   * Bucket 1 means incorrect data was returned.
   * Bucket 2 means null data was returned when some data was expected.
   */
  protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
    Status verifyStatus = Status.OK;
    long startTime = System.nanoTime();
    if (!cells.isEmpty()) {
      for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
        if (!entry.getValue().toString().equals(buildDeterministicValue(key, entry.getKey()))) {
          verifyStatus = Status.UNEXPECTED_STATE;
          break;
        }
      }
    } else {
      // This assumes that null data is never valid
      verifyStatus = Status.ERROR;
    }
    long endTime = System.nanoTime();
    measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
    measurements.reportStatus("VERIFY", verifyStatus);
  }

  long nextKeynum() {
//    long keynum;
//    if (keychooser instanceof ExponentialGenerator) {
//      do {
//        keynum = transactioninsertkeysequence.lastValue() - keychooser.nextValue().intValue();
//      } while (keynum < 0);
//    } else {
//      do {
//        keynum = keychooser.nextValue().intValue();
//      } while (keynum > transactioninsertkeysequence.lastValue());
//    }
//    return keynum;
    return zipfian_chooser.zipf();
  }

  // wzy:
  long uniformNextKeynum() {
    long keynum;
    if (uniform_keychooser instanceof ExponentialGenerator) {
      do {
        keynum = transactioninsertkeysequence.lastValue() - uniform_keychooser.nextValue().intValue();
      } while (keynum < 0);
    } else {
      do {
        keynum = uniform_keychooser.nextValue().intValue();
      } while (keynum > transactioninsertkeysequence.lastValue());
    }
    return keynum;
  }

  public void doTransactionBegin(DB db) throws SQLException {
    // do transaction begin
//    System.out.println("ready to begin");
//    System.out.println("db = " + db);
    db.beginTransaction();
  }

  public void doInteractiveTransactionBegin(DB db) throws SQLException {
    // do transaction begin
//    System.out.println("ready to begin");
//    System.out.println("db = " + db);
    db.beginInteractiveTransaction(retryCnt);
  }

  public void doInteractiveTransactionBegin(DB db, int retryCnt_) throws SQLException {
    // do transaction begin
    db.beginInteractiveTransaction(retryCnt_);
  }

  public void doTransactionCommit(DB db) throws SQLException {
    // do transaction commit
//    System.out.println("ready to commit");
//    System.out.println("db = " + db);
    db.commitTransaction();
  }

  // wzy 提交交互性事务
  public void doInteractiveTransactionCommit(DB db) throws SQLException {
    // do transaction commit
//    System.out.println("ready to commit");
//    System.out.println("db = " + db);
    db.commitInteractiveTransaction();
  }

  public void doTransactionRollback(DB db) throws SQLException {
    // do transaction abort/rollback
//    System.out.println("ready to rollback");
//    System.out.println("db = " + db);
    db.rollbackTransaction();
  }

  public void doInteractiveTransactionRollback(DB db) throws SQLException {
    // do transaction abort/rollback
//    System.out.println("ready to rollback");
//    System.out.println("db = " + db);
    db.rollbackInteractiveTransaction();
  }

  public void doTransactionRead(DB db) {
//    System.err.println("read");
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
      fields = new HashSet<String>();
      fields.add(fieldname);
    } else if (dataintegrity) {
      // pass the full field list if dataintegrity is on for verification
      fields = new HashSet<String>(fieldnames);
    }
    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    db.read(table, keyname, fields, cells);

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }
  }

  public boolean doTransactionReadResult(DB db, int tid, boolean res, boolean uniform_flag){
    // choose a random key
    // TODO: choose a uniform key
    long keynum = 0L;
    if (uniform_flag) keynum = uniformNextKeynum();
    else keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
      fields = new HashSet<String>();
      fields.add(fieldname);
    } else if (dataintegrity) {
      // pass the full field list if dataintegrity is on for verification
      fields = new HashSet<String>(fieldnames);
    }

    try {
      Integer currentSize = write_set_size_map.get(tid);
      if (currentSize == null) {
        write_set_size_map.put(tid, 0); // 如果tid不存在，初始化为 1
      } else {
        write_set_size_map.put(tid, currentSize + 1); // 如果tid存在，累加
      }
      ops_set_map.computeIfAbsent(tid, k -> new ArrayList<>()).add(2);    // 操作标识
      read_key.computeIfAbsent(tid, k -> new ArrayList<>()).add(keyname);
      record_fieds.computeIfAbsent(tid, k -> new ArrayList<>()).add(fields);
    } catch (Exception e) {
      System.out.println("Initialize failed for tid: " + tid + "write_set_size_ : " + write_set_size_map.get(tid) + " : " + e.toString());
    }

    if (!res) return res;

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    Status s = db.read(table, keyname, fields, cells);
    if (dataintegrity) {
      verifyRow(keyname, cells);
    }
    return s == Status.OK;
  }

  public void doTransactionRead(DB db, int tid) throws SQLException {
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
      fields = new HashSet<String>();
      fields.add(fieldname);
    } else if (dataintegrity) {
      // pass the full field list if dataintegrity is on for verification
      fields = new HashSet<String>(fieldnames);
    }


    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    Status s = db.read(table, keyname, fields, cells);

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }
    if (s != Status.OK) throw new SQLException("read failed");
  }

  public void doTransactionReadModifyWrite(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    // do the transaction

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();


    long ist = measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    db.read(table, keyname, fields, cells);

    db.update(table, keyname, values);

    long en = System.nanoTime();

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }

    measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
    measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
  }

  public void doTransactionScan(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String startkeyname = buildKeyName(keynum);

    // choose a random scan length
    int len = scanlength.nextValue().intValue();

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    db.scan(table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>());
  }

  public void doTransactionUpdate(DB db) {
//    System.err.println("update1");
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    db.update(table, keyname, values);
  }

  // wzy
  public boolean doTransactionUpdateResult(DB db, int tid, boolean res, boolean uniform_flag){
    // choose a random key
    long keynum = 0L;
    if (uniform_flag) keynum = uniformNextKeynum();
    else keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    try {
      Integer currentSize = write_set_size_map.get(tid);
      if (currentSize == null) {
        write_set_size_map.put(tid, 0); // 如果tid不存在，初始化为 1
      } else {
        write_set_size_map.put(tid, currentSize + 1); // 如果tid存在，累加
      }
      ops_set_map.computeIfAbsent(tid, k -> new ArrayList<>()).add(1);    // 操作标识
      record_key.computeIfAbsent(tid, k -> new ArrayList<>()).add(keyname);
      record_values.computeIfAbsent(tid, k -> new ArrayList<>()).add(values);
    } catch (Exception e) {
      System.out.println("Initialize failed for tid: " + tid + "write_set_size_ : " + write_set_size_map.get(tid) + " : " + e.toString());
    }

    // List存放
//    ops_set_size_++;
//    ops_container.add(1);
//    write_record_key.add(keyname);
//    write_record_values.add(values);

    if (!res) return res;   // 对于已经失败的事务，还会添加到写集

    Status s = db.update(table, keyname, values);      // wzy: 为什么会卡住？
    return s == Status.OK;
//    if (s != Status.OK) throw new SQLException("update failed");
  }

  // wzy
  public void doTransactionUpdate(DB db, int tid) throws SQLException{
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    Status s = db.update(table, keyname, values);      // wzy: 为什么会卡住？
    if (s != Status.OK) throw new SQLException("update failed");
  }

  // wzy
  public void redoTransactionUpdate(DB db, int tid) throws SQLException{
    if (record_table.get(tid) == null) return;
    List<String> temp_table = record_table.get(tid);
    List<String> keyname = record_key.get(tid);
    List<HashMap<String, ByteIterator>> values = record_values.get(tid);

    for (int i = 0; i < temp_table.size(); i++) {
      Status s = db.update(temp_table.get(i), keyname.get(i), values.get(i));
      if (s != Status.OK) throw new SQLException("redo update failed");
    }
  }

  // wzy
  public boolean redoTransactionUpdateResult(DB db, int tid, boolean interactive_) throws SQLException{
    Integer size = write_set_size_map.get(tid);
    if (size == null) return true;

    List<Integer> ops_set = ops_set_map.get(tid);
    List<String> write_keyname = record_key.get(tid);
    List<HashMap<String, ByteIterator>> write_values = record_values.get(tid);
    List<String> read_keyname = read_key.get(tid);
    List<HashSet<String>> temp_read_fields = record_fieds.get(tid);

    int write_idx = 0, read_idx = 0;
    for (int i = 0; i < size; i++) {
      try {
        Status s = Status.OK;
        // 交互型事务sleep
        if (interactive_) {
          try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(this.minMs, this.maxMs));
          } catch (Exception e) {
            // .. do nothing
          }
        }
        if (ops_set.get(i) == 1) {
          s = db.update(table, write_keyname.get(write_idx), write_values.get(write_idx));
          write_idx++;
        } else if (ops_set.get(i) == 2) {
          HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
          s = db.read(table, read_keyname.get(read_idx), temp_read_fields.get(read_idx), cells);
          read_idx++;
        }
        if (s != Status.OK) return false;
      } catch (Exception e) {
        System.out.print("[redoTransactionUpdateResult error] i = " + i + ", ops_set_map size = " + ops_set.size() + ", write_idx = " + write_idx + ", read_idx = " + read_idx);
      }
    }
    return true;
  }

  // 包含读写操作
  public boolean redoTransactionOpsResult(DB db, int tid) throws SQLException{
    int write_idx = 0;
    int read_idx = 0;
    for (int i = 0; i < ops_set_size_; i++) {
      try {
        Status s = Status.OK;
        if (ops_container.get(i) == 1) {
          s = db.update(table, write_record_key.get(write_idx), write_record_values.get(write_idx));
          write_idx++;
        }
        else if (ops_container.get(i) == 2) {
          HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
          s = db.read(table, read_record_key_name.get(read_idx), read_fields.get(read_idx), cells);
          read_idx++;
        }
        if (s != Status.OK) return false;
      } catch (Exception e) {
        System.out.println("[redoTransactionOpsResult error] i = " + i + ", ops_set_size_ = " + ops_set_size_ + "ops_container size = " + ops_container.size()
            + "\n write_idx = " + write_idx + ", read_idx = " + read_idx
            + "\n write_record_key size = " + write_record_key.size() + ", write_record_values size = " + write_record_values.size()
            + "\n read_record_key_name size = " + read_record_key_name.size() + ", read_fields size = " + read_fields.size());
        return true;
      }
    }
    return true;
  }

  public void doRemainedOps (DB db, Object threadstate, boolean retry, int tid, boolean interactive_) throws SQLException{
    if (counter1 <= 0) return;
    while (counter1 > 0) {
      String operation = operationchooser.nextString();
      if (operation == null) {
        return;
      }
      switch (operation) {
        case "READ":
          doTransactionRead(db, tid);
          break;
        case "UPDATE":
          doTransactionUpdate(db, tid);
          break;
        case "INSERT":
          doTransactionInsert(db);
          break;
        case "SCAN":
          doTransactionScan(db);
          break;
        default:
          doTransactionReadModifyWrite(db);
      }
      counter1--;
    }
  }

  public void doTransactionInsert(DB db) {
    // choose the next key
    long keynum = transactioninsertkeysequence.nextValue();

    try {
      String dbkey = buildKeyName(keynum);

      HashMap<String, ByteIterator> values = buildValues(dbkey);
      db.insert(table, dbkey, values);
    } finally {
      transactioninsertkeysequence.acknowledge(keynum);
    }
  }

  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    final double updateproportion = Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double insertproportion = Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double scanproportion = Double.parseDouble(p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    final double readmodifywriteproportion = Double.parseDouble(p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));

    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }

    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
    }

    if (insertproportion > 0) {
      operationchooser.addValue(insertproportion, "INSERT");
    }

    if (scanproportion > 0) {
      operationchooser.addValue(scanproportion, "SCAN");
    }

    if (readmodifywriteproportion > 0) {
      operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
    }
    return operationchooser;
  }

  protected static DiscreteGenerator regenerateOperationGenerator(double readproportion, double updateproportion) {
    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }
    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
    }
    return operationchooser;
  }
}
