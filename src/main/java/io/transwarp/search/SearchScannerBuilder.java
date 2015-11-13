package io.transwarp.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class SearchScannerBuilder extends BasicRowKeySchema implements
    ScannerBuilder {
  private Configuration config;
  static {
    Configuration.addDefaultResource("query.xml");
  }
  private final String TABLENAME_PREFIX = "io.transwarp.tablename.prefix";
  private final String TABLENAME_TIMEFORMAT = "io.transwarp.tablename.timeformat";
  private final String TABLENAME_POSTFIX = "io.transwarp.tablename.postfix";
  private FilterList filterList;
  private long startTime;
  private long endTime;
  private byte[] dc = null;
  private List<Pair<byte[], byte[]>> outPutList;

  /**
   * Build Scanner for time interval
   * 
   * @param startTime
   *          (inclusive)
   * @param endTime
   *          (exclusive)
   * @param dataCenter
   */

  public SearchScannerBuilder() {

    config = HBaseConfiguration.create();
    outPutList = new ArrayList<Pair<byte[], byte[]>>();
  }

  // helper for adding filter
  private void addFilter(final byte[] cf, final byte[] col, final CompareOp op,
      final byte[] val) {
    SingleColumnValueFilter filter = new SingleColumnValueFilter(cf, col, op,
        val);
    filterList.addFilter(filter);
  }

  public void setSourceIPRange(String startIP, String stopIP) {
    addFilter(cf, colSourceIP, CompareOp.GREATER_OR_EQUAL,
        BaseUtils.encodeIPV4(startIP));
    addFilter(cf, colSourceIP, CompareOp.LESS_OR_EQUAL,
        BaseUtils.encodeIPV4(stopIP));
  }

  public void setSourceIP(String IP) {
    addFilter(cf, colSourceIP, CompareOp.EQUAL, BaseUtils.encodeIPV4(IP));
  }

  public void setSourcePort(String string) {
    addFilter(cf, colSourcePort, CompareOp.EQUAL, BaseUtils.encodePort(string));
  }

  public void setDestIPRange(String startIP, String stopIP) {
    addFilter(cf, colDestIP, CompareOp.GREATER_OR_EQUAL,
        BaseUtils.encodeIPV4(startIP));
    addFilter(cf, colDestIP, CompareOp.LESS_OR_EQUAL,
        BaseUtils.encodeIPV4(stopIP));
  }

  public void setDestIP(String IP) {
    addFilter(cf, colDestIP, CompareOp.EQUAL, BaseUtils.encodeIPV4(IP));
  }

  public void setDestPort(String string) {
    BinaryComparator comp = new BinaryComparator(BaseUtils.encodePort(string));
    SingleColumnValueFilter filter = new SingleColumnValueFilter(cf,
        colDestPort, CompareOp.EQUAL, comp);
    filterList.addFilter(filter);
  }

  // TODO what is matchURL?
  public void setMatchingURL(String matchURL) {
    SingleColumnValueFilter filter = new SingleColumnValueFilter(cf, colUrl,
        CompareFilter.CompareOp.EQUAL, new SubstringComparator(matchURL));
    filterList.addFilter(filter);
  }

  public void setProtocol(String protcl) {
    BinaryComparator comp = new BinaryComparator(Bytes.toBytes(protcl));
    SingleColumnValueFilter filter = new SingleColumnValueFilter(cf,
        colProtcol, CompareOp.EQUAL, comp);
    filterList.addFilter(filter);
  }

  public void addOutPutColumn(byte[] family, byte[] qualifer) {
    Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>(family, qualifer);
    if (outPutList.contains(pair)) {
      System.err.println("Duplicate output column");
      return;
    }
    outPutList.add(pair);
  }

  public void addOutPutColumn(List<Pair<byte[], byte[]>> list) {
    for (Pair<byte[], byte[]> entry : list)
      addOutPutColumn(entry.getFirst(), entry.getSecond());
  }

  /**
   * Build scanner for several days
   * 
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public ResultScanner buildScanner() throws IOException, InterruptedException {
    ArrayList<ResultScanner> scanners = new ArrayList<ResultScanner>();
    ArrayList<HTable> tables = new ArrayList<HTable>();
    long cur = startTime;
    HConnection connection = HConnectionManager.createConnection(config);

    String tableNamePrefix = config.get(TABLENAME_PREFIX);
    String tableNameTimeFormat = config.get(TABLENAME_TIMEFORMAT);
    String tableNamePost = config.get(TABLENAME_POSTFIX);
    
    if (tableNamePrefix == null)
      tableNamePrefix="";
    if (tableNameTimeFormat == null)
      tableNameTimeFormat="yyyyMMdd";
    if (tableNamePost == null)
      tableNamePost="";
   
    System.out.println("Use Table Format: "+ tableNamePrefix+tableNameTimeFormat+tableNamePost);
    HBaseAdmin admin = new HBaseAdmin(connection);
    while (true) {
      boolean needQuit = false;
      long endOfDay = BaseUtils.startOfNextDay(cur);
      if (endOfDay >= endTime) {
        endOfDay = endTime;
        needQuit = true;
      }
     
      String tableName = BaseUtils.getTableName(tableNamePrefix,tableNameTimeFormat,tableNamePost, cur);
      if (admin.tableExists(tableName)) {
        HTable table;
        try {
          table = new HTable(config, tableName);
          tables.add(table);
          scanners.addAll(buildDayScanner(table, cur, endOfDay));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      
      // config.set("hbase.zookeeper.quorum", "test-01,test-04,test-03");
      // config.set("zookeeper.znode.parent", "/hyperbase1");
      if (needQuit == true)
        break;
      cur = endOfDay;
    }
    admin.close();
    connection.close();
//    int threads = scanners.size();
    int threads = hashKey;

    return new SearchScanner(scanners, tables, new MergeResultComparator(),
        threads);
  }

  /**
   * Build scanner for a day , can only ensure precision to be seconds
   * 
   * @param table
   * @param start
   * @param end
   * @return
   * @throws IOException
   */
  private ArrayList<ResultScanner> buildDayScanner(HTable table, long start,
      long end) throws IOException {
    ArrayList<ResultScanner> scanners = new ArrayList<ResultScanner>();
    long startSeconds = start / 1000;
    long endSeconds = end / 1000;
    long startT = startSeconds / hashKey;
    long endT = endSeconds / hashKey;

    for (int i = 0; i < hashKey; i++) {
      long startKey = 0;
      long endKey = 0;

      if (i < startSeconds % hashKey)
        startKey = ((startT + 1) * hashKey + i) * 1000;
      else
        startKey = ((startT) * hashKey + i) * 1000;

      if (i >= endSeconds % hashKey)
        endKey = ((endT) * hashKey + i) * 1000;
      else
        endKey = ((endT + 1) * hashKey + i) * 1000;

      Scan scan = getScan(startKey, endKey);
      ResultScanner scanner = table.getScanner(scan);
      scanners.add(scanner);
    }

    return scanners;
  }

  /**
   * Get scan for specific second
   * 
   * @param start
   * @param end
   * @return
   */
  private Scan getScan(long start, long end) {
    // need to reverse the start and end ,since row key is generated by Max.long
    // -time
    Scan scan = new Scan(RowKeyEncoder.composeKey(dc, end),
        RowKeyEncoder.composeKey(dc, start));
    scan.setFilter(filterList);
//    for (Pair<byte[], byte[]> col : outPutList) {
//      scan.addColumn(col.getFirst(), col.getSecond());
//    }
    return scan;
  }
  
  public ResultScanner buildScannerFromParser (ScanOptionParser parser) throws IOException,IllegalArgumentException, InterruptedException{
    String dataCenter = parser.getDataCenter();
    
//    SearchScannerBuilder scanner = new SearchScannerBuilder(time[0], time[1], dataCenter);

    dc = RowKeyEncoder.encodeDataCenter(dataCenter);
    this.filterList = new FilterList();
    this.startTime = parser.getStartTime();
    this.endTime = parser.getEndTime();
    
    String srcStartIP = parser.getSrcStartIP();
    if (null != srcStartIP) {
      String srcEndIP = parser.getSrcEndIP();
      if (srcEndIP != null)
        setSourceIPRange(srcStartIP, srcEndIP);
      else
        setSourceIP(srcStartIP);
    }
    String srcPort = parser.getSrcPort();
    if (srcPort != null)
      setSourcePort(srcPort);

    String destStartIP = parser.getDestStartIP();
    if (null != destStartIP) {
      String destEndIP = parser.getDestEndIP();
      if (destEndIP != null)
        setDestIPRange(destStartIP, destEndIP);
      else
        setDestIP(destStartIP);
    }
    String destPort = parser.getDestPort();
    if (destPort != null)
      setDestPort(destPort);
    
    String url = parser.getURL();
    if(null != url)
      setMatchingURL(url);

    String proctl = parser.getProtocolType();
    if (null != proctl)
      setProtocol(proctl);
    
   return buildScanner(); 
  }
}