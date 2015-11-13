package io.transwarp.search;

import org.apache.hadoop.hbase.util.Bytes;

public class BasicRowKeySchema {
  public final static int DATACENTER_LEN = 5;
  public final static int HASH_LEN = 1;
  public final static int TIME_LEN = 8;
  public final static int UUID_LEN = 8;

  public final static byte[] cf = Bytes.toBytes("f");  
  public final static byte[] colSourceIP = Bytes.toBytes("a");
  public final static byte[] colSourcePort = Bytes.toBytes("b");
  public final static byte[] colDestIP = Bytes.toBytes("c");
  public final static byte[] colDestPort = Bytes.toBytes("d");
  public final static byte[] colUrl = Bytes.toBytes("e");
  public final static byte[] colProtcol = Bytes.toBytes("f");
  public final static byte[] colBiz = Bytes.toBytes("g");
  public final static int hashKey = 100;
  
  public final static int PROCTL = 0;
  public final static int BIZTYPE = 0;
  public final static int LEN = 1;
  public final static int UP = 2;
  public final static int DOWN =3;
}
