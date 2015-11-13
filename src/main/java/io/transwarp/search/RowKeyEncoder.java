package io.transwarp.search;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyEncoder extends BasicRowKeySchema {
  /*
   * compose key for specific time 
   */
  static byte[] composeKey(byte[] dc ,long time) {
    int hash =  (int) ((time/1000) % hashKey);
    byte[] resH = {(byte) hash};
    ByteBuffer res = ByteBuffer.allocate(TIME_LEN);
    // so ,this will ensure return of a [start, end) like result, instead of (start, end]
    res.putLong((Long.MAX_VALUE- time) +1);
    return BaseUtils.arrayConcat(dc,resH,res.array()); 
  }

  static public byte[] encodeDataCenter(String dataCenter) {
    //ByteBuffer res = ByteBuffer.allocate(DATACENTER_LEN);
    //res.put(Bytes.toBytes(dataCenter));
    //return res.array();
    return Bytes.toBytes(dataCenter);
  }
  
  static public String decodeRowKey(byte[] rowKey){
    String dataCenter = Bytes.toString(rowKey, 0, DATACENTER_LEN);
    int hashKeyInt = rowKey[DATACENTER_LEN];
    String hash = String.format("%02d", rowKey[DATACENTER_LEN]);
    long timeLongRev = Bytes.toLong(rowKey, DATACENTER_LEN+HASH_LEN, TIME_LEN );
    long timeLong = Long.MAX_VALUE - timeLongRev;
    String timeMS = Long.toString(timeLongRev);
    long uuid = Bytes.toLong(rowKey, DATACENTER_LEN+HASH_LEN+TIME_LEN, UUID_LEN);
//    String suuid = Long.toHexString(uuid).toUpperCase();
    String suuid = String.valueOf(uuid);
    String res =dataCenter+hash+timeMS+suuid;
    if((timeLong/1000)%hashKey != hashKeyInt){
      System.out.println("[Debug] incorrect hashkey: "+res);
    }
    return res;
  }
  
  static public String getDataCenter(Result res){
    String dataCenter = Bytes.toString(res.getRow(),0, DATACENTER_LEN);
    return dataCenter;
  }
  
  static public String getTime(Result res){
    long timeLongRev = Bytes.toLong(res.getRow(),DATACENTER_LEN+HASH_LEN, TIME_LEN );
    long timeLong = Long.MAX_VALUE - timeLongRev;
    Date date = new Date(timeLong);
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSS");
    return formatter.format(date);
  }
}
