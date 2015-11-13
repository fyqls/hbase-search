package io.transwarp.search;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

public class BaseUtils {
  static public byte[] arrayConcat(byte[]... args){
    ByteArrayOutputStream res = new ByteArrayOutputStream();
    for(byte[] tmp: args){
      try {
        res.write(tmp);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return res.toByteArray();
  }
  
  /**
   * return millisecond of the start of next day
   * 
   * @param cur
   * @return
   */
  static public long startOfNextDay(long cur) {
    Date date = new Date(cur);
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    cal.add(Calendar.DATE, 1);

    return cal.getTime().getTime();
   }
   
  /**
   * For Debug 
   * 
   * @param time
   * @return
   */
  static public String formatDate(String format, long time){
    Date date = new Date(time);
    SimpleDateFormat sf = new SimpleDateFormat(format);
    return sf.format(date);
  }
  
  /**
   * Get the table name for specific date
   * @param tableNamePost 
   * @param tableNameTimeFormat 
   * @param tableNamePrefix 
   * @param now
   * @return
   */
  static public String getTableName(String tableNamePrefix, String tableNameTimeFormat, String tableNamePost, long now) {
    String tableName = tableNamePrefix + formatDate(tableNameTimeFormat,now)+tableNamePost;
    //For Debug
//    tableName = "2014-12-03";
    return tableName;
  }
/**
 * Temporally for IPV4
 * 
 * @param ip
 * @return
 */
  static public byte[] encodeIPV4(String ip){
    String[] tmp = ip.split("\\.");
    byte[] res= new byte[4];
    int i = 0;
    for(String num: tmp){
     int field=  Integer.valueOf(num);
     if(field> 255|| field<0 || i >=4){
      System.err.println("Invalid IP address: " + ip);
      System.err.println("End Query");
      System.exit(1);
    }
     res[i++] = (byte)field;
    }
    
    return res;
  }
  
  public static byte[] encodePort(String port){
    int ret = Integer.valueOf(port);
    if(ret > 65535 || ret < 0){
      System.err.println("Invalid Port: " + port);
      System.err.println("End Query");
      //System.exit(1);
    }
    return Bytes.toBytes(ret);
  }
 /**
 * Temporally for IPV6
 * 
 * @param ip
 * @return
 */
  static public byte[] encodeIPV6(String ip){
    String[] tmp = ip.split("\\.");
    byte[] res= new byte[16];
    int i = 0;
    for(String num: tmp){
     num = "0x" + num;
     int field=  Integer.decode(num);
     if(field> 0xFFFF || field<0 || i >=16){
      System.err.println("Invalid IP address: " + ip);
      System.err.println("End Query");
      System.exit(1);
    }
     res[i++] = (byte)(field>>8);
     res[i++] = (byte) field;
    }
    
    return res;
  }
  
  static public String decodeIPV4(byte[] ip){
    String res = null;
    int i=0;
    for(; i< 3;i++){
      res += String.valueOf(ip[i]) +".";
    }
    return res +=String.valueOf(ip[i]);
  }
  
  /**
  * 
  *  
  * @param startTime
  * @return
  */
  static public long parseTimeToLong(String startTime , String format) {
    SimpleDateFormat formatter = new SimpleDateFormat(format);
    Date date = null;
    try {
      date = (Date)formatter.parse(startTime);
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return date.getTime();
  }
}
