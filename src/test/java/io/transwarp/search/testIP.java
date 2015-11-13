package io.transwarp.search;

import org.apache.hadoop.hbase.util.Bytes;


public class testIP {

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    String ip="123.23.4.7";
    int num = ipToint(ip);
    String aa = intToIp(1846736813);
    System.out.println(aa);
  }
  
  public static int ipToint(String ip){
    int aa = Bytes.toInt(encodeIP(ip));
    return aa;
  }
  
  
  public static String intToIp(int i) {  
    return ((i >> 24) & 0xFF) + "." + ((i >> 16) & 0xFF) + "."  
            + ((i >> 8) & 0xFF) + "." + (i & 0xFF);  
  } 
  
  private static byte[] encodeIP(String str) {
    byte[] result;
    result = encodeIPV4(str);
    return result;
  }

  public static byte[] encodeIPV4(String ip) {
    byte[] longs = new byte[4];
    String[] ips = ip.split("\\.");
    if (ips.length != 4) {
      return longs;
    }
    for (int i = 0; i < ips.length; i++) {
      int tmpIp = Integer.valueOf(ips[i]);
      System.out.println(tmpIp);
      longs[i] = (byte) tmpIp;
    }

    return longs;
  }

}
