package io.transwarp.search;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class ScanOutputFormater extends BasicRowKeySchema{
  private List<Pair<String,byte[]>> outList;
  private String delim = "|";
  private String outPath = "/tmp" ;
  private int linesPerFile = 1000;
  private long now = System.currentTimeMillis();
  private String cmdID = "0000";
  private String proviceID= "";
  private int previewPages = 0;
  private int previewLines =0;
  
  public ScanOutputFormater(List<String> format){
    outList = new ArrayList<Pair<String,byte[]>>();
    for(String col : format){
      if(col.equals("logId")){
        addList(col,null);
        continue;
      }
      if(col.equals("houseId")){
        addList(col,null);
        continue;
      }
      if(col.equals("time")){
        addList(col,null);
        continue;
      }
      if(col.equals("sourceIP")){
        addList(col,colSourceIP);
        continue;
      }
      if(col.equals("sourcePort")){
        addList(col,colSourcePort);
        continue;
      }
      if(col.equals("destIP")){
        addList(col,colDestIP);
        continue;
      }
      if(col.equals("destPort")){
        addList(col,colDestPort);
        continue;
      }
      if(col.equals("url")){
        addList(col,colUrl);
        continue;
      }
      if(col.equals("proctl")){
        addList(col,colProtcol);
        continue;
      }
      if(col.equals("business")){
        addList(col,colProtcol);
        continue;
      }
      if(col.equals("length")){
        addList(col,colProtcol);
        continue;
      }
      if(col.equals("upTraffic")){
        addList(col,colProtcol);
        continue;
      }
      if(col.equals("downTraffic")){
        addList(col,colProtcol);
        continue;
      }
//      if(col.equals("uuid")){
//        addList(col,null);
//        continue;
//      }
      System.out.println("Unsupport Out Put Format: " + col);
      }
  }
 
  public void setDelim(String delim){
    this.delim = delim;
  }  
 
  public void setOutPath(String path){
    outPath = path;
  }

  public void setLinesPerFile(int lines){
    linesPerFile = lines;
  }
  
  public void setCommandID(String id){
    cmdID = id;
  }
  
  public void setProvinceID(String id){
    proviceID = id;
  }
  
  private void addList(String key, byte[] val){
    Pair<String,byte[]> p = new Pair<String,byte[]>(key,val);
    /*
     * to avoid duplicate added column
     */
    for(Pair<String,byte[]> tmp : outList){
      if(key.equals(tmp.getFirst()))
        return;
      if(Bytes.equals(val, tmp.getSecond()))
        p.setSecond(null);
    }
    outList.add(p);
  }
 
  /**
  * get column list for scan
  *  
  *  
  * @return
  */
  public List<byte[]> getColumns(){
    List<byte[]> list = new ArrayList<byte[]>();
    Iterator<Pair<String ,byte[]>> iter =outList.iterator();
    if(iter.hasNext()){
      byte[] col = iter.next().getSecond();
      if(null != col)
        list.add(col);
    }
    return list;
  }

  /**
  * print the row base on the output format
  *  
  * @param res
  * @return
  */
  private String printRow(Result res){
    Iterator<Pair<String ,byte[]>> iter =outList.iterator();
    String row = "";
    while(iter.hasNext()){
      Pair<String ,byte[]> col = iter.next();
      String val = appendFromCol(col.getFirst(), res);
      row += val +  delim;
    }
    // strip out last delim
    return row.substring(0, row.length()-1);
  }
 
  /**
  * get control family and qualifiers (column) need to scan
  *  
  *  
  * @return
  */
  public List<Pair<byte[], byte[]>> getScanColumn(){
    List<Pair<byte[], byte[]>> list = new  ArrayList<Pair<byte[], byte[]>>();
    Iterator<Pair<String ,byte[]>> iter =outList.iterator();
    while(iter.hasNext()){
      Pair<String ,byte[]> col = iter.next();
      if(col.getSecond() == null)
        continue;
      list.add(new Pair<byte[],byte[]>(cf,col.getSecond()));
    }
    return list;
  }
 
  private String getTime(Result res) {
    return RowKeyEncoder.getTime(res);
  }
  
  private String getDateCenter(Result res){
   return RowKeyEncoder.getDataCenter(res);
  }
  
  private String getIPColumn(Result res, byte[] columnFamily, byte[] column){
    KeyValue kv = res.getColumnLatest(columnFamily, column);
    String str = "";
    if (null ==kv)
      return " ";
    byte[] ip= kv.getValue();
    if(ip.length != 4)
      return "Invalid IP";
    for(int i=0;i<3;i++)
      str +=String.valueOf((int)ip[i]&0x0FF)+".";
    str += String.valueOf((int)ip[3]&0x0FF);
    return str;
  }
  
  private String getPortColumn(Result res, byte[] columnFamily, byte[] column){
    KeyValue kv = res.getColumnLatest(columnFamily, column);
    if (null ==kv)
      return " ";
    byte[] port = kv.getValue();
    if (port.length != 4)
      return "Invalid port";
    return String.valueOf(Bytes.toInt(port)); 
  }
  
  private String getSrcIP(Result res){
    return getIPColumn(res, cf, colSourceIP);
  }
  
  private String getSrcPort(Result res){
    return getPortColumn(res, cf, colSourcePort);
  }
  
  private String getDestIP(Result res){
    return getIPColumn(res, cf, colDestIP);
  }
  
  private String getDestPort(Result res){
    return getPortColumn(res, cf, colDestPort);
  }
 
  private String getProtclType(Result res){
    return getFromProtcl(res, PROCTL);
  }
  
  private String getBizType(Result res){
    return getFromBiz(res, BIZTYPE);
  }
  
  private String getLength(Result res){
    return getFromBiz(res, LEN);
  }
  
  private String getUpTraffic(Result res){
    return getFromBiz(res, UP);
  }
  
  private String getDownTraffic(Result res){
    return getFromBiz(res, DOWN);
  }
  
  private String getFromProtcl(Result res, int p){
    KeyValue kv = res.getColumnLatest(cf, colProtcol);
    if(null == kv)
      return " ";
    return Bytes.toString(kv.getValue());
  }
  
  private String getFromBiz(Result res, int p){
    KeyValue kv = res.getColumnLatest(cf, colBiz);
    if(null == kv)
      return " ";
    String str[] = Bytes.toString(kv.getValue()).split("\\|");
    return str[p];
  }
  
  private String getURL(Result res){
    KeyValue kv = res.getColumnLatest(cf, colUrl);
    if(null == kv)
      return " ";
    return Bytes.toString(kv.getValue());
  }
  
  private String appendFromCol(String col,Result res){
    if(col.equals("time"))
      return getTime(res);
    if(col.equals("logId"))
      return RowKeyEncoder.decodeRowKey(res.getRow());
    if(col.equals("houseId"))
      return getDateCenter(res);
    if(col.equals("sourceIP"))
      return getSrcIP(res);
    if(col.equals("sourcePort"))
      return getSrcPort(res);
    if(col.equals("destIP"))
      return getDestIP(res);
    if(col.equals("destPort"))
      return getDestPort(res);
    if(col.equals("url"))
      return getURL(res);
    if(col.equals("proctl"))
      return getProtclType(res);
    if(col.equals("business"))
      return getBizType(res);
    if(col.equals("length"))
      return getLength(res);
    if(col.equals("upTraffic"))
      return getUpTraffic(res);
    if(col.equals("downTraffic"))
      return getDownTraffic(res);
//    if(col.equals("uuid"))
//      return getUUID(res);
    System.err.println("unimplement out put format: "+ col);
    return null;
  }


  private String getUUID(Result res) {
    CRC32 crc = new CRC32();
    crc.reset();
    byte[] rowkey = res.getRow();
    crc.update(rowkey, 0, 5);
    crc.update(rowkey, 6, 8);
    List<KeyValue> kv = res.list();
    for(KeyValue tmp: kv){
      crc.update(tmp.getValue());
    }
    return String.valueOf(crc.getValue());
  }

  public void setPreview(int pages, int lines){
    previewPages = pages;
    previewLines = lines;
  }
  public void saveQuery(ResultScanner scanner) throws FileNotFoundException, IOException {
    ScanOutputStream outStream = null;
    String current = BaseUtils.formatDate("yyyyMMddHHmmss",now);
    // Create the directory
    if(outPath != null){
      File mkPath = new File(outPath);
      if(!mkPath.exists())
        mkPath.mkdirs();
    }
    
    outStream = new ScanOutputStream(outPath,proviceID, cmdID,current,linesPerFile );
    outStream.setPreview(previewPages, previewLines);

    try {
      Iterator<Result> iter = scanner.iterator();
      while (iter.hasNext()) {
        Result res= iter.next();
        //suppose every line is about 256 bytes,flush to disk about every other 16k lines, which is about 1M in size
        String report = printRow(res)+"\n";
        outStream.writeLine(report);
      }
      outStream.close();
      outStream.renameLastFile();
      scanner.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } 
  }
}
