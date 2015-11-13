package io.transwarp.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ScanOptionParser {
  private Options options = new Options();
  private CommandLineParser parser;
  private CommandLine line;
  private HelpFormatter formatter ;
  
  private final String startIP = "s";
  private final String endIP = "e";
  private final String port = "p";
  private Map<String,String> srcIP =null;
  private Map<String,String> destIP =null;
  private String dc;
  private long startTime;
  private long endTime;
  
  public ScanOptionParser(String args[]) throws ParseException{
   options.addOption("help", false,"print this message");
   options.addOption("dataCenter",true,"dataCenter code to query");
   options.addOption("commandId",true,"command ID");
   options.addOption("linesPerFile",true,"lines Per File,default is 1000");
   String outPutFormat = "Out Put Format, specify sequence of columns in output, concat each column name with / \n";
//       outPutFormat += "default sequence is logId/houseId/sourceIP/destIP/sourcePort/destPort/url/time/uuid \n";
       outPutFormat += "default sequence is logId/houseId/sourceIP/destIP/sourcePort/destPort/url/time \n";
       outPutFormat += "logId\t row key in database\n";
       outPutFormat += "houseId\t data center ID\n";
       outPutFormat += "sourceIP\t source IP address\n";
       outPutFormat += "destIP\t destination IP address\n";
       outPutFormat += "sourcePort\t source Port address\n";
       outPutFormat += "destPort\t destination Port address\n";
       outPutFormat += "url\t url to query\n";
       outPutFormat += "time\t the time when connection happends\n";
       outPutFormat += "proctl\t proctcol of connection\n";
       outPutFormat += "business\t connection business type\n";
       outPutFormat += "length\t the time length of connetion\n";
       outPutFormat += "upTraffic\t size of upward network traffic\n";
       outPutFormat += "downTraffic\t size of downward network traffic\n";
//       outPutFormat += "uuid\t uuid for a type of record\n";
   options.addOption("outPutFormat",true,outPutFormat);
   options.addOption("time",true,"start/end , specify start time(inclusive) and end time(exclusive) in yyyyMMdd-HH:MM:ss format");
   options.addOption("srcIP",true,"source IP (range), eg: 192.168.1.1:port or 192.168.1.1/192.168.1.255");
   options.addOption("destIP",true,"destination IP (range), eg: 192.168.1.1:port or 192.168.1.1/192.168.1.255");
   options.addOption("url",true,"url to query");
   options.addOption("preview",true,"arguments for preview in (pages* linesPerPage), default is 0");
   options.addOption("outPath",true,"out put file to save the result");
   options.addOption("protcl",true,"protocol to query");
   formatter = new HelpFormatter();
   
    parser = new BasicParser();
    line = parser.parse(options, args);
    if (line.hasOption("help")) {
      formatter.printHelp("help", options);
      throw new ParseException("");
    }
    srcIP = getSrcIP();
    destIP = getDestIP();
    
    //check necessary arguments
    getTime();
    dc = getDC();
  }
  
  private void getTime() {
    if (false == line.hasOption("time")) {
      System.err.println("Please give a time interval to query");
      formatter.printHelp("time", options);
      // time is a required arguments, will exit if null
      System.exit(1);
      return;
    }
    String res[] = line.getOptionValue("time").split("\\/");
    if (res.length != 2) {
      System.err.println("Error Time Range, please give start time (inclusive) and end time (exclusive)");
      formatter.printHelp("time", options);
      System.exit(1);
      return ;
    }
    startTime = BaseUtils.parseTimeToLong(res[0], "yyyyMMdd-HH:mm:ss");
    endTime = BaseUtils.parseTimeToLong(res[1], "yyyyMMdd-HH:mm:ss");
    
    if (this.startTime > this.endTime) {
      System.err.println(
          "Invalid arguments, start time exceed end time");
      System.exit(1);
    }
  }

  private String[] getPreview(){
    if(line.hasOption("preview")){
      String args = line.getOptionValue("preview");
      String tmp[] = args.split("\\*");
      if(tmp.length!=2){
        System.err.println("Invalid argument for preview ");
        System.exit(1);
      }
     return tmp; 
    }
    return null;
  }
  
  public int getPreviewLinesPerPage(){
   String [] res = getPreview();
   if (res != null){
     return Integer.valueOf(res[1]);
   }
   return 0;
  } 
  
  public int getPriviewPages(){
    String [] res = getPreview();
    if (res != null){
      return Integer.valueOf(res[0]);
    }
    return 0;
  }
  
  public long getStartTime(){
    return startTime;
  }
  
  public long getEndTime(){
    return endTime;
  }
  
  public String getOutPath(){
    if( line.hasOption("outPath")){
      return line.getOptionValue("outPath");
    }
    return null;
  }
 
  public String getCmdID(){
    if(line.hasOption("commandId"))
      return line.getOptionValue("commandId");
    return null; 
  }
  
  public int getLinesPerFile(){
    int ret = 1000;
    if(line.hasOption("linesPerFile")){
      int lines = Integer.parseInt(line.getOptionValue("linesPerFile"));
      if (lines <1)
        System.err.println("Invalide argument, lines per file: " + lines + "use default value: " + ret);
      else
        ret = lines;
    }
    return ret;
  }
 
  private String getDC(){
    if(false == line.hasOption("dataCenter")){
      System.err.println("Please give a data center to query");
      formatter.printHelp("dataCenter", options);
      // dataCenter is a required arguments, will exit if null
      System.exit(1);
      }
    return line.getOptionValue("dataCenter");
  }
  
  public String getDataCenter(){
    return dc;
  }
  
  public Map<String, String> getSrcIP(){
    return getIP("srcIP");
  }
  
  public Map<String, String> getDestIP(){
    return getIP("destIP");
  }
  
  private Map<String,String> getIP(String option){
   if(false == line.hasOption(option))
     return null;
   Map<String,String> res = new HashMap<String,String>(); 
   String tmp[] = line.getOptionValue(option).split("\\:");
   if(tmp.length == 2)
     res.put(port, tmp[1]);
   String ip[] = tmp[0].split("\\/");
   res.put(startIP, ip[0]);
   if(ip.length == 2)
     res.put(endIP, ip[1]);
   
   return res;
  }
 
  public String getURL(){
    if(false == line.hasOption("url"))
      return null;
    return line.getOptionValue("url");
  }
  
  public String getProtocolType(){
    if(false == line.hasOption("protcl"))
      return null;
    return line.getOptionValue("protcl");
  }
  
  public List<String> getOutPutFormat(){
    List<String> list = new ArrayList<String>();
    String format;
    if(false == line.hasOption("outPutFormat"))
       format= "logId/houseId/sourceIP/destIP/sourcePort/destPort/url/time";
    else
      format = line.getOptionValue("outPutFormat");
    String res[] = format.split("\\/");
    for (String tmp: res){
       list.add(tmp);
    }
   return list; 
  }
  
  public String getProviceID() {
    return dc.substring(0, 2);
  }

  private String getKey(Map<String,String> IP, String key){
    if (IP == null)
      return null;
    String res = null;
    if (IP.containsKey(key))
      res = IP.get(key);
    return res; 
  }
  
  public String getSrcStartIP() {
      return getKey(srcIP, startIP);
  }

  public String getSrcEndIP() {
    return getKey(srcIP,endIP);
  }
  public String getSrcPort() {
    return getKey(srcIP,port);
  }
  public String getDestStartIP() {
    return getKey(destIP,startIP);
  }

  public String getDestEndIP() {
    return getKey(destIP,endIP);
  }
  public String getDestPort() {
    return getKey(destIP,port);
  }
}
