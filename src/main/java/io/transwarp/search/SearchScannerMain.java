package io.transwarp.search;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.ResultScanner;

public class SearchScannerMain {
  public static void main(String[] args) {
    // String input =
    // "-dataCenter 01048 -time 20141125-00:00:00/20141126-00:00:00 -commandId 8888 -outPath /home/stefan/test ";
    // String input = "-help";
    // args = input.split("\\s+");

    try {
      ScanOptionParser parser = new ScanOptionParser(args);
      SearchScannerBuilder builder = new SearchScannerBuilder();
      ScanOutputFormater formater = new ScanOutputFormater(
          parser.getOutPutFormat());
      
      formater.setCommandID(parser.getCmdID());
      formater.setLinesPerFile(parser.getLinesPerFile());
      formater.setOutPath(parser.getOutPath());
      formater.setProvinceID(parser.getProviceID());
      formater.setPreview(parser.getPriviewPages(), parser.getPreviewLinesPerPage());
      
//      builder.addOutPutColumn(formater.getScanColumn());

      ResultScanner scanner = builder.buildScannerFromParser(parser);
      formater.saveQuery(scanner);
      System.out.println("Finish query");
      System.exit(0);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ParseException e1) {
      // TODO Auto-generated catch block
      System.out.println(e1.getMessage());
    }
    System.exit(1);
  }
}
