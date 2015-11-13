package io.transwarp.search;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HbaseClientDemo {

  public static void main(String[] args) {
    String tableName = "2014-12-05";
    if (args.length != 1) {
      System.err
          .println("Invalid arguments , please give a table name, use default table name "
              + tableName);
      tableName = args[1];
    }

    System.out.println("Table Name: " + tableName);
    try {
      Configuration conf = HBaseConfiguration.create();
      HTable table = new HTable(conf, tableName);
      System.out.println("Open Table Succeed");
      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);
      Iterator<Result> itor = scanner.iterator();
      if (itor.hasNext()) {
        System.out.println("Print first Row");
        System.out.println(itor.next().toString());
      }
      table.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
