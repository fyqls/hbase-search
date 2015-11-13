package io.transwarp.search;

import java.io.IOException;

import org.apache.hadoop.hbase.client.ResultScanner;

public interface ScannerBuilder {
  
  public ResultScanner buildScanner() throws IOException, InterruptedException;
  
}
