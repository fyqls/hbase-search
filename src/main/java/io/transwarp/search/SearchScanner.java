package io.transwarp.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

public class SearchScanner extends AbstractClientScanner {
  
  
  private boolean closed = false;
  private List<HTable> tables = null;
  private MergeSortScanner mergeSortScanner = null;
  private ExecutorService pool = null;
  
  SearchScanner(List<ResultScanner> scanners, List<HTable> tables, Comparator<Result> mergeResultComparator, int threads) throws IOException, InterruptedException {
    this.tables = new ArrayList<HTable>(tables);
    pool = Executors.newFixedThreadPool(threads);
    mergeSortScanner = new MergeSortScanner(scanners,mergeResultComparator, pool);
  }

  @Override
  public Result next() throws IOException {
    return mergeSortScanner.next();
  }

  @Override
  public Result[] next(int num) throws IOException {
    ArrayList<Result> resultSets = new ArrayList<Result>(num);
    for(int i=0; i< num; i++){
      Result tmp = next();
      if (tmp == null)
        break;
      resultSets.add(tmp);
    }
    return resultSets.toArray(new Result[resultSets.size()]);
  }
  
  @Override
  public void close() {
    if (this.closed) {
      return;
    }
    mergeSortScanner.close();
    for (HTable table: tables)
      try {
        table.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    this.closed = true;
  }
}
