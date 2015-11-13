package io.transwarp.search;

import java.util.Comparator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class MergeResultComparator extends BasicRowKeySchema implements Comparator<Result> {
  
  @Override
  public int compare(Result o1, Result o2) {
    // TODO Auto-generated method stub
    int res = Bytes.compareTo(o1.getRow(),0,DATACENTER_LEN,o2.getRow(), 0,DATACENTER_LEN);
    if (res == 0)
      res = Bytes.compareTo(o1.getRow(),DATACENTER_LEN+HASH_LEN,TIME_LEN,o2.getRow(), DATACENTER_LEN+HASH_LEN,TIME_LEN);
    return res;
  }
}