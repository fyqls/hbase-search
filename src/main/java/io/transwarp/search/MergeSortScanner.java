package io.transwarp.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Pair;

public class MergeSortScanner extends AbstractClientScanner {
  
//  private List<Scan> scans = null;
  private List<ResultScanner> scanners = null;
  ExecutorService pool = null;
  static final int defaul_poolsize = 100;
  static final int default_caching = 50;
  boolean managed = false;
  private PriorityQueue<InternalScanner> heap = null;
  static final int refillScannerProportion = 2;
  static final int needRefileProportion = 2;
  private boolean closed = false;
  
  
  public MergeSortScanner(List<ResultScanner> scanners,
      Comparator<Result> mergeResultComparator, ExecutorService pool) throws IOException, InterruptedException {
    // TODO:check start/stop range not overlapped
    this.scanners = scanners;
    this.pool = pool;
    if(this.scanners.size() != 0)
       this.heap = new PriorityQueue<InternalScanner>(this.scanners.size(), new InternalScannerComparator(mergeResultComparator));
    init();
  }
  
  private void init() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(scanners.size());
    for (final ResultScanner s : scanners) {
      pool.execute(new Runnable() {
        @Override
        public void run() {
          try {
            InternalScanner scanner = new InternalScanner(s, default_caching);
            synchronized (heap) {
              heap.add(scanner);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
          latch.countDown();
        }
      });
    }
    latch.await();
  }
  
  enum RES_CODE {
    WOULD_BLOCK,
    SUCC
  }
  
  static class InternalScannerComparator implements Comparator<InternalScanner> {
    private Comparator<Result> comparator = null;
    
    public InternalScannerComparator(Comparator<Result> mergeResultComparator) {
      this.comparator = mergeResultComparator;
    }
    
    @Override
    public int compare(InternalScanner o1, InternalScanner o2) {
      Pair<RES_CODE, Result> p1 = o1.peek();
      Pair<RES_CODE, Result> p2 = o2.peek();
      if (p1.getFirst() == RES_CODE.WOULD_BLOCK) {
        return -1;
      } else if (p2.getFirst() == RES_CODE.WOULD_BLOCK) {
        return 1;
      } else if (p1.getSecond() == null) {
        return -1;
      } else if (p2.getSecond() == null) {
        return 1;
      } else {
        return comparator.compare(p1.getSecond(), p2.getSecond());
      }
    }
    
  }
  
  static class InternalScanner {
    private ResultScanner scanner = null;
    private Queue<Result> cachingQueue = null;
    private int caching = -1;
    private boolean isFinished = false;
    
    public InternalScanner(ResultScanner scaner, int caching) throws IOException {
      this.scanner = scaner;
      this.caching = caching;
      if (this.caching <= 0) {
        throw new IOException("must set scan caching.");
      }
      this.cachingQueue = new LinkedList<Result>();
      refillCaching();
    }
    
    public boolean needRefill() {
      return !isFinished && this.cachingQueue.size() < this.caching / needRefileProportion;
    }
    
    public void refillCaching() throws IOException {
      Result result = null;
      int count = 0;
      while ((result = this.scanner.next()) != null) {
        this.cachingQueue.add(result);
        if (++count >= this.caching) {
          break;
        }
      }
      this.isFinished = (result == null);
    }
    
    public Pair<RES_CODE, Result> peek() {
      Result r = this.cachingQueue.peek();
      if (r != null) {
        return new Pair<RES_CODE, Result>(RES_CODE.SUCC, r);
      } else {
        return new Pair<RES_CODE, Result>(this.isFinished ? RES_CODE.SUCC : RES_CODE.WOULD_BLOCK, null);
      }
    }
    
    public Pair<RES_CODE, Result> poll() {
      Result r = this.cachingQueue.poll();
      if (r != null) {
        return new Pair<RES_CODE, Result>(RES_CODE.SUCC, r);
      } else {
        return new Pair<RES_CODE, Result>(this.isFinished ? RES_CODE.SUCC : RES_CODE.WOULD_BLOCK, null);
      }
    }
    
    public void close() {
      this.scanner.close();
    }
    
    @Override
    public String toString() {
      return this.scanner.toString();
    }
  }

  @Override
  public Result next() throws IOException {
    if (closed) {
      return null;
    }
    InternalScanner scanner = null;
    if(heap==null)
      return null;
    while ((scanner = this.heap.poll()) != null) {
      Pair<RES_CODE, Result> p = scanner.poll();
      if (p.getFirst() == RES_CODE.WOULD_BLOCK) {
        int i = heap.size() / refillScannerProportion;
        i = (i == 0 ? 1 : i);
        List<InternalScanner> scanners = new ArrayList<InternalScanner>(i);
        scanners.add(scanner);
        pickScanners(scanners, i - 1);
        heap.add(scanner);
        // fetch
        final CountDownLatch latch = new CountDownLatch(scanners.size());
        for (final InternalScanner s : scanners) {
          pool.execute(new Runnable() {
            @Override
            public void run() {
              try {
                s.refillCaching();
              } catch (IOException e) {
                e.printStackTrace();
              }
              latch.countDown();
            }
          });
        }
        try {
          latch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
          // TODO: 
          return null;
        }
      } else if (p.getSecond() != null) {
        heap.add(scanner);
        return p.getSecond();
      } else {
        // scanner finished
        scanner.close();
      }
    }
    return null;
  }
  
  private void pickScanners(List<InternalScanner> scanners, int pickNum) {
    for (InternalScanner scanner : heap) {
      if (scanners.size() >= pickNum) {
        break;
      } else if (scanner.needRefill()) {
        scanners.add(scanner);
      }
    }
  }

  @Override
  public Result[] next(int nbRows) throws IOException {
    // Collect values to be returned here
    ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
    for(int i = 0; i < nbRows; i++) {
      Result next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Result[resultSets.size()]);
  }

  @Override
  public void close() {
    this.closed = true;
    if(heap == null)
      return;
    synchronized (heap) {
      while (!heap.isEmpty()) {
        heap.poll().close();
      }
    }
    if (this.managed) {
      this.pool.shutdownNow();
    }
  }
}
