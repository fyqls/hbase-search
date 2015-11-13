package io.transwarp.search;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanOutputStream {
  private GzipCompressorOutputStream os= null;
  private BufferedWriter writer=null;
  private int lines = 0;
  private int linesPerFile = 1000;
  private String outPath = null;
  private String proviceID = null;
  private String cmdId = null;
  private String current = null;
  private int previewLinesPerPage= 1000;
  private int previewPages = 2;
  private int previewLines = 0;
  private int lastFile=0;
  
  public  ScanOutputStream(String outPath, String proviceID, String cmdId, String current, int linesPerFile) throws FileNotFoundException, IOException{
    this.outPath = outPath;
    this.proviceID = proviceID;
    this.cmdId = cmdId;
    this.current = current;
    this.linesPerFile = linesPerFile;
    
    lastFile = 1;
    if (outPath == null) {
      if (writer == null)
        writer = new BufferedWriter(new OutputStreamWriter(System.out));
    }else if(os == null){
      String fileName = getFileName(outPath, proviceID, cmdId, lastFile, current)+".txt.gz";
      String fileNameTmp = getFileNameTmp(fileName);
      os = new GzipCompressorOutputStream(new FileOutputStream(fileNameTmp));
    }
  }
  
  public void setPreview(int files, int linesPerfiles){
    previewPages = files;
    previewLinesPerPage = linesPerfiles;
    previewLines = previewLinesPerPage*previewPages;
  }

  private void markComplete(){
    String fileName = getFileName(outPath, proviceID, cmdId, lastFile, current)+".txt.gz";
    String fileNameTmp = getFileNameTmp(fileName);
    // rename file to mark completion
    rename(fileNameTmp, fileName);
  }
  private void rollForward() throws IOException {
    os.flush();
    os.close();
    markComplete();
    lastFile++;
   String fileName = getFileName(outPath, proviceID, cmdId, lastFile, current)+".txt.gz";
   String fileNameTmp = getFileNameTmp(fileName);
    os = new GzipCompressorOutputStream(new FileOutputStream(fileNameTmp));
  }

  /*
   * write a line to file
   * 
   * 
   */
  public void writeLine(String str) throws IOException{
    if (outPath == null) {
      if (writer == null)
        writer = new BufferedWriter(new OutputStreamWriter(System.out));
      writer.write(str);
      return;
    }

    // to avoid devide 0 error
    if (lines > 0 && lines <= previewLines ) {
      if (lines % previewLinesPerPage == 0)
        rollForward();
    } else if ((lines - previewLines) != 0 && (lines - previewLines) % linesPerFile == 0) {
      rollForward();
    }
    os.write(Bytes.toBytes(str));
    // os.flush();
    lines++;
  }
  
  public void close() throws IOException {
    if (writer != null)
      writer.close();
    if (os != null) {
      os.flush();
      os.close();

      markComplete();
      System.out.println("[Debug] total result: " + lines);
    }
  }

  public void renameLastFile() {
    if (outPath == null) {
      return;
    }
    String fileName = getFileName(outPath, proviceID, cmdId, lastFile, current);
    String oldFileName = fileName +".txt.gz";
    String newFileName = fileName + ".ok.txt.gz";
    // rename file to mark completion
    rename(oldFileName, newFileName);
  }

  public void flush() throws IOException {
    if(writer != null)
      writer.flush();
    if(os != null)
      os.flush();
  }
  
  private void rename(String before, String after){
    File old = new File(before);
    File renameFile = new File(after);
    if(renameFile.exists())
      System.err.println("File already existed, overwrite the file :" + after);
    if(false == old.renameTo(renameFile))
      System.err.println("Rename file " + before +" to " + after +" failed");
  }
 
  private String getFileNameTmp(String fileName){
    return fileName + ".tmp";
  }
  
  private String getFileName(String outPath, String proviceID,String cmdId, int seq, String now){
    if(seq > 999)
      System.err.println("Surpass maximum file id avaliable, Maximum Id is 1000, current id is " + seq);
    String seq3 = String.format("%03d", seq); 
    String fileName = outPath+"/"+proviceID+cmdId+seq3+ now;
    return fileName;
  }
}
