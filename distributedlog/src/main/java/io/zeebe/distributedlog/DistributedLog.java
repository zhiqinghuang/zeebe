package io.zeebe.distributedlog;

import io.zeebe.logstreams.log.LogStream;

public class DistributedLog {

  private static LogStream logStreamForPartition0;

  public static LogStream getLogStreamForPartition0() {
    return logStreamForPartition0;
  }

  public static void setLogStreamForPartition0(LogStream logStreamForPartition0) {
    DistributedLog.logStreamForPartition0 = logStreamForPartition0;
  }
}
