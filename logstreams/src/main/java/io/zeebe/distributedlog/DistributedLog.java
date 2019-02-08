package io.zeebe.distributedlog;

import io.zeebe.logstreams.log.LogStream;

public class DistributedLog {

  private static LogStream logStreamForPartition0;
  private static DistributedLogstream distributedLog;

  public static LogStream getLogStreamForPartition0() {
    return logStreamForPartition0;
  }

  public static void setLogStreamForPartition0(LogStream logStreamForPartition0) {
    DistributedLog.logStreamForPartition0 = logStreamForPartition0;
  }

  public static DistributedLogstream getDistributedLog() {
    return distributedLog;
  }

  public static void setDistributedLog(DistributedLogstream distributedLog) {
    DistributedLog.distributedLog = distributedLog;
  }
}
