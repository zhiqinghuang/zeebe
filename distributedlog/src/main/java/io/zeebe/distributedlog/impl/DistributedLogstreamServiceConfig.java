package io.zeebe.distributedlog.impl;

import io.atomix.primitive.service.ServiceConfig;
import io.zeebe.logstreams.log.LogStream;

public class DistributedLogstreamServiceConfig extends ServiceConfig {

  private LogStream logstream;

  public DistributedLogstreamServiceConfig(LogStream logstream) {
    this.logstream = logstream;
  }

  public LogStream getLogstream() {
    return logstream;
  }
}
