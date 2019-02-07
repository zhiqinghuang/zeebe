package io.zeebe.distributedlog.impl;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;
import io.zeebe.distributedlog.DistributedLogstreamType;
import io.zeebe.logstreams.log.LogStream;

public class DistributedLogstreamConfig extends PrimitiveConfig<DistributedLogstreamConfig> {

  private LogStream logStream;

  public DistributedLogstreamConfig(){
   super();
  }

  public DistributedLogstreamConfig(LogStream logStream){
    //TODO: identify logStreams for different partitions
    this.logStream = logStream;
  }

  @Override
  public PrimitiveType getType() {
    return DistributedLogstreamType.instance();
  }

  public LogStream getLogStream() {
    return logStream;
  }

  public void setLogStream(LogStream logStream) {
    this.logStream = logStream;
  }
}
