package io.zeebe.distributedlog.impl;

import io.zeebe.distributedlog.DistributedLogstream;
import io.zeebe.distributedlog.LogEventListener;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import java.nio.ByteBuffer;

public class DistributedLogstreamPartition implements Service<DistributedLogstreamPartition> {
  private final int paritionId;
  private final Injector<DistributedLogstream>  distributedLogstreamInjector = new Injector<>();
  private DistributedLogstream distributedLog;

  private final String partitionName;

  public DistributedLogstreamPartition(int paritionId) {
    this.paritionId = paritionId;
    this.distributedLog = distributedLog;
    partitionName = String.valueOf(paritionId);
  }

  public void append(ByteBuffer blockBuffer, long commitPosition) {
    distributedLog.append(partitionName, blockBuffer, commitPosition);
  }

  public void addListener(LogEventListener listener) {
    distributedLog.addListener(partitionName, listener);
  }

  public void removeListener(LogEventListener listener) {
    distributedLog.removeListener(partitionName, listener);
  }

  public Injector<DistributedLogstream> getDistributedLogstreamInjector() {
    return distributedLogstreamInjector;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    distributedLog = distributedLogstreamInjector.getValue();
  }

  @Override
  public DistributedLogstreamPartition get() {
    return this;
  }
}
