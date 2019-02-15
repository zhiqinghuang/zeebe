package io.zeebe.logstreams.impl;

import io.zeebe.distributedlog.DistributedLogstream;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.SchedulingHints;
import io.zeebe.util.sched.channel.ActorConditions;

public class LogStorageAppenderListernerService implements Service<LogStorageAppenderListener> {

  private final Injector<DistributedLogstream> distributedLogstreamInjector = new Injector<>();
  private final LogStream logStream;

  private LogStorageAppenderListener logStorageAppenderListener;
  private final ActorConditions onLogStorageAppendedConditions;

  public LogStorageAppenderListernerService(LogStream logStream, ActorConditions onLogStorageAppendedConditions) {
    this.logStream = logStream;
    this.onLogStorageAppendedConditions = onLogStorageAppendedConditions;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    this.logStorageAppenderListener =
        new LogStorageAppenderListener(
            logStream.getLogStorage(), logStream, distributedLogstreamInjector.getValue(), onLogStorageAppendedConditions);

    startContext.async(
        startContext
            .getScheduler()
            .submitActor(logStorageAppenderListener, true, SchedulingHints.ioBound()));
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    stopContext.async(logStorageAppenderListener.close());
  }

  @Override
  public LogStorageAppenderListener get() {
    return this.logStorageAppenderListener;
  }

  public Injector<DistributedLogstream> getDistributedLogstreamInjector() {
    return distributedLogstreamInjector;
  }
}
