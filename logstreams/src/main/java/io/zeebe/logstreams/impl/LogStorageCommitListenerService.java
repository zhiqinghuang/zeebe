package io.zeebe.logstreams.impl;

import io.zeebe.distributedlog.DistributedLogstream;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.SchedulingHints;
import io.zeebe.util.sched.channel.ActorConditions;

public class LogStorageCommitListenerService implements Service<LogStorageCommitListener> {

  private final Injector<DistributedLogstream> distributedLogstreamInjector = new Injector<>();
  private final LogStream logStream;

  private LogStorageCommitListener logStorageCommitListener;
  private final ActorConditions onLogStorageAppendedConditions;

  public LogStorageCommitListenerService(LogStream logStream, ActorConditions onLogStorageAppendedConditions) {
    this.logStream = logStream;
    this.onLogStorageAppendedConditions = onLogStorageAppendedConditions;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    this.logStorageCommitListener =
        new LogStorageCommitListener(
            logStream.getLogStorage(), logStream, distributedLogstreamInjector.getValue(), onLogStorageAppendedConditions);

    startContext.async(
        startContext
            .getScheduler()
            .submitActor(logStorageCommitListener, true, SchedulingHints.ioBound()));
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    stopContext.async(logStorageCommitListener.close());
  }

  @Override
  public LogStorageCommitListener get() {
    return this.logStorageCommitListener;
  }

  public Injector<DistributedLogstream> getDistributedLogstreamInjector() {
    return distributedLogstreamInjector;
  }
}
