package io.zeebe.distributedlog.impl;

import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceExecutor;
import io.zeebe.distributedlog.DistributedLogstreamClient;
import io.zeebe.distributedlog.DistributedLogstreamService;
import io.zeebe.distributedlog.DistributedLogstreamType;
import io.zeebe.logstreams.log.LogStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedLogstreamService
    extends AbstractPrimitiveService<DistributedLogstreamClient>
    implements DistributedLogstreamService {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultDistributedLogstreamService.class);

  private LogStream logstream;

  private long currentPosition;

  public DefaultDistributedLogstreamService(DistributedLogstreamServiceConfig config) {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);
    currentPosition = 0;
    this.logstream = config.getLogstream();
    LOG.info("I will write to logstream {}", logstream.getLogName());
   }

  @Override
  protected void configure(ServiceExecutor executor) {
    super.configure(executor);
    LOG.info("Creating log file for {}", getServiceName());
  }

  @Override
  public void append(String bytes) {
     LOG.info("Appended to {} at position {}", getServiceName(), currentPosition);
    currentPosition++;
   // LOG.info("{} {} {}", getServiceName(), getServiceId().id(), getPrimitiveType().name());
  }

  @Override
  public void backup(BackupOutput backupOutput) {
    // TODO
    backupOutput.writeObject(currentPosition);
    LOG.info("BackUp at pos {}", currentPosition);

  }

  @Override
  public void restore(BackupInput backupInput) {
    // TODO
    long pos = backupInput.readObject();
    currentPosition = pos;
    LOG.info("restore at pos {}", pos);
  }

  @Override
  public void close() {
    super.close();
    LOG.info("Closing {}", getServiceName());
  }
}
