package io.zeebe.logstreams.impl;

import io.zeebe.distributedlog.CommitLogEvent;
import io.zeebe.distributedlog.DistributedLogstream;
import io.zeebe.distributedlog.LogEventListener;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.channel.ActorConditions;
import io.zeebe.util.sched.future.ActorFuture;
import java.nio.ByteBuffer;
import org.slf4j.Logger;

/**
 * Listen to the committed events in DistributedLogstream and appends the bytes to the logstorage.
 * This should run in all replicas including leader and followers.
 */
public class LogStorageAppenderListener extends Actor implements LogEventListener {

  public static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;
  private final LogStorage logStorage;
  private final LogStream logStream;
  private final DistributedLogstream distributedLog;
  private final ActorConditions onLogStorageAppendedConditions;

  private long lastCommittedPosition;

  public LogStorageAppenderListener(LogStorage logStorage,
    LogStream logStream, DistributedLogstream distributedLog,
    ActorConditions onLogStorageAppendedConditions) {
    this.logStorage = logStorage;
    this.logStream = logStream;
    this.distributedLog = distributedLog;
    this.onLogStorageAppendedConditions = onLogStorageAppendedConditions;
  }

  @Override
  protected void onActorStarting() {
    distributedLog.addListener(this);
  }

  @Override
  public void onCommit(CommitLogEvent event) {
    byte[] committedBytes = event.getCommittedBytes();
    long commitPosition = event.getCommitPosition();
    actor.call(() -> append(commitPosition, committedBytes));
  }

  private void append(long commitPosition, byte[] committedBytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(committedBytes);
    logStorage.append(buffer);
    onLogStorageAppendedConditions.signalConsumers();
    //TODO: Check if the commitPosition is correct;
    lastCommittedPosition = commitPosition;
    logStream.setCommitPosition(commitPosition);
    LOG.info("Writing commited logentry with commit position {}", commitPosition);
  }

  public ActorFuture<?> close() {
    distributedLog.removeListener(this);
    return actor.close();
  }
}
