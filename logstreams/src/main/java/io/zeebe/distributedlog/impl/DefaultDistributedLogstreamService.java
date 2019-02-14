/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.distributedlog.impl;

import com.google.common.collect.Sets;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.SessionId;
import io.zeebe.distributedlog.DistributedLog;
import io.zeebe.distributedlog.DistributedLogstreamClient;
import io.zeebe.distributedlog.DistributedLogstreamService;
import io.zeebe.distributedlog.DistributedLogstreamType;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import java.nio.ByteBuffer;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedLogstreamService
    extends AbstractPrimitiveService<DistributedLogstreamClient>
    implements DistributedLogstreamService {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultDistributedLogstreamService.class);

  private LogStream logstream;
  private LogStorage logStorage;

  protected Set<SessionId> listeners = Sets.newLinkedHashSet();

  private long currentPosition;

  public DefaultDistributedLogstreamService(DistributedLogstreamServiceConfig config) {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);
    currentPosition = 0;
    this.logstream = DistributedLog.getLogStreamForPartition0(); // TODO: this is temporary hack.
    this.logStorage = logstream.getLogStorage();
    LOG.info(
        "ConfigId {}, I will write to logstream {}", config.getConfigId(), logstream.getLogName());
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    super.configure(executor);
    LOG.info("Creating log file for {}. I'm member {}", getServiceName(), this.getLocalMemberId());
  }

  @Override
  public void append(byte[] blockBuffer) {
    // UnsafeBuffer buffer = new UnsafeBuffer(blockBuffer);
    final ByteBuffer buffer = ByteBuffer.wrap(blockBuffer);
    final long position = logStorage.append(buffer);
    if (position > 0) {
      currentPosition = position;
      LOG.info("Appended at position {}", currentPosition);
      publish(blockBuffer);
    } else {
      LOG.info("Error Appending : {} ", position);
    }
  }

  @Override
  public void listen() {
    listeners.add(getCurrentSession().sessionId());
  }

  @Override
  public void unlisten() {
    listeners.remove(getCurrentSession().sessionId());
  }

  private void publish(byte[] appendBytes) {
    listeners.forEach(listener -> getSession(listener).accept(client -> client.change(appendBytes)));
  }

  @Override
  public void backup(BackupOutput backupOutput) {
    // TODO
  }

  @Override
  public void restore(BackupInput backupInput) {
    // TODO
  }

  @Override
  public void close() {
    super.close();
    LOG.info("Closing {}", getServiceName());
  }
}
