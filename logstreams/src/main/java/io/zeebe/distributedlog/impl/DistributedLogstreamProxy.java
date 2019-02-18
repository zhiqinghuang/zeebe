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

import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.utils.concurrent.Futures;
import io.zeebe.distributedlog.AsyncDistributedLogstream;
import io.zeebe.distributedlog.CommitLogEvent;
import io.zeebe.distributedlog.DistributedLogstream;
import io.zeebe.distributedlog.DistributedLogstreamClient;
import io.zeebe.distributedlog.DistributedLogstreamService;
import io.zeebe.distributedlog.LogEventListener;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLogstreamProxy
    extends AbstractAsyncPrimitive<AsyncDistributedLogstream, DistributedLogstreamService>
    implements AsyncDistributedLogstream, DistributedLogstreamClient {

  private CompletableFuture<Void> appendFuture;

  private static final Logger LOG = LoggerFactory.getLogger(DistributedLogstreamProxy.class);
  private Set<LogEventListener> eventListeners;

  protected DistributedLogstreamProxy(
      ProxyClient<DistributedLogstreamService> client, PrimitiveRegistry registry) {
    super(client, registry);
    eventListeners = new LinkedHashSet<>();
  }

  @Override
  public CompletableFuture<Void> append(long commitPosition, ByteBuffer blockBuffer) {
    appendFuture = new CompletableFuture<Void>();

    final byte[] buffer = new byte[blockBuffer.remaining()];
    blockBuffer.get(buffer);

    getProxyClient()
        .acceptBy(name(), service -> service.append(commitPosition, buffer))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                appendFuture.completeExceptionally(error);
                LOG.error("Append completed with an error.", error);
              } else {
                appendFuture.complete(null);
              }
            });
    return appendFuture.thenApply(result -> result).whenComplete((r, e) -> appendFuture = null);
  }

  @Override
  public DistributedLogstream sync() {
    return sync(Duration.ofMillis(10000));
  }

  @Override
  public DistributedLogstream sync(Duration duration) {
    return new BlockingDistributedLogstream(this, duration.toMillis());
  }

  @Override
  public void appended() {
    if (appendFuture != null) {
      appendFuture.complete(null);
    }
  }

  @Override
  public void change(CommitLogEvent event) {
    eventListeners.forEach(listener -> listener.onCommit(event));
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(LogEventListener listener) {
    if (eventListeners.isEmpty()) {
      eventListeners.add(listener);
      return getProxyClient().acceptBy(name(), service -> service.listen()).thenApply(v -> null);
    } else {
      eventListeners.add(listener);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(LogEventListener listener) {
    if (eventListeners.remove(listener) && eventListeners.isEmpty()) {
      return getProxyClient().acceptBy(name(), service -> service.unlisten()).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<AsyncDistributedLogstream> connect() {
    return super.connect()
        .thenCompose(
            v ->
                Futures.allOf(
                    this.getProxyClient().getPartitions().stream().map(ProxySession::connect)))
        .thenApply(v -> this);
  }
}
