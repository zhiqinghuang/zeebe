package io.zeebe.distributedlog.impl;

import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.utils.concurrent.Futures;
import io.zeebe.distributedlog.AsyncDistributedLogstream;
import io.zeebe.distributedlog.DistributedLogstream;
import io.zeebe.distributedlog.DistributedLogstreamClient;
import io.zeebe.distributedlog.DistributedLogstreamService;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLogstreamProxy
    extends AbstractAsyncPrimitive<AsyncDistributedLogstream, DistributedLogstreamService>
    implements AsyncDistributedLogstream, DistributedLogstreamClient {

  private CompletableFuture<Void> appendFuture;

  private static final Logger LOG = LoggerFactory.getLogger(DistributedLogstreamProxy.class);

  protected DistributedLogstreamProxy(
      ProxyClient<DistributedLogstreamService> client, PrimitiveRegistry registry) {
    super(client, registry);
  }

  @Override
  public CompletableFuture<Void> append(String bytes) {
    appendFuture = new CompletableFuture<Void>();

    getProxyClient()
        .acceptBy(name(), service -> service.append(bytes))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                appendFuture.completeExceptionally(error);
                LOG.error("Append completed with an error.", error);
              } else {
                appendFuture.complete(null);
                LOG.debug("Append was successful.");
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
  public void failed() {
    if (appendFuture != null) {
      appendFuture.complete(null);
    }
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
