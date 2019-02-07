package io.zeebe.distributedlog.impl;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.zeebe.distributedlog.AsyncDistributedLogstream;
import io.zeebe.distributedlog.DistributedLogstream;
import io.zeebe.distributedlog.DistributedLogstreamBuilder;
import io.zeebe.distributedlog.DistributedLogstreamService;
import io.zeebe.logstreams.log.LogStream;
import java.util.concurrent.CompletableFuture;

public class DefaultDistributedLogstreamBuilder extends DistributedLogstreamBuilder {

  public DefaultDistributedLogstreamBuilder(
      String name,
      DistributedLogstreamConfig config,
      PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  public CompletableFuture<DistributedLogstream> buildAsync() {
    return newProxy(
            DistributedLogstreamService.class,
            new DistributedLogstreamServiceConfig(config.getLogStream()))
        .thenCompose(
            proxyClient ->
                new DistributedLogstreamProxy(proxyClient, managementService.getPrimitiveRegistry())
                    .connect())
        .thenApply(AsyncDistributedLogstream::sync);
  }

  @Override
  public DistributedLogstreamBuilder withProtocol(ProxyProtocol proxyProtocol) {
    return this.withProtocol((PrimitiveProtocol) proxyProtocol);
  }

  public DistributedLogstreamBuilder withLogStream(LogStream logStream) {
    config.setLogStream(logStream);
    return this;
  }
}
