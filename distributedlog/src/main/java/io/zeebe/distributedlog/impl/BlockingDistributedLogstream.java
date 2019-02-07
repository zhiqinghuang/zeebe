package io.zeebe.distributedlog.impl;

import io.atomix.primitive.Synchronous;
import io.zeebe.distributedlog.AsyncDistributedLogstream;
import io.zeebe.distributedlog.DistributedLogstream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BlockingDistributedLogstream extends Synchronous<AsyncDistributedLogstream>
    implements DistributedLogstream {

  private final DistributedLogstreamProxy distributedLogstreamProxy;
  private final long timeout;

  public BlockingDistributedLogstream(DistributedLogstreamProxy distributedLogstreamProxy, long timeout) {
    super(distributedLogstreamProxy);
    this.distributedLogstreamProxy = distributedLogstreamProxy;
    this.timeout = timeout;
  }

  @Override
  public void append(String bytes) {

    try {
       distributedLogstreamProxy.append(bytes).get(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    }
    return;
  }

  @Override
  public AsyncDistributedLogstream async() {
    return distributedLogstreamProxy;
  }
}
