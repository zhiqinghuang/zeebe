package io.zeebe.distributedlog;

import io.atomix.primitive.AsyncPrimitive;
import java.util.concurrent.CompletableFuture;

public interface AsyncDistributedLogstream extends AsyncPrimitive {

  CompletableFuture<Void> append(String bytes);

  @Override
  DistributedLogstream sync();
}
