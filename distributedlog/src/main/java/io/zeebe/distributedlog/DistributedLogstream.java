package io.zeebe.distributedlog;

import io.atomix.primitive.SyncPrimitive;

public interface DistributedLogstream extends SyncPrimitive {

  void append(String bytes);

  @Override
  AsyncDistributedLogstream async();
}
