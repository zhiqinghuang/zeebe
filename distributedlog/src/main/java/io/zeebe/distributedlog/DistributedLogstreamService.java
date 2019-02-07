package io.zeebe.distributedlog;

import io.atomix.primitive.operation.Command;

public interface DistributedLogstreamService {

  @Command
  void append(String bytes);

}
