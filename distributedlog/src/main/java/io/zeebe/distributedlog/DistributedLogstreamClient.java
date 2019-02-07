package io.zeebe.distributedlog;

import io.atomix.primitive.event.Event;

public interface DistributedLogstreamClient {

  @Event
  void appended();

  @Event
  void failed();

}
