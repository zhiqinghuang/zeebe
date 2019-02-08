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

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;
import io.zeebe.distributedlog.DistributedLogstreamType;
import io.zeebe.logstreams.log.LogStream;

public class DistributedLogstreamConfig extends PrimitiveConfig<DistributedLogstreamConfig> {

  private LogStream logStream;

  public DistributedLogstreamConfig() {
    super();
  }

  public DistributedLogstreamConfig(LogStream logStream) {
    // TODO: identify logStreams for different partitions
    this.logStream = logStream;
  }

  @Override
  public PrimitiveType getType() {
    return DistributedLogstreamType.instance();
  }

  public LogStream getLogStream() {
    return logStream;
  }

  public void setLogStream(LogStream logStream) {
    this.logStream = logStream;
  }
}
