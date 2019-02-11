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
package io.zeebe.distributedlog;

import io.zeebe.logstreams.log.LogStream;

public class DistributedLog {

  private static LogStream logStreamForPartition0;
  private static DistributedLogstream distributedLog;

  public static LogStream getLogStreamForPartition0() {
    return logStreamForPartition0;
  }

  public static void setLogStreamForPartition0(LogStream logStreamForPartition0) {
    DistributedLog.logStreamForPartition0 = logStreamForPartition0;
  }

  public static DistributedLogstream getDistributedLog() {
    return distributedLog;
  }

  public static void setDistributedLog(DistributedLogstream distributedLog) {
    DistributedLog.distributedLog = distributedLog;
  }
}
