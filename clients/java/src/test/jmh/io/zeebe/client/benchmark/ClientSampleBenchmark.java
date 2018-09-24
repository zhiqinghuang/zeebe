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
package io.zeebe.client.benchmark;

import io.zeebe.client.api.ZeebeFuture;
import io.zeebe.client.api.commands.Topology;
import io.zeebe.client.api.events.DeploymentEvent;
import io.zeebe.client.api.response.CreateJobResponse;
import io.zeebe.client.api.response.CreateWorkflowInstanceResponse;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(Threads.MAX)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ClientSampleBenchmark {

  @Benchmark
  public ZeebeFuture<Topology> requestTopology(BenchmarkContext context) {
    return context.requestTopology();
  }

  @Benchmark
  public DeploymentEvent deployWorkflow(BenchmarkContext context) {
    return context.deployWorkflow(BenchmarkContext.WORKFLOW, "workflow.bpmn").join();
  }

  @Benchmark
  public CreateWorkflowInstanceResponse createWorkflowInstanceByKey(BenchmarkContext context) {
    return context.createWorkflowInstanceByKey(1).join();
  }

  @Benchmark
  public CreateWorkflowInstanceResponse createWorkflowInstanceByBpmnIdLastVersion(
      BenchmarkContext context) {
    return context.createWorkflowInstanceByLatestVersion(1).join();
  }

  @Benchmark
  public CreateWorkflowInstanceResponse createWorkflowInstanceByBpmnIdAndVersion(
      BenchmarkContext context) {
    return context.createWorkflowInstanceByVersion(1).join();
  }

  @Benchmark
  public CreateJobResponse createJob(BenchmarkContext context) {
    return context.createJob().join();
  }

  @Benchmark
  public Void publishMessage(BenchmarkContext context) {
    return context.publishMessage("testMessage", "123").join();
  }
}
