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

import io.zeebe.broker.Broker;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.commands.Workflow;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.util.FileUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 4, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(Threads.MAX)
public class ClientBenchmark {

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void createWorkflowInstanceByKeyThroughput(Context context) {
    createWorkflowInstanceByKey(context);
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void createWorkflowInstanceByKeySample(Context context) {
    createWorkflowInstanceByKey(context);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void createWorkflowInstanceByBpmnIdLastVersionThroughput(Context context) {
    createWorkflowInstanceByBpmnIdLastVersion(context);
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void createWorkflowInstanceByBpmnIdLastVersionSample(Context context) {
    createWorkflowInstanceByBpmnIdLastVersion(context);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void createWorkflowInstanceByBpmnIdAndVersionThroughput(Context context) {
    createWorkflowInstanceByBpmnIdAndVersion(context);
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void createWorkflowInstanceByBpmnIdAndVersionSample(Context context) {
    createWorkflowInstanceByBpmnIdAndVersion(context);
  }

  public void createWorkflowInstanceByKey(Context context) {
    context
        .getClient()
        .workflowClient()
        .newCreateInstanceCommand()
        .workflowKey(context.getWorkflows().get(1).getWorkflowKey())
        .payload(Collections.singletonMap("foo", "bar"))
        .send()
        .join();
  }

  public void createWorkflowInstanceByBpmnIdLastVersion(Context context) {
    context
        .getClient()
        .workflowClient()
        .newCreateInstanceCommand()
        .bpmnProcessId(context.getWorkflows().get(1).getBpmnProcessId())
        .latestVersion()
        .payload(Collections.singletonMap("foo", "bar"))
        .send()
        .join();
  }

  public void createWorkflowInstanceByBpmnIdAndVersion(Context context) {
    final Workflow workflow = context.getWorkflows().get(1);
    context
        .getClient()
        .workflowClient()
        .newCreateInstanceCommand()
        .bpmnProcessId(workflow.getBpmnProcessId())
        .version(workflow.getVersion())
        .payload(Collections.singletonMap("foo", "bar"))
        .send()
        .join();
  }

  @State(Scope.Benchmark)
  public static class Context {

    private Broker broker;
    private Path tempFolder;
    private ZeebeClient client;
    private List<Workflow> workflows;

    public ZeebeClient getClient() {
      return client;
    }

    public List<Workflow> getWorkflows() {
      return workflows;
    }

    @Setup
    public void setUp() throws IOException {
      Configurator.setLevel("io.zeebe", Level.WARN);
      final BrokerCfg brokerCfg = new BrokerCfg();
      tempFolder = Files.createTempDirectory("zeebe-jmh");
      broker = new Broker(brokerCfg, tempFolder.toAbsolutePath().toString(), null);
      client =
          ZeebeClient.newClientBuilder()
              .brokerContactPoint(brokerCfg.getNetwork().getGateway().toSocketAddress().toString())
              .build();

      workflows = deployWorkflows();
    }

    @TearDown
    public void tearDown() throws IOException {
      client.close();
      broker.close();
      FileUtil.deleteFolder(tempFolder.toAbsolutePath().toString());
    }

    private List<Workflow> deployWorkflows() {
      final BpmnModelInstance workflow =
          Bpmn.createExecutableProcess("testProcess").startEvent().endEvent().done();

      return IntStream.range(0, 3)
          .boxed()
          .map(
              i ->
                  client
                      .workflowClient()
                      .newDeployCommand()
                      .addWorkflowModel(workflow, "workflow.bpmn")
                      .send()
                      .join()
                      .getDeployedWorkflows()
                      .get(0))
          .collect(Collectors.toList());
    }
  }
}
