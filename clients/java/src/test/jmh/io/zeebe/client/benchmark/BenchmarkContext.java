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
import io.zeebe.client.api.ZeebeFuture;
import io.zeebe.client.api.commands.Topology;
import io.zeebe.client.api.commands.Workflow;
import io.zeebe.client.api.events.DeploymentEvent;
import io.zeebe.client.api.response.CreateJobResponse;
import io.zeebe.client.api.response.CreateWorkflowInstanceResponse;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.util.FileUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class BenchmarkContext {

  public static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("testProcess").startEvent().endEvent().done();

  private Broker broker;
  private Path tempFolder;
  private ZeebeClient client;
  private List<Workflow> workflows;

  public ZeebeClient getClient() {
    return client;
  }

  @Setup
  public void setUp() throws IOException {
    Configurator.setLevel("io.zeebe", Level.WARN);
    final BrokerCfg brokerCfg = new BrokerCfg();
    tempFolder = Files.createTempDirectory("zeebe-jmh");
    broker = new Broker(brokerCfg, tempFolder.toAbsolutePath().toString(), null);
    final String contactPoint = brokerCfg.getNetwork().getGateway().toSocketAddress().toString();
    client = ZeebeClient.newClientBuilder().brokerContactPoint(contactPoint).build();

    workflows = deployWorkflows();
  }

  @TearDown
  public void tearDown() throws IOException {
    client.close();
    broker.close();
    FileUtil.deleteFolder(tempFolder.toAbsolutePath().toString());
  }

  private List<Workflow> deployWorkflows() {
    return IntStream.range(0, 3)
        .boxed()
        .map(i -> deployWorkflow(WORKFLOW, "workflow.bpmn").join().getDeployedWorkflows().get(0))
        .collect(Collectors.toList());
  }

  public ZeebeFuture<Topology> requestTopology() {
    return client.newTopologyRequest().send();
  }

  public ZeebeFuture<DeploymentEvent> deployWorkflow(
      final BpmnModelInstance modelInstance, final String resourceName) {
    return client
        .workflowClient()
        .newDeployCommand()
        .addWorkflowModel(modelInstance, resourceName)
        .send();
  }

  public ZeebeFuture<CreateWorkflowInstanceResponse> createWorkflowInstanceByKey(
      final int workflowIndex) {
    final Workflow workflow = workflows.get(workflowIndex);
    return client
        .workflowClient()
        .newCreateInstanceCommand()
        .workflowKey(workflow.getWorkflowKey())
        .payload(new Payload())
        .send();
  }

  public ZeebeFuture<CreateWorkflowInstanceResponse> createWorkflowInstanceByVersion(
      final int workflowIndex) {
    final Workflow workflow = workflows.get(workflowIndex);
    return client
        .workflowClient()
        .newCreateInstanceCommand()
        .bpmnProcessId(workflow.getBpmnProcessId())
        .version(workflow.getVersion())
        .payload(new Payload())
        .send();
  }

  public ZeebeFuture<CreateWorkflowInstanceResponse> createWorkflowInstanceByLatestVersion(
      final int workflowIndex) {
    final Workflow workflow = workflows.get(workflowIndex);
    return client
        .workflowClient()
        .newCreateInstanceCommand()
        .bpmnProcessId(workflow.getBpmnProcessId())
        .latestVersion()
        .payload(new Payload())
        .send();
  }

  public ZeebeFuture<CreateJobResponse> createJob() {
    return client
        .jobClient()
        .newCreateCommand()
        .jobType("jmh")
        .payload(new Payload())
        .addCustomHeader("foo", 123)
        .addCustomHeader("bar", "bazz")
        .send();
  }

  public ZeebeFuture<Void> publishMessage(final String messageName, final String correlationKey) {
    return client
        .workflowClient()
        .newPublishMessageCommand()
        .messageName(messageName)
        .correlationKey(correlationKey)
        .payload(new Payload())
        .send();
  }

  public static class Payload {
    public int marks = 123;
    public String orderId = "fooBar-123";
    public Map<String, Object> nested = Collections.singletonMap("hello", "world");
  }
}
