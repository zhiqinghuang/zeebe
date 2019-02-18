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
package io.zeebe.test.broker.protocol.clientapi;

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.ControlMessageType;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.SubscriptionUtil;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.deployment.ResourceType;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.JobBatchIntent;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.broker.protocol.MsgPackHelper;
import io.zeebe.test.util.MsgPackUtil;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.SocketAddress;
import io.zeebe.transport.Transports;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.clock.ControlledActorClock;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.junit.rules.ExternalResource;

public class ClientApiRule extends ExternalResource {

  private static final String DEFAULT_WORKER = "defaultWorker";
  public static final long DEFAULT_LOCK_DURATION = 10000L;

  protected ClientTransport transport;

  protected final int nodeId;
  protected final Supplier<SocketAddress> brokerAddressSupplier;
  protected final int partitionCount;
  protected int nextPartitionId = 0;

  protected MsgPackHelper msgPackHelper;

  private final Int2ObjectHashMap<PartitionTestClient> testPartitionClients =
      new Int2ObjectHashMap<>();
  private final ControlledActorClock controlledActorClock = new ControlledActorClock();
  private ActorScheduler scheduler;

  protected int defaultPartitionId = -1;

  public ClientApiRule(final Supplier<SocketAddress> brokerAddressSupplier) {
    this(0, brokerAddressSupplier);
  }

  public ClientApiRule(final int nodeId, final Supplier<SocketAddress> brokerAddressSupplier) {
    this(nodeId, 1, brokerAddressSupplier);
  }

  public ClientApiRule(
      int nodeId, int partitionCount, Supplier<SocketAddress> brokerAddressSupplier) {
    this.nodeId = nodeId;
    this.partitionCount = partitionCount;
    this.brokerAddressSupplier = brokerAddressSupplier;
  }

  @Override
  protected void before() throws Throwable {
    scheduler =
        ActorScheduler.newActorScheduler()
            .setCpuBoundActorThreadCount(1)
            .setActorClock(controlledActorClock)
            .build();
    scheduler.start();

    transport = Transports.newClientTransport("gateway").scheduler(scheduler).build();

    msgPackHelper = new MsgPackHelper();
    transport.registerEndpoint(nodeId, brokerAddressSupplier.get());

    final List<Integer> partitionIds = doRepeatedly(this::getPartitionIds).until(p -> !p.isEmpty());
    defaultPartitionId = partitionIds.get(0);
  }

  @Override
  protected void after() {
    if (transport != null) {
      transport.close();
    }

    if (scheduler != null) {
      scheduler.stop();
    }
  }

  /** targets the default partition by default */
  public ExecuteCommandRequestBuilder createCmdRequest() {
    return new ExecuteCommandRequestBuilder(transport.getOutput(), nodeId, msgPackHelper)
        .partitionId(defaultPartitionId);
  }

  public ExecuteCommandRequestBuilder createCmdRequest(int partition) {
    return new ExecuteCommandRequestBuilder(transport.getOutput(), nodeId, msgPackHelper)
        .partitionId(partition);
  }

  public ControlMessageRequestBuilder createControlMessageRequest() {
    return new ControlMessageRequestBuilder(transport.getOutput(), nodeId, msgPackHelper);
  }

  public PartitionTestClient partitionClient() {
    return partitionClient(defaultPartitionId);
  }

  public PartitionTestClient partitionClient(final int partitionId) {
    if (!testPartitionClients.containsKey(partitionId)) {
      testPartitionClients.put(partitionId, new PartitionTestClient(this, partitionId));
    }
    return testPartitionClients.get(partitionId);
  }

  public ExecuteCommandRequest activateJobs(final String type) {
    return activateJobs(defaultPartitionId, type, DEFAULT_LOCK_DURATION);
  }

  public ExecuteCommandRequest activateJobs(
      final int partitionId, final String type, final long lockDuration, final int amount) {
    // to make sure that job already exist
    partitionClient(partitionId)
        .receiveJobs()
        .withIntent(JobIntent.CREATED)
        .withType(type)
        .getFirst();

    return createCmdRequest(partitionId)
        .type(ValueType.JOB_BATCH, JobBatchIntent.ACTIVATE)
        .command()
        .put("type", type)
        .put("worker", DEFAULT_WORKER)
        .put("timeout", lockDuration)
        .put("amount", amount)
        .put("jobs", Collections.emptyList())
        .done()
        .send();
  }

  public ExecuteCommandRequest activateJobs(
      final int partitionId, final String type, final long lockDuration) {
    return activateJobs(partitionId, type, lockDuration, 10);
  }

  public void waitForPartition(final int partitions) {
    waitUntil(() -> getPartitionIds().size() >= partitions);
  }

  @SuppressWarnings("unchecked")
  public List<Integer> getPartitionIds() {
    try {
      final ControlMessageResponse response = requestTopology();

      final Map<String, Object> data = response.getData();
      final int partitionsCount = ((Long) data.get("partitionsCount")).intValue();

      return IntStream.range(0, partitionsCount).boxed().collect(Collectors.toList());
    } catch (final Exception e) {
      return Collections.EMPTY_LIST;
    }
  }

  public ControlMessageResponse requestTopology() {
    return createControlMessageRequest()
        .partitionId(Protocol.DEPLOYMENT_PARTITION)
        .messageType(ControlMessageType.REQUEST_TOPOLOGY)
        .data()
        .done()
        .sendAndAwait();
  }

  public int getDefaultPartitionId() {
    return defaultPartitionId;
  }

  public ClientTransport getTransport() {
    return transport;
  }

  public ControlledActorClock getClock() {
    return controlledActorClock;
  }

  /** @return the workflow key */
  public long deployWorkflow(BpmnModelInstance modelInstance) {
    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    Bpmn.writeModelToStream(outStream, modelInstance);
    final byte[] resource = outStream.toByteArray();

    final Map<String, Object> deploymentResource = new HashMap<>();
    deploymentResource.put("resource", resource);
    deploymentResource.put("resourceType", ResourceType.BPMN_XML.name());
    deploymentResource.put("resourceName", "testProcess.bpmn");

    final ExecuteCommandResponse commandResponse =
        createCmdRequest()
            .partitionId(Protocol.DEPLOYMENT_PARTITION)
            .type(ValueType.DEPLOYMENT, DeploymentIntent.CREATE)
            .command()
            .put(DeploymentRecord.RESOURCES, Collections.singletonList(deploymentResource))
            .done()
            .sendAndAwait();

    return commandResponse.getKey();
  }

  public long createWorkflowInstance(long workflowKey, DirectBuffer payload) {
    final ExecuteCommandResponse response =
        createCmdRequest()
            .partitionId(nextPartitionId())
            .type(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.CREATE)
            .command()
            .put(WorkflowInstanceRecord.PROP_WORKFLOW_KEY, workflowKey)
            .put(WorkflowInstanceRecord.PROP_WORKFLOW_PAYLOAD, BufferUtil.bufferAsArray(payload))
            .done()
            .sendAndAwait();

    assertThat(response.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.getIntent()).isEqualTo(WorkflowInstanceIntent.ELEMENT_ACTIVATING);

    return response.getKey();
  }

  public void publishMessage(String messageName, String correlationKey) {
    publishMessage(messageName, correlationKey, MsgPackUtil.asMsgPack("{}"));
  }

  public void publishMessage(String messageName, String correlationKey, DirectBuffer payload) {
    publishMessage(messageName, correlationKey, payload, Duration.ofHours(1).toMillis());
  }

  public void publishMessage(
      String messageName, String correlationKey, DirectBuffer payload, long ttl) {
    final ExecuteCommandResponse response =
        createCmdRequest()
            .partitionId(partitionForCorrelationKey(correlationKey))
            .type(ValueType.MESSAGE, MessageIntent.PUBLISH)
            .command()
            .put("name", messageName)
            .put("correlationKey", correlationKey)
            .put("timeToLive", ttl)
            .put("payload", BufferUtil.bufferAsArray(payload))
            .done()
            .sendAndAwait();

    assertThat(response.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.getIntent()).isEqualTo(MessageIntent.PUBLISHED);
  }

  public void cancelWorkflowInstance(long workflowInstanceKey) {
    final int partitionId = RecordingExporter
      .workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
      .withWorkflowInstanceKey(workflowInstanceKey)
      .getFirst()
      .getMetadata().getPartitionId();

    final ExecuteCommandResponse response = createCmdRequest()
      .partitionId(partitionId)
      .type(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.CANCEL)
      .key(workflowInstanceKey)
      .command()
      .done()
      .sendAndAwait();

    assertThat(response.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.getIntent()).isEqualTo(WorkflowInstanceIntent.ELEMENT_TERMINATING);
  }

  protected int nextPartitionId() {
    return nextPartitionId++ % partitionCount;
  }

  protected int partitionForCorrelationKey(String correlationKey) {
    return SubscriptionUtil.getSubscriptionPartitionId(
        BufferUtil.wrapString(correlationKey), partitionCount);
  }
}
