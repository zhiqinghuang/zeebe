/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.workflow.variables;

import static io.zeebe.broker.workflow.gateway.ParallelGatewayStreamProcessorTest.PROCESS_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.exporter.record.Assertions;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.value.VariableRecordValue;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.intent.VariableIntent;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.clientapi.PartitionTestClient;
import io.zeebe.test.util.record.RecordingExporter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class WorkflowInstanceVariableTest {

  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess(PROCESS_ID)
          .startEvent()
          .serviceTask("task", t -> t.zeebeTaskType("test"))
          .endEvent()
          .done();

  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
  public ClientApiRule apiRule = new ClientApiRule(brokerRule::getClientAddress);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

  private PartitionTestClient testClient;

  @Before
  public void init() {
    testClient = apiRule.partitionClient();
  }

  @Test
  public void shouldCreateVariableByWorkflowInstanceCreation() {
    // given
    testClient.deploy(WORKFLOW);

    // when
    final long workflowInstanceKey = testClient.createWorkflowInstance(PROCESS_ID, "{'x':1}");

    // then
    final Record<VariableRecordValue> variableRecord =
        RecordingExporter.variableRecords(VariableIntent.CREATED).getFirst();
    Assertions.assertThat(variableRecord.getValue())
        .hasScopeKey(workflowInstanceKey)
        .hasName("x")
        .hasValue("1");
  }

  @Test
  public void shouldCreateVariableByJobCompletion() {
    // given
    testClient.deploy(WORKFLOW);

    final long workflowInstanceKey = testClient.createWorkflowInstance(PROCESS_ID);

    // when
    testClient.completeJobOfType("test", "{'x':1}");

    // then
    final Record<VariableRecordValue> variableRecord =
        RecordingExporter.variableRecords(VariableIntent.CREATED).getFirst();
    Assertions.assertThat(variableRecord.getValue())
        .hasScopeKey(workflowInstanceKey)
        .hasName("x")
        .hasValue("1");
  }

  @Test
  public void shouldCreateVariableByOutputMapping() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess(PROCESS_ID)
            .startEvent()
            .serviceTask("task", t -> t.zeebeTaskType("test").zeebeOutput("$.x", "$.y"))
            .endEvent()
            .done());

    final long workflowInstanceKey = testClient.createWorkflowInstance(PROCESS_ID);

    // when
    testClient.completeJobOfType("test", "{'x':1}");

    // then
    final Record<VariableRecordValue> variableRecord =
        RecordingExporter.variableRecords(VariableIntent.CREATED).withName("y").getFirst();
    Assertions.assertThat(variableRecord.getValue())
        .hasScopeKey(workflowInstanceKey)
        .hasName("y")
        .hasValue("1");
  }

  @Test
  public void shouldCreateVariableByUpdatePayload() {
    // given
    testClient.deploy(WORKFLOW);

    final long workflowInstanceKey = testClient.createWorkflowInstance(PROCESS_ID);

    // when
    testClient.updatePayload(workflowInstanceKey, "{'x':1}");

    // then
    final Record<VariableRecordValue> variableRecord =
        RecordingExporter.variableRecords(VariableIntent.CREATED).getFirst();
    Assertions.assertThat(variableRecord.getValue())
        .hasScopeKey(workflowInstanceKey)
        .hasName("x")
        .hasValue("1");
  }

  @Test
  public void shouldCreateMultipleVariablesFromPayload() {
    // given
    testClient.deploy(WORKFLOW);

    // when
    testClient.createWorkflowInstance(PROCESS_ID, "{'x':1, 'y':2}");

    // then
    assertThat(RecordingExporter.variableRecords(VariableIntent.CREATED).limit(2))
        .extracting(Record::getValue)
        .extracting(v -> tuple(v.getName(), v.getValue()))
        .hasSize(2)
        .contains(tuple("x", "1"), tuple("y", "2"));
  }

  @Test
  public void shouldUpdateVariableByJobCompletion() {
    // given
    testClient.deploy(WORKFLOW);

    final long workflowInstanceKey = testClient.createWorkflowInstance(PROCESS_ID, "{'x':1}");

    // when
    testClient.completeJobOfType("test", "{'x':2}");

    // then
    final Record<VariableRecordValue> variableRecord =
        RecordingExporter.variableRecords(VariableIntent.UPDATED).getFirst();
    Assertions.assertThat(variableRecord.getValue())
        .hasScopeKey(workflowInstanceKey)
        .hasName("x")
        .hasValue("2");
  }

  @Test
  public void shouldUpdateVariableByOutputMapping() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess(PROCESS_ID)
            .startEvent()
            .serviceTask("task", t -> t.zeebeTaskType("test").zeebeOutput("$.x", "$.y"))
            .endEvent()
            .done());

    final long workflowInstanceKey = testClient.createWorkflowInstance(PROCESS_ID, "{'y':1}");

    // when
    testClient.completeJobOfType("test", "{'x':2}");

    // then
    final Record<VariableRecordValue> variableRecord =
        RecordingExporter.variableRecords(VariableIntent.UPDATED).getFirst();
    Assertions.assertThat(variableRecord.getValue())
        .hasScopeKey(workflowInstanceKey)
        .hasName("y")
        .hasValue("2");
  }

  @Test
  public void shouldUpdateVariableByUpdatePayload() {
    // given
    testClient.deploy(WORKFLOW);

    final long workflowInstanceKey = testClient.createWorkflowInstance(PROCESS_ID, "{'x':1}");

    // when
    testClient.updatePayload(workflowInstanceKey, "{'x':2}");

    // then
    final Record<VariableRecordValue> variableRecord =
        RecordingExporter.variableRecords(VariableIntent.UPDATED).getFirst();
    Assertions.assertThat(variableRecord.getValue())
        .hasScopeKey(workflowInstanceKey)
        .hasName("x")
        .hasValue("2");
  }

  @Test
  public void shouldUpdateMultipleVariablesFromPayload() {
    // given
    testClient.deploy(WORKFLOW);

    testClient.createWorkflowInstance(PROCESS_ID, "{'x':1, 'y':2, 'z':3}");

    // when
    testClient.completeJobOfType("test", "{'x':1, 'y':4, 'z':5}");

    // then
    assertThat(RecordingExporter.variableRecords(VariableIntent.UPDATED).limit(2))
        .extracting(Record::getValue)
        .extracting(v -> tuple(v.getName(), v.getValue()))
        .hasSize(2)
        .contains(tuple("y", "4"), tuple("z", "5"));
  }

  @Test
  public void shouldCreateAndUpdateVariablesFromPayload() {
    // given
    testClient.deploy(WORKFLOW);

    final long workflowInstanceKey = testClient.createWorkflowInstance(PROCESS_ID, "{'x':1}");

    final Record<VariableRecordValue> variableCreated =
        RecordingExporter.variableRecords(VariableIntent.CREATED).getFirst();

    // when
    testClient.updatePayload(workflowInstanceKey, "{'x':2, 'y':3}");

    // then
    assertThat(
            RecordingExporter.variableRecords()
                .skipUntil(r -> r.getPosition() > variableCreated.getPosition())
                .limit(2))
        .extracting(
            record ->
                tuple(
                    record.getMetadata().getIntent(),
                    record.getValue().getName(),
                    record.getValue().getValue()))
        .hasSize(2)
        .contains(tuple(VariableIntent.UPDATED, "x", "2"), tuple(VariableIntent.CREATED, "y", "3"));
  }
}
