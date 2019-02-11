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
package io.zeebe.broker.it.clustering;

import io.zeebe.broker.Broker;
import io.zeebe.broker.it.GrpcClientRule;
import io.zeebe.broker.util.LogStreamPrinter;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.builder.ProcessBuilder;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.clientapi.ExecuteCommandResponse;
import java.util.Collection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class DistributedLogTest {

  public Timeout testTimeout = Timeout.seconds(60);
  public ClusteringRule clusteringRule = new ClusteringRule(1, 3, 3);
  public GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);
  public ClientApiRule apiRule =
      new ClientApiRule(
          () ->
              clusteringRule
                  .getBrokers()
                  .iterator()
                  .next()
                  .getConfig()
                  .getNetwork()
                  .getClient()
                  .toSocketAddress());

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(testTimeout).around(clusteringRule).around(apiRule);

  @Test
  public void shouldReplicateLog() {

    // Write something to the logstream
    final ProcessBuilder processBuilder = Bpmn.createExecutableProcess();
    final BpmnModelInstance process = processBuilder.startEvent().endEvent().done();

    // when
    final ExecuteCommandResponse resp = apiRule.partitionClient().deployWithResponse(process);

    // TODO: read from logstreams and check if replication was successful
    final Collection<Broker> brokers = clusteringRule.getBrokers();
    for (Broker broker : brokers) {
      LogStreamPrinter.printRecords(broker, 0);
    }
  }
}
