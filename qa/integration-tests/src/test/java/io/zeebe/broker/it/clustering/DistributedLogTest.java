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
  public ClientApiRule apiRule = new ClientApiRule(() -> clusteringRule.getBrokers().iterator().next().getConfig().getNetwork().getClient().toSocketAddress());



  @Rule
  public RuleChain ruleChain =
    RuleChain.outerRule(testTimeout).around(clusteringRule).around(apiRule);


  @Test
  public void shouldReplicateLog(){

    //Write something to the logstream
    final ProcessBuilder processBuilder = Bpmn.createExecutableProcess();
    final BpmnModelInstance process =
      processBuilder.startEvent().endEvent().done();

    // when
    final ExecuteCommandResponse resp = apiRule.partitionClient().deployWithResponse(process);

    //And read from logstream
    Collection<Broker> brokers = clusteringRule.getBrokers();
    for(Broker broker: brokers){
      LogStreamPrinter.printRecords(broker, 0);
    }
  }
}
