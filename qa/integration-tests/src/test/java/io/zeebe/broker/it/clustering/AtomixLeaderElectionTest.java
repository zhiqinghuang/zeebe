package io.zeebe.broker.it.clustering;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.followerPartitionServiceName;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.leaderPartitionServiceName;
import static io.zeebe.raft.RaftServiceNames.leaderOpenLogStreamServiceName;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.Broker;
import io.zeebe.servicecontainer.ServiceContainer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class AtomixLeaderElectionTest {


  public Timeout testTimeout = Timeout.seconds(60);
  public ClusteringRule clusteringRule = new ClusteringRule();

  @Rule
  public RuleChain ruleChain =
    RuleChain.outerRule(testTimeout).around(clusteringRule);

  @Test
  public void shouldStartLeaderServices() {
    // given
    final int partition = 1;
    final int leader = clusteringRule.getLeaderForPartition(partition).getNodeId();

    Collection<Broker> brokers = clusteringRule.getBrokers();


    Broker broker = brokers.iterator().next(); // TODO:
    List<Integer> leadingPartitions = clusteringRule
      .getBrokersLeadingPartitions(broker.getConfig().getNetwork().getClient().toSocketAddress());

    /*try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
*/
    Iterator<Integer> iter = leadingPartitions.iterator();
    while(iter.hasNext()) {
      Integer partitionId = iter.next();
      assertLeaderServicesHasStarted(broker, partitionId);
    }

  }

  private void assertLeaderServicesHasStarted(Broker broker, int partitionId){
    String logName = String.format("partition-%d", partitionId);
    ServiceContainer serviceContainer = broker.getBrokerContext().getServiceContainer();

    assertThat(serviceContainer.hasService(leaderPartitionServiceName(logName))).isTrue();
    assertThat(serviceContainer.hasService(followerPartitionServiceName(logName))).isFalse();
    assertThat(serviceContainer.hasService(leaderOpenLogStreamServiceName(logName, 0))).isTrue();
  }
}
