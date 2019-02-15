package io.zeebe.distributedlog;

import io.zeebe.servicecontainer.testing.ServiceContainerRule;
import io.zeebe.test.util.TestUtil;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Rule;
import org.junit.Test;

public class DistributedLogTest {
  @Rule
  public ActorSchedulerRule actorScheduler = new ActorSchedulerRule();
  @Rule
  public ServiceContainerRule serviceContainerRule = new ServiceContainerRule(actorScheduler);

  List<String> members = Arrays.asList("1");// , "2", "3");

  @Rule
  public DistributedLogRule node1 = new DistributedLogRule(serviceContainerRule, 1, 0, 1, 1, members);
//  @Rule
//  public DistributedLogRule node2 = new DistributedLogRule(serviceContainerRule, 2, 0, 1, 3, members);
//  @Rule
//  public DistributedLogRule node3 = new DistributedLogRule(serviceContainerRule, 3, 0, 1, 3, members);



  @Test
  public void shouldReplicate() throws ExecutionException, InterruptedException, TimeoutException {

    node1.waitUntilNodesJoined();

    node1.becomeLeader();

    String message = "record1";
    long position = node1.writeEvent(message);

    TestUtil.waitUntil(() -> node1.eventAppended(message, position), 500);
  }
}
