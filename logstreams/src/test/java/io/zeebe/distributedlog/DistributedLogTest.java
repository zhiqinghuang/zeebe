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

import io.zeebe.servicecontainer.testing.ServiceContainerRule;
import io.zeebe.test.util.TestUtil;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class DistributedLogTest {

  public ActorSchedulerRule actorSchedulerRule1 = new ActorSchedulerRule();
  public ActorSchedulerRule actorSchedulerRule2 = new ActorSchedulerRule();
  public ActorSchedulerRule actorSchedulerRule3 = new ActorSchedulerRule();

  public ServiceContainerRule serviceContainerRule1 = new ServiceContainerRule(actorSchedulerRule1);

  public ServiceContainerRule serviceContainerRule2 = new ServiceContainerRule(actorSchedulerRule2);

  public ServiceContainerRule serviceContainerRule3 = new ServiceContainerRule(actorSchedulerRule3);

  List<String> members = Arrays.asList("1", "2", "3");

  public DistributedLogRule node1 =
      new DistributedLogRule(serviceContainerRule1, 1, 0, 1, 3, members, null);

  public DistributedLogRule node2 =
      new DistributedLogRule(
          serviceContainerRule2, 2, 0, 1, 3, members, Collections.singletonList(node1.getNode()));

  public DistributedLogRule node3 =
      new DistributedLogRule(
          serviceContainerRule3, 3, 0, 1, 3, members, Collections.singletonList(node2.getNode()));

  public Timeout timeoutRule = Timeout.seconds(60);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(timeoutRule)
          .around(actorSchedulerRule1)
          .around(serviceContainerRule1)
          .around(actorSchedulerRule2)
          .around(serviceContainerRule2)
          .around(actorSchedulerRule3)
          .around(serviceContainerRule3)
          .around(node3)
          .around(node2)
          .around(node1);

  @Test
  public void shouldReplicateSingleEvent()
      throws ExecutionException, InterruptedException, TimeoutException {
    node1.waitUntilNodesJoined();
    node2.waitUntilNodesJoined();
    node3.waitUntilNodesJoined();

    node1.becomeLeader();

    final Event event = writeEvent("record");
    checkIfEventReplicated(event);
  }

  @Test
  public void shouldReplicateMultipleEvents()
      throws ExecutionException, InterruptedException, TimeoutException {

    node1.waitUntilNodesJoined();
    node2.waitUntilNodesJoined();
    node3.waitUntilNodesJoined();

    node1.becomeLeader();

    final Event event1 = writeEvent("record1");
    final Event event2 = writeEvent("record2");
    final Event event3 = writeEvent("record3");

    checkIfEventReplicated(event1);
    checkIfEventReplicated(event2);
    checkIfEventReplicated(event3);
  }

  private Event writeEvent(String message) {
    final Event event = new Event();
    event.message = message;
    event.position = node1.writeEvent(message);
    return event;
  }

  private class Event {
    String message;
    long position;
  }

  private void checkIfEventReplicated(Event event) {
    TestUtil.waitUntil(() -> node1.eventAppended(event.message, event.position), 100);
    TestUtil.waitUntil(() -> node2.eventAppended(event.message, event.position), 100);
    TestUtil.waitUntil(() -> node3.eventAppended(event.message, event.position), 100);
  }
}
