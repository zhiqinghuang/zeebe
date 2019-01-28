/*
 * Copyright © 2019  camunda services GmbH (info@camunda.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package io.zeebe.broker.clustering.base.gossip;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryBuilder;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.profile.Profile;
import io.atomix.utils.net.Address;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.ArrayList;
import java.util.List;

public class AtomixService implements Service<Atomix> {

  private final BrokerCfg configuration;
  private Atomix atomix;

  public AtomixService(final BrokerCfg configuration) {
    this.configuration = configuration;
  }

  @Override
  public void start(ServiceStartContext startContext) {

    //    startContext.run(
    //        () -> {
    final ClusterCfg clusterCfg = configuration.getCluster();

    final int stepSize = 15;

    final int nodeId = clusterCfg.getNodeId();
    final String host = configuration.getNetwork().getManagement().getHost();
    final int port = configuration.getNetwork().getManagement().getPort() + stepSize;

    final List<String> initialContactPoints = clusterCfg.getInitialContactPoints();

    final BootstrapDiscoveryBuilder builder = BootstrapDiscoveryProvider.builder();

    final List<Node> nodes = new ArrayList<>();
    initialContactPoints.forEach(
        contactAddress -> {
          final String[] address = contactAddress.split(":");
          final int memberPort = Integer.parseInt(address[1]) + stepSize;

          nodes.add(Node.builder().withAddress(Address.from(address[0], memberPort)).build());
        });

    atomix =
        Atomix.builder()
            .withMemberId(Integer.toString(nodeId))
            .withAddress(Address.from(host, port))
            .withMembershipProvider(builder.withNodes(nodes).build())
            .withProfiles(Profile.dataGrid())
            .build();

    final ActorFuture<Void> atomixStartFuture = new CompletableActorFuture<>();
    atomix
        .start()
        .thenAccept(atomixStartFuture::complete)
        .exceptionally(
            t -> {
              atomixStartFuture.completeExceptionally(t);
              return null;
            });
    //        });

    startContext.async(atomixStartFuture);
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    atomix.stop();
  }

  @Override
  public Atomix get() {
    return atomix;
  }
}
