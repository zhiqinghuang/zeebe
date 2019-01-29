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
import io.zeebe.broker.Loggers;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;

public class AtomixService implements Service<Atomix> {

  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

  private final BrokerCfg configuration;
  private Atomix atomix;

  public AtomixService(final BrokerCfg configuration) {
    this.configuration = configuration;
  }

  @Override
  public void start(ServiceStartContext startContext) {
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

          final Node node =
              Node.builder().withAddress(Address.from(address[0], memberPort)).build();
          LOG.info("Node to contact: {}", node.address());
          nodes.add(node);
        });

    atomix =
        Atomix.builder()
            .withMemberId(Integer.toString(nodeId))
            .withAddress(Address.from(host, port))
            .withMembershipProvider(builder.withNodes(nodes).build())
            .withProfiles(Profile.dataGrid(1))
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

    atomix
        .getMembershipService()
        .addListener(
            (membershipEvent -> {
              LOG.info(membershipEvent.toString());
            }));

    final CompletableFuture<Void> startFuture = atomix.start();
    startContext.async(mapCompletableFuture(startFuture));
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    final CompletableFuture<Void> stopFuture = atomix.stop();
    stopContext.async(mapCompletableFuture(stopFuture));
  }

  @Override
  public Atomix get() {
    return atomix;
  }

  private ActorFuture<Void> mapCompletableFuture(CompletableFuture<Void> atomixFuture) {
    final ActorFuture<Void> mappedActorFuture = new CompletableActorFuture<>();

    atomixFuture
        .thenAccept(mappedActorFuture::complete)
        .exceptionally(
            t -> {
              mappedActorFuture.completeExceptionally(t);
              return null;
            });
    return mappedActorFuture;
  }
}
