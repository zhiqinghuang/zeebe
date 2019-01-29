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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryBuilder;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.profile.Profile;
import io.atomix.utils.net.Address;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.broker.system.configuration.NetworkCfg;
import io.zeebe.broker.system.configuration.SocketBindingCfg;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
    final String localMemberId = Integer.toString(nodeId);

    final NetworkCfg networkCfg = configuration.getNetwork();
    final String host = networkCfg.getManagement().getHost();
    final int port = networkCfg.getManagement().getPort() + stepSize;

    final NodeDiscoveryProvider discoveryProvider =
        createDiscoveryProvider(clusterCfg, stepSize, localMemberId);
    final Properties properties = createNodeProperties(networkCfg);

    atomix =
        Atomix.builder()
            .withClusterId("zeebe-cluster")
            .withMemberId(localMemberId)
            .withProperties(properties)
            .withAddress(Address.from(host, port))
            .withMembershipProvider(discoveryProvider)
            .withProfiles(Profile.dataGrid(1))
            .build();

    // only logging purpose for now
    atomix
        .getMembershipService()
        .addListener(mEvent -> LOG.info("Member {} receives {}", localMemberId, mEvent.toString()));

//    final CompletableFuture<Void> startFuture = atomix.start();
//    startContext.async(mapCompletableFuture(startFuture));
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

  private NodeDiscoveryProvider createDiscoveryProvider(
      ClusterCfg clusterCfg, int stepSize, String localMemberId) {
    final BootstrapDiscoveryBuilder builder = BootstrapDiscoveryProvider.builder();
    final List<String> initialContactPoints = clusterCfg.getInitialContactPoints();

    final List<Node> nodes = new ArrayList<>();
    initialContactPoints.forEach(
        contactAddress -> {
          final String[] address = contactAddress.split(":");
          final int memberPort = Integer.parseInt(address[1]) + stepSize;

          final Node node =
              Node.builder().withAddress(Address.from(address[0], memberPort)).build();
          LOG.info("Member {} will contact node: {}", localMemberId, node.address());
          nodes.add(node);
        });
    return builder.withNodes(nodes).build();
  }

  private Properties createNodeProperties(NetworkCfg networkCfg) {
    final Properties properties = new Properties();

    final ObjectMapper objectMapper = new ObjectMapper();
    try {
      addAddressToProperties(
          "replicationAddress", networkCfg.getReplication(), properties, objectMapper);
      addAddressToProperties(
          "subscriptionAddress", networkCfg.getSubscription(), properties, objectMapper);
      addAddressToProperties(
          "managementAddress", networkCfg.getManagement(), properties, objectMapper);
      addAddressToProperties("clientAddress", networkCfg.getClient(), properties, objectMapper);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return properties;
  }

  private void addAddressToProperties(
      String addressName,
      SocketBindingCfg socketBindingCfg,
      Properties properties,
      ObjectMapper objectMapper)
      throws JsonProcessingException {
    final InetSocketAddress inetSocketAddress =
        socketBindingCfg.toSocketAddress().toInetSocketAddress();
    final String value = objectMapper.writeValueAsString(inetSocketAddress);
    properties.setProperty(addressName, value);
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
