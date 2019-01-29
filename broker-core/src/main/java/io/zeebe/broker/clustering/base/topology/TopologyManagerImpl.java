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
package io.zeebe.broker.clustering.base.topology;

import static io.zeebe.broker.clustering.base.gossip.GossipCustomEventEncoding.readNodeInfo;
import static io.zeebe.broker.clustering.base.gossip.GossipCustomEventEncoding.readPartitions;
import static io.zeebe.broker.clustering.base.gossip.GossipCustomEventEncoding.writeNodeInfo;
import static io.zeebe.broker.clustering.base.gossip.GossipCustomEventEncoding.writePartitions;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.core.Atomix;
import io.atomix.cluster.Member;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.gossip.Gossip;
import io.zeebe.gossip.GossipCustomEventListener;
import io.zeebe.gossip.GossipMembershipListener;
import io.zeebe.gossip.GossipSyncRequestHandler;
import io.zeebe.gossip.dissemination.GossipSyncRequest;
import io.zeebe.protocol.impl.data.cluster.TopologyResponseDto;
import io.zeebe.raft.Raft;
import io.zeebe.raft.RaftStateListener;
import io.zeebe.raft.state.RaftState;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.LogUtil;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;

public class TopologyManagerImpl extends Actor
    implements TopologyManager, RaftStateListener, ClusterMembershipEventListener {
  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

  public static final DirectBuffer CONTACT_POINTS_EVENT_TYPE =
      BufferUtil.wrapString("contact_points");
  public static final DirectBuffer PARTITIONS_EVENT_TYPE = BufferUtil.wrapString("partitions");

  private final MembershipListener membershipListner = new MembershipListener();
  private final ContactPointsChangeListener contactPointsChangeListener =
      new ContactPointsChangeListener();
  private final PartitionChangeListener partitionChangeListener = new PartitionChangeListener();
  private final KnownContactPointsSyncHandler localContactPointsSycHandler =
      new KnownContactPointsSyncHandler();
  private final KnownPartitionsSyncHandler knownPartitionsSyncHandler =
      new KnownPartitionsSyncHandler();

  private final Topology topology;
  private final Atomix atomix;

  private List<TopologyMemberListener> topologyMemberListers = new ArrayList<>();
  private List<TopologyPartitionListener> topologyPartitionListers = new ArrayList<>();

  public TopologyManagerImpl(
      Gossip gossip, Atomix atomix, NodeInfo localBroker, ClusterCfg clusterCfg) {
    this.atomix = atomix;
    this.topology =
        new Topology(
            localBroker,
            clusterCfg.getClusterSize(),
            clusterCfg.getPartitionsCount(),
            clusterCfg.getReplicationFactor());
  }

  @Override
  public String getName() {
    return "topology";
  }

  @Override
  protected void onActorStarting() {

  }

  @Override
  protected void onActorClosing() {

  }

  public void onRaftStarted(Raft raft) {
    actor.run(
        () -> {
          raft.registerRaftStateListener(this);

          onStateChange(raft, raft.getState());
        });
  }

  public void updatePartition(
      int partitionId, int replicationFactor, NodeInfo member, RaftState raftState) {
    final PartitionInfo updatedPartition =
        topology.updatePartition(partitionId, replicationFactor, member, raftState);

    notifyPartitionUpdated(updatedPartition, member);
  }

  public void onRaftRemoved(Raft raft) {
    actor.run(
        () -> {
          final NodeInfo memberInfo = topology.getLocal();

          topology.removePartitionForMember(raft.getPartitionId(), memberInfo);

          raft.unregisterRaftStateListener(this);

          publishLocalPartitions();
        });
  }

  @Override
  public void onStateChange(Raft raft, RaftState raftState) {
    actor.run(
        () -> {
          final NodeInfo memberInfo = topology.getLocal();

          updatePartition(
              raft.getPartitionId(), raft.getReplicationFactor(), memberInfo, raft.getState());

          publishLocalPartitions();
        });
  }

  @Override
  public void event(ClusterMembershipEvent clusterMembershipEvent) {
    Member eventSource = clusterMembershipEvent.subject();
    Member localNode = atomix.getMembershipService().getLocalMember();
    switch (clusterMembershipEvent.type()) {
      case METADATA_CHANGED:
        LOG.debug(
            "Member {} receives metadata change of member {}",
            localNode,
            eventSource.id());
        updatePartitionInfo(eventSource);
        break;
      case MEMBER_ADDED:
          Properties newProperties = eventSource.properties();

        LOG.debug(
            "Im node {}. new member {} added",
            atomix.getMembershipService().getLocalMember().id(),
            newNode.id());
        String replicationAddress = newProperties.getProperty("replicationAddress");
        ObjectMapper mapper = new ObjectMapper();
        String managementAddress = newProperties.getProperty("managementAddress");
        String clientApiAddress = newProperties.getProperty("clientAddress");
        String subscriptionAddress = newProperties.getProperty("subscriptionAddress");
        try {
          InetSocketAddress replication =
              mapper.readValue(replicationAddress, InetSocketAddress.class);
          InetSocketAddress management =
              mapper.readValue(managementAddress, InetSocketAddress.class);
          InetSocketAddress client = mapper.readValue(clientApiAddress, InetSocketAddress.class);
          InetSocketAddress subscription =
              mapper.readValue(subscriptionAddress, InetSocketAddress.class);

          NodeInfo nodeInfo =
              new NodeInfo(
                  Integer.parseInt(newNode.id().id()),
                  new SocketAddress(client),
                  new SocketAddress(management),
                  new SocketAddress(replication),
                  new SocketAddress(subscription));
          topology.addMember(nodeInfo);
          notifyMemberAdded(nodeInfo);

          updatePartitionInfo(newNode);

        } catch (IOException e) {
          e.printStackTrace();
        }
        break;
      default:
        LOG.info(
            "Im node {}, event received from {} {}",
            atomix.getMembershipService().getLocalMember().id(),
            clusterMembershipEvent.subject().id(),
            clusterMembershipEvent.type());
    }

    LOG.info(
        "Im node {}. Topology {}",
        atomix.getMembershipService().getLocalMember().id(),
        topology.asDto().toString());
  }

  private void updatePartitionInfo(io.atomix.cluster.Member node) {
    Properties properties = node.properties();
    for (String p : properties.stringPropertyNames()) {
      if (p.startsWith("partition")) {
        int partitionId = Integer.parseInt(p.split("-")[1]);
        PartitionInfo partitionInfo =
            topology.updatePartition(
                partitionId,
                0,
                topology.getMember(Integer.parseInt(node.id().id())),
                RaftState.valueOf(properties.getProperty(p)));

        notifyPartitionUpdated(partitionInfo, topology.getMember(Integer.parseInt(node.id().id())));
      }
    }
  }

  private class ContactPointsChangeListener implements GossipCustomEventListener {
    @Override
    public void onEvent(int senderId, DirectBuffer payload) {
      final DirectBuffer payloadCopy = BufferUtil.cloneBuffer(payload);

      actor.run(
          () -> {
            LOG.trace("Received API event from member {}.", senderId);

            final NodeInfo newMember = readNodeInfo(0, payloadCopy);
            final boolean memberAdded = topology.addMember(newMember);
            if (memberAdded) {
              notifyMemberAdded(newMember);
            }
          });
    }
  }

  private class MembershipListener implements GossipMembershipListener {
    @Override
    public void onAdd(Member member) {
      // noop; we listen on the availability of contact points, see ContactPointsChangeListener
    }

    @Override
    public void onRemove(Member member) {
      final NodeInfo topologyMember = topology.getMember(member.getId());
      if (topologyMember != null) {
        topology.removeMember(topologyMember);
        notifyMemberRemoved(topologyMember);
      }
    }
  }

  private class PartitionChangeListener implements GossipCustomEventListener {
    @Override
    public void onEvent(int senderId, DirectBuffer payload) {
      final DirectBuffer payloadCopy = BufferUtil.cloneBuffer(payload);

      actor.run(
          () -> {
            final NodeInfo member = topology.getMember(senderId);

            if (member != null) {
              readPartitions(payloadCopy, 0, member, TopologyManagerImpl.this);
              LOG.trace("Received raft state change event for member {} {}", senderId, member);
            } else {
              LOG.trace("Received raft state change event for unknown member {}", senderId);
            }
          });
    }
  }

  private class KnownContactPointsSyncHandler implements GossipSyncRequestHandler {
    private final ExpandableArrayBuffer writeBuffer = new ExpandableArrayBuffer();

    @Override
    public ActorFuture<Void> onSyncRequest(GossipSyncRequest request) {
      return actor.call(
          () -> {
            LOG.trace("Got API sync request");

            for (NodeInfo member : topology.getMembers()) {
              final int length = writeNodeInfo(member, writeBuffer, 0);
              request.addPayload(member.getNodeId(), writeBuffer, 0, length);
            }

            LOG.trace("Send API sync response.");
          });
    }
  }

  private class KnownPartitionsSyncHandler implements GossipSyncRequestHandler {
    private final ExpandableArrayBuffer writeBuffer = new ExpandableArrayBuffer();

    @Override
    public ActorFuture<Void> onSyncRequest(GossipSyncRequest request) {
      return actor.call(
          () -> {
            LOG.trace("Got RAFT state sync request.");

            for (NodeInfo member : topology.getMembers()) {
              final int length = writePartitions(member, writeBuffer, 0);
              request.addPayload(member.getNodeId(), writeBuffer, 0, length);
            }

            LOG.trace("Send RAFT state sync response.");
          });
    }
  }

  private void publishLocalPartitions() {
    final MutableDirectBuffer eventBuffer = new ExpandableArrayBuffer();
    final int length = writePartitions(topology.getLocal(), eventBuffer, 0);

   // gossip.publishEvent(PARTITIONS_EVENT_TYPE, eventBuffer, 0, length);

    for (PartitionInfo p : topology.getLocal().getLeaders()) {
      atomix
          .getMembershipService()
          .getLocalMember()
          .properties()
          .setProperty("partition-" + p.getPartitionId(), "LEADER");
    }
    for (PartitionInfo p : topology.getLocal().getFollowers()) {
      atomix
          .getMembershipService()
          .getLocalMember()
          .properties()
          .setProperty("partition-" + p.getPartitionId(), "FOLLOWER");
    }
  }

  public ActorFuture<Void> close() {
    return actor.close();
  }

  @Override
  public ActorFuture<TopologyResponseDto> getTopologyDto() {
    return actor.call(topology::asDto);
  }

  @Override
  public void addTopologyMemberListener(TopologyMemberListener listener) {
    actor.run(
        () -> {
          topologyMemberListers.add(listener);

          // notify initially
          topology
              .getMembers()
              .forEach(
                  (m) -> {
                    LogUtil.catchAndLog(LOG, () -> listener.onMemberAdded(m, topology));
                  });
        });
  }

  @Override
  public void removeTopologyMemberListener(TopologyMemberListener listener) {
    actor.run(
        () -> {
          topologyMemberListers.remove(listener);
        });
  }

  @Override
  public void addTopologyPartitionListener(TopologyPartitionListener listener) {
    actor.run(
        () -> {
          topologyPartitionListers.add(listener);

          // notify initially
          topology
              .getPartitions()
              .forEach(
                  (p) ->
                      LogUtil.catchAndLog(
                          LOG,
                          () -> {
                            final NodeInfo leader = topology.getLeader(p.getPartitionId());
                            if (leader != null) {
                              listener.onPartitionUpdated(p, leader);
                            }

                            final List<NodeInfo> followers =
                                topology.getFollowers(p.getPartitionId());
                            if (followers != null && !followers.isEmpty()) {
                              followers.forEach(
                                  follower -> listener.onPartitionUpdated(p, follower));
                            }
                          }));
        });
  }

  @Override
  public void removeTopologyPartitionListener(TopologyPartitionListener listener) {
    actor.run(
        () -> {
          topologyPartitionListers.remove(listener);
        });
  }

  private void notifyMemberAdded(NodeInfo memberInfo) {
    for (TopologyMemberListener listener : topologyMemberListers) {
      LogUtil.catchAndLog(LOG, () -> listener.onMemberAdded(memberInfo, topology));
    }
  }

  private void notifyMemberRemoved(NodeInfo memberInfo) {
    for (TopologyMemberListener listener : topologyMemberListers) {
      LogUtil.catchAndLog(LOG, () -> listener.onMemberRemoved(memberInfo, topology));
    }
  }

  private void notifyPartitionUpdated(PartitionInfo partitionInfo, NodeInfo member) {
    for (TopologyPartitionListener listener : topologyPartitionListers) {
      LogUtil.catchAndLog(LOG, () -> listener.onPartitionUpdated(partitionInfo, member));
    }
  }

  @Override
  public <R> ActorFuture<R> query(Function<ReadableTopology, R> query) {
    return actor.call(() -> query.apply(topology));
  }
}
