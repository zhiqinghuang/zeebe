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
package io.zeebe.broker.clustering.base.partitions;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.RAFT_SERVICE_GROUP;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.leaderPartitionServiceName;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.partitionRoleChangeListenerServiceName;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.raftInstallServiceName;
import static io.zeebe.broker.logstreams.LogStreamServiceNames.stateStorageFactoryServiceName;
import static io.zeebe.raft.RaftServiceNames.raftServiceName;

import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.core.election.Leader;
import io.atomix.core.election.LeaderElection;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.raft.RaftPersistentConfiguration;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.broker.logstreams.state.StateStorageFactory;
import io.zeebe.broker.logstreams.state.StateStorageFactoryService;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.logstreams.LogStreams;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.raft.Raft;
import io.zeebe.raft.RaftStateListener;
import io.zeebe.raft.controller.MemberReplicateLogController;
import io.zeebe.raft.state.RaftState;
import io.zeebe.servicecontainer.CompositeServiceBuilder;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.transport.ClientTransport;
import io.zeebe.util.sched.channel.OneToOneRingBufferChannel;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.Collection;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.slf4j.Logger;

/**
 * Service used to install the necessary services for creating a partition, namely logstream and
 * raft. Also listens to raft state changes (Leader, Follower) and installs the corresponding {@link
 * Partition} service(s) into the broker for other components (like client api or stream processing)
 * to attach to.
 */
public class PartitionInstallService implements Service<Void>, RaftStateListener {
  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

  private final BrokerCfg brokerCfg;
  private final Injector<ClientTransport> clientTransportInjector = new Injector<>();
  private final RaftPersistentConfiguration configuration;
  private final PartitionInfo partitionInfo;

  private ServiceStartContext startContext;
  private ServiceName<LogStream> logStreamServiceName;

  private ServiceName<StateStorageFactory> stateStorageFactoryServiceName;
  private final Injector<LeaderElection> leaderElectionInjector = new Injector<>();
  private LeaderElection<String> leaderElection;
  private final Injector<Atomix> atomixInjector = new Injector<>();
  private Atomix atomix;
  private MemberId localMember;

  private String logName;

  public PartitionInstallService(
      final BrokerCfg brokerCfg, final RaftPersistentConfiguration configuration) {
    this.brokerCfg = brokerCfg;
    this.configuration = configuration;
    this.partitionInfo =
        new PartitionInfo(configuration.getPartitionId(), configuration.getReplicationFactor());
  }

  @Override
  public Void get() {
    return null;
  }

  @Override
  public void start(final ServiceStartContext startContext) {
    this.startContext = startContext;

    leaderElection = leaderElectionInjector.getValue();
    atomix = atomixInjector.getValue();
    localMember = atomix.getMembershipService().getLocalMember().id();

    final int partitionId = configuration.getPartitionId();
    logName = String.format("partition-%d", partitionId);

    final ServiceName<Void> raftInstallServiceName =
        raftInstallServiceName(partitionId); // TODO: change the name

    final CompositeServiceBuilder partitionInstall =
        startContext.createComposite(raftInstallServiceName);

    installPartitionStorage(partitionInstall, partitionId, logName);
    installRaftPartitionServices(partitionInstall, partitionId, logName);
    installPartitionRoleChangeListeners(partitionInstall, partitionId);
    partitionInstall.install();
  }

  private void installPartitionRoleChangeListeners(
      CompositeServiceBuilder partitionInstall, int partitionId) {
    PartitionRoleChangeListeners service =
        new PartitionRoleChangeListeners(
            atomix,
            leaderElection,
            logName,
            partitionInfo,
            logStreamServiceName,
            stateStorageFactoryServiceName);

    partitionInstall
        .createService(partitionRoleChangeListenerServiceName(logName), service)
        .dependency(logStreamServiceName, service.getLogStreamInjector())
        .install();
  }

  private void installPartitionStorage(
      CompositeServiceBuilder partitionInstall, int partitionId, String logName) {

    final String snapshotPath = configuration.getSnapshotsDirectory().getAbsolutePath();

    logStreamServiceName =
        LogStreams.createFsLogStream(partitionId)
            .logDirectory(configuration.getLogDirectory().getAbsolutePath())
            .logSegmentSize((int) configuration.getLogSegmentSize())
            .logName(logName)
            .snapshotStorage(LogStreams.createFsSnapshotStore(snapshotPath).build())
            .buildWith(partitionInstall);

    final StateStorageFactoryService stateStorageFactoryService =
        new StateStorageFactoryService(configuration.getStatesDirectory());
    stateStorageFactoryServiceName = stateStorageFactoryServiceName(logName);
    partitionInstall
        .createService(stateStorageFactoryServiceName, stateStorageFactoryService)
        .install();
  }

  private void installRaftPartitionServices(
      CompositeServiceBuilder partitionInstall, int partitionId, String logName) {
    final ClientTransport clientTransport = clientTransportInjector.getValue();
    final ServiceName<Raft> raftServiceName = raftServiceName(logName);

    final OneToOneRingBufferChannel messageBuffer =
        new OneToOneRingBufferChannel(
            new UnsafeBuffer(
                new byte
                    [(MemberReplicateLogController.REMOTE_BUFFER_SIZE)
                        + RingBufferDescriptor.TRAILER_LENGTH]));

    final Raft raftService =
        new Raft(
            logName,
            brokerCfg.getRaft(),
            brokerCfg.getCluster().getNodeId(),
            clientTransport,
            configuration,
            messageBuffer,
            this);

    raftService.addMembersWhenJoined(configuration.getMembers());

    partitionInstall
        .createService(raftServiceName, raftService)
        .dependency(logStreamServiceName, raftService.getLogStreamInjector())
        .dependency(stateStorageFactoryServiceName)
        .group(RAFT_SERVICE_GROUP)
        .install();
  }

  @Override
  public ActorFuture<Void> onMemberLeaving(final Raft raft, final Collection<Integer> nodeIds) {
    final ServiceName<Partition> partitionServiceName = leaderPartitionServiceName(raft.getName());

    final int raftMemberSize = nodeIds.size() + 1; // raft does not count itself as member
    final int replicationFactor = partitionInfo.getReplicationFactor();

    ActorFuture<Void> leaveHandledFuture = CompletableActorFuture.completed(null);

    // TODO: Should we remove leader services when quorum is not reached? Probably new leader
    // election is triggered and the services are removed/installed
    /* if (startContext.hasService(partitionServiceName)) {
      if (raftMemberSize < replicationFactor) {
        LOG.debug(
            "Removing partition service for {}. Replication factor not reached, got {}/{}.",
            partitionInfo,
            raftMemberSize,
            replicationFactor);

        leaveHandledFuture = startContext.removeService(partitionServiceName);
      } else {
        LOG.debug(
            "Not removing partition {}, replication factor still reached, got {}/{}.",
            partitionInfo,
            raftMemberSize,
            replicationFactor);
      }
    }*/

    return leaveHandledFuture;
  }

  @Override
  public void onMemberJoined(final Raft raft, final Collection<Integer> currentNodeIds) {
    if (raft.getState() == RaftState.LEADER) {
      Leader<String> currentLeader = leaderElection.getLeadership().leader();
      if (currentLeader == null || !currentLeader.id().equals(localMember.id())) {
        leaderElection.anoint(localMember.id());
      }
      // Don't do anything here. All services are now handled via atomix event listener
      // installLeaderPartition(raft.getName());
    }
  }

  @Override
  public void onStateChange(final Raft raft, final RaftState raftState) {
    switch (raftState) {
      case LEADER:
        String localMemberId = atomix.getMembershipService().getLocalMember().id().id();
        boolean success = leaderElection.anoint(localMemberId);
        if (!success) {
          LOG.error("Couldn't anoint myself {}", localMemberId);
        }
        LOG.info("Anoint myself {} for partition {}", localMemberId, partitionInfo.getPartitionId());
        break;
      case FOLLOWER:
        // Don't do anything. the leader will anoint itself and we will receive the events
        // installFollowerPartition(raft.getName());
        break;
      case CANDIDATE:
      default:
        // removeFollowerPartitionService(raft.getName());
        break;
    }
  }

  public Injector<ClientTransport> getClientTransportInjector() {
    return clientTransportInjector;
  }

  public Injector<LeaderElection> getLeaderElectionInjector() {
    return this.leaderElectionInjector;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }
}
