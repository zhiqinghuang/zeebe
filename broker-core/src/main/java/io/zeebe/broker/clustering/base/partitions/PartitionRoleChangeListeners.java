package io.zeebe.broker.clustering.base.partitions;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.FOLLOWER_PARTITION_GROUP_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.LEADER_PARTITION_GROUP_NAME;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.followerPartitionServiceName;
import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.leaderPartitionServiceName;
import static io.zeebe.raft.RaftServiceNames.leaderOpenLogStreamServiceName;

import io.atomix.core.Atomix;
import io.atomix.core.election.Leader;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeadershipEvent;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.broker.logstreams.state.StateStorageFactory;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.raft.controller.LeaderOpenLogStreamAppenderService;
import io.zeebe.raft.state.RaftState;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import org.slf4j.Logger;


public class PartitionRoleChangeListeners implements Service<Void> {
  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

  private Atomix atomix;
  private LeaderElection<String> leaderElection;
  private final Injector<LogStream> logStreamInjector = new Injector<>();
  private LogStream logStream;
  private ServiceStartContext startContext;
  private String logName;
  private final PartitionInfo partitionInfo;
  private final ServiceName<LogStream> logStreamServiceName;
  private final ServiceName<StateStorageFactory> stateStorageFactoryServiceName;

  public PartitionRoleChangeListeners(Atomix atomix,
    LeaderElection<String> leaderElection, String logName,
    PartitionInfo partitionInfo,
    ServiceName<LogStream> logStreamServiceName,
    ServiceName<StateStorageFactory> stateStorageFactoryServiceName) {
    this.atomix = atomix;
    this.leaderElection = leaderElection;
    this.logName = logName;
    this.partitionInfo = partitionInfo;
    this.logStreamServiceName = logStreamServiceName;
    this.stateStorageFactoryServiceName = stateStorageFactoryServiceName;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    this.startContext = startContext;

    logStream = logStreamInjector.getValue();

    LOG.info("Adding leadership event listener");
    leaderElection.addListener(this::startServicesOnLeaderShipEvent);
  }

  private void startServicesOnLeaderShipEvent(LeadershipEvent<String> leadershipEvent) {
    String localMemberId = atomix.getMembershipService().getLocalMember().id().id();
    Leader<String> oldLeader = leadershipEvent.oldLeadership().leader();
    Leader<String> newLeader = leadershipEvent.newLeadership().leader();
    if (newLeader == null) {
      return;
    }

    final boolean leadershipChanged = !newLeader.equals(oldLeader);
    if (leadershipChanged) {
      final boolean wasLeader = oldLeader != null && localMemberId.equals(oldLeader.id());
      final boolean becomeLeader = localMemberId.equals(newLeader.id());
      if (wasLeader) {
        transitionToFollower();
        LOG.info(
          "Atomix leadership changed. Member {} transitioning to follower for partition {}",
          localMemberId,
          leadershipEvent.topic());
      } else if (becomeLeader) {
        transitionToLeader();
        LOG.info(
          "Atomix leadership changed. Member {} transitioning to leader for partition {}",
          localMemberId,
          leadershipEvent.topic());
      }
    }
  }

  private void transitionToLeader() {
    removeFollowerPartitionService(logName);

    //TODO: This must be installed here. But with current Raft it creates a circular dependency.
    final ServiceName<Void> openLogStreamServiceName =
      leaderOpenLogStreamServiceName(logName, 0); // TODO: remove term

    startContext
      .createService(openLogStreamServiceName, new LeaderOpenLogStreamAppenderService(logStream))
      .install();

    installLeaderPartition(logName);
  }

  private void transitionToFollower() {
    installFollowerPartition(logName);
  }


  private void installLeaderPartition(final String logName) {
    final ServiceName<Partition> partitionServiceName = leaderPartitionServiceName(logName);

    //    final int raftMemberSize = raft.getMemberSize() + 1; // raft does not count itself as
    // member
    //    final int replicationFactor = partitionInfo.getReplicationFactor();

    if (!startContext.hasService(partitionServiceName)) {
      /* if (raftMemberSize >= replicationFactor) {
        LOG.debug(
            "Installing partition service for {}. Replication factor reached, got {}/{}.",
            partitionInfo,
            raftMemberSize,
            replicationFactor);
      */

      final Partition partition = new Partition(partitionInfo, RaftState.LEADER);

      startContext
        .createService(partitionServiceName, partition)
        // .dependency(leaderInitialEventCommittedServiceName(logName, logName))
        .dependency(logStreamServiceName, partition.getLogStreamInjector())
        .dependency(stateStorageFactoryServiceName, partition.getStateStorageFactoryInjector())
        .group(LEADER_PARTITION_GROUP_NAME)
        .install();
         LOG.info("Installing leader partition {}", partitionServiceName);
      //      } else {
      //        LOG.debug(
      //            "Not installing partition service for {}. Replication factor not reached, got
      // {}/{}.",
      //            partitionInfo,
      //            raftMemberSize,
      //            replicationFactor);
      //      }
    }
  }

  private void installFollowerPartition(String raftName) {
    final Partition partition = new Partition(partitionInfo, RaftState.FOLLOWER);
    final ServiceName<Partition> partitionServiceName = followerPartitionServiceName(raftName);

    if (!startContext.hasService(partitionServiceName)) {
      LOG.debug("Installing follower partition service for {}", partitionInfo);
      startContext
        .createService(partitionServiceName, partition)
        .dependency(logStreamServiceName, partition.getLogStreamInjector())
        .dependency(stateStorageFactoryServiceName, partition.getStateStorageFactoryInjector())
        .group(FOLLOWER_PARTITION_GROUP_NAME)
        .install();
    }
  }

  private void removeFollowerPartitionService(String raftName) {
    final ServiceName<Partition> partitionServiceName = followerPartitionServiceName(raftName);

    if (startContext.hasService(partitionServiceName)) {
      LOG.debug("Removing follower partition service for partition {}", partitionInfo);
      startContext.removeService(partitionServiceName);
    }
  }

  @Override
  public Void get() {
    return null;
  }

  public Injector<LogStream> getLogStreamInjector() {
    return logStreamInjector;
  }
}
