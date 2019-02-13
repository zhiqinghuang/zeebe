package io.zeebe.broker.clustering.base;

import io.atomix.core.Atomix;
import io.atomix.core.election.LeaderElection;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.zeebe.broker.Loggers;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import org.slf4j.Logger;

public class LeaderElectionService implements Service<LeaderElection> {

  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

  private Atomix atomix;
  private final Injector<Atomix> atomixInjector = new Injector<>();

  private LeaderElection<String> leaderElection;

  private final int partitionId;

  public LeaderElectionService(int partitionId) {
    this.partitionId = partitionId;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    LOG.info("Creating leader election");

    atomix = atomixInjector.getValue();

    // TODO: create election for partitions that the node owns.
    leaderElection =
        atomix
            .<String>leaderElectionBuilder(String.valueOf(partitionId))
            .withProtocol(
                MultiRaftProtocol.builder()
                    // TODO: It is better to define our own partitioner so that this leaderelection run on
                    // the corresponding partition.
                    // .withPartitioner((k, p) -> p.get(Integer.parseInt(k)))
                    .build())
            .build();
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    leaderElection.close();
  }

  @Override
  public LeaderElection<String> get() {
    return leaderElection;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }
}
