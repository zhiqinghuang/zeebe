package io.zeebe.broker.clustering.base;

import io.atomix.cluster.MemberId;
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

  @Override
  public void start(ServiceStartContext startContext) {
    LOG.info("Creating leader election");

    atomix = atomixInjector.getValue();

    // TODO: create election for partitions that the node owns.
    leaderElection =
        atomix
            .<String>leaderElectionBuilder("partition-0")
            .withProtocol(MultiRaftProtocol.builder().build())
            .build();

    // TODO: promote the primary leader
    leaderElection.run(atomix.getMembershipService().getLocalMember().id());

    LOG.info("Running leader election");

    // TODO:
    leaderElection.addListener(
        e -> {
          LOG.info(
              "Member {} receives leadership event{}",
              atomix.getMembershipService().getLocalMember().id(),
              e);
        });
  }

  @Override
  public void stop(ServiceStopContext stopContext) {}

  @Override
  public LeaderElection<MemberId> get() {
    return leaderElection;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }
}
