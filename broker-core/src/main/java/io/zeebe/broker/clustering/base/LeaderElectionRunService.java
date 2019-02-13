package io.zeebe.broker.clustering.base;

import io.atomix.core.Atomix;
import io.atomix.core.election.LeaderElection;
import io.zeebe.broker.Loggers;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import org.slf4j.Logger;

public class LeaderElectionRunService implements Service<Void> {
  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

  private final Injector<LeaderElection> leaderElectionInjector = new Injector<>();
  private LeaderElection<String> leaderElection;

  private final Injector<Atomix> atomixInjector = new Injector<>();

  @Override
  public void start(ServiceStartContext startContext) {
    leaderElection = leaderElectionInjector.getValue();
    Atomix atomix = atomixInjector.getValue();
    LOG.info("Running leader election");
    leaderElection.run(atomix.getMembershipService().getLocalMember().id().id());
  }

  @Override
  public Void get() {
    return null;
  }

  public Injector<LeaderElection> getLeaderElectionInjector() {
    return leaderElectionInjector;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }
}
