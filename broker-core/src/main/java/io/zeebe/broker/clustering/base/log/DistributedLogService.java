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
package io.zeebe.broker.clustering.base.log;

import io.atomix.core.Atomix;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.zeebe.broker.Loggers;
import io.zeebe.distributedlog.DistributedLogstream;
import io.zeebe.distributedlog.DistributedLogstreamBuilder;
import io.zeebe.distributedlog.DistributedLogstreamType;
import io.zeebe.distributedlog.impl.DistributedLogstreamConfig;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import org.slf4j.Logger;

public class DistributedLogService implements Service<DistributedLogstream> {

  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

  private Injector<Atomix> atomixInjector = new Injector();
  private Atomix atomix;

  private DistributedLogstream distributedLog;

  private final String distributedLogName = "distributed-logstream";

  @Override
  public void start(ServiceStartContext startContext) {
    this.atomix = atomixInjector.getValue();

    distributedLog =
        atomix
            .<DistributedLogstreamBuilder, DistributedLogstreamConfig, DistributedLogstream>
                primitiveBuilder(distributedLogName, DistributedLogstreamType.instance())
            .withProtocol(MultiRaftProtocol.builder().build()) // TODO: Use a custom partitioner
            .build();
    LOG.info("Set up distributed log primitive");
  }

  @Override
  public void stop(ServiceStopContext stopContext) {}

  @Override
  public DistributedLogstream get() {
    return distributedLog;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }
}
