package io.zeebe.distributedlog;

import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.DISTRIBUTED_LOG_SERVICE;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStorageAppenderRootService;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStorageAppenderServiceName;
import static io.zeebe.util.buffer.BufferUtil.bufferAsString;
import static io.zeebe.util.buffer.BufferUtil.wrapString;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;
import io.zeebe.distributedlog.impl.DistributedLogstreamConfig;
import io.zeebe.logstreams.LogStreams;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.servicecontainer.ServiceContainer;
import io.zeebe.servicecontainer.testing.ServiceContainerRule;
import io.zeebe.test.util.TestUtil;
import io.zeebe.transport.SocketAddress;
import io.zeebe.transport.impl.util.SocketUtil;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.future.ActorFuture;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.DirectBuffer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLogRule extends ExternalResource {
  private final ActorScheduler actorScheduler;
  private final ServiceContainer serviceContainer;
  private final int nodeId;
  private final int partition;
  private final SocketAddress socketAddress;
  private final List<Node> otherNodes;
  private LogStream logStream;
  private BufferedLogStreamReader uncommittedReader;
  private BufferedLogStreamReader committedReader;
  private Atomix atomix;
  private int numParitions;
  private int replicationFactor;
  private List<String> members;
  private DistributedLogstream distributedLog;
  private LogStreamWriterImpl writer = new LogStreamWriterImpl();

  private final RecordMetadata metadata = new RecordMetadata();
  private CompletableFuture<Void> nodeStarted;
  public static final Logger LOG = LoggerFactory.getLogger("io.zeebe.distributedlog.test");
  private final String logName;

  public DistributedLogRule(
      ServiceContainerRule serviceContainerRule,
      final int nodeId,
      final int partition,
      int numPartitions,
      int replicationFactor,
      List<String> members,
      List<Node> otherNodes) {
    this.actorScheduler = serviceContainerRule.getActorScheduler();
    this.serviceContainer = serviceContainerRule.get();
    this.nodeId = nodeId;
    this.partition = partition;
    this.numParitions = numPartitions;
    this.replicationFactor = replicationFactor;
    this.socketAddress = SocketUtil.getNextAddress();
    this.members = members;
    this.logName = String.format("%d-%d", this.partition, this.nodeId);
    this.otherNodes = otherNodes;
  }

  public Node getNode() {
    return Node.builder()
        .withAddress(new Address(socketAddress.host(), socketAddress.port()))
        .build();
  }

  @Override
  protected void before() throws IOException {
    nodeStarted =
        createAtomixNode()
            .whenComplete(
                (r, t) -> {
                  createDistributedLog();
                  try {
                    createLogStream();
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                });
  }

  @Override
  protected void after() {
    if (serviceContainer.hasService(logStorageAppenderRootService(logName))) {
      logStream.closeAppender().join(); // If opened
    }
    closeLogStream();

    closeDistributedLog();
    stopAtomixNode();
  }

  private void stopAtomixNode() {
    atomix.stop();
  }

  private void closeLogStream() {
    logStream.close();
  }

  private void closeDistributedLog() {
    serviceContainer.removeService(DISTRIBUTED_LOG_SERVICE);
  }

  private void createLogStream() throws IOException {

    ActorFuture<LogStream> logStreamFuture =
        LogStreams.createFsLogStream(partition)
            .logName(logName)
            .deleteOnClose(true)
            .logDirectory(Files.createTempDirectory("dl-test-" + nodeId + "-").toString())
            .serviceContainer(serviceContainer)
            .build();
    logStream = logStreamFuture.join();

    uncommittedReader = new BufferedLogStreamReader(logStream, true);
    committedReader = new BufferedLogStreamReader(logStream, false);
  }

  private void createDistributedLog() {
    distributedLog =
        atomix
            .<DistributedLogstreamBuilder, DistributedLogstreamConfig, DistributedLogstream>
                primitiveBuilder("distributed-log", DistributedLogstreamType.instance())
            .withProtocol(MultiRaftProtocol.builder().build())
            .build();

    ActorFuture<DistributedLogstream> dlFuture =
        serviceContainer.createService(DISTRIBUTED_LOG_SERVICE, () -> distributedLog).install();
    dlFuture.join();
  }

  private CompletableFuture<Void> createAtomixNode() throws IOException {

    AtomixBuilder atomixBuilder =
        Atomix.builder()
            .withClusterId("dl-test")
            .withMemberId(String.valueOf(nodeId))
            .withAddress(Address.from(socketAddress.host(), socketAddress.port()));
    if (otherNodes != null) {
      atomixBuilder.withMembershipProvider(
          BootstrapDiscoveryProvider.builder().withNodes(otherNodes).build());
    }

    final PrimaryBackupPartitionGroup systemGroup =
        PrimaryBackupPartitionGroup.builder("system").withNumPartitions(1).build();

    final String raftPartitionGroupName = "raft-atomix";

    String rootDirectory = Files.createTempDirectory("dl-test-" + nodeId + "-").toString();
    final File raftDirectory = new File(rootDirectory, raftPartitionGroupName);
    Files.createDirectory(raftDirectory.toPath());

    final RaftPartitionGroup partitionGroup =
        RaftPartitionGroup.builder(raftPartitionGroupName)
            .withNumPartitions(numParitions)
            .withPartitionSize(replicationFactor)
            .withMembers(members)
            .withDataDirectory(raftDirectory)
            .withFlushOnCommit()
            .build();

    atomixBuilder.withManagementGroup(systemGroup).withPartitionGroups(partitionGroup);

    atomix = atomixBuilder.build();

    return atomix.start();
  }

  public void becomeLeader() {
    logStream.openAppender();

    TestUtil.waitUntil(
        () -> serviceContainer.hasService(logStorageAppenderServiceName(logName)), 250);
  }

  public long writeEvent(final String message) {
    writer.wrap(logStream);

    AtomicLong writePosition = new AtomicLong();
    final DirectBuffer value = wrapString(message);

    TestUtil.doRepeatedly(
            () -> writer.positionAsKey().metadataWriter(metadata.reset()).value(value).tryWrite())
        .until(
            position -> {
              if (position != null && position >= 0) {
                writePosition.set(position);
                return true;
              } else {
                return false;
              }
            },
            "Failed to write event with message {}",
            message);
    return writePosition.get();
  }

  protected void waitUntilNodesJoined()
      throws ExecutionException, InterruptedException, TimeoutException {
    nodeStarted.get(50, TimeUnit.SECONDS);
  }

  public boolean eventAppended(String message, long writePosition) {
    uncommittedReader.seek(writePosition);
    if (uncommittedReader.hasNext()) {
      LoggedEvent event = uncommittedReader.next();
      String messageRead =
          bufferAsString(event.getValueBuffer(), event.getValueOffset(), event.getValueLength());
      long eventPosition = event.getPosition();
      boolean isEqual = (message.equals(messageRead) && eventPosition == writePosition);
      return isEqual;
    }
    return false;
  }
}
