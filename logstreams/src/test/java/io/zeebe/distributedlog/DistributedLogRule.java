package io.zeebe.distributedlog;

import static io.zeebe.util.buffer.BufferUtil.bufferAsString;
import static io.zeebe.util.buffer.BufferUtil.wrapString;

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
  private LogStream logStream;
  private BufferedLogStreamReader uncommittedReader;
  private BufferedLogStreamReader committedReader;
  private Atomix atomix;
  private int numParitions;
  private int replicationFactor;
  private List<String> members;
  private DistributedLogstream distributedLog;
  LogStreamWriterImpl writer = new LogStreamWriterImpl();

  private final RecordMetadata metadata = new RecordMetadata();
  private CompletableFuture<Void> nodeStarted;
  public static final Logger LOG = LoggerFactory.getLogger("io.zeebe.distributedlog.test");

  public DistributedLogRule(
      final ServiceContainerRule serviceContainerRule,
      final int nodeId,
      final int partition,
      int numParitions,
      int replicationFactor,
      List<String> members) {
    this.actorScheduler = serviceContainerRule.getActorScheduler();
    this.serviceContainer = serviceContainerRule.get();
    this.nodeId = nodeId;
    this.partition = partition;
    this.numParitions = numParitions;
    this.replicationFactor = replicationFactor;
    this.socketAddress = SocketUtil.getNextAddress();
    this.members = members;
  }

  @Override
  protected void before() throws IOException {
    nodeStarted =
        createAtomixNode()
            .whenComplete(
                (r, t) -> {
                  LOG.info("Creating logstream");
                  createDistributedLog();
                  try {
                    createLogStream();
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                });
  }

  private void createLogStream() throws IOException {
 LOG.info("Creating log stream");
    final String logName = String.format("%d-%d", partition, nodeId);

    logStream =
        LogStreams.createFsLogStream(partition)
            .logName(logName)
            .deleteOnClose(true)
            .logDirectory(Files.createTempDirectory("dl-test-" + nodeId + "-").toString())
            .serviceContainer(serviceContainer)
            .build()
            .join();

    uncommittedReader = new BufferedLogStreamReader(logStream, true);
    committedReader = new BufferedLogStreamReader(logStream, false);
    LOG.info("created");
  }

  private void createDistributedLog() {
    LOG.info("Creating distributed log");
    distributedLog =
        atomix
            .<DistributedLogstreamBuilder, DistributedLogstreamConfig, DistributedLogstream>
                primitiveBuilder("distributed-log", DistributedLogstreamType.instance())
            .withProtocol(MultiRaftProtocol.builder().build())
            .build();
    LOG.info("created");

  }

  private CompletableFuture<Void> createAtomixNode() throws IOException {
    AtomixBuilder atomixBuilder =
        Atomix.builder()
            .withClusterId("dl-test")
            .withMemberId(String.valueOf(nodeId))
            .withAddress(Address.from(socketAddress.host(), socketAddress.port()));
    //   .withMembershipProvider(discoveryProvider);

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
    nodeStarted.get(20, TimeUnit.SECONDS);
  }

  public boolean eventAppended(String message, long writePosition) {
    uncommittedReader.seek(writePosition);
    if (uncommittedReader.hasNext()) {
      LoggedEvent event = uncommittedReader.next();
      String messageRead =
          bufferAsString(event.getValueBuffer(), event.getValueOffset(), event.getValueLength());
      return message.equals(messageRead) && event.getPosition() == writePosition;
    }
    return false;
  }
}
