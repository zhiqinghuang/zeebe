package io.zeebe.distributedlog;

public class CommitLogEvent {
  private long commitPosition;
  private byte[] committedBytes;

  public CommitLogEvent(long commitPosition, byte[] appendBytes) {
    this.commitPosition = commitPosition;
    this.committedBytes = appendBytes;
  }

  public long getCommitPosition() {
    return commitPosition;
  }

  public CommitLogEvent setCommitPosition(long commitPosition) {
    this.commitPosition = commitPosition;
    return this;
  }

  public byte[] getCommittedBytes() {
    return committedBytes;
  }

  public CommitLogEvent setCommittedBytes(byte[] committedBytes) {
    this.committedBytes = committedBytes;
    return this;
  }
}
