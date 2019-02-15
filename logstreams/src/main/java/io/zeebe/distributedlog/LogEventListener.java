package io.zeebe.distributedlog;

@FunctionalInterface
public interface LogEventListener {

  void onCommit(CommitLogEvent event);
}
