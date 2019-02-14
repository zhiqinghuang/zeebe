package io.zeebe.distributedlog;

@FunctionalInterface
public interface LogEventListener {

  void onAppend(byte[] appendedBytes);
}
