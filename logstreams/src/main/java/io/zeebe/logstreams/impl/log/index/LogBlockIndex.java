/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.logstreams.impl.log.index;

import static io.zeebe.logstreams.impl.log.index.LogBlockIndexDescriptor.entryAddressOffset;
import static io.zeebe.logstreams.impl.log.index.LogBlockIndexDescriptor.entryLogPositionOffset;
import static io.zeebe.logstreams.impl.log.index.LogBlockIndexDescriptor.entryOffset;
import static io.zeebe.logstreams.impl.log.index.LogBlockIndexDescriptor.indexSizeOffset;

import io.zeebe.logstreams.rocksdb.ZbRocksDb;
import io.zeebe.logstreams.state.StateController;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

/**
 * Block index, mapping an event's position to the physical address of the block in which it resides
 * in storage.
 *
 * <p>Each Event has a position inside the stream. This position addresses it uniquely and is
 * assigned when the entry is first published to the stream. The position never changes and is
 * preserved through maintenance operations like compaction.
 *
 * <p>In order to read an event, the position must be translated into the "physical address" of the
 * block in which it resides in storage. Then, the block can be scanned for the event position
 * requested.
 */
public class LogBlockIndex {
  private final ColumnFamilyHandle handle;
  protected final ZbRocksDb rocksDb;
  protected long lastVirtualPosition = -1;

  private final MutableDirectBuffer value = new UnsafeBuffer(new byte[Long.BYTES]);

  public LogBlockIndex(StateController controller) {
    handle = controller.getColumnFamilyHandle(RocksDB.DEFAULT_COLUMN_FAMILY);
    this.rocksDb = controller.getDb();
  }

  /**
   * Returns the physical address of the block in which the log entry identified by the provided
   * position resides.
   *
   * @param position a virtual log position
   * @return the physical address of the block containing the log entry identified by the provided
   *     virtual position
   */
  public long lookupBlockAddress(long position) {
    final int offset = lookupOffset(position);
    final int entryAddressOffset = entryAddressOffset(offset);

    return offset >= 0 ? getLong(entryAddressOffset) : offset;
  }

  public long getLong(int entryAddressOffset) {
    rocksDb.get(handle, entryAddressOffset, value);
    return value.getLong(0);
  }

  public void putLong(int entryAddressOffset, long longValue) {
    value.putLong(0, longValue);
    rocksDb.put(handle, entryAddressOffset, value.byteArray(), 0, Long.BYTES);
  }

  public int getInt(int index) {
    rocksDb.get(handle, index, value);
    return value.getInt(0);
  }

  public void putInt(int index, int intValue) {
    value.putInt(0, intValue);
    rocksDb.put(handle, index, value.byteArray(), 0, Integer.BYTES);
  }

  /**
   * Returns the position of the first log entry of the the block in which the log entry identified
   * by the provided position resides.
   *
   * @param position a virtual log position
   * @return the position of the block containing the log entry identified by the provided virtual
   *     position
   */
  public long lookupBlockPosition(long position) {
    final int offset = lookupOffset(position);
    return offset >= 0 ? getLong(entryLogPositionOffset(offset)) : offset;
  }

  /**
   * Returns the offset of the block in which the log entry identified by the provided position
   * resides.
   *
   * @param position a virtual log position
   * @return the offset of the block containing the log entry identified by the provided virtual
   *     position
   */
  protected int lookupOffset(long position) {
    final int idx = lookupIndex(position);
    return idx >= 0 ? entryOffset(idx) : idx;
  }

  /**
   * Returns the index of the block in which the log entry identified by the provided position
   * resides.
   *
   * @param position a virtual log position
   * @return the index of the block containing the log entry identified by the provided virtual
   *     position
   */
  protected int lookupIndex(long position) {
    final int lastEntryIdx = size() - 1;

    int low = 0;
    int high = lastEntryIdx;

    int idx = -1;

    if (low == high) {
      final int entryOffset = entryOffset(low);
      final long entryValue = getLong(entryLogPositionOffset(entryOffset));

      if (entryValue <= position) {
        idx = low;
      }

      high = -1;
    }

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final int entryOffset = entryOffset(mid);

      if (mid == lastEntryIdx) {
        idx = mid;
        break;
      } else {
        final long entryValue = getLong(entryLogPositionOffset(entryOffset));
        final long nextEntryValue = getLong(entryLogPositionOffset(entryOffset(mid + 1)));

        if (entryValue <= position && position < nextEntryValue) {
          idx = mid;
          break;
        } else if (entryValue < position) {
          low = mid + 1;
        } else if (entryValue > position) {
          high = mid - 1;
        }
      }
    }

    return idx;
  }

  /**
   * Invoked by the log Appender thread after it has first written one or more entries to a block.
   *
   * @param logPosition the virtual position of the block (equal or smaller to the v position of the
   *     first entry in the block)
   * @param storageAddr the physical address of the block in the underlying storage
   * @return the new size of the index.
   */
  public int addBlock(long logPosition, long storageAddr) {
    final int currentIndexSize = getInt(indexSizeOffset()); // volatile get not necessary
    final int entryOffset = entryOffset(currentIndexSize);
    final int newIndexSize = 1 + currentIndexSize;

    if (lastVirtualPosition >= logPosition) {
      final String errorMessage =
          String.format(
              "Illegal value for position.Value=%d, last value in index=%d. Must provide positions in ascending order.",
              logPosition, lastVirtualPosition);
      throw new IllegalArgumentException(errorMessage);
    }

    lastVirtualPosition = logPosition;

    // write next entry
    putLong(entryLogPositionOffset(entryOffset), logPosition);
    putLong(entryAddressOffset(entryOffset), storageAddr);

    // increment size
    putInt(indexSizeOffset(), newIndexSize);

    return newIndexSize;
  }

  /** @return the current size of the index */
  public int size() {
    return getInt(indexSizeOffset());
  }

  public long getLogPosition(int idx) {
    boundsCheck(idx, size());

    final int entryOffset = entryOffset(idx);

    return getLong(entryLogPositionOffset(entryOffset));
  }

  public long getAddress(int idx) {
    boundsCheck(idx, size());

    final int entryOffset = entryOffset(idx);

    return getLong(entryAddressOffset(entryOffset));
  }

  private static void boundsCheck(int idx, int size) {
    if (idx < 0 || idx >= size) {
      throw new IllegalArgumentException(
          String.format("Index out of bounds. index=%d, size=%d.", idx, size));
    }
  }
}
