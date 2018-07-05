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
package io.zeebe.msgpack.mapping;

/** Represents a static constructor to constructing the node ids for the {@link MsgPackTree}. */
public class MsgPackTreeNodeIdConstructor {
  public static final byte JSON_PATH_SEPARATOR = '[';
  public static final byte JSON_PATH_SEPARATOR_END = ']';

  public static byte[] construct(byte[] parentId, byte[] nodeName) {
    final int length = parentId.length + nodeName.length + 2;

    final byte[] newName = new byte[length];
    int offset = 0;
    System.arraycopy(parentId, 0, newName, 0, parentId.length);
    offset += parentId.length;
    newName[offset] = JSON_PATH_SEPARATOR;
    offset += 1;
    System.arraycopy(nodeName, 0, newName, offset, nodeName.length);
    offset += nodeName.length;
    newName[offset] = JSON_PATH_SEPARATOR_END;

    return newName;
  }

  public static byte[] getLastParentId(byte[] nodeId) {

    final int indexOfLastSeparator = lastIndexOf(JSON_PATH_SEPARATOR, nodeId);

    final byte[] parentId = new byte[indexOfLastSeparator];
    System.arraycopy(nodeId, 0, parentId, 0, indexOfLastSeparator);
    return parentId;
  }

  /**
   * Extracts the index of the array value from the current node id.
   *
   * <p>Example: * given nodeId = "$[arrayId][0]" * returns "0" as string Even if the nodeId
   * contains square brackets in the node name like "$[array[Id]][0]" will return the right index
   * "0".
   *
   * @param nodeId the node id which contains the index
   * @return the array value index
   */
  public static byte[] getArrayValueIndex(byte[] nodeId) {

    final int index = lastIndexOf(JSON_PATH_SEPARATOR, nodeId);

    final int separatorLength = 1;
    final int startIndex = index + separatorLength;
    final int endIndex = nodeId.length - separatorLength;
    final int length = endIndex - startIndex;

    final byte[] nodeName = new byte[length];
    System.arraycopy(nodeId, startIndex, nodeName, 0, length);
    return nodeName;
  }

  public static final int lastIndexOf(byte toSearchedByte, byte[] content) {
    for (int i = content.length - 1; i >= 0; i--) {
      if (content[i] == toSearchedByte) {
        return i;
      }
    }
    return -1;
  }
}
