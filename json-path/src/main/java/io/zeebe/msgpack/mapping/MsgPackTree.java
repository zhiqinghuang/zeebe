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

import static io.zeebe.msgpack.mapping.MsgPackNodeType.EXISTING_LEAF_NODE;
import static io.zeebe.msgpack.mapping.MsgPackNodeType.EXTRACTED_LEAF_NODE;

import io.zeebe.msgpack.spec.MsgPackWriter;
import java.util.*;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Represents a tree data structure, for a msg pack document.
 *
 * <p>The nodes of the tree can be either a real node, which has child's, or a leaf, which has a
 * mapping in the corresponding msg pack document to his value.
 *
 * <p>The message pack document tree can be created from scratch from a underlying document. This
 * can be done with the {@link MsgPackDocumentIndexer}. It can also be constructed from only a port
 * of a message pack document. This can be done with the {@link MsgPackDocumentExtractor}.
 *
 * <p>The message pack tree can consist from two different message pack documents. The underlying
 * document, from which the tree is completely build and the extract document, which can be a part
 * of another message pack document. The tree representation of the extract document will be as well
 * added to the current message pack tree object.
 *
 * <p>Since the leafs contains a mapping, which consist of position and length, it is necessary that
 * both documents are available for the message pack tree, so the leaf value can be resolved later.
 * The leafs have to be distinguished, is it a leaf from the underlying document or is it from the
 * extract document. For this distinction the {@link MsgPackNodeType#EXISTING_LEAF_NODE} and {@link
 * MsgPackNodeType#EXTRACTED_LEAF_NODE} are used.
 */
public class MsgPackTree {

  private static class Node {
    final DirectBuffer nodeId = new UnsafeBuffer(0, 0);
    final Set<DirectBuffer> childs = new LinkedHashSet<>();

    MsgPackNodeType msgPackNodeType;
    long leafMapping;
  }

  private final Map<DirectBuffer, Node> nodes;
  private final DirectBuffer bufferForSearch = new UnsafeBuffer(0, 0);

  protected final DirectBuffer underlyingDocument = new UnsafeBuffer(0, 0);
  protected DirectBuffer extractDocument;

  public MsgPackTree() {
    nodes = new HashMap<>();
  }

  public long size() {
    return nodes.size();
  }

  public void wrap(DirectBuffer underlyingDocument) {
    clear();
    this.underlyingDocument.wrap(underlyingDocument);
  }

  public void clear() {
    extractDocument = null;
    nodes.clear();
  }

  public Set<DirectBuffer> getChilds(byte[] nodeId) {
    bufferForSearch.wrap(nodeId);
    return nodes.get(bufferForSearch).childs;
  }

  public void addLeafNode(byte[] nodeId, long position, int length) {
    final Node node = new Node();
    node.leafMapping = (position << 32) | length;
    node.msgPackNodeType = extractDocument == null ? EXISTING_LEAF_NODE : EXTRACTED_LEAF_NODE;
    node.nodeId.wrap(nodeId);
    nodes.put(node.nodeId, node);
  }

  private void addParentNode(byte[] nodeId, MsgPackNodeType nodeType) {
    final Node node = new Node();
    node.msgPackNodeType = nodeType;
    node.nodeId.wrap(nodeId);

    final Node existingNode = nodes.get(node.nodeId);
    if (null == existingNode) {
      nodes.put(node.nodeId, node);
    } else {
      existingNode.leafMapping = 0;
      existingNode.msgPackNodeType = nodeType;
    }
  }

  public void addMapNode(byte[] nodeId) {
    addParentNode(nodeId, MsgPackNodeType.MAP_NODE);
  }

  public void addArrayNode(byte[] nodeId) {
    addParentNode(nodeId, MsgPackNodeType.ARRAY_NODE);
  }

  public void addChildToNode(byte[] childName, byte[] parentId) {
    bufferForSearch.wrap(parentId);
    nodes.get(bufferForSearch).childs.add(new UnsafeBuffer(childName));
  }

  public boolean isLeaf(byte[] nodeId) {
    bufferForSearch.wrap(nodeId);
    final Node node = nodes.get(bufferForSearch);

    if (node != null) {
      return node.leafMapping != 0;
    }
    return false;
  }

  public boolean isArrayNode(byte[] nodeId) {
    bufferForSearch.wrap(nodeId);
    final Node node = nodes.get(bufferForSearch);
    return node.msgPackNodeType == MsgPackNodeType.ARRAY_NODE;
  }

  public boolean isMapNode(byte[] nodeId) {
    bufferForSearch.wrap(nodeId);
    final Node node = nodes.get(bufferForSearch);
    return node != null && node.msgPackNodeType == MsgPackNodeType.MAP_NODE;
  }

  public void setExtractDocument(DirectBuffer documentBuffer) {
    this.extractDocument = documentBuffer;
  }

  public void writeLeafMapping(MsgPackWriter writer, byte[] leafId) {
    bufferForSearch.wrap(leafId);
    final Node node = nodes.get(bufferForSearch);
    final long mapping = node.leafMapping;
    final MsgPackNodeType nodeType = node.msgPackNodeType;
    final int position = (int) (mapping >> 32);
    final int length = (int) mapping;
    DirectBuffer relatedBuffer = underlyingDocument;
    if (nodeType == EXTRACTED_LEAF_NODE) {
      relatedBuffer = extractDocument;
    }
    writer.writeRaw(relatedBuffer, position, length);
  }

  public void merge(MsgPackTree sourceTree) {
    extractDocument = sourceTree.underlyingDocument;

    for (Node node : sourceTree.nodes.values()) {
      final DirectBuffer key = node.nodeId;

      final Node newNode = new Node();
      newNode.nodeId.wrap(key);

      MsgPackNodeType nodeType = node.msgPackNodeType;
      if (nodeType == EXISTING_LEAF_NODE) {
        nodeType = EXTRACTED_LEAF_NODE;
      }
      newNode.msgPackNodeType = nodeType;
      newNode.leafMapping = node.leafMapping;

      if (key.capacity() == 1 && key.getByte(0) == Mapping.JSON_ROOT_PATH_BYTE) {
        nodes.computeIfAbsent(key, (k) -> newNode).childs.addAll(node.childs);
      } else {
        newNode.childs.addAll(node.childs);
        nodes.put(newNode.nodeId, newNode);
      }
    }
  }
}
