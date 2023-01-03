/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.lsm.sstable.bplustree.writer;

import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaConfig;
import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaDescriptor;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeEntry;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeHeader;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNode;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNodeType;
import org.apache.iotdb.lsm.sstable.fileIO.TiFileOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Map;
import java.util.Queue;

public class BPlusTreeWriter implements IBPlusTreeWriter {

  private Queue<BPlusTreeEntry> currentBPlusTreeEntryQueue;

  private Queue<BPlusTreeEntry> upperLevelBPlusTreeEntryQueue;

  private TiFileOutputStream fileOutput;

  private final TagSchemaConfig bPlushTreeConfig =
      TagSchemaDescriptor.getInstance().getTagSchemaConfig();

  private BPlusTreeHeader bPlushTreeHeader;

  private ByteBuffer byteBuffer;

  public BPlusTreeWriter(TiFileOutputStream fileOutput) {
    this.fileOutput = fileOutput;
    this.currentBPlusTreeEntryQueue = new ArrayDeque<>();
    this.upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
    bPlushTreeHeader = new BPlusTreeHeader();
  }

  public BPlusTreeWriter(Queue<BPlusTreeEntry> bPlusTreeEntryQueue, TiFileOutputStream fileOutput) {
    this.currentBPlusTreeEntryQueue = bPlusTreeEntryQueue;
    this.upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
    this.fileOutput = fileOutput;
    bPlushTreeHeader = new BPlusTreeHeader();
  }

  /**
   * generate a b+ tree and header for records and write to disk
   *
   * @param records a map that holds all records, the map can be unordered
   * @param ordered whether the queue is in order
   * @return start offset of the b+ tree
   * @throws IOException
   */
  @Override
  public long write(Map<String, Long> records, boolean ordered) throws IOException {
    setCurrentBPlusTreeEntryQueue(records, ordered);
    BPlusTreeHeader bPlusTreeHeader = writeBPlusTree();
    return fileOutput.write(bPlusTreeHeader);
  }

  /**
   * generate a b+ tree for records and write to disk
   *
   * @param records a map that holds all records, the map can be unordered
   * @param ordered whether the queue is in order
   * @return b+ tree header
   * @throws IOException
   */
  @Override
  public BPlusTreeHeader writeBPlusTree(Map<String, Long> records, boolean ordered)
      throws IOException {
    setCurrentBPlusTreeEntryQueue(records, ordered);
    return writeBPlusTree();
  }

  /**
   * generate a b+ tree and header for records and write to disk
   *
   * @param records a queue that holds all records, the queue can be unordered
   * @param ordered whether the queue is in order
   * @return start offset of the b+ tree
   * @throws IOException
   */
  @Override
  public long write(Queue<BPlusTreeEntry> records, boolean ordered) throws IOException {
    setCurrentBPlusTreeEntryQueue(records, ordered);
    BPlusTreeHeader bPlusTreeHeader = writeBPlusTree();
    return fileOutput.write(bPlusTreeHeader);
  }

  /**
   * generate a b+ tree for records and write to disk
   *
   * @param records a queue that holds all records, the queue can be unordered
   * @param ordered whether the queue is in order
   * @return b+ tree header
   * @throws IOException
   */
  @Override
  public BPlusTreeHeader writeBPlusTree(Queue<BPlusTreeEntry> records, boolean ordered)
      throws IOException {
    setCurrentBPlusTreeEntryQueue(records, ordered);
    return writeBPlusTree();
  }

  /**
   * collect the records to be written to the disk, and only call write or writeBPlusTree to
   * actually write to the disk, if the written records are ordered, you can directly call the write
   * or writeBPlusTree methods to write to disk, otherwise call the sortAndWrite and
   * sortAndWriteBPlusTree methods
   *
   * @param name name of the record
   * @param offset offset of the record
   * @return this
   */
  @Override
  public IBPlusTreeWriter collectRecord(String name, long offset) {
    currentBPlusTreeEntryQueue.add(new BPlusTreeEntry(name, offset));
    return this;
  }

  /**
   * generate a b+ tree and header for records and write to disk
   *
   * @return start offset of the b+ tree
   * @throws IOException
   */
  @Override
  public long write() throws IOException {
    BPlusTreeHeader bPlusTreeHeader = writeBPlusTree();
    return fileOutput.write(bPlusTreeHeader);
  }

  public BPlusTreeHeader writeBPlusTreeOnlyUseDegree() throws IOException {
    setBPlushTreeHeader();
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
    BPlusTreeEntry bPlusTreeEntry = null;
    if (currentBPlusTreeEntryQueue.size() <= bPlushTreeConfig.getDegree()) {
      return directWriteOneBPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
    }
    int count = 0;
    while (!currentBPlusTreeEntryQueue.isEmpty()) {
      bPlusTreeEntry = currentBPlusTreeEntryQueue.poll();
      if (!bPlusTreeNode.needToSplit(bPlushTreeConfig.getDegree())) {
        bPlusTreeNode.add(bPlusTreeEntry);
      } else {
        writeBPlusTreeNode(bPlusTreeNode);
        count++;
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
        bPlusTreeNode.add(bPlusTreeEntry);
      }
    }
    if (bPlusTreeEntry != null) {
      bPlushTreeHeader.setMax(bPlusTreeEntry.getName());
    }
    if (bPlusTreeNode.getCount() > 0) {
      writeBPlusTreeNode(bPlusTreeNode);
      count++;
    }
    currentBPlusTreeEntryQueue = upperLevelBPlusTreeEntryQueue;
    bPlushTreeHeader.setLeftNodeCount(count);
    return writeInternalNodeOnlyUseDegree();
  }

  /**
   * generate a b+ tree for records and write to disk
   *
   * @return b+ tree header
   * @throws IOException
   */
  @Override
  public BPlusTreeHeader writeBPlusTree() throws IOException {
    setBPlushTreeHeader();
    if (byteBuffer == null) {
      byteBuffer = ByteBuffer.allocate(bPlushTreeConfig.getBPlusTreePageSize());
      byteBuffer.position(5);
    }
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
    BPlusTreeEntry bPlusTreeEntry = null;
    int count = 0;
    while (!currentBPlusTreeEntryQueue.isEmpty()) {
      bPlusTreeEntry = currentBPlusTreeEntryQueue.poll();
      if (bPlusTreeNode.getCount() >= bPlushTreeConfig.getDegree()
          || !bPlusTreeNode.addAndSerialize(bPlusTreeEntry, byteBuffer)) {
        writeBPlusTreePage(bPlusTreeNode);
        count++;
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
        bPlusTreeNode.addAndSerialize(bPlusTreeEntry, byteBuffer);
      }
    }
    if (bPlusTreeEntry != null) {
      bPlushTreeHeader.setMax(bPlusTreeEntry.getName());
    }
    if (bPlusTreeNode.getCount() > 0) {
      writeBPlusTreePage(bPlusTreeNode);
      count++;
    }
    currentBPlusTreeEntryQueue = upperLevelBPlusTreeEntryQueue;
    bPlushTreeHeader.setLeftNodeCount(count);
    if (count == 1) {
      bPlushTreeHeader.setRootNodeOffset(bPlushTreeHeader.getFirstLeftNodeOffset());
      return bPlushTreeHeader;
    }
    return writeInternalNode();
  }

  /**
   * generate a b+ tree and header for records and write to disk, first the records are sorted
   *
   * @return start offset of the b+ tree
   * @throws IOException
   */
  @Override
  public long sortAndWrite() throws IOException {
    sortCurrentBPlusTreeEntryQueue();
    return write();
  }

  /**
   * generate a b+ tree for records and write to disk, first the records are sorted
   *
   * @return b+ tree header
   * @throws IOException
   */
  @Override
  public BPlusTreeHeader sortAndWriteBPlusTree() throws IOException {
    sortCurrentBPlusTreeEntryQueue();
    return writeBPlusTree();
  }

  private BPlusTreeHeader writeInternalNodeOnlyUseDegree() throws IOException {
    upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
    int size = currentBPlusTreeEntryQueue.size();
    while (!currentBPlusTreeEntryQueue.isEmpty()) {
      if (size <= bPlushTreeConfig.getDegree()) {
        return directWriteOneBPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
      }
      BPlusTreeEntry bPlusTreeEntry = currentBPlusTreeEntryQueue.poll();
      if (!bPlusTreeNode.needToSplit(bPlushTreeConfig.getDegree())) {
        bPlusTreeNode.add(bPlusTreeEntry);
      } else {
        writeBPlusTreeNode(bPlusTreeNode);
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
        bPlusTreeNode.add(bPlusTreeEntry);
      }
      if (currentBPlusTreeEntryQueue.isEmpty()) {
        if (bPlusTreeNode.getCount() > 0) {
          writeBPlusTreeNode(bPlusTreeNode);
        }
        currentBPlusTreeEntryQueue = upperLevelBPlusTreeEntryQueue;
        size = currentBPlusTreeEntryQueue.size();
        upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
      }
    }
    return bPlushTreeHeader;
  }

  private BPlusTreeHeader writeInternalNode() throws IOException {
    upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
    int count = 0;
    long rootNodeOffset = 0;
    while (!currentBPlusTreeEntryQueue.isEmpty()) {
      BPlusTreeEntry bPlusTreeEntry = currentBPlusTreeEntryQueue.poll();
      if (bPlusTreeNode.getCount() >= bPlushTreeConfig.getDegree()
          || !bPlusTreeNode.addAndSerialize(bPlusTreeEntry, byteBuffer)) {
        rootNodeOffset = writeBPlusTreePage(bPlusTreeNode);
        count++;
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
        bPlusTreeNode.addAndSerialize(bPlusTreeEntry, byteBuffer);
      }
      if (currentBPlusTreeEntryQueue.isEmpty()) {
        if (bPlusTreeNode.getCount() > 0) {
          rootNodeOffset = writeBPlusTreePage(bPlusTreeNode);
          count++;
        }
        if (count == 1) {
          bPlushTreeHeader.setRootNodeOffset(rootNodeOffset);
          return bPlushTreeHeader;
        }
        currentBPlusTreeEntryQueue = upperLevelBPlusTreeEntryQueue;
        upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
        count = 0;
      }
    }
    return bPlushTreeHeader;
  }

  public Queue<BPlusTreeEntry> getBPlusTreeEntryQueue() {
    return currentBPlusTreeEntryQueue;
  }

  public TiFileOutputStream getFileOutput() {
    return fileOutput;
  }

  public void setFileOutput(TiFileOutputStream fileOutput) {
    this.fileOutput = fileOutput;
  }

  private void setBPlushTreeHeader() {
    if (!currentBPlusTreeEntryQueue.isEmpty()) {
      bPlushTreeHeader.setMax(currentBPlusTreeEntryQueue.peek().getName());
      bPlushTreeHeader.setMin(currentBPlusTreeEntryQueue.peek().getName());
      bPlushTreeHeader.setFirstLeftNodeOffset(fileOutput.getPosition());
    }
  }

  private long writeBPlusTreeNode(BPlusTreeNode bPlusTreeNode) throws IOException {
    long startOffset = fileOutput.write(bPlusTreeNode);
    upperLevelBPlusTreeEntryQueue.add(new BPlusTreeEntry(bPlusTreeNode.getMin(), startOffset));
    return startOffset;
  }

  private long writeBPlusTreePage(BPlusTreeNode bPlusTreeNode) throws IOException {
    byteBuffer.put(0, bPlusTreeNode.getbPlusTreeNodeType().getType());
    byteBuffer.putInt(1, bPlusTreeNode.getCount());
    byteBuffer.flip();
    long startOffset = fileOutput.getPosition();
    fileOutput.write(byteBuffer.array(), 0, byteBuffer.limit());
    upperLevelBPlusTreeEntryQueue.add(new BPlusTreeEntry(bPlusTreeNode.getMin(), startOffset));
    byteBuffer.clear();
    byteBuffer.position(5);
    return startOffset;
  }

  private BPlusTreeHeader directWriteOneBPlusTreeNode(BPlusTreeNodeType type) throws IOException {
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode(type);
    for (BPlusTreeEntry bPlusTreeEntry : currentBPlusTreeEntryQueue) {
      bPlusTreeNode.add(bPlusTreeEntry);
    }
    if (bPlusTreeNode.getCount() > 0) {
      long rootNodeOffset = writeBPlusTreeNode(bPlusTreeNode);
      bPlushTreeHeader.setRootNodeOffset(rootNodeOffset);
    }
    if (type.equals(BPlusTreeNodeType.LEAF_NODE)) {
      bPlushTreeHeader.setLeftNodeCount(1);
    }
    return bPlushTreeHeader;
  }

  private void setCurrentBPlusTreeEntryQueue(Map<String, Long> records, boolean ordered) {
    if (ordered) {
      records.forEach(
          (key, value) -> currentBPlusTreeEntryQueue.add(new BPlusTreeEntry(key, value)));
    } else {
      records.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach(
              entry ->
                  currentBPlusTreeEntryQueue.add(
                      new BPlusTreeEntry(entry.getKey(), entry.getValue())));
    }
  }

  private void setCurrentBPlusTreeEntryQueue(Queue<BPlusTreeEntry> records, boolean ordered) {
    if (ordered) {
      currentBPlusTreeEntryQueue = records;
    } else {
      records.stream()
          .sorted(Comparator.comparing(BPlusTreeEntry::getName))
          .forEach(bPlusTreeEntry -> currentBPlusTreeEntryQueue.add(bPlusTreeEntry));
    }
  }

  private void sortCurrentBPlusTreeEntryQueue() {
    Queue<BPlusTreeEntry> queue = new ArrayDeque<>();
    currentBPlusTreeEntryQueue.stream()
        .sorted(Comparator.comparing(BPlusTreeEntry::getName))
        .forEach(queue::add);
    currentBPlusTreeEntryQueue = queue;
  }

  @Override
  public void close() throws IOException {
    if (currentBPlusTreeEntryQueue != null) {
      currentBPlusTreeEntryQueue.clear();
    }
    if (upperLevelBPlusTreeEntryQueue != null) {
      upperLevelBPlusTreeEntryQueue.clear();
    }
    if (fileOutput != null) {
      fileOutput.close();
    }
  }
}
