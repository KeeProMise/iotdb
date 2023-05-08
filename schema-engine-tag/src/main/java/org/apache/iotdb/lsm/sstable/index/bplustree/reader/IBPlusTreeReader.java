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
package org.apache.iotdb.lsm.sstable.index.bplustree.reader;

import org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeEntry;
import org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeHeader;
import org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeNode;
import org.apache.iotdb.lsm.sstable.interator.IDiskIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Read the b+ tree on disk, which must be generated by {@link
 * org.apache.iotdb.lsm.sstable.index.bplustree.writer.IBPlusTreeWriter IBPlusTreeWriter}
 */
public interface IBPlusTreeReader extends IDiskIterator<BPlusTreeNode> {

  /**
   * Read a {@link org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeHeader
   * BPlusTreeHeader} from the given offset
   *
   * @param bPlusTreeStartOffset a non-negative integer counting the number of bytes from the
   *     beginning of the BPlusTreeHeader object
   * @return a BPlusTreeHeader object
   * @exception EOFException if this input stream reaches the end before reading eight bytes.
   * @exception IOException the stream has been closed and the contained input stream does not
   *     support reading after close, or another I/O error occurs.
   */
  BPlusTreeHeader readBPlusTreeHeader(long bPlusTreeStartOffset) throws IOException;

  /**
   * Read a {@link org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeNode BPlusTreeNode}
   * from the default offset
   *
   * @return a BPlusTreeNode object
   * @exception EOFException if this input stream reaches the end before reading eight bytes.
   * @exception IOException the stream has been closed and the contained input stream does not
   *     support reading after close, or another I/O error occurs.
   */
  BPlusTreeNode readBPlusTreeRootNode() throws IOException;

  /**
   * Read a {@link org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeNode BPlusTreeNode}
   * from the default offset
   *
   * @param bPlusTreeRootNodeOffset a non-negative integer counting the number of bytes from the
   *     beginning of the BPlusTreeNode object
   * @return a BPlusTreeNode object
   * @exception EOFException if this input stream reaches the end before reading eight bytes.
   * @exception IOException the stream has been closed and the contained input stream does not
   *     support reading after close, or another I/O error occurs.
   */
  BPlusTreeNode readBPlusTreeRootNode(long bPlusTreeRootNodeOffset) throws IOException;

  /**
   * Get all {@link org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeEntry
   * BPlusTreeEntry} that can match the names
   *
   * @param names the name of all entries that need to be found
   * @return a list, If a certain name cannot find the corresponding entry, the entry will not be
   *     written into the list
   * @exception EOFException if this input stream reaches the end before reading eight bytes.
   * @exception IOException the stream has been closed and the contained input stream does not
   *     support reading after close, or another I/O error occurs.
   */
  List<BPlusTreeEntry> getBPlusTreeEntries(Set<String> names) throws IOException;

  /**
   * Get all {@link org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeEntry
   * BPlusTreeEntry} that can match the names
   *
   * @param rootNode {@link org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeNode
   *     BPlusTreeNode} of the b+ tree
   * @param names the name of all entries that need to be found
   * @return a list, If a certain name cannot find the corresponding entry, the entry will not be
   *     written into the list
   * @exception EOFException if this input stream reaches the end before reading eight bytes.
   * @exception IOException the stream has been closed and the contained input stream does not
   *     support reading after close, or another I/O error occurs.
   */
  List<BPlusTreeEntry> getBPlusTreeEntries(BPlusTreeNode rootNode, Set<String> names)
      throws IOException;

  /**
   * Get all {@link org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeEntry
   * BPlusTreeEntry} that can match the names
   *
   * @param bPlusTreeHeader {@link
   *     org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeHeader BPlusTreeHeader} of the
   *     b+ tree
   * @param names the name of all entries that need to be found
   * @return a list, If a certain name cannot find the corresponding entry, the entry will not be
   *     written into the list
   * @exception EOFException if this input stream reaches the end before reading eight bytes.
   * @exception IOException the stream has been closed and the contained input stream does not
   *     support reading after close, or another I/O error occurs.
   */
  List<BPlusTreeEntry> getBPlusTreeEntries(BPlusTreeHeader bPlusTreeHeader, Set<String> names)
      throws IOException;

  /**
   * Closes this reader and releases any system resources associated with the reader.
   *
   * @exception IOException if an I/O error occurs.
   */
  void close() throws IOException;
}