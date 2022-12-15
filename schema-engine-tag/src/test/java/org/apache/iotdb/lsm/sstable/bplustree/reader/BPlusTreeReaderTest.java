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
package org.apache.iotdb.lsm.sstable.bplustree.reader;

import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaDescriptor;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeEntry;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeHeader;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNode;
import org.apache.iotdb.lsm.sstable.bplustree.writer.BPlusTreeWriter;
import org.apache.iotdb.lsm.sstable.fileIO.FileOutput;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.assertEquals;

public class BPlusTreeReaderTest {

  File file;

  BPlusTreeWriter bPlusTreeWriter;

  BPlusTreeReader bPlusTreeReader;

  Queue<BPlusTreeEntry> orderedQueue;

  int degree;

  long offset;

  @Before
  public void setUp() throws Exception {
    file = new File("BPlusTreeReaderTest.txt");
    degree = TagSchemaDescriptor.getInstance().getTagSchemaConfig().getDegree();
    TagSchemaDescriptor.getInstance().getTagSchemaConfig().setDegree(3);
    orderedQueue = new ArrayDeque<>();
    orderedQueue.add(new BPlusTreeEntry("aaa", 0));
    orderedQueue.add(new BPlusTreeEntry("bbb", 1));
    orderedQueue.add(new BPlusTreeEntry("c", 2));
    orderedQueue.add(new BPlusTreeEntry("dd", 3));
    orderedQueue.add(new BPlusTreeEntry("eeeee", 4));
    orderedQueue.add(new BPlusTreeEntry("fff", 5));
    orderedQueue.add(new BPlusTreeEntry("gggg", 6));
    orderedQueue.add(new BPlusTreeEntry("hhhhhhhhhh", 7));
    orderedQueue.add(new BPlusTreeEntry("x", 8));
    orderedQueue.add(new BPlusTreeEntry("yyyy", 9));
    orderedQueue.add(new BPlusTreeEntry("zz", 10));

    FileOutputStream fileOutputStream = new FileOutputStream(file);
    FileOutput fileOutput = new FileOutput(fileOutputStream, 1024 * 1024);

    bPlusTreeWriter = new BPlusTreeWriter(fileOutput);
    offset = bPlusTreeWriter.write(orderedQueue, true);

    bPlusTreeReader = new BPlusTreeReader(file, offset);
  }

  @After
  public void tearDown() throws Exception {
    if (bPlusTreeWriter != null) {
      bPlusTreeWriter.close();
    }
    if (bPlusTreeReader != null) {
      bPlusTreeReader.close();
    }
    TagSchemaDescriptor.getInstance().getTagSchemaConfig().setDegree(degree);
    file.delete();
  }

  @Test
  public void testGetBPlusTreeHeader() throws IOException {
    BPlusTreeHeader bPlusTreeHeader = bPlusTreeReader.readBPlusTreeHeader(offset);
    assertEquals(bPlusTreeHeader.getMax(), "zz");
    assertEquals(bPlusTreeHeader.getMin(), "aaa");
    assertEquals(bPlusTreeHeader.getFirstLeftNodeOffset(), 0);
    assertEquals(bPlusTreeHeader.getLeftNodeCount(), 4);
  }

  @Test
  public void testIterateOverRecords() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    FileInputStream fileInputStream = new FileInputStream(file);
    fileInputStream.getChannel().read(buffer);
    fileInputStream.close();

    buffer.flip();
    List<BPlusTreeNode> bPlusTreeNodes = new ArrayList<>();

    int i = 0;
    while (i < 7) {
      BPlusTreeNode bPlusTreeNode = new BPlusTreeNode();
      bPlusTreeNode.deserialize(buffer);
      bPlusTreeNodes.add(bPlusTreeNode);
      i++;
    }
    buffer.clear();

    i = 0;
    while (bPlusTreeReader.hasNext()) {
      assertEquals(bPlusTreeNodes.get(i), bPlusTreeReader.next());
      i++;
    }
  }
}
