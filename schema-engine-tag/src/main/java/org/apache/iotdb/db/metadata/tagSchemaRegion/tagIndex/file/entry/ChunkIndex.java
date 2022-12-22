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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry;

import org.apache.iotdb.lsm.sstable.bplustree.entry.IEntry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ChunkIndex implements IEntry {
  private List<ChunkIndexEntry> chunkIndexEntries;

  private ChunkIndexHeader chunkIndexHeader;

  public ChunkIndex(List<ChunkIndexEntry> chunkIndexEntries, ChunkIndexHeader chunkIndexHeader) {
    this.chunkIndexEntries = chunkIndexEntries;
    this.chunkIndexHeader = chunkIndexHeader;
  }

  public ChunkIndex() {
    this.chunkIndexEntries = new ArrayList<>();
  }

  public List<ChunkIndexEntry> getChunkIndexEntries() {
    return chunkIndexEntries;
  }

  public void addChunkIndexEntry(ChunkIndexEntry chunkIndexEntry) {
    this.chunkIndexEntries.add(chunkIndexEntry);
  }

  public void setChunkIndexEntries(List<ChunkIndexEntry> chunkIndexEntries) {
    this.chunkIndexEntries = chunkIndexEntries;
  }

  public int getAllCount() {
    if (chunkIndexEntries == null || chunkIndexEntries.size() == 0) {
      return 0;
    }
    int count = 0;
    for (ChunkIndexEntry chunkIndexEntry : chunkIndexEntries) {
      count += chunkIndexEntry.getCount();
    }
    return count;
  }

  public ChunkIndexHeader getChunkIndexHeader() {
    return chunkIndexHeader;
  }

  public void setChunkIndexHeader(ChunkIndexHeader chunkIndexHeader) {
    this.chunkIndexHeader = chunkIndexHeader;
  }

  @Override
  public String toString() {
    return "ChunkIndex{"
        + "chunkIndexEntries="
        + chunkIndexEntries
        + ", chunkIndexHeader="
        + chunkIndexHeader
        + '}';
  }

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    chunkIndexHeader.serialize(out);
    for (ChunkIndexEntry chunkIndexEntry : chunkIndexEntries) {
      chunkIndexEntry.serialize(out);
    }
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    chunkIndexHeader.serialize(byteBuffer);
    for (ChunkIndexEntry chunkIndexEntry : chunkIndexEntries) {
      chunkIndexEntry.serialize(byteBuffer);
    }
  }

  @Override
  public IEntry deserialize(DataInputStream input) throws IOException {
    chunkIndexHeader = new ChunkIndexHeader();
    chunkIndexHeader.deserialize(input);
    chunkIndexEntries = new ArrayList<>(chunkIndexHeader.getSize());
    for (int i = 0; i < chunkIndexHeader.getSize(); i++) {
      ChunkIndexEntry chunkIndexEntry = new ChunkIndexEntry();
      chunkIndexEntry.deserialize(input);
      chunkIndexEntries.add(chunkIndexEntry);
    }
    return this;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    chunkIndexHeader = new ChunkIndexHeader();
    chunkIndexHeader.deserialize(byteBuffer);
    chunkIndexEntries = new ArrayList<>(chunkIndexHeader.getSize());
    for (int i = 0; i < chunkIndexHeader.getSize(); i++) {
      ChunkIndexEntry chunkIndexEntry = new ChunkIndexEntry();
      chunkIndexEntry.deserialize(byteBuffer);
      chunkIndexEntries.add(chunkIndexEntry);
    }
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChunkIndex that = (ChunkIndex) o;
    return Objects.equals(chunkIndexEntries, that.chunkIndexEntries)
        && Objects.equals(chunkIndexHeader, that.chunkIndexHeader);
  }

  @Override
  public int hashCode() {
    return Objects.hash(chunkIndexEntries, chunkIndexHeader);
  }
}
