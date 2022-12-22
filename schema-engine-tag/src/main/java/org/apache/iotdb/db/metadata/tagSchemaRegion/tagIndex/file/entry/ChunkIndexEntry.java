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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ChunkIndexEntry implements IEntry {

  private long offset;

  private int count;

  private int idMax;

  private int idMin;

  public ChunkIndexEntry() {}

  public ChunkIndexEntry(long offset, int count, int idMax, int idMin) {
    this.offset = offset;
    this.count = count;
    this.idMax = idMax;
    this.idMin = idMin;
  }

  public boolean intersect(ChunkIndexEntry chunkIndexEntry) {
    return chunkIndexEntry.idMax >= idMin && chunkIndexEntry.idMin <= idMax;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public int getIdMax() {
    return idMax;
  }

  public void setIdMax(int idMax) {
    this.idMax = idMax;
  }

  public int getIdMin() {
    return idMin;
  }

  public void setIdMin(int idMin) {
    this.idMin = idMin;
  }

  @Override
  public String toString() {
    return "ChunkIndexEntry{"
        + "offset="
        + offset
        + ", count="
        + count
        + ", idMax="
        + idMax
        + ", idMin="
        + idMin
        + '}';
  }

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    ReadWriteIOUtils.write(offset, out);
    ReadWriteIOUtils.write(count, out);
    ReadWriteIOUtils.write(idMax, out);
    ReadWriteIOUtils.write(idMin, out);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(offset, byteBuffer);
    ReadWriteIOUtils.write(count, byteBuffer);
    ReadWriteIOUtils.write(idMax, byteBuffer);
    ReadWriteIOUtils.write(idMin, byteBuffer);
  }

  @Override
  public IEntry deserialize(DataInputStream input) throws IOException {
    offset = ReadWriteIOUtils.readLong(input);
    count = ReadWriteIOUtils.readInt(input);
    idMax = ReadWriteIOUtils.readInt(input);
    idMin = ReadWriteIOUtils.readInt(input);
    return this;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    offset = ReadWriteIOUtils.readLong(byteBuffer);
    count = ReadWriteIOUtils.readInt(byteBuffer);
    idMax = ReadWriteIOUtils.readInt(byteBuffer);
    idMin = ReadWriteIOUtils.readInt(byteBuffer);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChunkIndexEntry that = (ChunkIndexEntry) o;
    return offset == that.offset
        && count == that.count
        && idMax == that.idMax
        && idMin == that.idMin;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, count, idMax, idMin);
  }
}
