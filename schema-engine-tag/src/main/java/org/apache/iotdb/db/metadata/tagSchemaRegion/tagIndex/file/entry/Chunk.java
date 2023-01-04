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

import org.apache.iotdb.lsm.sstable.diskentry.IDiskEntry;

import org.roaringbitmap.RoaringBitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/** Represents a chunk structure in the tiFile */
public class Chunk implements IDiskEntry {
  // Describe some information about the chunk
  private ChunkHeader chunkHeader;

  // save ids
  private RoaringBitmap roaringBitmap;

  @Override
  public int serialize(DataOutputStream out) throws IOException {
    int len = roaringBitmap.serializedSizeInBytes();
    roaringBitmap.serialize(out);
    len += chunkHeader.serialize(out);
    return len;
  }

  @Override
  public IDiskEntry deserialize(DataInputStream input) throws IOException {
    roaringBitmap = new RoaringBitmap();
    roaringBitmap.deserialize(input);
    chunkHeader = new ChunkHeader();
    chunkHeader.deserialize(input);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Chunk chunk = (Chunk) o;
    return Objects.equals(chunkHeader, chunk.chunkHeader)
        && Objects.equals(roaringBitmap, chunk.roaringBitmap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(chunkHeader, roaringBitmap);
  }

  public ChunkHeader getChunkHeader() {
    return chunkHeader;
  }

  public void setChunkHeader(ChunkHeader chunkHeader) {
    this.chunkHeader = chunkHeader;
  }

  public RoaringBitmap getRoaringBitmap() {
    return roaringBitmap;
  }

  public void setRoaringBitmap(RoaringBitmap roaringBitmap) {
    this.roaringBitmap = roaringBitmap;
  }

  @Override
  public String toString() {
    return "Chunk{" + "chunkHeader=" + chunkHeader + ", roaringBitmap=" + roaringBitmap + '}';
  }
}
