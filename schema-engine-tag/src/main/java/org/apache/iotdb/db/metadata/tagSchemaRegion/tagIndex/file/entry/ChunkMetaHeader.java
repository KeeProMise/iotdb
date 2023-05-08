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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/** Record additional information of chunk index */
public class ChunkMetaHeader implements IDiskEntry {
  // How many chunk index entries are in the chunk index
  private int size;

  public ChunkMetaHeader() {}

  public ChunkMetaHeader(int size) {
    this.size = size;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  @Override
  public String toString() {
    return "ChunkMetaHeader{" + "size=" + size + '}';
  }

  @Override
  public int serialize(DataOutputStream out) throws IOException {
    return ReadWriteIOUtils.write(size, out);
  }

  @Override
  public IDiskEntry deserialize(DataInputStream input) throws IOException {
    size = ReadWriteIOUtils.readInt(input);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChunkMetaHeader that = (ChunkMetaHeader) o;
    return size == that.size;
  }

  @Override
  public int hashCode() {
    return Objects.hash(size);
  }
}