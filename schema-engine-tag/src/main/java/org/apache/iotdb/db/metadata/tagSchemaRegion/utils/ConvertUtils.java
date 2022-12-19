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
package org.apache.iotdb.db.metadata.tagSchemaRegion.utils;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.Chunk;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkHeader;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;

import org.roaringbitmap.RoaringBitmap;

public class ConvertUtils {
  public static Chunk getChunkFromMemChunk(MemChunk memChunk) {
    Chunk chunk = new Chunk();
    chunk.setRoaringBitmap(memChunk.getRoaringBitmap());
    chunk.setChunkHeader(getChunkHeaderFromMemChunk(memChunk));
    return null;
  }

  public static ChunkHeader getChunkHeaderFromMemChunk(MemChunk memChunk) {
    RoaringBitmap roaringBitmap = memChunk.getRoaringBitmap();
    ChunkHeader chunkHeader = new ChunkHeader();
    chunkHeader.setSize(roaringBitmap.serializedSizeInBytes());
    int[] results = roaringBitmap.stream().toArray();
    chunkHeader.setSize(results.length);
    //    if (results.length != 0) {
    //      chunkHeader.setMinID(results[0]);
    //      chunkHeader.setMaxID(results[results.length - 1]);
    //    }
    return chunkHeader;
  }
}
