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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.query;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.lsm.context.QueryContext;
import org.apache.iotdb.lsm.levelProcess.QueryLevelProcess;

import java.util.ArrayList;
import java.util.List;

public class MemChunkGroupQuery extends QueryLevelProcess<MemChunkGroup, MemChunk> {
  @Override
  public List<MemChunk> getChildren(MemChunkGroup memNode, QueryContext context) {
    List<MemChunk> memChunks = new ArrayList<>();
    String tagValue = (String) context.getKey();
    MemChunk child = memNode.get(tagValue);
    if (child != null) memChunks.add(child);
    return memChunks;
  }

  @Override
  public void query(MemChunkGroup memNode, QueryContext context) {}
}