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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable;

import java.util.HashMap;
import java.util.Map;

/** used to manage tagValue -> MemChunk */
public class MemTagValueGroup {

  // manage tagValue -> MemChunkGroup
  private Map<String, MemChunkGroup> memChunkGroupMap;

  public MemTagValueGroup() {
    memChunkGroupMap = new HashMap<>();
  }

  public void put(String tagValue) {
    if (!memChunkGroupMap.containsKey(tagValue)) {
      memChunkGroupMap.put(tagValue, new MemChunkGroup());
    }
  }

  @Override
  public String toString() {
    return memChunkGroupMap.toString();
  }

  public MemChunkGroup get(String tagValue) {
    return memChunkGroupMap.get(tagValue);
  }

  public void remove(String tagValue) {
    memChunkGroupMap.remove(tagValue);
  }

  public boolean isEmpty() {
    return memChunkGroupMap.isEmpty();
  }
}