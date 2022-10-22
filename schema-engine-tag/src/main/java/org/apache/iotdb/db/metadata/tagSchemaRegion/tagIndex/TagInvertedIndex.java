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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaConfig;
import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaDescriptor;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.DeletionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.InsertionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.QueryRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.deletion.DeletionManager;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.insertion.InsertionManager;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.query.QueryManager;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.recover.RecoverManager;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal.WALManager;
import org.apache.iotdb.lsm.context.DeleteRequestContext;
import org.apache.iotdb.lsm.context.InsertRequestContext;
import org.apache.iotdb.lsm.context.QueryRequestContext;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** tag reverse index implementation class */
public class TagInvertedIndex implements ITagInvertedIndex {

  private static final Logger logger = LoggerFactory.getLogger(TagInvertedIndex.class);

  private static final TagSchemaConfig tagSchemaConfig =
      TagSchemaDescriptor.getInstance().getTagSchemaConfig();

  private InsertionManager insertionManager;

  private DeletionManager deletionManager;

  private QueryManager queryManager;

  private WALManager walManager;

  private RecoverManager recoverManager;

  // the maximum number of device ids managed by a working memTable
  private int numOfDeviceIdsInMemTable;

  // (maxDeviceID / numOfDeviceIdsInMemTable) -> MemTable
  private Map<Integer, MemTable> immutableMemTables;

  private MemTable workingMemTable;

  // the largest device id saved by the current MemTable
  private int maxDeviceID;

  public TagInvertedIndex(String schemaDirPath) {
    try {
      walManager = new WALManager(schemaDirPath);
      insertionManager = new InsertionManager(walManager);
      deletionManager = new DeletionManager(walManager);
      recoverManager = new RecoverManager(walManager);
      queryManager = new QueryManager();
      workingMemTable = new MemTable(MemTable.WORKING);
      immutableMemTables = new HashMap<>();
      numOfDeviceIdsInMemTable = tagSchemaConfig.getNumOfDeviceIdsInMemTable();
      maxDeviceID = 0;
      recover();
    } catch (IOException e) {
      logger.error("create TagInvertedIndex fail", e);
    }
  }

  public synchronized void recover() {
    recoverManager.recover(this);
  }

  /**
   * insert tags and id using insert request context
   *
   * @param insertionRequest insert request context
   */
  @Override
  public synchronized void addTags(InsertionRequest insertionRequest) {
    int id = insertionRequest.getValue();
    // if the device id can not be saved to the current working MemTable
    if (!inWorkingMemTable(id)) {
      workingMemTable.setStatus(MemTable.IMMUTABLE);
      immutableMemTables.put(maxDeviceID / numOfDeviceIdsInMemTable, workingMemTable);
      workingMemTable = new MemTable(MemTable.WORKING);
    }
    MemTable memTable = workingMemTable;
    maxDeviceID = id;
    try {
      insertionManager.process(memTable, insertionRequest, new InsertRequestContext());
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * insert tags and device id
   *
   * @param tags tags like: <tagKey,tagValue>
   * @param id INT32 device id
   */
  @Override
  public synchronized void addTags(Map<String, String> tags, int id) {
    // if the device id can not be saved to the current working MemTable
    if (!inWorkingMemTable(id)) {
      workingMemTable.setStatus(MemTable.IMMUTABLE);
      immutableMemTables.put(maxDeviceID / numOfDeviceIdsInMemTable, workingMemTable);
      workingMemTable = new MemTable(MemTable.WORKING);
    }
    MemTable memTable = workingMemTable;
    maxDeviceID = id;
    try {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        addTag(memTable, tag.getKey(), tag.getValue(), id);
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * delete tags and id using delete request context
   *
   * @param deletionRequest delete request context
   */
  @Override
  public void removeTags(DeletionRequest deletionRequest) {
    int id = deletionRequest.getValue();
    MemTable memTable = null;
    if (inWorkingMemTable(id)) {
      memTable = workingMemTable;
    } else {
      memTable = immutableMemTables.get(id / numOfDeviceIdsInMemTable);
    }
    try {
      deletionManager.process(memTable, deletionRequest, new DeleteRequestContext());
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * delete tags and id using delete request context
   *
   * @param tags tags like: <tagKey,tagValue>
   * @param id INT32 device id
   */
  @Override
  public synchronized void removeTags(Map<String, String> tags, int id) {
    List<MemTable> memTables = new ArrayList<>();
    if (inWorkingMemTable(id)) {
      memTables.add(workingMemTable);
    } else {
      memTables.add(immutableMemTables.get(id / numOfDeviceIdsInMemTable));
    }
    try {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        removeTag(memTables, tag.getKey(), tag.getValue(), id);
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * get all matching device ids
   *
   * @param tags tags like: <tagKey,tagValue>
   * @return device ids
   */
  @Override
  public synchronized List<Integer> getMatchedIDs(Map<String, String> tags) {
    List<MemTable> memTables = new ArrayList<>();
    memTables.add(workingMemTable);
    memTables.addAll(immutableMemTables.values());
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    int i = 0;
    try {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        RoaringBitmap rb = getMatchedIDs(memTables, tag.getKey(), tag.getValue());
        if (rb == null) continue;
        else {
          if (i == 0) roaringBitmap = rb;
          else roaringBitmap = RoaringBitmap.and(roaringBitmap, rb);
          i++;
        }
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return Arrays.stream(roaringBitmap.toArray()).boxed().collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "TagInvertedIndex{"
        + "numOfDeviceIdsInMemTable="
        + numOfDeviceIdsInMemTable
        + ", workingMemTable="
        + workingMemTable
        + ", immutableMemTables="
        + immutableMemTables
        + ", maxDeviceID="
        + maxDeviceID
        + '}';
  }

  /**
   * determine whether the id can be saved to the current MemTable
   *
   * @param id INT32 device id
   * @return return true if it can, otherwise return false
   */
  private boolean inWorkingMemTable(int id) {
    return id / numOfDeviceIdsInMemTable == maxDeviceID / numOfDeviceIdsInMemTable;
  }

  private List<String> generateKeys(String tagKey, String tagValue) {
    List<String> keys = new ArrayList<>();
    keys.add(tagKey);
    keys.add(tagValue);
    return keys;
  }

  private void addTag(MemTable memTable, String tagKey, String tagValue, int id) throws Exception {
    InsertionRequest insertionRequest = new InsertionRequest(generateKeys(tagKey, tagValue), id);
    insertionManager.process(memTable, insertionRequest, new InsertRequestContext());
  }

  private void removeTag(List<MemTable> memTables, String tagKey, String tagValue, int id)
      throws Exception {
    DeletionRequest deletionRequest = new DeletionRequest(generateKeys(tagKey, tagValue), id);
    for (MemTable memTable : memTables) {
      deletionManager.process(memTable, deletionRequest, new DeleteRequestContext());
    }
  }

  private RoaringBitmap getMatchedIDs(List<MemTable> memTables, String tagKey, String tagValue)
      throws Exception {
    QueryRequest queryRequest = new QueryRequest(generateKeys(tagKey, tagValue));
    for (MemTable memTable : memTables) {
      queryManager.process(memTable, queryRequest, new QueryRequestContext());
    }
    return queryRequest.getResult();
  }

  @TestOnly
  public void clear() throws IOException {
    walManager.close();
  }
}
