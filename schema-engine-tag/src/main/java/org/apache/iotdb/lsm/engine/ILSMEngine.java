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
package org.apache.iotdb.lsm.engine;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.lsm.request.IDeletionIRequest;
import org.apache.iotdb.lsm.request.IInsertionIRequest;
import org.apache.iotdb.lsm.request.IQueryIRequest;

import java.io.IOException;

public interface ILSMEngine {

  <K, V, R> void insert(IInsertionIRequest<K, V, R> insertionRequest);

  <K, R> void query(IQueryIRequest<K, R> queryRequest);

  <K, V, R> void delete(IDeletionIRequest<K, V, R> deletionRequest);

  void recover();

  @TestOnly
  void clear() throws IOException;
}
