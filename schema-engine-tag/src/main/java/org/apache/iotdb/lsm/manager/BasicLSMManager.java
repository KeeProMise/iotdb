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
package org.apache.iotdb.lsm.manager;

import org.apache.iotdb.lsm.context.RequestContext;
import org.apache.iotdb.lsm.levelProcess.LevelProcessChain;
import org.apache.iotdb.lsm.request.IRequest;

/** basic lsm manager */
public abstract class BasicLSMManager<T, R extends IRequest, C extends RequestContext>
    implements ILSMManager<T, R, C> {

  // the level process of the first layer of memory nodes
  LevelProcessChain<T, R, C> levelProcessChain;

  public BasicLSMManager() {}

  public BasicLSMManager(LevelProcessChain<T, R, C> levelProcessChain) {
    this.levelProcessChain = levelProcessChain;
  }

  /**
   * processing of the root memory node
   *
   * @param root root memory node
   * @param context request context
   * @throws Exception
   */
  @Override
  public void process(T root, R request, C context) {
    preProcess(root, request, context);
    levelProcessChain.process(root, request, context);
    postProcess(root, request, context);
  }

  public void setLevelProcessChain(LevelProcessChain<T, R, C> levelProcessChain) {
    this.levelProcessChain = levelProcessChain;
  }
}
