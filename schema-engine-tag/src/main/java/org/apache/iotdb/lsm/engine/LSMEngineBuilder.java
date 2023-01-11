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

import org.apache.iotdb.lsm.context.applicationcontext.ApplicationContext;
import org.apache.iotdb.lsm.context.applicationcontext.ApplicationContextGenerator;
import org.apache.iotdb.lsm.context.requestcontext.DeleteRequestContext;
import org.apache.iotdb.lsm.context.requestcontext.FlushRequestContext;
import org.apache.iotdb.lsm.context.requestcontext.InsertRequestContext;
import org.apache.iotdb.lsm.context.requestcontext.QueryRequestContext;
import org.apache.iotdb.lsm.context.requestcontext.RequestContext;
import org.apache.iotdb.lsm.levelProcess.ILevelProcessor;
import org.apache.iotdb.lsm.levelProcess.LevelProcessorChain;
import org.apache.iotdb.lsm.manager.DeletionManager;
import org.apache.iotdb.lsm.manager.FlushManager;
import org.apache.iotdb.lsm.manager.IDiskQueryManager;
import org.apache.iotdb.lsm.manager.IMemManager;
import org.apache.iotdb.lsm.manager.InsertionManager;
import org.apache.iotdb.lsm.manager.MemQueryManager;
import org.apache.iotdb.lsm.manager.QueryManager;
import org.apache.iotdb.lsm.manager.RecoverManager;
import org.apache.iotdb.lsm.manager.WALManager;
import org.apache.iotdb.lsm.request.IDeletionRequest;
import org.apache.iotdb.lsm.request.IFlushRequest;
import org.apache.iotdb.lsm.request.IInsertionRequest;
import org.apache.iotdb.lsm.request.IRequest;
import org.apache.iotdb.lsm.request.ISingleQueryRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Build the LSMEngine object
 *
 * @param <T> The type of root memory node handled by this engine
 */
public class LSMEngineBuilder<T extends IMemManager> {

  private static final Logger logger = LoggerFactory.getLogger(LSMEngineBuilder.class);

  // The constructed LSMEngine object
  private LSMEngine<T> lsmEngine;

  public LSMEngineBuilder() {
    lsmEngine = new LSMEngine<>();
  }

  /**
   * build WalManager for lsmEngine
   *
   * @param walManager WalManager object
   */
  public LSMEngineBuilder<T> buildWalManager(WALManager walManager) {
    lsmEngine.setWalManager(walManager);
    return this;
  }

  /**
   * build InsertionManager for lsmEngine
   *
   * @param levelProcessChain insert level processors chain
   * @param <R> extends IInsertionRequest
   */
  public <R extends IInsertionRequest> LSMEngineBuilder<T> buildInsertionManager(
      LevelProcessorChain<T, R, InsertRequestContext> levelProcessChain) {
    InsertionManager<T, R> insertionManager = new InsertionManager<>(lsmEngine.getWalManager());
    insertionManager.setLevelProcessorsChain(levelProcessChain);
    buildInsertionManager(insertionManager);
    return this;
  }

  /**
   * build InsertionManager for lsmEngine
   *
   * @param insertionManager InsertionManager object
   * @param <R> extends IInsertionRequest
   */
  public <R extends IInsertionRequest> LSMEngineBuilder<T> buildInsertionManager(
      InsertionManager<T, R> insertionManager) {
    lsmEngine.setInsertionManager(insertionManager);
    return this;
  }

  /**
   * build DeletionManager for lsmEngine
   *
   * @param levelProcessChain delete level processors chain
   * @param <R> extends IDeletionRequest
   */
  public <R extends IDeletionRequest> LSMEngineBuilder<T> buildDeletionManager(
      LevelProcessorChain<T, R, DeleteRequestContext> levelProcessChain) {
    DeletionManager<T, R> deletionManager = new DeletionManager<>(lsmEngine.getWalManager());
    deletionManager.setLevelProcessorsChain(levelProcessChain);
    buildDeletionManager(deletionManager);
    return this;
  }

  /**
   * build DeletionManager for lsmEngine
   *
   * @param deletionManager DeletionManager object
   * @param <R> extends IDeletionRequest
   */
  public <R extends IDeletionRequest> LSMEngineBuilder<T> buildDeletionManager(
      DeletionManager<T, R> deletionManager) {
    lsmEngine.setDeletionManager(deletionManager);
    return this;
  }

  /**
   * build FlushManager for lsmEngine
   *
   * @param <R> extends IFlushRequest
   * @param levelProcessChain flush level processors chain
   * @param memManager
   */
  public <R extends IFlushRequest, C extends FlushRequestContext>
      LSMEngineBuilder<T> buildFlushManager(
          LevelProcessorChain<T, R, C> levelProcessChain,
          T memManager,
          String flushDirPath,
          String flushFilePrefix) {
    FlushManager<T, R> flushManager =
        new FlushManager<>(lsmEngine.getWalManager(), memManager, flushDirPath, flushFilePrefix);
    flushManager.setLevelProcessorsChain(
        (LevelProcessorChain<T, R, FlushRequestContext>) levelProcessChain);
    buildFlushManager(flushManager);
    return this;
  }

  /**
   * build FlushManager for lsmEngine
   *
   * @param flushManager DeletionManager object
   * @param <R> extends IDeletionRequest
   */
  public <R extends IFlushRequest, C extends FlushRequestContext>
      LSMEngineBuilder<T> buildFlushManager(FlushManager<T, R> flushManager) {
    lsmEngine.setFlushManager(flushManager);
    return this;
  }

  /**
   * build QueryManager for lsmEngine
   *
   * @param levelProcessChain query level processors chain
   * @param <R> extends IQueryRequest
   */
  public <R extends ISingleQueryRequest> LSMEngineBuilder<T> buildQueryManager(
      LevelProcessorChain<T, R, QueryRequestContext> levelProcessChain) {
    MemQueryManager<T, R> memQueryManager = new MemQueryManager<>();
    memQueryManager.setLevelProcessorsChain(levelProcessChain);
    buildQueryManager(memQueryManager);
    return this;
  }

  /**
   * build QueryManager for lsmEngine
   *
   * @param memQueryManager memQueryManager object
   * @param <R> extends IQueryRequest
   */
  public <R extends ISingleQueryRequest> LSMEngineBuilder<T> buildQueryManager(
      MemQueryManager<T, R> memQueryManager) {
    QueryManager<T> queryManager = new QueryManager<>();
    queryManager.setRootMemNode(lsmEngine.getRootMemNode());
    queryManager.setMemQueryManager((MemQueryManager<T, ISingleQueryRequest>) memQueryManager);
    lsmEngine.setQueryManager(queryManager);
    return this;
  }

  /**
   * build DiskQueryManager for lsmEngine
   *
   * @param diskQueryManager memQueryManager object
   */
  public LSMEngineBuilder<T> buildDiskQueryManager(IDiskQueryManager diskQueryManager) {
    QueryManager<T> queryManager = lsmEngine.getQueryManager();
    queryManager.setDiskQueryManager(diskQueryManager);
    return this;
  }

  /** build RecoverManager for lsmEngine */
  public LSMEngineBuilder<T> buildRecoverManager(String flushDirPath, boolean enableFlush)
      throws IOException {
    RecoverManager<LSMEngine<T>> recoverManager =
        new RecoverManager<>(lsmEngine.getWalManager(), enableFlush, flushDirPath);
    lsmEngine.setRecoverManager(recoverManager);
    return this;
  }

  /**
   * build root memory node for lsmEngine
   *
   * @param rootMemNode root memory node
   */
  public LSMEngineBuilder<T> buildRootMemNode(T rootMemNode) {
    lsmEngine.setRootMemNode(rootMemNode);
    return this;
  }

  /**
   * build level processors from ApplicationContext object
   *
   * @param applicationContext ApplicationContext object
   * @param enableFlush enable flush or not
   * @param memManager
   */
  private LSMEngineBuilder<T> buildLevelProcessors(
      ApplicationContext applicationContext,
      T memManager,
      String flushDirPath,
      String flushFilePrefix,
      boolean enableFlush) {
    LevelProcessorChain<T, IInsertionRequest, InsertRequestContext> insertionLevelProcessChain =
        generateLevelProcessorsChain(applicationContext.getInsertionLevelProcessClass());
    LevelProcessorChain<T, IDeletionRequest, DeleteRequestContext> deletionLevelProcessChain =
        generateLevelProcessorsChain(applicationContext.getDeletionLevelProcessClass());
    LevelProcessorChain<T, ISingleQueryRequest, QueryRequestContext> queryLevelProcessChain =
        generateLevelProcessorsChain(applicationContext.getQueryLevelProcessClass());
    if (enableFlush) {
      LevelProcessorChain<T, IFlushRequest, FlushRequestContext> flushLevelProcessChain =
          generateLevelProcessorsChain(applicationContext.getFlushLevelProcessClass());
      return buildQueryManager(queryLevelProcessChain)
          .buildInsertionManager(insertionLevelProcessChain)
          .buildDeletionManager(deletionLevelProcessChain)
          .buildFlushManager(flushLevelProcessChain, memManager, flushDirPath, flushFilePrefix);
    } else {
      return buildQueryManager(queryLevelProcessChain)
          .buildInsertionManager(insertionLevelProcessChain)
          .buildDeletionManager(deletionLevelProcessChain);
    }
  }

  /**
   * Scan the classes of the package and build level processors based on the class annotations
   *
   * @param packageName package name
   */
  private LSMEngineBuilder<T> buildLevelProcessors(
      String packageName,
      T memManager,
      String flushDirPath,
      String flushFilePrefix,
      boolean enableFlush) {
    try {
      ApplicationContext property =
          ApplicationContextGenerator.GeneratePropertyWithAnnotation(packageName, enableFlush);
      buildLevelProcessors(property, memManager, flushDirPath, flushFilePrefix, enableFlush);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return this;
  }

  /**
   * build all LSM managers
   *
   * @param applicationContext ApplicationContext object
   * @param walManager WalManager object
   * @param memManager
   */
  public LSMEngineBuilder<T> buildLSMManagers(
      ApplicationContext applicationContext,
      WALManager walManager,
      T memManager,
      String flushDirPath,
      String flushFilePrefix,
      IDiskQueryManager diskQueryManager,
      boolean enableFlush) {
    try {
      buildWalManager(walManager)
          .buildLevelProcessors(
              applicationContext, memManager, flushDirPath, flushFilePrefix, enableFlush)
          .buildRecoverManager(flushDirPath, enableFlush);
      if (enableFlush) {
        this.buildDiskQueryManager(diskQueryManager);
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return this;
  }

  /**
   * Scan the classes of the package and build all LSM managers based on the class annotations
   *
   * @param packageName package name
   * @param walManager WalManager object
   */
  public LSMEngineBuilder<T> buildLSMManagers(
      String packageName,
      WALManager walManager,
      T memManager,
      String flushDirPath,
      String flushFilePrefix,
      IDiskQueryManager diskQueryManager,
      boolean enableFlush) {
    try {
      ApplicationContext property =
          ApplicationContextGenerator.GeneratePropertyWithAnnotation(packageName, enableFlush);
      buildLSMManagers(
          property,
          walManager,
          memManager,
          flushDirPath,
          flushFilePrefix,
          diskQueryManager,
          enableFlush);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return this;
  }

  /**
   * Get the built lsmEngine
   *
   * @return LSMEngine object
   */
  public LSMEngine<T> build() {
    return lsmEngine;
  }

  /**
   * generate level processors Chain
   *
   * @param levelProcessorClassNames Save all level processor class names in hierarchical order
   * @param <R> extends IRequest
   * @param <C> extends RequestContext
   * @return level Processors Chain
   */
  private <R extends IRequest, C extends RequestContext>
      LevelProcessorChain<T, R, C> generateLevelProcessorsChain(
          List<String> levelProcessorClassNames) {
    LevelProcessorChain<T, R, C> levelProcessChain = new LevelProcessorChain<>();
    try {
      if (levelProcessorClassNames.size() > 0) {
        ILevelProcessor iLevelProcess =
            levelProcessChain.nextLevel(generateLevelProcessor(levelProcessorClassNames.get(0)));
        for (int i = 1; i < levelProcessorClassNames.size(); i++) {
          iLevelProcess =
              iLevelProcess.nextLevel(generateLevelProcessor(levelProcessorClassNames.get(i)));
        }
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return levelProcessChain;
  }

  /**
   * generate level processor
   *
   * @param className level processor class name
   * @param <R> extends IRequest
   * @param <C> extends RequestContext
   * @return level processor
   * @throws ClassNotFoundException
   * @throws NoSuchMethodException
   * @throws InvocationTargetException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  private <R extends IRequest, C extends RequestContext>
      ILevelProcessor<T, ?, R, C> generateLevelProcessor(String className)
          throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
              InstantiationException, IllegalAccessException {
    Class c = Class.forName(className);
    ILevelProcessor<T, ?, R, C> result =
        (ILevelProcessor<T, ?, R, C>) c.getDeclaredConstructor().newInstance();
    return result;
  }
}
