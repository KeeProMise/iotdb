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
package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;

public class TagSchemaRegionLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TagSchemaRegionLoader.class);
  private static URLClassLoader urlClassLoader = null;
  private static final String TAG_SCHEMA_REGION_CLASS_NAME =
      "org.apache.iotdb.db.metadata.tagSchemaRegion.TagSchemaRegion";
  private static final String TAG_SCHEMA_CONF_DESCRIPTOR_CLASS_NAME =
      "org.apache.iotdb.db.metadata.tagSchemaRegion.TagSchemaDescriptor";
  private static final String LIB_PATH =
      ".." + File.separator + "lib" + File.separator + "tag-schema-region" + File.separator;

  public TagSchemaRegionLoader() {}

  /**
   * Load the jar files for RSchemaRegion and create an instance of it. The jar files should be
   * located in "../lib/rschema-region". If jar files cannot be found, the function will return
   * null.
   *
   * @param storageGroup
   * @param schemaRegionId
   * @param node
   * @return
   */
  public ISchemaRegion loadRSchemaRegion(
      PartialPath storageGroup, SchemaRegionId schemaRegionId, IStorageGroupMNode node) {
    ISchemaRegion region = null;
    LOGGER.info("Creating instance for schema-engine-rocksdb");
    try {
      loadRSchemaRegionJar();
      Class<?> classForTagSchemaRegion = urlClassLoader.loadClass(TAG_SCHEMA_REGION_CLASS_NAME);
      Class<?> classForTagSchemaDescriptor =
          urlClassLoader.loadClass(TAG_SCHEMA_CONF_DESCRIPTOR_CLASS_NAME);
      Constructor<?> constructor =
          classForTagSchemaRegion.getConstructor(
              PartialPath.class,
              SchemaRegionId.class,
              IStorageGroupMNode.class,
              classForTagSchemaDescriptor);
      Object rSchemaLoader = classForTagSchemaDescriptor.getConstructor().newInstance();
      region =
          (ISchemaRegion)
              constructor.newInstance(storageGroup, schemaRegionId, node, rSchemaLoader);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException
        | MalformedURLException
        | RuntimeException e) {
      LOGGER.error("Cannot initialize RSchemaRegion", e);
      return null;
    }
    return region;
  }

  /**
   * Load the jar files for rocksdb and RSchemaRegion. The jar files should be located in directory
   * "../lib/rschema-region". If the jar files have been loaded, it will do nothing.
   */
  private void loadRSchemaRegionJar() throws MalformedURLException {
    LOGGER.info("Loading jar for schema-engine-rocksdb");
    if (urlClassLoader == null) {
      File[] jars = new File(LIB_PATH).listFiles();
      if (jars == null) {
        throw new RuntimeException(
            String.format("Cannot get jars from %s", new File(LIB_PATH).getAbsolutePath()));
      }
      List<URL> dependentJars = new LinkedList<>();
      for (File jar : jars) {
        if (jar.getName().endsWith(".jar")) {
          dependentJars.add(new URL("file:" + jar.getAbsolutePath()));
        }
      }
      urlClassLoader = new URLClassLoader(dependentJars.toArray(new URL[] {}));
    }
  }
}
