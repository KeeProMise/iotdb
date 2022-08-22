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
package org.apache.iotdb.db.metadata.idtable;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.DiskSchemaEntry;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class AppendOnlyDiskSchemaManagerTest {

  private String systemDir =
      FilePathUtils.regularizePath(IoTDBDescriptor.getInstance().getConfig().getSystemDir())
          + "storage_groups";

  private String storageGroupPath = "root.AppendOnlyDiskSchemaManagerTest";

  private AppendOnlyDiskSchemaManager appendOnlyDiskSchemaManager;

  private boolean isEnableIDTable = false;

  private String originalDeviceIDTransformationMethod = null;

  @Before
  public void setUp() throws Exception {
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    originalDeviceIDTransformationMethod =
        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("AutoIncrement_INT");
    EnvironmentUtils.envSetUp();
    appendOnlyDiskSchemaManager =
        new AppendOnlyDiskSchemaManager(
            SystemFileFactory.INSTANCE.getFile(systemDir + File.separator + storageGroupPath));
  }

  @After
  public void tearDown() throws Exception {
    appendOnlyDiskSchemaManager.close();
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
    appendOnlyDiskSchemaManager = null;
  }

  @Test
  public void serialize() {
    for (int i = 0; i < 10; i++) {
      DiskSchemaEntry diskSchemaEntry =
          new DiskSchemaEntry(
              DeviceIDFactory.getInstance().getDeviceID(storageGroupPath + ".d" + i).toStringID(),
              storageGroupPath + ".d" + i + ".s",
              "s",
              Byte.parseByte("0"),
              Byte.parseByte("0"),
              Byte.parseByte("0"),
              false);
      appendOnlyDiskSchemaManager.serialize(diskSchemaEntry);
    }
  }

  @Test
  public void recover() {
    serialize();
    IDTable idTable =
        new IDTableHashmapImpl(
            SystemFileFactory.INSTANCE.getFile(systemDir + File.separator + storageGroupPath));
    appendOnlyDiskSchemaManager.recover(idTable);
    for (int i = 0; i < 10; i++) {
      assertNotNull(idTable.getDeviceEntry(storageGroupPath + ".d" + i).getDeviceID());
      assertEquals(
          idTable.getDeviceEntry(storageGroupPath + ".d" + i).getDeviceID(),
          DeviceIDFactory.getInstance()
              .getDeviceID(
                  DeviceIDFactory.getInstance()
                      .getDeviceID(storageGroupPath + ".d" + i)
                      .toStringID()));
      assertEquals(
          idTable.getDeviceEntry(storageGroupPath + ".d" + i).getDeviceID(),
          DeviceIDFactory.getInstance().getDeviceID(storageGroupPath + ".d" + i));
    }
  }

  @Test
  public void getAllSchemaEntry() {
    serialize();
    try {
      Collection<DiskSchemaEntry> diskSchemaEntrys =
          appendOnlyDiskSchemaManager.getAllSchemaEntry();
      int i = 0;
      for (DiskSchemaEntry diskSchemaEntry : diskSchemaEntrys) {
        assertEquals(diskSchemaEntry.seriesKey, storageGroupPath + ".d" + i + ".s");
        i++;
      }
    } catch (IOException e) {
      fail("throw wrong exception");
    }
  }
}
