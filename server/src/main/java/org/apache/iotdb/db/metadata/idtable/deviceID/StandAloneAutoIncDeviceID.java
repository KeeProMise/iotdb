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
package org.apache.iotdb.db.metadata.idtable.deviceID;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Using auto-incrementing id as device id,A complete auto-increment id consists of schemaRegionID
 * and autoIncrementID, where the upper 32 bits are schemaRegionID and the lower 32 bits are
 * autoIncrementID
 */
public class StandAloneAutoIncDeviceID extends SHA256DeviceID implements IStatefulDeviceID {

  /** logger */
  private static Logger logger = LoggerFactory.getLogger(IDTable.class);

  // stand-alone auto-increment id uses LocalConfigNode to obtain schemaRegionId
  private static LocalConfigNode configManager;

  // using map to maintain the mapping from schemaRegionId to list<deviceID>, each list<deviceID>
  // maintains the auto-increment id of the schemaRegion
  private static Map<Integer, List<IDeviceID>> deviceIDsMap;

  // starting with 0,the maximum value is Integer.MAX_VALUE，if the schemaRegionId==-1 of a
  // StandAloneAutoIncDeviceID instance object, it means that the device corresponding to the
  // StandAloneAutoIncDeviceID instance does not exist
  int schemaRegionId;

  // starting with 0,the maximum value is Integer.MAX_VALUE
  int autoIncrementID;

  static {
    deviceIDsMap = new ConcurrentHashMap<>();
    configManager = LocalConfigNode.getInstance();
  }

  public StandAloneAutoIncDeviceID() {}

  public StandAloneAutoIncDeviceID(String devicePath) {
    super(devicePath);
  }

  /**
   * get a StandAloneAutoIncDeviceID instance, create it if it doesn't exist
   *
   * @param deviceID device path for write/read operation, and device id for read operation
   * @return a StandAloneAutoIncDeviceID instance
   */
  public static StandAloneAutoIncDeviceID getDeviceIDWithAutoCreate(String deviceID) {
    if (deviceID.startsWith("`") && deviceID.endsWith("`")) {
      return fromAutoIncDeviceID(deviceID);
    } else {
      return buildDeviceID(deviceID);
    }
  }

  /**
   * get a StandAloneAutoIncDeviceID instance, only for read operation
   *
   * @param deviceID device path or device id for read operation
   * @return if the device exists, return a StandAloneAutoIncDeviceID instance, if it does not
   *     exist,return a StandAloneAutoIncDeviceID instance,the object is guaranteed to be different
   *     from the deviceID object of any device managed by the system (equals==false).
   */
  public static StandAloneAutoIncDeviceID getDeviceID(String deviceID) {
    if (deviceID.startsWith("`") && deviceID.endsWith("`")) {
      return fromAutoIncDeviceID(deviceID);
    } else {
      return fromDevicePath(deviceID);
    }
  }

  /**
   * get device id from a standAloneAutoIncDeviceID
   *
   * @param deviceID StandAloneAutoIncDeviceID deviceID, like: "`1`"
   * @return standAloneAutoIncDeviceID
   */
  // todo qurey/write
  private static StandAloneAutoIncDeviceID fromAutoIncDeviceID(String deviceID) {
    deviceID = deviceID.substring(1, deviceID.length() - 1);
    long id = Long.parseLong(deviceID);
    int schemaRegionId = (int) (id >>> 32);
    int autoIncrementID = (int) id;
    List<IDeviceID> deviceIDs = deviceIDsMap.get(schemaRegionId);
    synchronized (deviceIDs) {
      return (StandAloneAutoIncDeviceID) deviceIDs.get(autoIncrementID);
    }
  }

  // todo qurey
  private static StandAloneAutoIncDeviceID fromDevicePath(String devicePath) {
    try {
      // Use idtable to determine whether the device has been created
      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath(devicePath));
      StandAloneAutoIncDeviceID deviceID = new StandAloneAutoIncDeviceID(devicePath);
      if (idTable.getDeviceEntry(deviceID) != null) {
        deviceID = (StandAloneAutoIncDeviceID) idTable.getDeviceEntry(deviceID).getDeviceID();
      } else {
        // todo
        deviceID.schemaRegionId = -1;
        deviceID.autoIncrementID = 0;
        List<IDeviceID> deviceIDs =
            deviceIDsMap.computeIfAbsent(deviceID.schemaRegionId, integer -> new ArrayList<>());
        // todo
        synchronized (deviceIDs) {
          if (deviceIDs.size() == 0) deviceIDs.add(deviceID.autoIncrementID, deviceID);
          else deviceIDs.set(0, deviceID);
        }
      }
      return deviceID;
    } catch (IllegalPathException e) {
      logger.error(e.getMessage());
      return null;
    }
  }

  // todo
  private static StandAloneAutoIncDeviceID buildDeviceID(String devicePath) {
    try {
      PartialPath path = new PartialPath(devicePath);
      // todo
      // Use idtable to determine whether the device has been created
      IDTable idTable = IDTableManager.getInstance().getIDTable(path);
      StandAloneAutoIncDeviceID deviceID = new StandAloneAutoIncDeviceID(devicePath);
      // this device is added for the first time
      if (idTable.getDeviceEntry(deviceID) == null) {
        SchemaRegionId schemaRegionId = configManager.getBelongedSchemaRegionId(path);
        deviceID.schemaRegionId = schemaRegionId.getId();
        List<IDeviceID> deviceIDs =
            deviceIDsMap.computeIfAbsent(deviceID.schemaRegionId, integer -> new ArrayList<>());
        synchronized (deviceIDs) {
          deviceID.autoIncrementID = deviceIDs.size();
          deviceIDs.add(deviceID.autoIncrementID, deviceID);
        }
      } else {
        deviceID = (StandAloneAutoIncDeviceID) idTable.getDeviceEntry(deviceID).getDeviceID();
      }
      return deviceID;
    } catch (MetadataException e) {
      logger.error(e.getMessage());
      return null;
    }
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StandAloneAutoIncDeviceID)) {
      return false;
    }
    return super.equals(o);
  }

  @Override
  public String toString() {
    return "AutoIncrementDeviceID{"
        + "l1="
        + l1
        + ", l2="
        + l2
        + ", l3="
        + l3
        + ", l4="
        + l4
        + ", schemaRegionId="
        + schemaRegionId
        + ", autoIncrementID="
        + autoIncrementID
        + '}';
  }

  @Override
  public String toStringID() {
    long stringID = (long) schemaRegionId << 32;
    stringID |= autoIncrementID;
    return "`" + stringID + '`';
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(schemaRegionId, byteBuffer);
    ReadWriteIOUtils.write(autoIncrementID, byteBuffer);
  }

  public static StandAloneAutoIncDeviceID deserialize(ByteBuffer byteBuffer) {
    StandAloneAutoIncDeviceID autoIncrementDeviceID = new StandAloneAutoIncDeviceID();
    autoIncrementDeviceID.l1 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l2 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l3 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l4 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.schemaRegionId = ReadWriteIOUtils.readInt(byteBuffer);
    autoIncrementDeviceID.autoIncrementID = ReadWriteIOUtils.readInt(byteBuffer);
    return autoIncrementDeviceID;
  }

  private void parseAutoIncrementDeviceID(SHA256DeviceID sha256DeviceID) {
    this.l1 = sha256DeviceID.l1;
    this.l2 = sha256DeviceID.l2;
    this.l3 = sha256DeviceID.l3;
    this.l4 = sha256DeviceID.l4;
  }

  /**
   * write device id to the static variable deviceIDs
   *
   * @param devicePath device path of the time series
   * @param deviceID device id
   */
  @Override
  public void recover(String devicePath, String deviceID) {
    buildSHA256(devicePath);
    deviceID = deviceID.substring(1, deviceID.length() - 1);
    long id = Long.parseLong(deviceID);
    this.schemaRegionId = (int) (id >>> 32);
    this.autoIncrementID = (int) id;
    List<IDeviceID> deviceIDs =
        deviceIDsMap.computeIfAbsent(schemaRegionId, integer -> new ArrayList<>());
    // if there is out-of-order data, write the deviceID to the correct index of the array
    synchronized (deviceIDs) {
      if (autoIncrementID < deviceIDs.size() && deviceIDs.get(autoIncrementID) != null) return;
      for (int i = deviceIDs.size(); i < autoIncrementID; i++) {
        deviceIDs.add(i, null);
      }
      deviceIDs.add(autoIncrementID, this);
    }
  }

  @TestOnly
  public static void reset() {
    deviceIDsMap.clear();
  }
}
