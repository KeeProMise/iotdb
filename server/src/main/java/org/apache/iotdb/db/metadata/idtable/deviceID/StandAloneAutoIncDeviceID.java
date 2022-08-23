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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Using auto-incrementing int as device id */
public class StandAloneAutoIncDeviceID implements IStatefulDeviceID {

  // maintain the mapping relationship of devicePath to autoIncrementID，in order reducing memory
  // consumption, the devicePath will actually be converted to SHA256DeviceID in advance
  private static final Map<SHA256DeviceID, StandAloneAutoIncDeviceID> devicePath2autoIncrementID;

  // increment from 0
  int autoIncrementID;

  static {
    devicePath2autoIncrementID = new ConcurrentHashMap<>();
  }

  public StandAloneAutoIncDeviceID() {}

  public StandAloneAutoIncDeviceID(int id) {
    this.autoIncrementID = id;
  }

  public StandAloneAutoIncDeviceID(String deviceID) {
    // if the device id string is a autoIncrementDeviceID form, like: "`1`",
    // convert string directly to autoIncrementID
    if (deviceID.startsWith("`") && deviceID.endsWith("`")) {
      fromAutoIncrementDeviceID(deviceID);
    } else {
      buildAutoIncrementDeviceID(deviceID);
    }
  }

  /**
   * convert string directly to autoIncrementID
   *
   * @param deviceID device ID， like: "`1`"
   */
  private void fromAutoIncrementDeviceID(String deviceID) {
    deviceID = deviceID.substring(1, deviceID.length() - 1);
    this.autoIncrementID = Integer.parseInt(deviceID);
  }

  /**
   * build device id from a device path
   *
   * @param devicePath device path, like: "root.sg.x.d1"
   */
  private void buildAutoIncrementDeviceID(String devicePath) {
    SHA256DeviceID sha256DeviceID = new SHA256DeviceID(devicePath);
    this.autoIncrementID =
        devicePath2autoIncrementID.computeIfAbsent(
                sha256DeviceID,
                sha256DeviceID1 -> new StandAloneAutoIncDeviceID(devicePath2autoIncrementID.size()))
            .autoIncrementID;
  }

  /**
   * make sure the hashcode of any AutoIncrementDeviceID object with the equal autoIncrementID are
   * equal,so use the autoIncrementID of type Long as the hashcode of the object
   */
  @Override
  public int hashCode() {
    return this.autoIncrementID;
  }

  /** make sure any AutoIncrementDeviceID objects with equal autoIncrementID variables are equal */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof StandAloneAutoIncDeviceID)) {
      return false;
    }
    StandAloneAutoIncDeviceID that = (StandAloneAutoIncDeviceID) o;
    return this.autoIncrementID == that.autoIncrementID;
  }

  @Override
  public String toString() {
    return "AutoIncrementDeviceID{" + "autoIncrementID=" + autoIncrementID + '}';
  }

  /**
   * to string format
   *
   * @return string format device id
   */
  @Override
  public String toStringID() {
    return "`" + autoIncrementID + "`";
  }

  /**
   * recover map devicePath2autoIncrementID
   *
   * @param devicePath device path read from the non-volatile storage medium, like: "root.sg.x.d1"
   * @param deviceID device ID read from the non-volatile storage medium, like: ""
   */
  @Override
  public void recover(String devicePath, String deviceID) {
    SHA256DeviceID sha256DeviceID = new SHA256DeviceID(devicePath);
    fromAutoIncrementDeviceID(deviceID);
    devicePath2autoIncrementID.put(sha256DeviceID, this);
  }

  @TestOnly
  public static void reset() {
    devicePath2autoIncrementID.clear();
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(autoIncrementID, byteBuffer);
  }

  public static StandAloneAutoIncDeviceID deserialize(ByteBuffer byteBuffer) {
    StandAloneAutoIncDeviceID autoIncrementDeviceID = new StandAloneAutoIncDeviceID();
    autoIncrementDeviceID.autoIncrementID = ReadWriteIOUtils.readInt(byteBuffer);
    return autoIncrementDeviceID;
  }
}
