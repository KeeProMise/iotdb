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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal;

import org.apache.iotdb.lsm.wal.WALRecord;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class WALEntry extends WALRecord {
  private int type;

  private List<String> keys;

  private int deviceID;

  public WALEntry() {
    super();
  }

  public WALEntry(int type, List<String> keys, int deviceID) {
    super();
    this.type = type;
    this.keys = keys;
    this.deviceID = deviceID;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(type, buffer);
    ReadWriteIOUtils.write(deviceID, buffer);
    ReadWriteIOUtils.write(keys.size(), buffer);
    for (String key : keys) {
      ReadWriteIOUtils.write(key, buffer);
    }
  }

  @Override
  public void deserialize(DataInputStream stream) throws IOException {
    this.type = stream.readInt();
    this.deviceID = stream.readInt();
    int length = stream.readInt();
    this.keys = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      String key = ReadWriteIOUtils.readString(stream);
      keys.add(key);
    }
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public List<String> getKeys() {
    return keys;
  }

  public void setKeys(List<String> keys) {
    this.keys = keys;
  }

  public int getDeviceID() {
    return deviceID;
  }

  public void setDeviceID(int deviceID) {
    this.deviceID = deviceID;
  }

  @Override
  public String toString() {
    return "WALEntry{" + "type=" + type + ", keys=" + keys + ", deviceID=" + deviceID + '}';
  }
}
