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
package org.apache.iotdb.db.protocol.influxdb.meta;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.utils.DataTypeUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.influxdb.InfluxDBException;

import java.util.ArrayList;
import java.util.List;

public class TagInfoRecords {

  private static final String TAG_INFO_DEVICE_ID = "root.TAG_INFO";
  private static final List<String> TAG_INFO_MEASUREMENTS = new ArrayList<>();
  private static final List<TSDataType> TAG_INFO_TYPES = new ArrayList<>();

  static {
    TAG_INFO_MEASUREMENTS.add("database_name");
    TAG_INFO_MEASUREMENTS.add("measurement_name");
    TAG_INFO_MEASUREMENTS.add("tag_name");
    TAG_INFO_MEASUREMENTS.add("tag_order");

    TAG_INFO_TYPES.add(TSDataType.TEXT);
    TAG_INFO_TYPES.add(TSDataType.TEXT);
    TAG_INFO_TYPES.add(TSDataType.TEXT);
    TAG_INFO_TYPES.add(TSDataType.INT32);
  }

  private final List<String> deviceIds;
  private final List<Long> times;
  private final List<List<String>> measurementsList;
  private final List<List<TSDataType>> typesList;
  private final List<List<Object>> valuesList;

  public TagInfoRecords() {
    deviceIds = new ArrayList<>();
    times = new ArrayList<>();
    measurementsList = new ArrayList<>();
    typesList = new ArrayList<>();
    valuesList = new ArrayList<>();
  }

  public void add(String database, String measurement, String tag, int order) {
    deviceIds.add(TAG_INFO_DEVICE_ID);
    // Multiple adjacent records, possibly with the same timestamp
    times.add(System.currentTimeMillis());
    measurementsList.add(TAG_INFO_MEASUREMENTS);
    typesList.add(TAG_INFO_TYPES);

    List<Object> values = new ArrayList<>();
    values.add(database);
    values.add(measurement);
    values.add(tag);
    values.add(order);
    valuesList.add(values);
  }

  public List<InsertRowPlan> convertToInsertRowPlans() {
    ArrayList<InsertRowPlan> insertRowPlans = new ArrayList<>();
    for (int i = 0; i < deviceIds.size(); i++) {
      // Prevent later inserted records from overwriting previous records
      long now = 0;
      if (now != times.get(i)) {
        now = times.get(i);
      } else {
        now = times.get(i) + 1;
      }
      try {
        insertRowPlans.add(
            new InsertRowPlan(
                new PartialPath(deviceIds.get(i)),
                now,
                measurementsList.get(i).toArray(new String[0]),
                DataTypeUtils.getValueBuffer(typesList.get(i), valuesList.get(i)),
                false));
      } catch (QueryProcessException | IllegalPathException | IoTDBConnectionException e) {
        throw new InfluxDBException(e.getMessage());
      }
    }
    return insertRowPlans;
  }

  public List<TSInsertRecordReq> convertToInsertRecordsReq(long sessionID)
      throws IoTDBConnectionException {
    ArrayList<TSInsertRecordReq> reqs = new ArrayList<>();
    long now = 0;
    for (int i = 0; i < deviceIds.size(); i++) {
      TSInsertRecordReq tsInsertRecordReq = new TSInsertRecordReq();
      tsInsertRecordReq.setSessionId(sessionID);
      // Prevent later inserted records from overwriting previous records
      if (now != times.get(i)) {
        now = times.get(i);
      } else {
        now = times.get(i) + 1;
      }
      tsInsertRecordReq.setTimestamp(now);
      tsInsertRecordReq.setIsAligned(false);
      tsInsertRecordReq.setPrefixPath(deviceIds.get(i));
      tsInsertRecordReq.setMeasurements(measurementsList.get(i));
      tsInsertRecordReq.setValues(
          DataTypeUtils.getValueBuffer(typesList.get(i), valuesList.get(i)));
      reqs.add(tsInsertRecordReq);
    }
    return reqs;
  }
}
