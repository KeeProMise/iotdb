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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAliasIT {

  private static final String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.sg",
        "CREATE TIMESERIES root.sg.d1.s1(speed) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d1.s2(temperature) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s1(speed) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s2(temperature) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s3(power) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 10.1, 20.7)",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(200, 15.2, 22.9)",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(300, 30.3, 25.1)",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(400, 50.4, 28.3)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(100, 11.1, 20.2, 80.0)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(200, 20.2, 21.8, 81.0)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(300, 45.3, 23.4, 82.0)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(400, 73.4, 26.3, 83.0)",
        "SET STORAGE GROUP TO root.sg1",
        "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.sg1.d1(timestamp, s1, s2) VALUES (0, -1, 1)",
        "INSERT INTO root.sg1.d1(timestamp, s1, s2) VALUES (1, -2, 2)",
        "INSERT INTO root.sg1.d1(timestamp, s1, s2) VALUES (2, -3, 3)",
        "SET STORAGE GROUP TO root.sg2",
        "CREATE TIMESERIES root.sg2.d1.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg2.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg2.d2.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg2.d2.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg2.d2.s3 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "INSERT INTO root.sg2.d1(timestamp,s1,s2) values(100, 10.1, 20.7)",
        "INSERT INTO root.sg2.d1(timestamp,s1,s2) values(200, 15.2, 22.9)",
        "INSERT INTO root.sg2.d1(timestamp,s1,s2) values(300, 30.3, 25.1)",
        "INSERT INTO root.sg2.d1(timestamp,s1,s2) values(400, 50.4, 28.3)",
        "INSERT INTO root.sg2.d2(timestamp,s1,s2,s3) values(100, 11.1, 20.2, 80.0)",
        "INSERT INTO root.sg2.d2(timestamp,s1,s2,s3) values(200, 20.2, 21.8, 81.0)",
        "INSERT INTO root.sg2.d2(timestamp,s1,s2,s3) values(300, 45.3, 23.4, 82.0)",
        "INSERT INTO root.sg2.d2(timestamp,s1,s2,s3) values(400, 73.4, 26.3, 83.0)"
      };

  private static final String COLUMN_TIME = "Time";
  private static final String COLUMN_TIMESERIES = "timeseries";
  private static final String COLUMN_VALUE = "value";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  // ---------------------------------- Use timeseries alias ---------------------------------

  @Test
  public void selectWithAliasTest() {
    String[] retArray =
        new String[] {"100,10.1,20.7,", "200,15.2,22.9,", "300,30.3,25.1,", "400,50.4,28.3,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select speed, temperature from root.sg.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,root.sg.d1.speed,root.sg.d1.temperature,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastSelectWithAliasTest() {
    String[] retArray =
        new String[] {"400,root.sg.d1.speed,50.4", "400,root.sg.d1.temperature,28.3"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select last speed, temperature from root.sg.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(COLUMN_TIME)
                  + ","
                  + resultSet.getString(COLUMN_TIMESERIES)
                  + ","
                  + resultSet.getString(COLUMN_VALUE);
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectDuplicatedPathsWithAliasTest() {
    String[] retArray =
        new String[] {
          "100,10.1,10.1,20.7,", "200,15.2,15.2,22.9,", "300,30.3,30.3,25.1,", "400,50.4,50.4,28.3,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select speed, speed, s2 from root.sg.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,root.sg.d1.speed,root.sg.d1.speed,root.sg.d1.s2,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(4, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastSelectDuplicatedPathsWithAliasTest() {
    String[] retArray =
        new String[] {
          "400,root.sg.d1.speed,50.4", "400,root.sg.d1.s1,50.4", "400,root.sg.d1.s2,28.3"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select last speed, s1, speed, s2 from root.sg.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(COLUMN_TIME)
                  + ","
                  + resultSet.getString(COLUMN_TIMESERIES)
                  + ","
                  + resultSet.getString(COLUMN_VALUE);
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAggregationWithAliasTest() {
    String[] retArray = new String[] {"4,4,28.3,26.3,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select count(speed), max_value(temperature) from root.sg.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals(
            "count(root.sg.d1.speed),count(root.sg.d2.speed),"
                + "max_value(root.sg.d1.temperature),max_value(root.sg.d2.temperature),",
            header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void AlterAliasTest() {
    String ret = "root.sg.d2.s3,powerNew,root.sg,FLOAT,RLE,SNAPPY";

    String[] retArray = {"100,80.0,", "200,81.0,", "300,82.0,", "400,83.0,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("ALTER timeseries root.sg.d2.s3 UPSERT ALIAS='powerNew'");
      try (ResultSet resultSet = statement.executeQuery("show timeseries root.sg.d2.s3")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression");
          assertEquals(ret, ans);
        }
      }

      try (ResultSet resultSet = statement.executeQuery("select powerNew from root.sg.d2")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,root.sg.d2.powerNew,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      fail(e.getMessage());
      e.printStackTrace();
    }
  }

  // ---------------------------------------- Use AS -----------------------------------------

  @Test
  public void selectWithAsTest() {
    String[] retArray =
        new String[] {"100,10.1,20.7,", "200,15.2,22.9,", "300,30.3,25.1,", "400,50.4,28.3,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select s1 as speed, s2 as temperature from root.sg2.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,speed,temperature,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Test some time series use alias while others stay unchanged. */
  @Test
  public void selectWithAsMixedTest() {
    String[] retArray =
        new String[] {"100,10.1,20.7,", "200,15.2,22.9,", "300,30.3,25.1,", "400,50.4,28.3,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select s1 as speed, s2 from root.sg2.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,speed,root.sg2.d1.s2,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * When one alias is used but wildcard is corresponding to multi time series, it should throw one
   * exception.
   */
  @Test
  public void selectWithAsFailTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // root.sg.*.s1 matches root.sg.d1.s1 and root.sg.d2.s1 both
      statement.executeQuery("select s1 as speed from root.sg2.*");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("alias 'speed' can only be matched with one time series"));
    }
  }

  /** When wild is exactly corresponding to one time series, the result will be correct. */
  @Test
  public void selectWithAsSingleTest() {
    String[] retArray = new String[] {"100,80.0,", "200,81.0,", "300,82.0,", "400,83.0,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // root.sg.*.s3 matches root.sg.d2.s3 exactly
      try (ResultSet resultSet = statement.executeQuery("select s3 as power from root.sg2.*")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,power,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void aggregationWithAsTest() {
    String[] retArray =
        new String[] {
          "4,28.3,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1) as s1_num, max_value(s2) as s2_max from root.sg2.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("s1_num,s2_max,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void aggregationWithAsFailTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // root.sg2.*.s1 matches root.sg2.d1.s1 and root.sg2.d2.s1 both
      statement.executeQuery("select count(s1) as s1_num from root.sg2.*");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("alias 's1_num' can only be matched with one time series"));
    }
  }

  @Test
  public void groupByWithAsTest() {
    String[] retArray =
        new String[] {
          "100,1,", "180,1,", "260,1,", "340,1,", "420,0,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1) as 's1_num' from root.sg2.d1 group by ([100,500), 80ms)")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,s1_num,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void alignByDeviceWithAsTest() {
    String[] retArray =
        new String[] {
          "100,root.sg2.d1,10.1,20.7,",
          "200,root.sg2.d1,15.2,22.9,",
          "300,root.sg2.d1,30.3,25.1,",
          "400,root.sg2.d1,50.4,28.3,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1 as speed, s2 as temperature from root.sg2.d1 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,Device,speed,temperature,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void alignByDeviceWithAsMixedTest() {
    String[] retArray =
        new String[] {
          "100,root.sg2.d1,10.1,20.7,",
          "200,root.sg2.d1,15.2,22.9,",
          "300,root.sg2.d1,30.3,25.1,",
          "400,root.sg2.d1,50.4,28.3,",
          "100,root.sg2.d2,11.1,20.2,",
          "200,root.sg2.d2,20.2,21.8,",
          "300,root.sg2.d2,45.3,23.4,",
          "400,root.sg2.d2,73.4,26.3,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select s1 as speed, s2 from root.sg2.* align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,Device,speed,s2,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void alignByDeviceWithAsFailTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // root.sg.*.s1 matches root.sg.d1.s1 and root.sg.d2.s1 both
      statement.executeQuery("select * as speed from root.sg2.d1 align by device");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("alias speed can only be matched with one time series"));
    }
  }

  @Test
  public void alignByDeviceWithAsDuplicatedTest() {
    String[] retArray =
        new String[] {
          "100,root.sg2.d1,10.1,10.1,",
          "200,root.sg2.d1,15.2,15.2,",
          "300,root.sg2.d1,30.3,30.3,",
          "400,root.sg2.d1,50.4,50.4,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select s1 as speed, s1 from root.sg2.d1 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,Device,speed,speed,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void alignByDeviceWithAsAggregationTest() {
    String[] retArray =
        new String[] {
          "root.sg2.d2,4,4,4,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1) as s1_num, count(s2), count(s3) as s3_num from root.sg2.d2 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Device,s1_num,count(s2),s3_num,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastWithAsTest() {
    String[] retArray = new String[] {"400,speed,50.4,FLOAT,", "400,root.sg2.d1.s2,28.3,FLOAT,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select last s1 as speed, s2 from root.sg2.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,timeseries,value,dataType,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastWithAsDuplicatedTest() {
    String[] retArray =
        new String[] {
          "400,speed,50.4,FLOAT,", "400,root.sg2.d1.s1,50.4,FLOAT,", "400,temperature,28.3,FLOAT,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last s1 as speed, s1, s2 as temperature from root.sg2.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals("Time,timeseries,value,dataType,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastWithAsFailTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // root.sg2.*.s1 matches root.sg2.d1.s1 and root.sg2.d2.s1 both
      statement.executeQuery("select last s1 as speed from root.sg2.*");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("alias 'speed' can only be matched with one time series"));
    }
  }

  @Test
  public void UDFAliasTest() {

    String[] expect = {
      "Time,-root.sg1.d1.s1,a,cos(root.sg1.d1.s2),b,",
      "Time,-root.sg1.d1.s1,a,cos(root.sg1.d1.s2),b,",
      "Time,-root.sg1.d1.s1,-root.sg1.d1.s1,a,sin(cos(tan(root.sg1.d1.s1))),cos(root.sg1.d1.s2),b,cos(root.sg1.d1.s2),"
    };
    String[] sqls = {
      "select -s1, sin(cos(tan(s1))) as a, cos(s2), top_k(s1 + s1, 'k'='1') as b from root.sg1.d1 WHERE time >= 1509466140000",
      "select -s1, sin(cos(tan(s1))) as a, cos(s2), top_k(s1 + s1, 'k'='1') as b from root.sg1.d1",
      "select -s1, -s1, sin(cos(tan(s1))) as a, sin(cos(tan(s1))), cos(s2), top_k(s1 + s1, 'k'='1') as b, cos(s2) from root.sg1.d1"
    };
    int count = 2;

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (int index = 0; index < count; index++) {

        try (ResultSet resultSet = statement.executeQuery(sqls[index])) {

          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          StringBuilder header = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            header.append(resultSetMetaData.getColumnName(i)).append(",");
          }
          Assert.assertEquals(expect[index], header.toString());
        }
      }

    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void UDFAliasResultTest() {

    String[] expect = {
      "0,-1,1,0.0", "1,-2,2,0.0", "2,-3,3,0.0",
    };
    String sql = "select s1, s2, sin(s1+s2) as a from root.sg1.d1 ";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(COLUMN_TIME)
                  + ","
                  + resultSet.getString("root.sg1.d1.s1")
                  + ","
                  + resultSet.getString("root.sg1.d1.s2")
                  + ","
                  + resultSet.getString("a");
          assertEquals(expect[cnt++], ans);
        }
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}