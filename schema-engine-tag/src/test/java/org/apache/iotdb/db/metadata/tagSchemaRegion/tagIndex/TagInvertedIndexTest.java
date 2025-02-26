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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaDescriptor;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TagInvertedIndexTest {
  private String[][] record =
      new String[][] {
        {"tag1=q", "tag2=a", "1"},
        {"tag1=q", "tag2=s", "2"},
        {"tag1=q", "tag2=a", "tag3=z", "3"},
        {"tag1=q", "tag3=v", "4"},
        {"tag1=q", "tag2=s", "5"},
        {"tag1=w", "tag2=d", "6"},
        {"tag1=q", "tag2=d", "tag3=e", "7"},
        {"tag1=t", "tag2=g", "8"},
        {"tag1=r", "tag2=d", "9"},
        {"tag1=t", "tag2=f", "10"},
        {"tag1=t", "tag2=h", "11"},
        {"tag1=q", "tag2=a", "tag3=l", "12"},
        {"tag1=y", "tag2=j", "13"},
        {"tag1=u", "tag2=k", "14"},
        {"tag1=q", "tag2=a", "tag3=x", "15"},
        {"tag1=q", "tag2=a", "tag4=z", "16"},
        {"tag1=y", "tag2=a", "tag4=z", "17"},
        {"tag1=q", "tag2=b", "tag3=x", "18"},
      };

  private int numOfDeviceIdsInMemTable;

  private TagInvertedIndex tagInvertedIndex;

  private String storageGroupDirPath;

  private String schemaRegionDirPath;

  private String storageGroupFullPath = "root/testTagIndex";

  private String schemaDir;

  private boolean enableFlush;

  protected final TagInvertedIndexTestParam testParam;

  @Parameterized.Parameters(name = "{0}")
  public static List<TagInvertedIndexTestParam> getTestNode() {
    return Arrays.asList(
        new TagInvertedIndexTestParam("FlushMode", true),
        new TagInvertedIndexTestParam("Non-FlushMode", false));
  }

  protected static class TagInvertedIndexTestParam {
    private final String testModeName;

    private final boolean enableFlush;

    private TagInvertedIndexTestParam(String testModeName, boolean enableFlush) {
      this.testModeName = testModeName;
      this.enableFlush = enableFlush;
    }
  }

  public TagInvertedIndexTest(TagInvertedIndexTestParam testParam) {
    this.testParam = testParam;
  }

  @Before
  public void setUp() throws Exception {
    numOfDeviceIdsInMemTable =
        TagSchemaDescriptor.getInstance().getTagSchemaConfig().getNumOfDeviceIdsInMemTable();
    TagSchemaDescriptor.getInstance().getTagSchemaConfig().setNumOfDeviceIdsInMemTable(3);
    enableFlush = TagSchemaDescriptor.getInstance().getTagSchemaConfig().isEnableFlush();
    TagSchemaDescriptor.getInstance().getTagSchemaConfig().setEnableFlush(testParam.enableFlush);
    schemaDir = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
    storageGroupDirPath = schemaDir + File.separator + storageGroupFullPath;
    schemaRegionDirPath = storageGroupDirPath + File.separator + 0;
    tagInvertedIndex = new TagInvertedIndex(schemaRegionDirPath);
  }

  @After
  public void tearDown() throws Exception {
    TagSchemaDescriptor.getInstance()
        .getTagSchemaConfig()
        .setNumOfDeviceIdsInMemTable(numOfDeviceIdsInMemTable);
    TagSchemaDescriptor.getInstance().getTagSchemaConfig().setEnableFlush(enableFlush);
    tagInvertedIndex.clear();
    tagInvertedIndex = null;
    FileUtils.deleteDirectoryAndEmptyParent(new File(schemaDir));
  }

  public void addTags() {
    List<Pair<Map<String, String>, Integer>> records = generateTags();
    for (Pair<Map<String, String>, Integer> pair : records) {
      tagInvertedIndex.addTags(pair.left, pair.right);
    }
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void removeTags() {
    Pair<Map<String, String>, Integer> tags = generateTag(record[0]);
    tagInvertedIndex.removeTags(tags.left, tags.right);
    tags = generateTag(record[1]);
    tagInvertedIndex.removeTags(tags.left, tags.right);
    tags = generateTag(record[3]);
    tagInvertedIndex.removeTags(tags.left, tags.right);
    tags = generateTag(record[11]);
    tagInvertedIndex.removeTags(tags.left, tags.right);
  }

  @Test
  public void getAllMatchedIDs() {
    addTags();
    Map<String, String> tags1 = new HashMap<>();
    tags1.put("tag1", "q");

    Map<String, String> tags2 = new HashMap<>();
    tags2.put("tag1", "q");
    tags2.put("tag2", "a");

    Map<String, String> tags3 = new HashMap<>();
    tags3.put("xxxxxxxx", "xxxxxxxx");

    List<Integer> ids = tagInvertedIndex.getAllMatchedIDs(tags1);
    List<Integer> verify = Arrays.asList(1, 2, 3, 4, 5, 7, 12, 15, 16, 18);
    assertEquals(verify, ids);

    ids = tagInvertedIndex.getAllMatchedIDs(tags2);
    verify = Arrays.asList(1, 3, 12, 15, 16);
    assertEquals(verify, ids);

    ids = tagInvertedIndex.getAllMatchedIDs(tags3);
    verify = Arrays.asList();
    assertEquals(verify, ids);

    removeTags();

    ids = tagInvertedIndex.getAllMatchedIDs(tags1);
    verify = Arrays.asList(3, 5, 7, 15, 16, 18);
    assertEquals(verify, ids);

    ids = tagInvertedIndex.getAllMatchedIDs(tags2);
    verify = Arrays.asList(3, 15, 16);
    assertEquals(verify, ids);

    ids = tagInvertedIndex.getAllMatchedIDs(tags3);
    verify = Arrays.asList();
    assertEquals(verify, ids);

    ids = tagInvertedIndex.getAllMatchedIDs(new HashMap<>());
    verify = Arrays.asList();
    assertEquals(verify, ids);
  }

  @Test
  public void testGetMatchedIDsIteratively() {
    addTags();
    Map<String, String> tags1 = new HashMap<>();
    tags1.put("tag1", "q");

    Map<String, String> tags2 = new HashMap<>();
    tags2.put("tag1", "q");
    tags2.put("tag2", "a");

    Map<String, String> tags3 = new HashMap<>();
    tags3.put("xxxxxxxx", "xxxxxxxx");

    List<Integer> ids = new ArrayList<>();
    Iterator<Integer> iterator = tagInvertedIndex.getMatchedIDsIteratively(tags1);
    setIDs(ids, iterator);
    List<Integer> verify = Arrays.asList(1, 2, 3, 4, 5, 7, 12, 15, 16, 18);
    assertEquals(verify, ids);

    ids = new ArrayList<>();
    iterator = tagInvertedIndex.getMatchedIDsIteratively(tags2);
    setIDs(ids, iterator);
    verify = Arrays.asList(1, 3, 12, 15, 16);
    assertEquals(verify, ids);

    ids = new ArrayList<>();
    iterator = tagInvertedIndex.getMatchedIDsIteratively(tags3);
    setIDs(ids, iterator);
    verify = Arrays.asList();
    assertEquals(verify, ids);

    removeTags();

    ids = new ArrayList<>();
    iterator = tagInvertedIndex.getMatchedIDsIteratively(tags1);
    setIDs(ids, iterator);
    verify = Arrays.asList(3, 5, 7, 15, 16, 18);
    assertEquals(verify, ids);

    ids = new ArrayList<>();
    iterator = tagInvertedIndex.getMatchedIDsIteratively(tags2);
    setIDs(ids, iterator);
    verify = Arrays.asList(3, 15, 16);
    assertEquals(verify, ids);

    ids = new ArrayList<>();
    iterator = tagInvertedIndex.getMatchedIDsIteratively(tags3);
    setIDs(ids, iterator);
    verify = Arrays.asList();
    assertEquals(verify, ids);

    ids = new ArrayList<>();
    iterator = tagInvertedIndex.getMatchedIDsIteratively(new HashMap<>());
    setIDs(ids, iterator);
    verify = Arrays.asList();
    assertEquals(verify, ids);
  }

  @Test
  public void testRecover() throws IOException {
    Map<String, String> tags1 = new HashMap<>();
    tags1.put("tag1", "q");

    Map<String, String> tags2 = new HashMap<>();
    tags2.put("tag1", "q");
    tags2.put("tag2", "a");
    addTags();
    removeTags();

    tagInvertedIndex.clear();
    tagInvertedIndex = new TagInvertedIndex(schemaRegionDirPath);

    List<Integer> ids = tagInvertedIndex.getAllMatchedIDs(tags1);
    List<Integer> verify = Arrays.asList(3, 5, 7, 15, 16, 18);
    assertEquals(verify, ids);

    ids = tagInvertedIndex.getAllMatchedIDs(tags2);
    verify = Arrays.asList(3, 15, 16);
    assertEquals(verify, ids);
  }

  private List<Pair<Map<String, String>, Integer>> generateTags() {
    List<Pair<Map<String, String>, Integer>> pairs = new ArrayList<>();
    for (String[] strings : record) {
      pairs.add(generateTag(strings));
    }
    return pairs;
  }

  private Pair<Map<String, String>, Integer> generateTag(String[] strings) {
    Map<String, String> tags = new HashMap<>();
    int i = 0;
    for (; i < strings.length - 1; i++) {
      String[] str = strings[i].split("=");
      tags.put(str[0], str[1]);
    }
    Pair<Map<String, String>, Integer> pair = new Pair<>(tags, Integer.valueOf(strings[i]));
    return pair;
  }

  private void setIDs(List<Integer> ids, Iterator<Integer> iterator) {
    while (iterator.hasNext()) {
      ids.add(iterator.next());
    }
  }
}
