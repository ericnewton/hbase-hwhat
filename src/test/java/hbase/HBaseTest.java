/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hbase;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTest {
  private static final Logger log = LoggerFactory.getLogger(HBaseTest.class);

  Configuration conf;
  MiniZooKeeperCluster zk;
  MiniHBaseCluster cluster;

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    String tmpDir = System.getProperty("user.dir") + "/target/mini-hbase";
    FileUtils.deleteDirectory(new File(tmpDir));
    conf.set("hbase.rootdir", "file://" + tmpDir);
    zk = new MiniZooKeeperCluster();
    int zkPort = zk.startup(new File(tmpDir, "zookeeper"));
    conf.setInt("hbase.zookeeper.property.clientPort", zkPort);
    conf.set("hbase.master", "local");
    conf.setInt("hbase.regionserver.info.port", -1);
    cluster = new MiniHBaseCluster(conf, 2);
    conf.set("hbase.master", cluster.getMaster().getServerName().getHostAndPort());
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    zk.shutdown();
  }


  @Test
  public void tenMillionTest() throws Exception {
    final boolean NUKE_TABLE = true, WRITE_DATA = true, READ_DATA = true;
    final int NUM_ROWS = 1000 * 1000;
    final int NUM_COLS = 10;
    long entriesWritten = 0;
    final byte[] cf = "cf".getBytes();

    Connection conn = ConnectionFactory.createConnection(conf);
    Admin admin = conn.getAdmin();
    Set<TableName> tables = new HashSet<>(Arrays.asList(admin.listTableNames()));
    TreeSet<Integer> rowsWritten = new TreeSet<>();
    TableName tableName = TableName.valueOf("test");

    if (NUKE_TABLE) {
      // Disable+delete the table if it exists
      if (tables.contains(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }

      // create the table
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      tableDesc.addFamily(new HColumnDescriptor(cf));
      byte[][] splits = new byte[9][2];
      for (int i = 1; i < 10; i++) {
        int split = 48 + i;
        splits[i - 1][0] = (byte) (split >>> 8);
        splits[i - 1][0] = (byte) (split);
      }
      admin.createTable(tableDesc, splits);
    }

    if (WRITE_DATA) {
      Table table = conn.getTable(tableName);
      try {
        table.setWriteBufferSize(1024 * 1024 * 50);
        System.out.println("Write buffer size: " + table.getWriteBufferSize());

        List<Put> puts = new ArrayList<Put>();
        // Write 1M rows * 10 columns = 10M k-v pairs
        for (int i = 0; i < NUM_ROWS; i++) {
          rowsWritten.add(i);
          Put p = new Put(Integer.toString(i).getBytes());
          for (int j = 0; j < NUM_COLS; j++) {
            byte[] value = new byte[50];
            Bytes.random(value);
            p.addColumn(cf, Integer.toString(j).getBytes(), value);
          }
          puts.add(p);

          // Flush the puts
          if (puts.size() == 1000) {
            Object[] results = new Object[1000];
            try {
              table.batch(puts, results);
            } catch (IOException e) {
              log.error("Failed to write data", e);
              log.info("Errors: {}", Arrays.toString(results));
            }

            entriesWritten += puts.size();
            puts.clear();

            if (entriesWritten % 50000 == 0) {
              log.info("Wrote {} entries", entriesWritten);
            }
          }
        }
        if (puts.size() > 0) {
          entriesWritten += puts.size();
          Object[] results = new Object[puts.size()];
          try {
            table.batch(puts, results);
          } catch (IOException e) {
            log.error("Failed to write data", e);
            log.info("Error: {}", Arrays.toString(results));
          }
        }

        log.info("Wrote {} entries in total", entriesWritten);
      } finally {
        log.info("Closing table used for writes");
        table.close();
      }
    }
    if (READ_DATA) {
      Table table = conn.getTable(tableName);
      try {
        TreeSet<Integer> rows = new TreeSet<Integer>();
        long rowsObserved = 0l;
        long entriesObserved = 0l;
        Scan s = new Scan();
        s.addFamily(cf);
        s.setMaxResultSize(-1);
        s.setBatch(-1);
        ResultScanner scanner = table.getScanner(s);
        String row = null;
        // Read all the records in the table
        for (Result result : scanner) {
          rowsObserved++;
          row = new String(result.getRow());
          rows.add(Integer.parseInt(row));
          if (rowsObserved % 10000 == 0) {
            log.info("Saw row {}", row);
          }
          while (result.advance()) {
            entriesObserved++;
          }
        }
        log.info("Last row in Result {}", row);

        // Verify that we see 1M rows and 10M cells
        log.info("Saw {} rows", rowsObserved);
        log.info("Saw {} cells", entriesObserved);

        rowsWritten.removeAll(rows);
        String toString = rowsWritten.toString();
        int len = Math.min(5000, toString.length());
        log.error("Missing {} rows: {}{}", new Object[] {rowsWritten.size(), toString.substring(0, len), (len == toString.length() ? "" : "...")});
        assertEquals(0, rowsWritten.size());
      } finally {
        table.close();
      }
    }
  }
}
