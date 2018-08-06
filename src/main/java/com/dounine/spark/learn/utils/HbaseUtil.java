package com.dounine.spark.learn.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.List;


public class HbaseUtil {
    protected Configuration cfg = null;
    protected Connection connection;

    public HbaseUtil() throws IOException {
        init();
    }

    protected void init() throws IOException {
        HadoopKrbLogin.login();
        this.cfg = createConf();
        this.connection = ConnectionFactory.createConnection(cfg);
    }

    public Configuration createConf() {
        return HbaseUtil.createHBaseConf();
    }

    public static Configuration createHBaseConf() {
        Configuration conf = new Configuration();
        conf.addResource("hbase-site.xml");
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("mapred-site.xml");
        conf.addResource("yarn-site.xml");

        return conf;
    }

    private Table getTable(TableName table) throws IOException {
        return connection.getTable(table);
    }

    public Put buildPut(String rowkey, String family, String[] qualifier, String[] data) {
        try {
            Put put = new Put(rowkey.getBytes());
            byte[] familyBytes = family.getBytes();
            for (int i = 0; i < qualifier.length; i++) {
                put.addColumn(familyBytes, qualifier[i].getBytes(), data[i].getBytes());
            }
            return put;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void put(TableName table, Put put) throws IOException {
        if (put == null) return;

        Table driver = getTable(table);
        try {
            driver.put(put);
        } finally {
            driver.close();
        }
    }

    public void put(TableName table, List<Put> puts) throws IOException {
        if (puts == null || puts.isEmpty()) return;

        Table driver = getTable(table);
        try {
            driver.put(puts);
        } finally {
            driver.close();
        }
    }

}
