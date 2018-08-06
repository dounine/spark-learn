package com.dounine.spark.learn.utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SparkHfile {
    protected Configuration cfg = null;

    public SparkHfile() {
        init();
    }

    private void init() {
        cfg = new Configuration();
        cfg.addResource("hbase-site.xml");
        cfg.addResource("core-site.xml");
        cfg.addResource("hdfs-site.xml");
        cfg.addResource("mapred-site.xml");
        cfg.addResource("yarn-site.xml");
    }

    public void rddFromHadoop() {
        SparkConf conf = new SparkConf().setAppName("demo").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // rdd from hfile
        // 重点在于TableInputFormat,需定制InputFormat,修改拿分片的规则,具体可见http://www.cnblogs.com/lxf20061900/p/3812812.html
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(cfg, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        // do some transformation
        JavaRDD<Object> rdd1 = hBaseRDD.mapPartitions((FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>, Object>)
                tuple2Iterator -> {
                    while (tuple2Iterator.hasNext()) {
                        Tuple2<ImmutableBytesWritable, Result> tuple = tuple2Iterator.next();
                        Result result = tuple._2;
                        String rowKey = Bytes.toString(result.getRow());
                        System.out.println("pk: " + rowKey);
                        System.out.println("columns: " + result2Map(result));
                    }
                    return null;
                });

        // do something, like write to MySql
        rdd1.foreachPartition((VoidFunction<Iterator<Object>>)
                objectIterator -> {
                    while (objectIterator.hasNext()) {
                        Object o = objectIterator.next();
                        System.out.println(o);
                    }
                });
    }

    private Map<String, String> result2Map(Result result) {
        Map<String, String> ret = new HashMap<String, String>();
        for (Cell cell : result.rawCells()) {
            byte[] qualifierByte = cell.getQualifierArray();
            if (qualifierByte == null || qualifierByte.length == 0) {
                continue;
            }

            byte[] valueByte = cell.getValueArray();
            if (valueByte == null || valueByte.length == 0) {
                ret.put(new String(qualifierByte), "");
            } else {
                ret.put(new String(qualifierByte), new String(valueByte));
            }
        }
        return ret;
    }

}
