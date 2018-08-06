package com.dounine.spark.learn;

import com.dounine.spark.learn.core.CustomTableInputFormat;
import com.dounine.spark.learn.utils.HadoopKrbLogin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SimpleApp {
    static Configuration cfg = null;
    static {
        HadoopKrbLogin.login();
        cfg = new Configuration();
        String tableName = "logTables";

        cfg.set(TableInputFormat.INPUT_TABLE, tableName);
        cfg.addResource("hbase-site.xml");
        cfg.addResource("core-site.xml");
        cfg.addResource("hdfs-site.xml");
        cfg.addResource("mapred-site.xml");
        cfg.addResource("yarn-site.xml");
    }
    public static void main(String[] args) {
        String logFile = "/soft/dounine/github/spark-learn/README.md"; // Should be some file on your system
        SparkConf sparkConf = new SparkConf()
                .setMaster("local") //非本地版本,请使用下面两句
//                .setMaster("spark://lake.dounine.com:7077")
//                .setJars(new String[]{"/soft/dounine/github/spark-learn/build/libs/spark-learn-1.0-SNAPSHOT.jar"})
                .setAppName("JavaWordCount");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(cfg, CustomTableInputFormat.class, ImmutableBytesWritable.class, Result.class);

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
//        JavaRDD<String> logData = sc.textFile(logFile).cache();
//
//        long numAs = logData.filter(s -> s.contains("a")).count();
//        long numBs = logData.map(new Function<String, Integer>() {
//            @Override
//            public Integer call(String v1) throws Exception {
//                return 1;
//            }
//        }).reduce((a,b)->a+b);
//
//        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
        rdd1.foreachPartition((VoidFunction<Iterator<Object>>)
                objectIterator -> {
                    while (null!=objectIterator&&objectIterator.hasNext()) {
                        Object o = objectIterator.next();
                        System.out.println(o);
                    }
                });

        jsc.stop();
    }

    private static Map<String, Object> result2Map(Result result) {
        Map<String, Object> ret = new HashMap<>();
        for (Cell cell : result.rawCells()) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] column = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            String columnName = Bytes.toString(column);
            if("durationTime".equals(columnName)){
                ret.put(Bytes.toString(family)+":"+Bytes.toString(column),Bytes.toLong(value));
            }else{
                ret.put(Bytes.toString(family)+":"+Bytes.toString(column),Bytes.toString(value));
            }
        }
        return ret;
    }

}
