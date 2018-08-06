package com.dounine.spark.learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "/soft/dounine/github/spark-learn/README.md"; // Should be some file on your system
        SparkConf sparkConf = new SparkConf()
                .setMaster("spark://lake.starsriver.cn:7077")
                .setJars(new String[]{"/soft/dounine/github/spark-learn/build/libs/spark-learn-1.0-SNAPSHOT.jar"})
                .setAppName("JavaWordCount");
        JavaSparkContext spark = new JavaSparkContext(sparkConf);
        JavaRDD<String> logData = spark.textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
