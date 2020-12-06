package com.stb.spark.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;

import java.util.concurrent.TimeoutException;

/***
 * Sample application use hdfs source
 */
public class SampleApplication {

    public static void main(String[] args) {

        SparkSession _sparkSession = SparkSession
                .builder()
                .appName("HDFS Spark application")
                .master("local[*]")
                .getOrCreate();

        Dataset<?> input = _sparkSession.readStream()
                .format("hdfs")
                .option("path", "hdfs.folder.path")
                .option("fileFormat", "XML")
                .option("split", "xmlArraySplitTag")
                .load();

//        Dataset<?> output = input.map(data -> data, Encoders.BYTE()); //transform spark functions
        try {
            input.writeStream()
                    .format("kafka")
                    .outputMode(OutputMode.Append())
                    .option("kafka.bootstrap.servers", "serves")
                    .start();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}