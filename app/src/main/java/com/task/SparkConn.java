package com.task;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkConn {
    public static final String APP_NAME = "Csv2Hive";
    private SparkSession spark;

    public SparkConn() {
        this.spark = SparkSession
                .builder()
                .appName(APP_NAME)
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
    }

    public SparkConn(SparkConf conf) {
        this.spark = SparkSession
                .builder()
                .config(conf)
                .appName(APP_NAME)
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
    }

    public SparkSession getSparkSession() {
        return this.spark;
    }
}
