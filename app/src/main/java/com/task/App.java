package com.task;

import static org.apache.spark.sql.functions.*;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class App extends SparkConn {

    public static void registerUDFs(SparkSession spark) {
        int time_zone_offset = 9;

        spark.udf().register("convertToKST", new UDF1<Timestamp, Timestamp>() {
            @Override
            public Timestamp call(Timestamp utcTimeStr) throws Exception {
                // UTC 문자열을 LocalDateTime으로 파싱
                LocalDateTime utcTime = utcTimeStr.toLocalDateTime();
                // UTC 시간을 KST로 변환
                ZonedDateTime kstTime = utcTime
                        .plusHours(time_zone_offset)
                        .atZone(ZoneId.of("Asia/Seoul"));
                return Timestamp.valueOf(kstTime.toLocalDateTime());
            }
        }, DataTypes.TimestampType);
    }

    public static void main(String[] args) { 
        Properties env = new Properties();
        Path currentPath = Paths.get("").toAbsolutePath();
        Path envfilePath = Paths.get(
                currentPath.toString(),
                "src/main",
                "resources",
                "env.properties");
        // 현재 날짜 가져오기
        LocalDate today = LocalDate.now();

        // 날짜를 문자열로 변환하기 (원하는 형식으로 포맷)
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String startDate = today.format(formatter);
        
        LocalDate tomorrow = today.plusDays(1);
        String endDate = tomorrow.format(formatter);
        if (args != null && args.length > 0) {
            startDate = args[0];
            endDate = args[1];
        }

        try {
            System.out.println("path " + envfilePath.toString());
            env.load(new FileInputStream(envfilePath.toString()));
            SparkConf conf = new SparkConf();
            conf.set("spark.sql.warehouse.dir", env.getProperty("HIVE_WAREHOUSE_DIR"));
            conf.set("spark.hadoop.javax.jdo.option.ConnectionURL", env.getProperty("HIVE_CONNECTION_URL"));
            conf.set("spark.hadoop.javax.jdo.option.ConnectionDriverName", env.getProperty("HIVE_DRIVER_NAME"));
            conf.set("spark.hadoop.javax.jdo.option.ConnectionUserName", env.getProperty("HIVE_CONNECTION_USER"));
            conf.set("spark.hadoop.javax.jdo.option.ConnectionPassword", env.getProperty("HIVE_CONNECTION_PASSWORD"));

            SparkConn conn = new SparkConn(conf);
            SparkSession spark = conn.getSparkSession();
            // UDF 등록
            registerUDFs(spark);

            Dataset<Row> df = spark
                    .read()
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .csv(env.getProperty("SOURCE_PATH"));

            df = df.withColumn(
                    "event_time_kst",
                    callUDF("convertToKST", df.col("event_time")));
            df = df.withColumn("event_date_kst",
                    to_date(col("event_time_kst"), "yyyy-MM-dd HH:mm:ss z"));
            df = df.withColumn("event_date",
                    date_format(col("event_date_kst"), "yyyyMMdd"))
                    .drop("event_time_kst");

            Dataset<Row> filteredData = df.filter("event_date_kst >= '" + startDate + "' AND event_date_kst <= '" + endDate + "'");

            filteredData.show(false);
            // df.printSchema();

            String tableName = "ecommerce_behavior";
            String partition = "event_date";
            String location = "/tmp/hive";
            // save
            String ddl = String.format("""
                    CREATE EXTERNAL TABLE IF NOT EXISTS %s
                    (event_time timestamp,
                        event_type string,
                        product_id int,
                        category_id long,
                        category_code string,
                        brand string,
                        price double,
                        user_id int,
                        user_session string,
                        event_time_kst timestamp)
                    PARTITIONED BY (%s STRING)
                    STORED AS PARQUET
                    LOCATION '%s'
                        """, tableName, partition, location);
            spark.sql(ddl);
            filteredData.write()
                    .partitionBy("event_date")
                    .option("compression", "snappy")
                    .format("parquet")
                    .mode("overwrite")
                    .saveAsTable(tableName);
            spark.stop();
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
        }
    }
}

