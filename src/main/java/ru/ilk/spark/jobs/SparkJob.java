package ru.ilk.spark.jobs;

import org.apache.spark.sql.SparkSession;
import ru.ilk.spark.SparkConfig;

class SparkJob {
    String appNameSparkJob;
    SparkSession sparkSession;
    public SparkJob(String appName) {
        appNameSparkJob = appName;  // Set the initial value for the class attribute x
        sparkSession = SparkSession
                .builder()
                .appName(appName)
                .master("local")
                .config(SparkConfig.sparkConf)
                .getOrCreate();
    }
}
