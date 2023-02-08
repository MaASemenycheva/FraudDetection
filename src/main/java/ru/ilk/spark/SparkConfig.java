package ru.ilk.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import ru.ilk.cassandra.CassandraConfig;
import ru.ilk.config.Config;

public class SparkConfig {
    private static Logger logger = Logger.getLogger(SparkConfig.class.getName());
    public static SparkConf sparkConf = new SparkConf();

    public static String transactionDatasouce;
    public static String customerDatasource;
    public static String modelPath;
    public static String preprocessingModelPath;
    static String shutdownMarker;
    public static Integer batchInterval;

    public static void load() {
        logger.info("Loading Spark Setttings");
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", Config.applicationConf.getString("config.spark.gracefulShutdown"))
                .set("spark.sql.streaming.checkpointLocation", Config.applicationConf.getString("config.spark.checkpoint"))
                .set("spark.cassandra.connection.host", Config.applicationConf.getString("config.cassandra.host"));
        shutdownMarker = Config.applicationConf.getString("config.spark.shutdownPath");
        batchInterval = Integer.valueOf(Config.applicationConf.getString("config.spark.batch.interval"));
        transactionDatasouce = Config.localProjectDir + Config.applicationConf.getString("config.spark.transaction.datasource");
        customerDatasource = Config.localProjectDir + Config.applicationConf.getString("config.spark.customer.datasource");
        modelPath = Config.localProjectDir + Config.applicationConf.getString("config.spark.model.path");
        preprocessingModelPath = Config.localProjectDir + Config.applicationConf.getString("config.spark.model.preprocessing.path");
    }

    public static void defaultSetting() {
        sparkConf.setMaster("local[*]")
                .set("spark.cassandra.connection.host", CassandraConfig.cassandraHost)
                .set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint");
        shutdownMarker = "/tmp/shutdownmarker";
        transactionDatasouce = "src/main/resources/data/transactions.csv";
        customerDatasource = "src/main/resources/data/customer.csv";
        modelPath = "src/main/resources/spark/training/RandomForestModel";
        preprocessingModelPath = "src/main/resources/spark/training/PreprocessingModel";
        batchInterval = 5000;
    }
}
