package ru.ilk.spark;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataReader {
    private static Logger logger = Logger.getLogger(DataReader.class.getName());

    public static Dataset<Row> read(String transactionDatasource,
                                    SparkSession sparkSession) {
        return sparkSession.read()
                .format("csv")
                .option("header", "true")
                .load(transactionDatasource);
    }

    public static Dataset<Row> readFromCassandra(String keySpace, String table, SparkSession sparkSession) {
        return sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", keySpace)
                .option("table", table)
                .option("pushdown", "true")
                .load();
    }
}
