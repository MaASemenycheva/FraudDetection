package ru.ilk.spark;

//import javafx.util.Pair;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.streaming.kafka010.HasOffsetRanges;


public class DataReader {
    private static Logger logger = Logger.getLogger(DataReader.class.getName());

    public static Dataset<Row> read(String transactionDatasource,
//                                    StructType schema,
                                    SparkSession sparkSession) {
        return sparkSession.read()
//                .option("header", "true")
//                .schema(schema)
//                .csv(transactionDatasource);
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
//    public static void getOffset (RDD<HasOffsetRanges> rdd, SparkSession sparkSession) {
//        HasOffsetRanges hasOffsetRanges = (HasOffsetRanges) rdd.toJavaRDD().rdd();
//
//        StructType featureSchema = new StructType();
//        featureSchema.add("partition", DataTypes.StringType, true);
//        featureSchema.add("offset", DataTypes.StringType, true);
//
//        List<Pair<Integer, Long>> rowRdd = (List<Pair<Integer, Long>>) Arrays.stream(hasOffsetRanges.offsetRanges()).map(offset ->
//                new Pair<Integer, Long>(offset.partition(), offset.untilOffset()));
//        sparkSession.createDataFrame((JavaRDD<Row>) rowRdd, featureSchema);
//    }
}
