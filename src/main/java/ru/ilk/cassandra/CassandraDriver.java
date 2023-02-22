package ru.ilk.cassandra;

import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class CassandraDriver {

    private static Logger logger = Logger.getLogger(CassandraDriver.class.getName());

    /* Read offsert from Cassandra for Dstream*/
    public static Optional<HashMap<TopicPartition, Long>> readOffset(
            String keySpace,
            String table,
            String topic,
            SparkSession sparkSession) {
        HashMap<TopicPartition, Long> fromOffsets = new HashMap<TopicPartition, Long>();
        Dataset<Row> df = sparkSession
                .read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", keySpace)
                .option("table", table)
                .option("pushdown", "true")
                .load()
                .select("partition", "offset");
        if (df.rdd().isEmpty()) {
            logger.info("No offset. Read from earliest");
            return Optional.empty();
        } else {

            df.show();
            List<Long> listOne = df.select("offset").as(Encoders.LONG()).collectAsList();
            List<Integer> listTwo = df.select("partition").as(Encoders.INT()).collectAsList();
            System.out.println("listOne " + listOne);
            for (int i = 0; i < listOne.size(); i++) {
                fromOffsets.put(new TopicPartition(topic, listTwo.get(i)), listOne.get(i));

            }
        }
        return Optional.of(fromOffsets);
    }

}
