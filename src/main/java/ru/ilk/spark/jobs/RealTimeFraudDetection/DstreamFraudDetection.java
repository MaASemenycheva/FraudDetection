package ru.ilk.spark.jobs.RealTimeFraudDetection;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.spark.connector.cql.CassandraConnector;
import javafx.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.javatuples.Triplet;
import ru.ilk.cassandra.CassandraConfig;
import ru.ilk.cassandra.CassandraDriver;
import ru.ilk.cassandra.dao.CreditcardTransactionRepository;
import ru.ilk.cassandra.dao.KafkaOffsetRepository;
import ru.ilk.config.Config;
import ru.ilk.creditcard.Schema;
import ru.ilk.kafka.KafkaConfig;
import ru.ilk.spark.DataReader;
import ru.ilk.spark.GracefulShutdown;
import ru.ilk.spark.SparkConfig;
import ru.ilk.spark.jobs.SparkJob;
import ru.ilk.utils.Utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.types.DataTypes.*;

public class DstreamFraudDetection extends SparkJob {
    private static Logger logger = Logger.getLogger(DstreamFraudDetection.class.getName());
    public DstreamFraudDetection(String appName) {
        super(appName);
        appName = "Fraud Detection using Dstream";
    }

    public static void main( String[] args ) {
        SparkJob sparkJobVariable = new SparkJob("Fraud Detection using Dstream");
        SparkSession sparkSession = sparkJobVariable.sparkSession;
        Config.parseArgs(args);
        Dataset<Row> customerDF = DataReader.readFromCassandra(
                CassandraConfig.keyspace,
                CassandraConfig.customer,
                sparkSession
        );

        Dataset<Row> customerAgeDF = customerDF.withColumn("age", (
                org.apache.spark.sql.functions.datediff(
                                org.apache.spark.sql.functions.current_date(),
                                org.apache.spark.sql.functions.to_date(customerDF.col("dob"))
                        ).divide(365)
                        .cast(IntegerType)));

        customerAgeDF.cache();
        customerAgeDF.show();

        /* Load Preprocessing Model and Random Forest Model saved by Spark ML Job i.e FraudDetectionTraining */
        PipelineModel preprocessingModel = PipelineModel.load(SparkConfig.preprocessingModelPath);
        RandomForestClassificationModel randomForestModel = RandomForestClassificationModel.load(SparkConfig.modelPath);




//        SparkConf sparkConf = new SparkConf();
//        sparkConf.setMaster("local[2]");
//        sparkConf.setAppName("WordCountingApp");
//        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
//
//        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
//        kafkaParams.put("key.deserializer", StringDeserializer.class);
//        kafkaParams.put("value.deserializer", StringDeserializer.class);
//        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
//        kafkaParams.put("auto.offset.reset", "latest");
//        kafkaParams.put("enable.auto.commit", false);

        HashMap<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, (String) KafkaConfig.kafkaParams.get("bootstrap.servers"));
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, (String) KafkaConfig.kafkaParams.get("group.id"));
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, (String) KafkaConfig.kafkaParams.get("auto.offset.reset"));


        CassandraConnector connector = CassandraConnector.apply(sparkSession.sparkContext().getConf());
        System.out.println("connector = " + connector);

        HashMap<String, String> mapData = new HashMap<String, String>();
        mapData.put("keyspace", CassandraConfig.keyspace);
        mapData.put("fraudTable", CassandraConfig.fraudTransactionTable);
        mapData.put("nonFraudTable", CassandraConfig.nonFraudTransactionTable);
        mapData.put("kafkaOffsetTable", CassandraConfig.kafkaOffsetTable);

        JavaStreamingContext streamingContext = new JavaStreamingContext(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()), Durations.seconds(SparkConfig.batchInterval));
//        Collection<String> topics = Arrays.asList("topicA", "topicB");
        HashSet<String> topics = new HashSet<String>();
        topics.add((String) KafkaConfig.kafkaParams.get("topic"));



        Optional<HashMap<TopicPartition, Long>> storedOffsets = CassandraDriver.readOffset(CassandraConfig.keyspace,
                CassandraConfig.kafkaOffsetTable, (String) KafkaConfig.kafkaParams.get("topic"), sparkSession
                );

        System.out.println("storedOffsets " + storedOffsets);

        if (storedOffsets == null) {
            JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            streamingContext,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                    );
        }


        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Assign(storedOffsets.get().keySet(), kafkaParams, storedOffsets.get())
        );

        JavaDStream<Triplet<String, Integer, Long>> transactionStream = stream.map(record -> new Triplet<>(record.value(), record.partition(), record.offset()));


        transactionStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {

//                Dataset<Row> kafkaTransactionDF = rdd.toDF("transaction", "partition", "offset")
                StructType schema = new StructType()
                        .add("transaction", DataTypes.StringType)
                        .add("partition", DataTypes.StringType)
                        .add("offset", DataTypes.StringType);
                Dataset<Row> kafkaTransactionDF = sparkSession.createDataFrame((List<Row>) rdd, schema);
                kafkaTransactionDF.show();

                kafkaTransactionDF.withColumn(Schema.kafkaTransactionStructureName, // nested structure with our json
                                org.apache.spark.sql.functions.from_json(
                                        kafkaTransactionDF.col("transaction"), Schema.kafkaTransactionSchema))
                        .select("transaction.*", "partition", "offset")
                        .withColumn("amt", org.apache.spark.sql.functions.lit(kafkaTransactionDF
                                .col("amt")).cast(DoubleType))
                        .withColumn("merch_lat", org.apache.spark.sql.functions.lit(kafkaTransactionDF
                                .col("merch_lat")).cast(DoubleType))
                        .withColumn("merch_long", org.apache.spark.sql.functions.lit(kafkaTransactionDF
                                .col("merch_long")).cast (DoubleType))
                        .withColumn("trans_time", org.apache.spark.sql.functions.lit(kafkaTransactionDF
                                .col("trans_time")).cast(TimestampType));

                sparkSession.sqlContext().sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800");

                String COLUMN_DOUBLE_UDF_NAME = "distanceUdf";
                sparkSession.udf().register(COLUMN_DOUBLE_UDF_NAME, (UDF4<String, String, String, String, Double>)
                        (lat1, lon1, lat2, lon2) -> {
                            Double distance = Utils.getDistance(Double.valueOf(lat1), Double.valueOf( lon1), Double.valueOf(lat2), Double.valueOf(lon2));
                            return distance;
                        }, DataTypes.DoubleType);

                Dataset<Row> processedTransactionDF1 = kafkaTransactionDF.join(
                                org.apache.spark.sql.functions.broadcast(customerAgeDF), "cc_num");

                Dataset<Row> processedTransactionDF = processedTransactionDF1.withColumn("distance",
                                org.apache.spark.sql.functions.lit(
                                        org.apache.spark.sql.functions.round(
                                                callUDF(COLUMN_DOUBLE_UDF_NAME,
                                                        processedTransactionDF1.col("lat"),
                                                        processedTransactionDF1.col("long"),
                                                        processedTransactionDF1.col("merch_lat"),
                                                        processedTransactionDF1.col("merch_long"))
                                                , 2)));

                Dataset<Row> featureTransactionDF = preprocessingModel.transform(processedTransactionDF);
                Dataset<Row> predictionDF = randomForestModel.transform(featureTransactionDF)
                        .withColumnRenamed("prediction", "is_fraud");



                /*
                 Connector Object is created in driver. It is serializable.
                 It is serialized and send to executor. Once the executor get it, they establish the real connection
                */

                predictionDF.foreachPartition(partitionOfRecords -> {
                    /*
                     * dbname and table name are initialzed in the driver. foreachPartition is called in the executor, hence dbname
                     * and table names have to be hashmap
                     */
                        String keyspace = mapData.get("keyspace");
                        String fraudTable = mapData.get("fraudTable");
                        String nonFraudTable = mapData.get("nonFraudTable");
                        String kafkaOffsetTable = mapData.get("kafkaOffsetTable");
                        System.out.println("keyspace = " + keyspace);


                             /*
                                      Writing to Fraud, NonFruad and Offset Table in single iteration
                                      Cassandra prepare statement is used because it avoids pasring of the column for every insert and hence efficient
                                      Offset is inserted last to achieve atleast once semantics. it is possible that it may read duplicate creditcard
                                      transactions from kafka while restart.
                                      Even though duplicate creditcard transaction are read from kafka, writing to Cassandra is idempotent. Becasue
                                      cc_num and trans_time is the primary key. So you cannot have duplicate records with same cc_num and trans_time.
                                      As a result we achive exactly once semantics.
                            */
                    connector.jWithSessionDo( session -> {
                        //Prepare Statement for all three tables
                        PreparedStatement preparedStatementFraud = session.prepare(CreditcardTransactionRepository.cqlTransactionPrepare(keyspace, fraudTable));
                        PreparedStatement preparedStatementNonFraud = session.prepare(CreditcardTransactionRepository.cqlTransactionPrepare(keyspace, nonFraudTable));
                        PreparedStatement preparedStatementOffset = session.prepare(KafkaOffsetRepository.cqlOffsetPrepare(keyspace, kafkaOffsetTable));

                        HashMap<Integer, Long> partitionOffset = new HashMap<>();
                        partitionOfRecords.forEachRemaining(record -> {
                            Double isFraud = record.<Double>getAs("is_fraud");
                            if (isFraud == 1.0) {
                                // Bind and execute prepared statement for Fraud Table
                                session.execute(CreditcardTransactionRepository.cqlTransactionBind(preparedStatementFraud, record));
                            }
                            else if(isFraud == 0.0) {
                                // Bind and execute prepared statement for NonFraud Table
                                session.execute(CreditcardTransactionRepository.cqlTransactionBind(preparedStatementNonFraud, record));
                            }
                            //Get max offset in the current match
                            Integer kafkaPartition = record.<Integer>getAs("partition");
                            Long offset = record.<Long>getAs("offset");
                            Long currentMaxOffset = partitionOffset.get(kafkaPartition);
                            if (offset > currentMaxOffset) {
                                partitionOffset.put(kafkaPartition, offset);
                            }
                            else {
                                partitionOffset.put(kafkaPartition, offset);
                            }
                        });
//                                partitionOffset.
                        partitionOffset.forEach( (k,v)-> {
                            // Bind and execute prepared statement for Offset Table
                            session.execute(KafkaOffsetRepository
                                    .cqlOffsetBind(preparedStatementOffset, new Pair<Integer, Long>(k,v)));

                        }
                        );
                        return partitionOffset;
                    });
                });


            }
            else {
                logger.info("Did not receive any data");
            }
        });

        streamingContext.start();
        GracefulShutdown.handleGracefulShutdown(1000, streamingContext.ssc(), sparkSession);
    }
}
