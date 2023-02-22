package ru.ilk.creditcard;//package ru.ilk.creditcard;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;

public class Schema {
    static String transactionStructureName = "transaction";

    public static StructType transactionSchema =  new StructType(new StructField[]{
            createStructField("cc_num", StringType,true),
            createStructField("first", StringType, true),
            createStructField("last", StringType, true),
            createStructField("trans_num", StringType, true),
            createStructField("trans_date", StringType, true),
            createStructField("trans_time", StringType, true),
            createStructField("unix_time", LongType, true),
            createStructField("category", StringType, true),
            createStructField("merchant", StringType, true),
            createStructField("amt", DoubleType, true),
            createStructField("merch_lat", DoubleType, true),
            createStructField("merch_long", DoubleType, true)
    });

    /* Transaction  Schema used while importing transaction data to Cassandra*/
    public static StructType fraudCheckedTransactionSchema =  new StructType(new StructField[]{
            createStructField("is_fraud", DoubleType, true)
    });

    /* Customer Schema used while importing customer data to Cassandra*/
    String customerStructureName = "customer";

    public static StructType customerSchema =  new StructType(new StructField[]{
            createStructField("cc_num", StringType, true),
            createStructField("first", StringType, true),
            createStructField("last", StringType, true),
            createStructField("gender", StringType, true),
            createStructField("street", StringType, true),
            createStructField("city", StringType, true),
            createStructField("state", StringType, true),
            createStructField("zip", StringType, true),
            createStructField("lat", DoubleType, true),
            createStructField("long_field", DoubleType, true),
            createStructField("job", StringType, true),
            createStructField("dob", TimestampType, true)
    });

    /* Schema of transaction msgs received from Kafka. Json msg is received from Kafka. Hence evey field is treated as String */
    public static String kafkaTransactionStructureName = transactionStructureName;

    public static StructType kafkaTransactionSchema =  new StructType(new StructField[]{
            createStructField("cc_num", StringType, true),
            createStructField("first", StringType, true),
            createStructField("last", StringType, true),
            createStructField("trans_num", StringType, true),
            createStructField("trans_time", TimestampType, true),
            createStructField("category", StringType, true),
            createStructField("merchant", StringType, true),
            createStructField("amt", StringType, true),
            createStructField("merch_lat", StringType, true),
            createStructField("merch_long", StringType, true)
    });
}
