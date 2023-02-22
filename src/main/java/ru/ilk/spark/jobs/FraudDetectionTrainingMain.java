package ru.ilk.spark.jobs;

import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import ru.ilk.cassandra.CassandraConfig;
import ru.ilk.config.Config;
import ru.ilk.spark.DataBalancing;
import ru.ilk.spark.DataReader;
import ru.ilk.spark.SparkConfig;
import ru.ilk.spark.algorithms.Algorithms;
import ru.ilk.spark.pipline.BuildPipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

public class FraudDetectionTrainingMain extends SparkJob {
    private static Logger log = Logger.getLogger(FraudDetectionTrainingMain.class.getName());
    public FraudDetectionTrainingMain(String appName) {
        super(appName);
        appName = "Balancing Fraud & Non-Fraud Dataset";
    }

    public static void main( String[] args ) throws IOException {
        SparkJob sparkJobVariable = new SparkJob("Balancing Fraud & Non-Fraud Dataset");
        SparkSession sparkSession = sparkJobVariable.sparkSession;
        Config.parseArgs(args);

        Dataset<Row> fraudTransactionDF = DataReader.readFromCassandra(
                        CassandraConfig.keyspace,
                        CassandraConfig.fraudTransactionTable,
                        sparkSession)
                .select("cc_num" ,
                        "category",
                        "merchant",
                        "distance",
                        "amt",
                        "age",
                        "is_fraud");

        fraudTransactionDF.show();
        System.out.println("count fraudTransactionDF= " + fraudTransactionDF.count());


        Dataset<Row> nonFraudTransactionDF = DataReader.readFromCassandra(
                        CassandraConfig.keyspace,
                        CassandraConfig.nonFraudTransactionTable,
                        sparkSession)
                .select("cc_num" ,
                        "category",
                        "merchant",
                        "distance",
                        "amt",
                        "age",
                        "is_fraud");
        nonFraudTransactionDF.show();
        System.out.println("count nonFraudTransactionDF = " + nonFraudTransactionDF.count());
        Dataset<Row> transactionDF = nonFraudTransactionDF.union(fraudTransactionDF);
        transactionDF.cache();

        transactionDF.show(false);

        List<String> coloumnNames = Arrays.asList("category",
                "merchant",
                "distance",
                "amt",
                "age");

        PipelineStage[] pipelineStages = BuildPipeline.createFeaturePipeline(transactionDF.schema(), coloumnNames);
        Pipeline pipeline = new Pipeline().setStages(pipelineStages);
        PipelineModel preprocessingTransformerModel = pipeline.fit(transactionDF);


        Dataset<Row> featureDF = preprocessingTransformerModel.transform(transactionDF);

        featureDF.show(false);

        Dataset<Row>[] randomSplit = featureDF.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainData = randomSplit[0];
        Dataset<Row> testData = randomSplit[1];

        Long featureDFCount = featureDF.count();
        Long trainDataCount = trainData.count();
        Long testDataCount = trainData.count();

        System.out.println("featureDFCount: " + featureDFCount);
        System.out.println("trainDataCount: " + trainDataCount);
        System.out.println("testDataCount: " + testDataCount);

        Dataset<Row> featureLabelDF = trainData.select("features", "is_fraud").cache();

        Dataset<Row> nonFraudDF = featureLabelDF.filter(featureLabelDF.col("is_fraud").contains("0.0"));

        Long nonFraudCount = nonFraudDF.count();

        System.out.println("nonFraudCount: " + nonFraudCount);

        Dataset<Row> fraudDF = featureLabelDF.filter(featureLabelDF.col("is_fraud").contains("1.0"));
        Long fraudCount = fraudDF.count();

        System.out.println("fraudCount: " + fraudCount);



        /* There will be very few fraud transaction and more normal transaction. Models created  from such
         * imbalanced data will not have good prediction accuracy. Hence balancing the dataset. K-means is used for balancing
         */
        Dataset<Row> balancedNonFraudDF = DataBalancing.createBalancedDataframe(nonFraudDF, fraudCount.intValue(), sparkSession);

        balancedNonFraudDF.show();

        System.out.println("balancedNonFraudDF.dtypes"  + Arrays.toString(balancedNonFraudDF.dtypes()));

//
        Dataset<Row> finalfeatureDF = fraudDF.union(balancedNonFraudDF);
//
        RandomForestClassificationModel randomForestModel = Algorithms.randomForestClassifier(finalfeatureDF, sparkSession);
        Dataset<Row> predictionDF = randomForestModel.transform(testData);
        predictionDF.show(false);

        Dataset<Row> predictionAndLabels =
                predictionDF.select(
                        predictionDF.col("prediction"),
                        predictionDF.col("is_fraud")
                                .cast(DoubleType)).cache();

        predictionAndLabels.show();

//        // confusion matrix
        Float tp = Float.valueOf(predictionAndLabels.toJavaRDD().filter(row ->
                ((Double)row.get(0)).doubleValue() == 1.0d && ((Double)row.get(1)).doubleValue() == 1.0d).count());
        Float fp = Float.valueOf(predictionAndLabels.toJavaRDD().filter(row ->
                ((Double)row.get(0)).doubleValue() == 1.0d && ((Double)row.get(1)).doubleValue() == 0.0d).count());
        Float tn = Float.valueOf(predictionAndLabels.toJavaRDD().filter(row ->
                ((Double)row.get(0)).doubleValue() == 0.0d && ((Double)row.get(1)).doubleValue() == 0.0d).count());
        Float fn = Float.valueOf(predictionAndLabels.toJavaRDD().filter(row ->
                ((Double)row.get(0)).doubleValue() == 0.0d && ((Double)row.get(1)).doubleValue() == 1.0d).count());

        double TN = predictionAndLabels.toJavaRDD().filter(row ->
                ((Double)row.get(0)).doubleValue() == 0.0d && ((Double)row.get(1)).doubleValue() == 0.0d).count();
        double FP = predictionAndLabels.toJavaRDD().filter(row ->
                ((Double)row.get(0)).doubleValue() == 1.0d && ((Double)row.get(1)).doubleValue() == 0.0d).count();
        double FN = predictionAndLabels.toJavaRDD().filter(row ->
                ((Double)row.get(0)).doubleValue() == 0.0d && ((Double)row.get(1)).doubleValue() == 1.0d).count();
        double TP = predictionAndLabels.toJavaRDD().filter(row ->
                ((Double)row.get(0)).doubleValue() == 1.0d && ((Double)row.get(1)).doubleValue() == 1.0d).count();
//
//
        System.out.println("tn " + tn);
        System.out.println("fp " + fp);
        System.out.println("fn " + fn);
        System.out.println("tp " + tp);

        System.out.printf("=================== Confusion matrix ==========================\n" +
                        "#############| %-15s                     %-15s\n" +
                        "-------------+-------------------------------------------------\n" +
                        "Predicted = 1| %-15f                     %-15f\n" +
                        "Predicted = 0| %-15f                     %-15f\n" +
                        "===============================================================",
                "Actual = 1",
                "Actual = 0", tp, fp, fn, tn);

        System.out.println();

        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels);
        System.out.println("metrics = " + metrics);
        /*True Positive Rate: Out of all fraud transactions, how  much we predicted correctly. It should be high as possible.*/
        System.out.println("True Positive Rate: " + tp/(tp + fn)); // tp/(tp + fn)

        /*Out of all the genuine transactions(not fraud), how much we predicted wrong(predicted as fraud). It should be low as possible*/
        System.out.println("False Positive Rate: " + fp/(fp + tn));

        System.out.println("Precision: " +  tp/(tp + fp));
        System.out.println("Accuracy: " +  (tp+tn)/(tp + tn + fp + fn));

        /* Save Preprocessing  and Random Forest Model */
        randomForestModel.save(SparkConfig.modelPath);
        preprocessingTransformerModel.save(SparkConfig.preprocessingModelPath);
    }
}
