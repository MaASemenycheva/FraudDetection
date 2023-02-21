package ru.ilk.spark;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

import static org.apache.spark.sql.types.DataTypes.*;

public class DataBalancing {

    private static Logger logger = Logger.getLogger(DataBalancing.class.getName());
    /*
    There will be more non-fruad transaction then fraund transaction. So non-fraud transactions must be balanced
    Kmeans Algorithm is used to balance non fraud transatiion.
    No. of non-fruad transactions  are balanced(reduced) to no. of fraud transaction
     */
    public static  Dataset<Row> createBalancedDataframe( Dataset<Row> df,
                                                         Integer reductionCount,
                                                         SparkSession sparkSession
    ) {
        KMeans kMeans = new KMeans().setK(reductionCount).setMaxIter(30);
        KMeansModel kMeansModel = kMeans.fit(df);
        StructType schema = createStructType(new StructField[]{
                createStructField("features", new VectorUDT(), false),
                createStructField("is_fraud", DoubleType, false)
        });
        Vector[] kMeansModelVectors = kMeansModel.clusterCenters();
        Row[] dataRow= {};
        double[][] input = new double[][]{};
        Double[] fdf = {};
        for (int i = 0; i < kMeansModelVectors.length; i++) {
//            fdf = (Double[]) append(fdf , (double[]) kMeansModelVectors[i].toArray());
            dataRow = append(dataRow, RowFactory.create(
//                    Vectors.dense(kMeansModelVectors[i].toArray())
                    Vectors.dense(kMeansModelVectors[i].toArray())

                    , 0.0));}


        Dataset<Row> dataset = sparkSession.createDataFrame(Arrays.asList(dataRow), schema);

//        Dataset<Row> dataset1 = sparkSession.createDataFrame(
//                Arrays.asList(RowFactory.create(org.apache.spark.ml.linalg.Vectors.dense(input[0]), 0.0)),
//                new StructType(new StructField[]{
//                        new StructField("vec", (new org.apache.spark.ml.linalg.VectorUDT()), false, Metadata.empty()),
//                        createStructField("is_fraud", DoubleType, false)
//                }));
//        dataset1.show();
        dataset.show();
        return dataset;
    }


    private static <T> T[] append(T[] arr, T element) {
        return ArrayUtils.add(arr, element);
    }
}