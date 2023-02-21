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
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
                new StructField("features", (new org.apache.spark.ml.linalg.VectorUDT()), false, Metadata.empty()),
                createStructField("is_fraud", DoubleType, false)
        });
        Vector[] kMeansModelVectors = kMeansModel.clusterCenters();
        Row[] dataRow= {};
        for (int i = 0; i < kMeansModelVectors.length; i++) {
            dataRow = append(dataRow, RowFactory.create(
                    org.apache.spark.ml.linalg.Vectors.dense(kMeansModelVectors[i].toArray())
                    , 0.0));
        }
        Dataset<Row> dataset = sparkSession.createDataFrame(Arrays.asList(dataRow), schema);
        dataset.show();
        return dataset;
    }

    private static <T> T[] append(T[] arr, T element) {
        return ArrayUtils.add(arr, element);
    }
}
