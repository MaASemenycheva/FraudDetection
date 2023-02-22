package ru.ilk.spark.pipline;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.sql.types.StructType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.spark.sql.types.DataTypes.*;

public class BuildPipeline {
    private static Logger logger = Logger.getLogger(BuildPipeline.class.getName());

    public static PipelineStage[] createFeaturePipeline(StructType schema, List<String> columns) {
        String[] featureColumns = {};
        Object[] preprocessingStages = Arrays.stream(schema.fields()).filter(field-> columns.contains(field.name())).flatMap(
                field -> {
                    //Empty PipelineStage
                    PipelineStage[] arrPipelineStage = {};

                    if (field.dataType() == StringType ) {
                        StringIndexer stringIndexer = new StringIndexer();
                        stringIndexer.setInputCol(field.name()).setOutputCol(field.name().toString() + "_indexed");
                        arrPipelineStage = append(arrPipelineStage, stringIndexer);
                    }
                    return Arrays.stream(arrPipelineStage);
                }).toArray();

        PipelineStage[] preprocessingStagesArr = Arrays.stream(preprocessingStages).toArray(PipelineStage[]::new);

        Object[] scaleFeatureColumns = Arrays.stream(schema.fields()).filter(field -> columns.contains(field.name())).flatMap(
                field -> {
                    String[] arrScaleFeatureColumns = {};
                    if (field.dataType() == DoubleType || field.dataType() == IntegerType || field.dataType() == FloatType) {
                        VectorAssembler numericAssembler = new VectorAssembler();
                        arrScaleFeatureColumns = append(arrScaleFeatureColumns, field.name());
                    }
                    return Arrays.stream(arrScaleFeatureColumns);
                }).toArray();

        String[] scaleFeatureColumnsArr = Arrays.stream(scaleFeatureColumns).map(Object::toString).toArray(String[]::new);

        VectorAssembler numericAssembler = new VectorAssembler();
        numericAssembler.setInputCols(scaleFeatureColumnsArr).setOutputCol("numericRawFeatures");
        VectorSlicer slicer = new VectorSlicer().setInputCol("numericRawFeatures").setOutputCol("slicedfeatures").setNames(scaleFeatureColumnsArr);
        StandardScaler scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("scaledfeatures").setWithStd(true).setWithMean(true);

        VectorAssembler vectorAssembler = new VectorAssembler();
        featureColumns = append(featureColumns, "scaledfeatures");
        vectorAssembler.setInputCols(featureColumns).setOutputCol("features");
        preprocessingStagesArr = append(preprocessingStagesArr, numericAssembler);
        preprocessingStagesArr = append(preprocessingStagesArr, slicer);
        preprocessingStagesArr = append(preprocessingStagesArr, scaler);
        preprocessingStagesArr = append(preprocessingStagesArr, vectorAssembler);
        return preprocessingStagesArr;
    }

    private static <T> T[] append(T[] arr, T element) {
        return ArrayUtils.add(arr, element);
    }
}


