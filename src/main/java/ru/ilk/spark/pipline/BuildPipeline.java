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
//        String[] scaleFeatureColumns = {};
//        Stream<PipelineStage> preprocessingStages = Arrays.stream(schema.fields()).filter(field-> columns.contains(field.name())).flatMap(
//                field -> {
//                    //Empty PipelineStage
//                    PipelineStage[] arrPipelineStage = {};
//                    // Using Arrays.stream() to convert array into Stream
//                    Stream<PipelineStage> streamPipelineStage = Arrays.stream(arrPipelineStage);
//
//                    if (field.dataType() == StringType ) {
//                        StringIndexer stringIndexer = new StringIndexer();
//                        stringIndexer.setInputCol(field.name()).setOutputCol(field.name().toString() + "_indexed");
//                        arrPipelineStage = append(arrPipelineStage, stringIndexer);
//                        System.out.println("arrPipelineStage = " + arrPipelineStage);
//                    }
////                    else if (field.dataType() == DoubleType || field.dataType() == IntegerType || field.dataType() == FloatType) {
////                        VectorAssembler numericAssembler = new VectorAssembler();
////                        scaleFeatureColumns = append(scaleFeatureColumns, field.name());
////                    }
//                    return Arrays.stream(arrPipelineStage);
////                    return streamPipelineStage;
//                });
//
//        System.out.println("preprocessingStages.length = " + preprocessingStages.toArray().length);

//        Stream<String> scaleFeatureColumns = Arrays.stream(schema.fields()).filter(field-> columns.contains(field.name())).flatMap(
//                field -> {
//                    //Empty PipelineStage
//                    String[] arrScaleFeatureColumns= {};
//                    if (field.dataType() == DoubleType || field.dataType() == IntegerType || field.dataType() == FloatType) {
//                        VectorAssembler numericAssembler = new VectorAssembler();
//                        arrScaleFeatureColumns = append(arrScaleFeatureColumns, field.name());
//                    }
//                    return Arrays.stream(arrScaleFeatureColumns);
//                });


        Object[] preprocessingStages = Arrays.stream(schema.fields()).filter(field-> columns.contains(field.name())).flatMap(
                field -> {
                    //Empty PipelineStage
                    PipelineStage[] arrPipelineStage = {};
                    // Using Arrays.stream() to convert array into Stream
                    Stream<PipelineStage> streamPipelineStage = Arrays.stream(arrPipelineStage);

                    if (field.dataType() == StringType ) {
                        StringIndexer stringIndexer = new StringIndexer();
                        stringIndexer.setInputCol(field.name()).setOutputCol(field.name().toString() + "_indexed");
                        arrPipelineStage = append(arrPipelineStage, stringIndexer);
                        System.out.println("arrPipelineStage = " + arrPipelineStage);
                    }
                    return Arrays.stream(arrPipelineStage);
                }).toArray();

        PipelineStage[] preprocessingStagesArr = Arrays.stream(preprocessingStages).toArray(PipelineStage[]::new);
        System.out.println("preprocessingStagesArr = " + preprocessingStagesArr);
        System.out.println("preprocessingStagesArr.length = " + preprocessingStagesArr.length);

//        System.out.println("preprocessingStages.length = " + preprocessingStages.toArray().length);




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
        System.out.println("scaleFeatureColumnsArr = " + scaleFeatureColumnsArr[0]);


        System.out.println("scaleFeatureColumns.length  = " + scaleFeatureColumns.length);
        System.out.println("@@@@@@@@@scaleFeatureColumns = " + scaleFeatureColumns[0]);

//        System.out.println("scaleFeatureColumns.length = " + scaleFeatureColumns.toArray().length);
//        String[] arrScaleFeatureColumns = scaleFeatureColumns.toArray(size -> new String[size]);

        VectorAssembler numericAssembler = new VectorAssembler();
        numericAssembler.setInputCols(scaleFeatureColumnsArr).setOutputCol("numericRawFeatures");
        VectorSlicer slicer = new VectorSlicer().setInputCol("numericRawFeatures").setOutputCol("slicedfeatures").setNames(scaleFeatureColumnsArr);
        StandardScaler scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("scaledfeatures").setWithStd(true).setWithMean(true);

        VectorAssembler vectorAssembler = new VectorAssembler();
        featureColumns = append(featureColumns, "scaledfeatures");
        vectorAssembler.setInputCols(featureColumns).setOutputCol("features");
//        PipelineStage[] arrPipelineStage1 = (PipelineStage[]) preprocessingStages.toArray();
        preprocessingStagesArr = append(preprocessingStagesArr, numericAssembler);
        preprocessingStagesArr = append(preprocessingStagesArr, slicer);
        preprocessingStagesArr = append(preprocessingStagesArr, scaler);
        preprocessingStagesArr = append(preprocessingStagesArr, vectorAssembler);
        return preprocessingStagesArr;
//
//        System.out.println("arrPipelineStage1 " + arrPipelineStage1);



















//        retrun arrPipelineStage1;

//        VectorAssembler numericAssembler = new VectorAssembler();
//        numericAssembler.setInputCols(scaleFeatureColumns).setOutputCol("numericRawFeatures");
//        VectorSlicer slicer = new VectorSlicer().setInputCol("numericRawFeatures").setOutputCol("slicedfeatures").setNames(scaleFeatureColumns);
//        StandardScaler scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("scaledfeatures").setWithStd(true).setWithMean(true);
//
//        VectorAssembler vectorAssembler = new VectorAssembler();
//        append(featureColumns, "scaledfeatures");
//        vectorAssembler.setInputCols(featureColumns).setOutputCol("features");
//        System.out.println("arrPipelineStage.length = " + arrPipelineStage.length);
//        append(preprocessingStages, numericAssembler);
//        append(preprocessingStages, slicer);
//        append(preprocessingStages, scaler);
//        append(preprocessingStages, vectorAssembler);
//        return preprocessingStages;



//        PipelineStage [] arrPipelineStage1 = {};
//        return arrPipelineStage1;
    }

    private static <T> T[] append(T[] arr, T element) {
        return ArrayUtils.add(arr, element);
    }
}



//        ArrayList<PipelineStage> preprocessingStages = (ArrayList<PipelineStage>) Arrays.stream(schema
//        .fields()).filter(
//                field -> columns.contains(field.name())
//
//        ).flatMap(
//                field -> {
//                    df.append(df);
//                    ArrayList<PipelineStage> preprocessingStages1 = new ArrayList<PipelineStage>();
//                    if (field.dataType() == StringType) {
//                        StringIndexer stringIndexer = new StringIndexer();
//                        stringIndexer.setInputCol(field.name()).setOutputCol(field.name() + "_indexed");
//                        preprocessingStages1.add(stringIndexer);
//                        System.out.println("scaleFeatureColumns === " + preprocessingStages1.size());
//                    } else if (field.dataType() == DoubleType || field.dataType() == IntegerType || field.dataType() == FloatType) {
//                        VectorAssembler numericAssembler = new VectorAssembler();
//                        scaleFeatureColumns.add(field.name());
//                    }
//
//                    return preprocessingStages1.stream();
//                }
//        );
//        System.out.println("preprocessingStages" + preprocessingStages.size());
//        System.out.println("preprocessingStages" + preprocessingStages.size());

//        Stream<PipelineStage> preprocessingStagesStream = Arrays.stream(schema.fields()).filter(field ->
//                columns.contains(field.name())).flatMap(
//                field -> {
//                    ArrayList<PipelineStage> listPipelineStage = new ArrayList<PipelineStage>();
//                    if (field.dataType() == StringType) {
//                        StringIndexer stringIndexer = new StringIndexer();
//                        stringIndexer.setInputCol(field.name()).setOutputCol(field.name().toString() + "_indexed");
//                        System.out.println("stringIndexer = " + stringIndexer);
//                        listPipelineStage.add(stringIndexer);
//                        System.out.println("dsd.size() " + listPipelineStage.size());
//                    } else if (field.dataType() == DoubleType || field.dataType() == IntegerType || field.dataType() == FloatType) {
//                        VectorAssembler numericAssembler = new VectorAssembler();
////                        append(scaleFeatureColumns, field.name());
//                        scaleFeatureColumns.add(field.name());
//                        System.out.println("scaleFeatureColumns.length " + scaleFeatureColumns.size());
//
//
//                    }
////              return null;
//                    System.out.println("arrPipelineStage = " + arrPipelineStage.length);
//                    return Arrays.stream(arrPipelineStage);
//                }
//        );
//        System.out.println("preprocessingStagesStream " + preprocessingStagesStream);
//        PipelineStage[] preprocessingStages = preprocessingStagesStream.toArray(value -> new PipelineStage[value]);
//        System.out.println("myNewArray2 = " + preprocessingStages.length);
//
//        VectorAssembler numericAssembler = new VectorAssembler();
//        numericAssembler.setInputCols(scaleFeatureColumns).setOutputCol("numericRawFeatures");
//        VectorSlicer slicer = new VectorSlicer().setInputCol("numericRawFeatures").setOutputCol("slicedfeatures").setNames(scaleFeatureColumns);
//        StandardScaler scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("scaledfeatures").setWithStd(true).setWithMean(true);
//
//        VectorAssembler vectorAssembler = new VectorAssembler();
//        append(featureColumns, "scaledfeatures");
//        vectorAssembler.setInputCols(featureColumns).setOutputCol("features");
//
//        append(preprocessingStages, numericAssembler);
//        append(preprocessingStages, slicer);
//        append(preprocessingStages, scaler);
//        append(preprocessingStages, vectorAssembler);
//        PipelineStage [] sdds = {};
//        return sdds;
//    }
//    public static PipelineStage[] createFeaturePipeline(StructType schema, List<String> columns) {
//        String[] featureColumns = {};
//        String[] scaleFeatureColumns = {};
//        PipelineStage[] preprocessingStages = (PipelineStage[]) Arrays.stream(schema.fields()).filter(field-> columns.contains(field.name())).flatMap(
//                field -> {
//                    //Empty PipelineStage
//                    PipelineStage[] arrPipelineStage = {};
//                    // Using Arrays.stream() to convert array into Stream
//                    Stream<PipelineStage> streamPipelineStage = Arrays.stream(arrPipelineStage);
//                    if (field.dataType()  == StringType) {
//                        StringIndexer stringIndexer = new StringIndexer();
//                        stringIndexer.setInputCol(field.name()).setOutputCol(field.name().toString() + "_indexed");
//                        append(arrPipelineStage, stringIndexer);
//                        return  Arrays.stream(arrPipelineStage);
//                    }
//                    else if (field.dataType()  ==  DoubleType) {
//                        VectorAssembler numericAssembler = new VectorAssembler();
//                        append(scaleFeatureColumns, field.name());
//                        return streamPipelineStage;
//                    } else {
//                        return streamPipelineStage;
//                    }
//
//                }).toArray();
//
//        VectorAssembler numericAssembler = new VectorAssembler();
//        numericAssembler.setInputCols(scaleFeatureColumns).setOutputCol("numericRawFeatures");
//        VectorSlicer slicer = new VectorSlicer().setInputCol("numericRawFeatures").setOutputCol("slicedfeatures").setNames(scaleFeatureColumns);
//        StandardScaler scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("scaledfeatures").setWithStd(true).setWithMean(true);
//
//        VectorAssembler vectorAssembler = new VectorAssembler();
//        append(featureColumns, "scaledfeatures");
//        vectorAssembler.setInputCols(featureColumns).setOutputCol("features");
//
//        append(preprocessingStages, numericAssembler);
//        append(preprocessingStages, slicer);
//        append(preprocessingStages, scaler);
//        append(preprocessingStages, vectorAssembler);
//        return preprocessingStages;
//    }


//    public static void createFeaturePipeline(StructType schema, List<String> columns) {
//        ArrayList<String> featureColumns = new ArrayList<String>();
//        ArrayList<String> scaleFeatureColumns = new ArrayList<String>();
//        ArrayList<PipelineStage>  preprocessingStages = new ArrayList<PipelineStage>();
////        Stream<Object> dfd = Arrays.stream(schema.fields()).filter(field -> columns.contains(field.name())).flatMap(field -> {
////            switch (field.dataType()) {
////                case StructType s -> {
////                    StringIndexer stringIndexer = new StringIndexer();
////                    stringIndexer.setInputCol(field.name()).setOutputCol(field.name().toString() + "_indexed");
////                    preprocessingStages.add(stringIndexer);
////                }
////                case NumericType n -> {
////                    VectorAssembler numericAssembler = new VectorAssembler();
////                    //featureColumns += (field.name)
////                    scaleFeatureColumns.add(field.name());
//////                    preprocessingStages =  new ArrayList<PipelineStage>();
////                }
////                default -> {
//////                    new ArrayList<PipelineStage>();
////                }
////            }
////            return null;
////        });
//
//
////        VectorAssembler numericAssembler = new VectorAssembler();
////        numericAssembler.setInputCols((String[]) scaleFeatureColumns.toArray()).setOutputCol("numericRawFeatures");
//        VectorSlicer slicer = new VectorSlicer().setInputCol("numericRawFeatures").setOutputCol("slicedfeatures").setNames((String[]) scaleFeatureColumns.toArray());
//        StandardScaler scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("scaledfeatures").setWithStd(true).setWithMean(true);
//
//        VectorAssembler vectorAssembler = new VectorAssembler();
//        featureColumns.add("scaledfeatures");
//        vectorAssembler.setInputCols((String[]) featureColumns.toArray()).setOutputCol("features");
////        preprocessingStages.add(numericAssembler);
//        preprocessingStages.add(slicer);
//        preprocessingStages.add(scaler);
//        preprocessingStages.add(vectorAssembler);
////        return (PipelineStage[]) preprocessingStages.toArray();
//        System.out.println(preprocessingStages + "preprocessingStages ");
//    }

//    private static <T> T[] append(T[] arr, T element) {
//        return ArrayUtils.add(arr, element);
//    }


