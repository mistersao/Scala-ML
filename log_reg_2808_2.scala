import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorSizeHint
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.evaluation._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat._
import org.apache.spark.ml.stat._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.getOrCreate
import spark.implicits._

val replacefunc = udf {(x: Double) => if (x == "?") 0.0 else x}

val filepath = "file:///..."

val df = spark.read.format("csv").option("header", value = true).option("delimiter", ";").load(filepath).cache()
df.printSchema()

val columns = df.columns.filter (_ contains "RULE_") ++ df.columns.filter (_ contains "default")

val df0 = df.select (columns.head, columns.tail: _*).drop ($"RULE_166_2").drop($"RULE_167_2").filter ($"default" < 2)

val df1 = df.select($"RULE_1_2",
$"RULE_3_2",
$"RULE_4_2",
$"RULE_5_2",
$"RULE_7_2",
$"RULE_8_2",
$"RULE_9_2",
$"RULE_13_2",
$"RULE_16_2",
$"RULE_18_2",
$"RULE_20_2",
$"RULE_27_2",
$"RULE_31_2",
$"RULE_35_2",
$"RULE_40_2",
$"RULE_49_2",
$"RULE_70_2",
$"RULE_83_2",
$"RULE_93_2",
$"RULE_94_2",
$"RULE_96_2",
$"RULE_97_2",
$"RULE_98_2",
$"RULE_99_2",
$"RULE_101_2",
$"RULE_102_2",
$"RULE_103_2",
$"RULE_104_2",
$"RULE_105_2",
$"RULE_106_2",
$"RULE_107_2",
$"RULE_108_2",
$"RULE_111_2",
$"RULE_113_2",
$"RULE_119_2",
$"RULE_120_2",
$"RULE_134_2",
$"RULE_135_2",
$"RULE_137_2",
$"RULE_139_2",
$"default".cast (IntegerType)
).filter ($"default" < 2)

val cols = df1.columns.filter (_ contains "RULE_")

def castColumnTo (df: DataFrame, columnName: String, targetType: DataType) : DataFrame = {df.withColumn (columnName, df (columnName).cast (targetType))}

def castAllTypedColumnsTo (df: DataFrame, sourceType: DataType, targetType: DataType) : DataFrame = {df.schema.filter (_.dataType == sourceType).foldLeft (df) {(foldedDf, col) => castColumnTo (foldedDf, col.name, targetType)}}

def normalizated = udf ((a: Double) => {
    if (a < 10) (a/10).toDouble
    else if (a >= 10) 1.toDouble
    else a.toDouble 
})

val df2 = castAllTypedColumnsTo (df1, StringType, DoubleType)

val df3 = df2.select(
normalizated($"RULE_1_2"),
normalizated($"RULE_3_2"),
normalizated($"RULE_4_2"),
normalizated($"RULE_5_2"),
normalizated($"RULE_7_2"),
normalizated($"RULE_8_2"),
normalizated($"RULE_9_2"),
normalizated($"RULE_13_2"),
normalizated($"RULE_16_2"),
normalizated($"RULE_18_2"),
normalizated($"RULE_20_2"),
normalizated($"RULE_27_2"),
normalizated($"RULE_31_2"),
normalizated($"RULE_35_2"),
normalizated($"RULE_40_2"),
normalizated($"RULE_49_2"),
normalizated($"RULE_70_2"),
normalizated($"RULE_83_2"),
normalizated($"RULE_93_2"),
normalizated($"RULE_94_2"),
normalizated($"RULE_96_2"),
normalizated($"RULE_97_2"),
normalizated($"RULE_98_2"),
normalizated($"RULE_99_2"),
normalizated($"RULE_101_2"),
normalizated($"RULE_102_2"),
normalizated($"RULE_103_2"),
normalizated($"RULE_104_2"),
normalizated($"RULE_105_2"),
normalizated($"RULE_106_2"),
normalizated($"RULE_107_2"),
normalizated($"RULE_108_2"),
normalizated($"RULE_111_2"),
normalizated($"RULE_113_2"),
normalizated($"RULE_119_2"),
normalizated($"RULE_120_2"),
normalizated($"RULE_134_2"),
normalizated($"RULE_135_2"),
normalizated($"RULE_137_2"),
normalizated($"RULE_139_2"),
$"default"
)

val cols2 = Array(
"RULE_1_2",
"RULE_3_2",
"RULE_4_2",
"RULE_5_2",
"RULE_7_2",
"RULE_8_2",
"RULE_9_2",
"RULE_13_2",
"RULE_16_2",
"RULE_18_2",
"RULE_20_2",
"RULE_27_2",
"RULE_31_2",
"RULE_35_2",
"RULE_40_2",
"RULE_49_2",
"RULE_70_2",
"RULE_83_2",
"RULE_93_2",
"RULE_94_2",
"RULE_96_2",
"RULE_97_2",
"RULE_98_2",
"RULE_99_2",
"RULE_101_2",
"RULE_102_2",
"RULE_103_2",
"RULE_104_2",
"RULE_105_2",
"RULE_106_2",
"RULE_107_2",
"RULE_108_2",
"RULE_111_2",
"RULE_113_2",
"RULE_119_2",
"RULE_120_2",
"RULE_134_2",
"RULE_135_2",
"RULE_137_2",
"RULE_139_2"
)

val old_columns = df3.columns
val columnsList = old_columns.zip(cols2:+"default").map(f=>{col(f._1).as(f._2)})
val df4 = df3.select(columnsList:_*)
df4.printSchema()

//Building Model

val assembler = new VectorAssembler().setInputCols(cols2).setOutputCol ("features")

val featureDf = assembler.transform (df4)
featureDf.printSchema ()

val indexer = new StringIndexer ().setInputCol ("default").setOutputCol ("label")
val labelDf = indexer.fit (featureDf).transform (featureDf)
labelDf.printSchema ()

val seed = 12345
val Array(trainingData, testData) = labelDf.randomSplit (Array (0.7, 0.3), seed)


val logisticRegression = new LogisticRegression ().setMaxIter(10000
).setLabelCol("label"
).setFeaturesCol("features")

val logisticRegressionModel = logisticRegression.fit (trainingData)

val predictionDf = logisticRegressionModel.transform (testData)
predictionDf.show (10)

val evaluator = new BinaryClassificationEvaluator ().setLabelCol ("label").setRawPredictionCol ("rawPrediction").setMetricName ("areaUnderROC")

val accuracy1 = evaluator.evaluate (predictionDf)
println ("Gini:" +" "+ (accuracy1-0.5)*2)

//Optional searching fo model with less regressors

val df3_chosen = df3.select(
"UDF(RULE_13_2)",
"UDF(RULE_113_2)",
"UDF(RULE_137_2)",
"UDF(RULE_134_2)",
"UDF(RULE_94_2)",
"UDF(RULE_31_2)",
"UDF(RULE_135_2)",
"UDF(RULE_97_2)",
"UDF(RULE_93_2)",
"UDF(RULE_101_2)",
"default"
)

val cols_chosen = Array(
"RULE_13_2",
"RULE_113_2",
"RULE_137_2",
"RULE_134_2",
"RULE_94_2",
"RULE_31_2",
"RULE_135_2",
"RULE_97_2",
"RULE_93_2",
"RULE_101_2"
)

val old_columns_chosen = df3_chosen.columns
val columnsList_chosen = old_columns_chosen.zip(cols_chosen:+"default").map(f=>{col(f._1).as(f._2)})
val df4_chosen = df3_chosen.select(columnsList_chosen:_*)
df4_chosen.printSchema()

val assembler_chosen = new VectorAssembler().setInputCols(cols_chosen).setOutputCol ("features")

val featureDf_chosen = assembler_chosen.transform (df4_chosen)
featureDf_chosen.printSchema ()

val indexer_chosen = new StringIndexer ().setInputCol ("default").setOutputCol ("label")
val labelDf_chosen = indexer_chosen.fit (featureDf_chosen).transform (featureDf_chosen)
labelDf_chosen.printSchema ()

val seed = 12345
val Array(trainingData_chosen, testData_chosen) = labelDf_chosen.randomSplit (Array (0.7, 0.3), seed)


val logisticRegression_chosen = new LogisticRegression ().setMaxIter(10000
).setLabelCol("label"
).setFeaturesCol("features")

val logisticRegressionModel_chosen = logisticRegression_chosen.fit (trainingData_chosen)

val predictionDf_chosen = logisticRegressionModel_chosen.transform (testData_chosen)
val predictionDf_chosen_total = logisticRegressionModel_chosen.transform (featureDf_chosen)

predictionDf_chosen.show (10)

val df5_chosen = logisticRegressionModel_chosen.transform (df4_chosen)

val evaluator_chosen = new BinaryClassificationEvaluator ().setLabelCol ("label").setRawPredictionCol ("rawPrediction").setMetricName ("areaUnderROC")

val accuracy1_chosen = evaluator_chosen.evaluate (predictionDf_chosen)
println ("Gini:" +" "+ (accuracy1_chosen-0.5)*2)

//Coefficients

logisticRegressionModel.write.overwrite().save("B:/...")

logisticRegressionModel_chosen.write.overwrite().save("B:/...")

logisticRegressionModel.coefficients.toArray.foreach (println)

val coeffs = cols2.zip(logisticRegressionModel.coefficients.toArray)

val coeffs_csv = sc.parallelize(coeffs).toDF("Rule","Coefficient")

coeffs_csv.coalesce(1).write.option("header", "true").option("sep", ";").mode("append").csv("file:///...")

val intercept = sc.parallelize(Array("Intecept") ++ Array(logisticRegressionModel.intercept.toString)).toDF("#","Intercept")



val coeffs_chosen = cols_chosen.zip(logisticRegressionModel_chosen.coefficients.toArray)

val coeffs_chosen_csv = sc.parallelize(coeffs_chosen).toDF("Rule","Coefficient")

coeffs_chosen_csv.coalesce(1).write.option("header", "true").option("sep", ";").mode("append").csv("file:///...")

val intercept_chosen = sc.parallelize(Array("Intecept") ++ Array(logisticRegressionModel_chosen.intercept.toString)).toDF("#","Intercept")


/*
//Summary

val trainingSummary = logisticRegressionModel.summary
val objectiveHistory = trainingSummary.objectiveHistory
println("objectiveHistory:")
objectiveHistory.foreach(println)

println("False positive rate by label:")
trainingSummary.falsePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
  println(s"label $label: $rate")
}

println("True positive rate by label:")
trainingSummary.truePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
  println(s"label $label: $rate")
}

println("Precision by label:")
trainingSummary.precisionByLabel.zipWithIndex.foreach { case (prec, label) =>
  println(s"label $label: $prec")
}

println("Recall by label:")
trainingSummary.recallByLabel.zipWithIndex.foreach { case (rec, label) =>
  println(s"label $label: $rec")
}


println("F-measure by label:")
trainingSummary.fMeasureByLabel.zipWithIndex.foreach { case (f, label) =>
  println(s"label $label: $f")
}

val accuracy = trainingSummary.accuracy
val falsePositiveRate = trainingSummary.weightedFalsePositiveRate
val truePositiveRate = trainingSummary.weightedTruePositiveRate
val fMeasure = trainingSummary.weightedFMeasure
val precision = trainingSummary.weightedPrecision
val recall = trainingSummary.weightedRecall
println(s"Accuracy: $accuracy\nFPR: $falsePositiveRate\nTPR: $truePositiveRate\n" +
s"F-measure: $fMeasure\nPrecision: $precision\nRecall: $recall")
*/

//Validation 201904

val filepath2 = "file:///..."

val log_reg_2808_2 = LogisticRegressionModel.load("B:/...")

val gf = spark.read.format("csv").option("header", value = true).option("delimiter", ";").load(filepath2).filter($"default" < 2).cache()

val gf1 = castColumnTo(castAllTypedColumnsTo (gf, StringType, DoubleType),"default",IntegerType)

def castColumnTo (df: DataFrame, columnName: String, targetType: DataType) : DataFrame = {df.withColumn (columnName, df (columnName).cast (targetType))}

val gf2 = gf1.select(
normalizated($"RULE_1_2"),
normalizated($"RULE_3_2"),
normalizated($"RULE_4_2"),
normalizated($"RULE_5_2"),
normalizated($"RULE_7_2"),
normalizated($"RULE_8_2"),
normalizated($"RULE_9_2"),
normalizated($"RULE_13_2"),
normalizated($"RULE_16_2"),
normalizated($"RULE_18_2"),
normalizated($"RULE_20_2"),
normalizated($"RULE_27_2"),
normalizated($"RULE_31_2"),
normalizated($"RULE_35_2"),
normalizated($"RULE_40_2"),
normalizated($"RULE_49_2"),
normalizated($"RULE_70_2"),
normalizated($"RULE_83_2"),
normalizated($"RULE_93_2"),
normalizated($"RULE_94_2"),
normalizated($"RULE_96_2"),
normalizated($"RULE_97_2"),
normalizated($"RULE_98_2"),
normalizated($"RULE_99_2"),
normalizated($"RULE_101_2"),
normalizated($"RULE_102_2"),
normalizated($"RULE_103_2"),
normalizated($"RULE_104_2"),
normalizated($"RULE_105_2"),
normalizated($"RULE_106_2"),
normalizated($"RULE_107_2"),
normalizated($"RULE_108_2"),
normalizated($"RULE_111_2"),
normalizated($"RULE_113_2"),
normalizated($"RULE_119_2"),
normalizated($"RULE_120_2"),
normalizated($"RULE_134_2"),
normalizated($"RULE_135_2"),
normalizated($"RULE_137_2"),
normalizated($"RULE_139_2"),
$"default"
)

val gf3_chosen = gf2.select(
"UDF(RULE_13_2)",
"UDF(RULE_113_2)",
"UDF(RULE_137_2)",
"UDF(RULE_134_2)",
"UDF(RULE_94_2)",
"UDF(RULE_31_2)",
"UDF(RULE_135_2)",
"UDF(RULE_97_2)",
"UDF(RULE_93_2)",
"UDF(RULE_101_2)",
"default"
)


val gf4 = gf2.select(columnsList_chosen:_*)
gf4.printSchema()

val gf5 = assembler_chosen.transform(gf4)

val gf6 = logisticRegressionModel_chosen.transform(gf5)
//gf3.show()

val evaluator2 = new BinaryClassificationEvaluator ().setLabelCol ("default").setRawPredictionCol ("rawPrediction").setMetricName ("areaUnderROC")

val accuracy2_chosen = evaluator_chosen.evaluate (gf6.withColumnRenamed("default","label"))
println ("Gini:" +" "+ (accuracy2_chosen-0.5)*2)

//Validation 201910

val filepath3 = "file:///..."

val bf = spark.read.format("csv").option("header", value = true).option("delimiter", ";").load(filepath3).filter($"default" < 2).cache()

val bf1 = castColumnTo(castAllTypedColumnsTo (bf, StringType, DoubleType),"default",IntegerType)

def castColumnTo (df: DataFrame, columnName: String, targetType: DataType) : DataFrame = {df.withColumn (columnName, df (columnName).cast (targetType))}

val bf2 = bf1.select(
normalizated($"RULE_1_2"),
normalizated($"RULE_3_2"),
normalizated($"RULE_4_2"),
normalizated($"RULE_5_2"),
normalizated($"RULE_7_2"),
normalizated($"RULE_8_2"),
normalizated($"RULE_9_2"),
normalizated($"RULE_13_2"),
normalizated($"RULE_16_2"),
normalizated($"RULE_18_2"),
normalizated($"RULE_20_2"),
normalizated($"RULE_27_2"),
normalizated($"RULE_31_2"),
normalizated($"RULE_35_2"),
normalizated($"RULE_40_2"),
normalizated($"RULE_49_2"),
normalizated($"RULE_70_2"),
normalizated($"RULE_83_2"),
normalizated($"RULE_93_2"),
normalizated($"RULE_94_2"),
normalizated($"RULE_96_2"),
normalizated($"RULE_97_2"),
normalizated($"RULE_98_2"),
normalizated($"RULE_99_2"),
normalizated($"RULE_101_2"),
normalizated($"RULE_102_2"),
normalizated($"RULE_103_2"),
normalizated($"RULE_104_2"),
normalizated($"RULE_105_2"),
normalizated($"RULE_106_2"),
normalizated($"RULE_107_2"),
normalizated($"RULE_108_2"),
normalizated($"RULE_111_2"),
normalizated($"RULE_113_2"),
normalizated($"RULE_119_2"),
normalizated($"RULE_120_2"),
normalizated($"RULE_134_2"),
normalizated($"RULE_135_2"),
normalizated($"RULE_137_2"),
normalizated($"RULE_139_2"),
$"default"
)

val bf3 = bf2.select(columnsList:_*)
bf3.printSchema()

val bf4 = assembler.transform(bf3)

val bf5 = log_reg_2808_2.transform(bf4)
//gf3.show()

val evaluator3 = new BinaryClassificationEvaluator ().setLabelCol ("default").setRawPredictionCol ("rawPrediction").setMetricName ("areaUnderROC")

val accuracy3 = evaluator3.evaluate (bf5)
println ("Gini:" +" "+ (accuracy3-0.5)*2)

val bf3_chosen = bf2.select(
"UDF(RULE_13_2)",
"UDF(RULE_113_2)",
"UDF(RULE_137_2)",
"UDF(RULE_134_2)",
"UDF(RULE_94_2)",
"UDF(RULE_31_2)",
"UDF(RULE_135_2)",
"UDF(RULE_97_2)",
"UDF(RULE_93_2)",
"UDF(RULE_101_2)",
"default"
)


val bf4_chosen = bf3_chosen.select(columnsList_chosen:_*)

val bf5_chosen = assembler_chosen.transform(bf4_chosen)

val bf6_chosen = logisticRegressionModel_chosen.transform(bf5_chosen)
//gf3.show()

val evaluator2 = new BinaryClassificationEvaluator ().setLabelCol ("default").setRawPredictionCol ("rawPrediction").setMetricName ("areaUnderROC")

val accuracy3_chosen = evaluator_chosen.evaluate (bf6_chosen.withColumnRenamed("default","label"))
println ("Gini:" +" "+ (accuracy3_chosen-0.5)*2)


//Statistics for Bad Rate
val df_br_stats = predictionDf_chosen_total.groupBy("probability").agg(
    count("default").as("total"),
    sum("default").as("bads")
    ).withColumn("badrate",$"bads"/$"total"
)

val gf_br_stats = gf6.groupBy("probability").agg(
    count("default").as("total"),
    sum("default").as("bads")
    ).withColumn("badrate",$"bads"/$"total"
)

val bf_br_stats = bf6_chosen.groupBy("probability").agg(
    count("default").as("total"),
    sum("default").as("bads")
    ).withColumn("badrate",$"bads"/$"total"
)


//Correlation Matrix

def corr(data: DataFrame, outputDir: String): Unit = {
    val cols = data.columns
    val ss_corr = new VectorAssembler().setInputCols(cols).setOutputCol("features").transform(data.select(cols.map(c => col(c).cast("double")): _*))
    val Row(cr: Matrix) = org.apache.spark.ml.stat.Correlation.corr(ss_corr, "features").head
    val rows = cr.toArray.grouped(cr.numRows).toSeq.transpose
    sc.parallelize(rows).map(_.toArray.map(_.toString).mkString(";")).repartition(1).saveAsTextFile("file:///" + outputDir)
}

val outputDir = "..."

corr(df0,outputDir)
