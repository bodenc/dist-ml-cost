package de.tuberlin.dima.mlbench.spark

import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.SparkSession




object gbtParameterTune_simple {
  def main(args: Array[String]) {

    implicit class BestParamMapCrossValidatorModel(cvModel: CrossValidatorModel) {
      def bestEstimatorParamMap: ParamMap = {
        cvModel.getEstimatorParamMaps
          .zip(cvModel.avgMetrics)
          .maxBy(_._2)
          ._1
      }
    }

    val options = args.map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => opt -> v
        case Array(opt) => opt -> "true"
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }.toMap

    val inputPath = options.get("inputPath").get
    val outputPath = options.get("outputPath").get
    val testPath = options.get("testPath").get


    val spark = SparkSession
      .builder()
      .appName("GBT Parameter Tuning and CV - simplified - DataSet API")
      .config("spark.driver.memory", "25g")
      .config("spark.driver.maxResultSize", "10g")
      //.master("local")
      .getOrCreate()

    val data = spark.read.format("libsvm")
      .option("numFeatures", "39")
      .load(inputPath)

    // Train a GBT model.
    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(16,32,64))
      .addGrid(gbt.maxDepth, Array(3,8,14))
      .build()

    print ("Starting Crossvalidation")

    val cv = new CrossValidator()
      .setEstimator(gbt)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)

    val cvModel = cv.fit(data)


    // extract the optimal hyperparameters
    var outString = ""
    println("cvModel.bestEstimatorParamMap => " + cvModel.bestEstimatorParamMap)
    outString +="cvModel.bestEstimatorParamMap => " + cvModel.bestEstimatorParamMap + "\n"


    val myDF = spark.sqlContext.sparkContext.parallelize(outString)
    myDF.saveAsTextFile(outputPath)
    spark.stop()

  }
}
