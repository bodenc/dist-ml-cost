package de.tuberlin.dima.mlbench.spark

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, LabeledPoint, StringIndexer, VectorIndexer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SparkSession


object lrParameterTune_simple {
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
    val numFeatures = options.get("numFeatures").get.toInt
    val numIters = options.get("numIterations").get.toInt



    val spark = SparkSession
      .builder()
      .appName("CV / Param Tune - Logistic Regression - DataSet API")
      .config("spark.driver.memory", "25g")
      .config("spark.driver.maxResultSize", "25g")
      .config("spark.kryoserializer.buffer.max", "1g")
      //.master("local")
      .getOrCreate()


    val data = spark.read.format("libsvm")
      .option("numFeatures", numFeatures)
      .load(inputPath)

    val t0 = System.nanoTime()
    val lr = new LogisticRegression()
      .setMaxIter(numIters)
      .setLabelCol("label")
      .setFeaturesCol("features")

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0, 0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 1))
      .build()

    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)


    val model = cv.fit(data)
    val t1 = System.nanoTime()
    println("Training Time : " + (t1 - t0) + "ns")

    var outString = ""
    println("cvModel.bestEstimatorParamMap => " + model.bestEstimatorParamMap)
    outString +="cvModel.bestEstimatorParamMap => " + model.bestEstimatorParamMap + "\n"


    val myDF = spark.sqlContext.sparkContext.parallelize(outString)
    myDF.saveAsTextFile(outputPath)
    spark.stop()
  }
}