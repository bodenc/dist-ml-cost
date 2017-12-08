package de.tuberlin.dima.mlbench.spark

import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object trainGBTDataSet {



  def main(args: Array[String]) {

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
      .appName("Gradient Boosting Tree - DataSet API - No Pipeline")
      .config("spark.driver.memory", "25g")
      .config("spark.driver.maxResultSize", "25g")
      .config("spark.kryoserializer.buffer.max", "1g")
      //.master("local")
      .getOrCreate()

    val data = spark.read.format("libsvm")
      .option("numFeatures", numFeatures)
      .load(inputPath)

    print("Training GBT Model for " + numIters + " iterations ..." + "\n")

    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMinInstancesPerNode(3)
      .setMaxDepth(10)
      .setMaxBins(64)
      .setMaxIter(numIters)
//      .setCheckpointInterval(50).setCacheNodeIds(true) // needed to avoid stack overflow after ~120 iter
    val model = gbt.fit(data)

    if(!testPath.equals("noTest")) {

      val test = spark.read.format("libsvm")
        .option("numFeatures", numFeatures)
        .load(testPath)

      // Instantiate metrics object
      val getPOne = udf((v: org.apache.spark.ml.linalg.DenseVector) => v(1))
      val myProbablities = model.transform(test).select(getPOne(col("probability")), col("label")).rdd.map(value => (value.getDouble(0), value.getDouble(1)))
      val metrics = new BinaryClassificationMetrics(myProbablities)
      val auPRC = metrics.areaUnderPR
      val auROC = metrics.areaUnderROC

      print("######################################################################## \n")
      print("numIterations, numFeatures, auROC, auPRC \n")
      print(numIters + "," + numFeatures + "," +  auROC + "," + auPRC  + "\n")
      print("######################################################################## \n")

      myProbablities.saveAsTextFile(outputPath + "-" + numIters + "-" + "-test")
    }
    else{
      model. write.overwrite().save(outputPath)
    }

    spark.stop()
  }
}