package de.tuberlin.dima.mlbench.spark

import java.nio.charset.Charset

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, _}

import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3


object trainLRHashing {

  val Seed = 0

  // Properties of the Criteo Data Set
  val NUM_LABELS = 1
  val NUM_INTEGER_FEATURES = 13
  val NUM_CATEGORICAL_FEATURES = 26
  val NUM_FEATURES = NUM_LABELS + NUM_INTEGER_FEATURES + NUM_CATEGORICAL_FEATURES

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
    val regParam = options.get("regParam").get.toDouble
    val catTreatment = options.getOrElse("catTreatment", "exact")


    val spark = SparkSession
      .builder()
      .appName("Logistic Regression - DataSet API - Custom Hashing")
      .config("spark.driver.memory", "25g")
      .config("spark.driver.maxResultSize", "25g")
      .config("spark.kryoserializer.buffer.max", "1g")
      // ###########################
      //.master("local")
      // ###########################
      .getOrCreate()

    import spark.implicits._

    // toggle handling of categorical variables
    var categoricalVarTreatment = false
    if(catTreatment.equals("exact")){
      categoricalVarTreatment = true
    }

    val data = readInput(spark.sparkContext, inputPath, "\t", categoricalVarTreatment, numFeatures).toDF("label", "features")

    print("Training LR Model for " + numIters + " iterations ..." + "\n")

    val lr = new LogisticRegression()
      .setMaxIter(numIters)
      .setRegParam(regParam)
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setStandardization(true)
    val model = lr.fit(data)

    print("######################################################################## \n")
    print("numIterations, numFeatures, catTreatment, regParam \n")
    print(numIters + "," + numFeatures + ","  +  catTreatment + "," + regParam + "\n")
    print("######################################################################## \n")


    if(!testPath.equals("noTest")) {


      val test = readInput(spark.sparkContext, testPath, "\t", categoricalVarTreatment, numFeatures).toDF("label", "features")

      val getPOne = udf((v: org.apache.spark.ml.linalg.DenseVector) => v(1))
      val myProbablities = model.transform(test).select(getPOne(col("probability")), col("label")).rdd.map(value => (value.getDouble(0), value.getDouble(1)))

      val metrics = new BinaryClassificationMetrics(myProbablities)
      val auPRC = metrics.areaUnderPR
      val auROC = metrics.areaUnderROC

      print("######################################################################## \n")
      print("numIterations, numFeatures, catTreatment, regParam, auROC, auPRC, loadingTime, trainingTime, testingTime \n")
      print(numIters + "," + numFeatures + ","  +  catTreatment + "," + regParam + "," +  auROC + "," + auPRC + "\n")
      print("######################################################################## \n")

      myProbablities.saveAsTextFile(outputPath + "-" + numIters + "-" + "-test")
    }
    else{
      model. write.overwrite().save(outputPath)
    }
    spark.stop()

  }




  /**
    * read the input file and separate label, integer and categorical features
    *
    * @param sc
    * @param input
    * @param delimiter
    * @return
    */
  def readInput(sc: SparkContext, input: String, delimiter: String = "\t", categoricalTreatmentExact: Boolean, numFeatures:Integer) = {
    val raw_input = sc.textFile(input) map { line =>
      val features = line.split(delimiter, -1)

      val label = features.take(NUM_LABELS).head.toInt
      val integerFeatures = features.slice(NUM_LABELS, NUM_LABELS + NUM_INTEGER_FEATURES)
        .map(string => if (string.isEmpty) 0 else string.toInt)
      val categorialFeatures = features.slice(NUM_LABELS + NUM_INTEGER_FEATURES, NUM_FEATURES)

      // add dimension so that similar values in diff. dimensions get a different hash
      // however this may not be benifical for criteo as suggested by
      // http://fastml.com/vowpal-wabbit-eats-big-data-from-the-criteo-competition-for-breakfast/

      if (categoricalTreatmentExact) {
        for (i <- categorialFeatures.indices) {
          categorialFeatures(i) = i + ":" + categorialFeatures(i)
        }
      }
      (label, integerFeatures, categorialFeatures)
    }

      val transformedFeatures = raw_input.map(x => {

        var label = x._1
        val intFeatures = x._2
        val catFeatures = x._3

        val hashedIndices = catFeatures
          .filter(!_.isEmpty)
          .map(murmurHash(_, 1, (numFeatures - NUM_INTEGER_FEATURES)))
          .groupBy(_._1)
          .map(colCount => (colCount._1 + NUM_INTEGER_FEATURES + 1, colCount._2.map(_._2).sum))
          .filter(_._2 != 0)
          .toSeq.sortBy(_._1)


        var indices = new ListBuffer[Int]()
        var values = new ListBuffer[Double]()

        for ((col, value) <- 1 to intFeatures.size zip intFeatures) {
          indices += col
          values += value
        }
        for ((col, value) <- hashedIndices){
          indices += col
          values += value.toDouble
        }

        val size = numFeatures + 13
        var outLabel = 0.0

        if (label <= 0) {
          outLabel = 0.0
        }
        else {
          outLabel = 1.0
        }


        val dataPoint = new SparseVector(size, indices.toArray, values.toArray)

        (outLabel, dataPoint)
      })


      transformedFeatures
    }

  /**
    * awkward hash function
    *
    * @param feature
    * @param count
    * @param numFeatures
    * @return
    */

  private def murmurHash(feature: String, count: Int, numFeatures: Int): (Int, Int) = {
    val hash = MurmurHash3.bytesHash(feature.getBytes(Charset.forName("UTF-8")), Seed)
    val index = scala.math.abs(hash) % numFeatures


    val value = if (hash >= 0) count else -1 * count
    (index, value)
  }
}