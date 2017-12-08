package de.tuberlin.dima.mlbench.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * transforms a criteo data set file from tsv to vw format
  *
  */
object transformLabelsToVW {

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
    val percDataPoints = options.get("percDataPoints").get.toDouble


    val conf = new SparkConf()
      // ##### REMOVE BEFORE FLIGHT ! ###########
      // .setMaster("local[*]")
      // ##### REMOVE BEFORE FLIGHT ! ###########
      .setAppName("transform raw criteo to VW")
    val sc = new SparkContext(conf)

    // extract numerical and caterorical features from text
    val data = sc.textFile(inputPath)


    val input = if(percDataPoints != 1.0) {
      data.sample(false, percDataPoints, 42)
    } else {
      data
    }

      val outRDD = input.map { line =>
      val features = line.split("\t", -1)

      val label = features.take(NUM_LABELS).head.toInt
      // adjust labels

      val integerFeatures = features.slice(NUM_LABELS, NUM_LABELS + NUM_INTEGER_FEATURES)
        .map(string => if (string.isEmpty) 0 else string.toInt)
      val categorialFeatures = features.slice(NUM_LABELS + NUM_INTEGER_FEATURES, NUM_FEATURES)



      // add dimension so that similar values in diff. dimensions get a different hash
 /*
      skipped as suggested in http://fastml.com/vowpal-wabbit-eats-big-data-from-the-criteo-competition-for-breakfast/
      for(i <-  categorialFeatures.indices){
        categorialFeatures(i) = i + ":" + categorialFeatures(i)
      }
*/
      (label, integerFeatures, categorialFeatures)
    }


    val transformedFeatures = outRDD.map(x => {


      val label = x._1
      val intFeatures = x._2
      val catFeatures = x._3

      val intStrings = for ((col, value) <- 1 to intFeatures.size zip intFeatures) yield s"I$col:$value"
      val catStrings = for (elem <- catFeatures) yield s"$elem"

      val outLine = label + " 1.0 |i " + intStrings.mkString(" ") + " |c " + catStrings.mkString(" ")

      outLine
    })

    transformedFeatures.saveAsTextFile(outputPath)

    sc.stop()

  }
}