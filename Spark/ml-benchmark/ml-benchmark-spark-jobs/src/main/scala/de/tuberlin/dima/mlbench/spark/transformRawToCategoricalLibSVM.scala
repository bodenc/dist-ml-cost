package de.tuberlin.dima.mlbench.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * transforms a criteo data set file from tsv to vw format
  *
  */
object transformRawToCategoricalLibSVM {

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

    //"hdfs://master1-1.mia.tu-berlin.de:8020/data/criteo_raw_test/"

    val inputPath = options.get("inputPath").get
    val outputPath = options.get("outputPath").get
    val percDataPoints = options.get("percDataPoints").get.toDouble


    val conf = new SparkConf()
      // ##### REMOVE BEFORE FLIGHT ! ###########
      // .setMaster("local[*]")
      // ##### REMOVE BEFORE FLIGHT ! ###########
      .setAppName("transform raw to categorical libsvm")


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
      val integerFeatures = features.slice(NUM_LABELS, NUM_LABELS + NUM_INTEGER_FEATURES)
        .map(string => if (string.isEmpty) 0 else string.toInt)
      val categorialFeatures = features.slice(NUM_LABELS + NUM_INTEGER_FEATURES, NUM_FEATURES)

  //    for(i <-  categorialFeatures.indices){
  //      categorialFeatures(i) = (i + integerFeatures.size + 1) + ":" + categorialFeatures(i)
  //    }



      (label, integerFeatures, categorialFeatures)
    }


    val transformedFeatures = outRDD.map(x => {


      val label = x._1
      val intFeatures = x._2
      val catFeatures = x._3.map(value => if(value != ""){ Integer.parseUnsignedInt(value,16).toString()} else "") // transform hex value to int

      val intStrings = for ((col, value) <- 1 to intFeatures.size zip intFeatures) yield s"$col:$value"
      val catStrings = for ((col, value) <- (intFeatures.size + 1) to  (1 + catFeatures.size + intFeatures.size) zip catFeatures) yield ( if(value!= "") {s"$col:$value"} else {""} )

      val outLine = label + " " + intStrings.mkString(" ") + " " + catStrings.mkString(" ") + "\n"

      outLine
    })

    transformedFeatures.saveAsTextFile(outputPath)

    sc.stop()

  }
}