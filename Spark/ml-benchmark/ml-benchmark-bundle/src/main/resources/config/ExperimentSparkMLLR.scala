package config


import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.data.{DataSet, ExperimentOutput}
import org.peelframework.core.beans.experiment.ExperimentSuite
import org.peelframework.dstat.beans.system.Dstat
import org.peelframework.spark.beans.system.Spark
import org.springframework.context.annotation.{Bean, Configuration}
import org.peelframework.spark.beans.experiment.SparkExperiment
import org.springframework.context.{ApplicationContext, ApplicationContextAware}
import org.peelframework.hadoop.beans.system.HDFS2



@Configuration
class ExperimentSparkMLLR extends ApplicationContextAware {

  /* The enclosing application context */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  @Bean(name = Array("benchmark.output"))
  def `benchmark.output`: ExperimentOutput = new ExperimentOutput(
    path = "{system.hadoop-2.path.input}/benchmark/",
    fs  = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )


  // Suite for checking the evaluation
  def `ex`(numRuns: Int): ExperimentSuite = new ExperimentSuite(
    for {
      topXXX /*     */ <- Seq("top006")
      //topXXX /*     */ <- Seq("top012", "all")
      numIterations /**/ <- Seq(1,2,3,4,5,6,7,8,9,10,25)
      //topXXX /*     */ <- Seq("top006")
  //    numIterations /**/ <- Seq(10)
      numFeatures <- Seq(262144)
      regParam <- Set(0.01)


      // file:///data/christoph.boden/criteo/
      //--testPath=$${system.hadoop-2.path.input}/test

    } yield new SparkExperiment(
      name = s"mlbench.spark.train.$topXXX.$numIterations.$regParam.$numFeatures",
      command =
        s"""
           |--class de.tuberlin.dima.mlbench.spark.trainLRHashing                \\
           |$${app.path.apps}/ml-benchmark-spark-jobs-1.0-SNAPSHOT.jar          \\
           |--inputPath=$${system.hadoop-2.path.input}/train                     \\
           |--testPath=noTest  \\
           |--outputPath=$${system.hadoop-2.path.input}/benchmark/$topXXX/$numIterations/$numFeatures/$regParam       \\
           |--numIterations=$numIterations                                   \\
           |--numFeatures=$numFeatures                                                    \\
           |--catTreatment="not_exact"    \\
           |--regParam=$regParam                   \\
      """.stripMargin.trim,
      config = ConfigFactory.parseString(
        s"""
           |system.default.config.slaves            = $${env.slaves.$topXXX.hosts}
           |system.default.config.parallelism.total = $${env.slaves.$topXXX.total.parallelism}
      """.stripMargin.trim),
      runs = 3,
      runner = ctx.getBean("spark-2.2.0", classOf[Spark]),
      systems = Set(ctx.getBean("dstat-0.7.2", classOf[Dstat])),
      inputs = Set(ctx.getBean("criteoTrain", classOf[DataSet])),
//      inputs = Set(ctx.getBean("criteoTrainWally", classOf[DataSet]),ctx.getBean("criteoTestWally", classOf[DataSet])),
      outputs = Set(ctx.getBean("benchmark.output", classOf[ExperimentOutput]))
    )
 )


  @Bean(name = Array("spark.lr.mllib"))
  def `spark.lr.mllib`: ExperimentSuite =
    ex(1)
}
