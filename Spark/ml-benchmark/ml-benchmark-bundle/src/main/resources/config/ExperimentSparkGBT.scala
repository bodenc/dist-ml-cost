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
class ExperimentSparkGBT extends ApplicationContextAware {

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

      numIterations /**/ <- Seq(1,2,3,4,5,7,9,10,15,20,50,100)
      topXXX /*       */ <- Seq("top006") // host configuration for experients defined in host.conf
      numFeatures <- Seq(39)
      regParam <- Set(0.01)

    } yield new SparkExperiment(
      name = s"mlbench.spark.train.$topXXX.$numIterations.$regParam.$numFeatures",
      command =
        s"""
           |--class de.tuberlin.dima.mlbench.spark.trainGBTDataSet                \\
           |$${app.path.apps}/ml-benchmark-spark-jobs-1.0-SNAPSHOT.jar          \\
           |--inputPath=$${system.hadoop-2.path.input}/train                     \\
           |--testPath=hdfs://master1-1.mia.tu-berlin.de:8020/data/criteo/test_libsvm_cat/full \\
           |--outputPath=$${system.hadoop-2.path.input}/benchmark/$topXXX/$numIterations/$numFeatures/$regParam       \\
           |--numIterations=$numIterations                                   \\
           |--numFeatures=$numFeatures                                                    \\
           |--regParam=$regParam                   \\
      """.stripMargin.trim,
      config = ConfigFactory.parseString(
        s"""
           |system.default.config.slaves            = $${env.slaves.$topXXX.hosts}
           |system.default.config.parallelism.total = $${env.slaves.$topXXX.total.parallelism}
      """.stripMargin.trim),
      runs = 1,
      runner = ctx.getBean("spark-2.2.0", classOf[Spark]),
      systems = Set(ctx.getBean("dstat-0.7.2", classOf[Dstat])),
      inputs = Set(ctx.getBean("criteoTrain", classOf[DataSet])),
      outputs = Set(ctx.getBean("benchmark.output", classOf[ExperimentOutput]))
    )
  )


  @Bean(name = Array("spark.gbt.mllib"))
  def `spark.gbt.mllib`: ExperimentSuite =
    ex(1)
}
