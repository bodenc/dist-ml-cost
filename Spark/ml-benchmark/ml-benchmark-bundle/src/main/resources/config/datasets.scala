/**
  * Copyright (C) 2017 TU Berlin DIMA
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *         http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package config;


import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.data.{CopiedDataSet, DataSet, ExperimentOutput, GeneratedDataSet}
import org.peelframework.flink.beans.system.Flink
import org.peelframework.flink.beans.job.FlinkJob
import org.peelframework.spark.beans.job.SparkJob
import org.peelframework.spark.beans.system.Spark
import org.peelframework.hadoop.beans.system.HDFS2
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}


@Configuration
class datasets extends ApplicationContextAware {

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  // paths to data sets for benchmark
  val inputCriteoTrainFull = "hdfs://"
  val inputCriteoTestFull = "hdfs://"


  // ************************************
  @Bean(name = Array("criteoTrain"))
  def `criteoTrain`: DataSet = new CopiedDataSet(
    src = inputCriteoTrainFull,
    dst = s"$${system.hadoop-2.path.input}/train",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("criteoTest"))
  def `criteoTest`: DataSet = new CopiedDataSet(
    src = inputCriteoTestFull,
    dst = s"$${system.hadoop-2.path.input}/test",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

}