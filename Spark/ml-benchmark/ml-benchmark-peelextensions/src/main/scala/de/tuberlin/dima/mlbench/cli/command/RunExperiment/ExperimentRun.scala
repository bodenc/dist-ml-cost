/**
  * Copyright (C) 2017 TU Berlin DIMA (peel@dima.tu-berlin.de)
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
package de.tuberlin.dima.mlbench.cli.command.RunExperiment

import java.lang.{System => Sys}

import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.peelframework.core.beans.experiment.{Experiment, ExperimentSuite}
import org.peelframework.core.beans.system.{Lifespan, System}
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.{Configurable, loadConfig}
import org.peelframework.core.graph.{Node, createGraph}
import org.peelframework.core.util.console._
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Execute all experiments in a suite, but erase all materialized data sets from HDFS */
@Service("suite:run_clean")
class Run extends Command {

  override val name = "suite:run_clean"

  override val help = "execute all experiments in a suite and clean hdfs after each experiment"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--force", "-f")
      .`type`(classOf[Boolean])
      .dest("app.suite.experiment.force")
      .action(Arguments.storeTrue)
      .help("re-execute successful runs")
    // arguments
    parser.addArgument("suite")
      .`type`(classOf[String])
      .dest("app.suite.name")
      .metavar("SUITE")
      .help("experiments suite to run")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.suite.experiment.force", if (ns.getBoolean("app.suite.experiment.force")) "true" else "false")
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
  }

  override def run(context: ApplicationContext) = {
    logger.info(s"Running experiments in suite '${Sys.getProperty("app.suite.name")}'")

    val suite = context.getBean(Sys.getProperty("app.suite.name"), classOf[ExperimentSuite])
    val graph = createGraph(suite)

    //TODO check for cycles in the graph
    if (graph.isEmpty) throw new RuntimeException("Experiment suite is empty!")

    // resolve experiment configurations
    for (e <- suite.experiments) e.config = loadConfig(graph, e)

    // generate runs to be executed
    val force = Sys.getProperty("app.suite.experiment.force", "false") == "true"
    val runs = for (e <- suite.experiments; i <- 1 to e.runs; r <- Some(e.run(i, force)); if force || !r.isSuccessful) yield r
    // filter experiments in the relevant runs
    val exps = runs.foldRight(List[Experiment[System]]())((r, es) => if (es.isEmpty || es.head != r.exp) r.exp :: es else es)

    // SUITE lifespan
    try {
      logger.info("Executing experiments in suite")
      for (exp <- exps) {

        val allSystems = for (n <- graph.reverse.traverse(); if graph.descendants(exp).contains(n)) yield n
        val inpSystems: Set[Node] = for (in <- exp.inputs; sys <- in.dependencies) yield sys
        val expSystems = (graph.descendants(exp, exp.inputs) diff Seq(exp)).toSet

        // EXPERIMENT lifespan
        try {
          logger.info("#" * 60)
          logger.info("Current experiment is '%s'".format(exp.name))

          // update config
          for (n <- graph.descendants(exp)) n match {
            case s: Configurable => s.config = exp.config
            case _ => Unit
          }

          logger.info("Setting up / updating systems required for input data sets")
          for (n <- inpSystems) n match {
            case s: System => if (s.isUp) s.update() else s.setUp()
            case _ => Unit
          }

          logger.info("Materializing experiment input data sets")
          for (n <- exp.inputs; path = n.resolve(n.path)) if (!n.fs.exists(path)) {
            try {
              n.materialize()
            } catch {
              case e: Throwable => n.fs.rmr(path); throw e // make sure the path is cleaned for the next try
            }
          } else {
            logger.info(s"Skipping already materialized path '$path'".yellow)
          }



          logger.info("Tearing down redundant systems before conducting experiment runs")
          for (n <- inpSystems diff expSystems) n match {
            case s: System if !(Lifespan.PROVIDED :: Lifespan.SUITE :: Nil contains s.lifespan) => s.tearDown()
            case _ => Unit
          }

          logger.info("Setting up systems with SUITE lifespan")
          for (n <- allSystems) n match {
            case s: System if (Lifespan.PROVIDED :: Lifespan.SUITE :: Nil contains s.lifespan) && !s.isUp => s.setUp()
            case _ => Unit
          }

          logger.info("Updating systems with PROVIDED or SUITE lifespan")
          for (n <- allSystems) n match {
            case s: System if Lifespan.PROVIDED :: Lifespan.SUITE :: Nil contains s.lifespan => s.update()
            case _ => Unit
          }

          logger.info("Setting up systems with EXPERIMENT lifespan")
          for (n <- expSystems) n match {
            case s: System if s.lifespan == Lifespan.EXPERIMENT => s.setUp()
            case _ => Unit
          }

          for (r <- runs if r.exp == exp) {
            for (n <- exp.outputs) n.clean()

            logger.info("Setting up systems with RUN lifespan")
            for (n <- allSystems) n match {
              case s: System if s.lifespan == Lifespan.RUN => s.setUp()
              case _ => Unit
            }

            try {
              r.execute() // run experiment
            }
            finally {
              logger.info("Tearing down systems with RUN lifespan")
              for (n <- allSystems) n match {
                case s: System if s.lifespan == Lifespan.RUN => s.tearDown()
                case _ => Unit
              }
            }

            for (n <- exp.outputs) n.clean()

            logger.info("Cleaning experiment input data sets")
            for (n <- exp.inputs; path = n.resolve(n.path)) if (n.fs.exists(path)) {
              try {
                n.fs.rmr(path, true)
              } catch {
                case e: Throwable => n.fs.rmr(path); throw e // make sure the path is cleaned for the next try
              }
            } else {
              logger.info(s"Skipping already materialized path '$path'".yellow)
            }
          }

        } catch {
          case e: Throwable =>
            logger.error(s"Exception for experiment '${exp.name}' in suite '${suite.name}': ${e.getMessage}".red)
            throw e

        } finally {
          logger.info("Tearing down systems with EXPERIMENT lifespan")
          for (n <- expSystems) n match {
            case s: System if s.lifespan == Lifespan.EXPERIMENT => s.tearDown()
            case _ => Unit
          }
        }
      }

    }
    catch {
      case e: Throwable =>
        logger.error(s"Exception in suite '${suite.name}': ${e.getMessage}".red)
        throw e

    } finally {
      // Explicit shutdown only makes sense if at least one experiment is in the list,
      // otherwise the systems would not have been configured at all
      if (exps.nonEmpty) {
        logger.info("#" * 60)
        logger.info("Tearing down systems with SUITE lifespan")
        for (n <- graph.traverse()) n match {
          case s: System if s.lifespan == Lifespan.SUITE => s.tearDown()
          case _ => Unit
        }
      }
    }
  }
}