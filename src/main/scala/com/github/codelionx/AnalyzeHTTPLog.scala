package com.github.codelionx

import java.io.File

import org.apache.flink.api.common.state.StateTtlConfig.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.backuity.clist
import org.backuity.clist.CliMain


/**
 * Skeleton for our Flink Job.
 *
 */
object AnalyzeHTTPLog extends CliMain[Unit](
  name = "AnalyzeHTTPLog",
  description = "Flink job for analyzing NASAs HTTP log"
) {

  var path = clist.opt[String](description = "specify path to the log file to analyze", default = "./access_log_Aug95")
  var cores = clist.opt[Int](description = "number of cores / parallelism", default = 4)

  override def run: Unit = {
    val env = setupEnvironment()

    val httpTextLog = getLogStream(env)
    httpTextLog.print()

    // execute program
    env.execute("Flink Scala API Skeleton")
  }

  private def setupEnvironment(): StreamExecutionEnvironment = {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // define parallelism
    env.setMaxParallelism(cores)
    env.setParallelism(cores)

    env
  }

  private def getLogStream(env: StreamExecutionEnvironment): DataStream[String] = {
    env.readTextFile(path, "ASCII")
  }
}
