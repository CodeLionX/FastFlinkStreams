package com.github.codelionx

import org.apache.flink.api.scala._


/**
 * Skeleton for our Flink Job.
 *
 */
object AnalyzeHTTPLog {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment


    // execute program
    env.execute("Flink Scala API Skeleton")
  }
}
