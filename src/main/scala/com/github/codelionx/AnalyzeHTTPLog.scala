package com.github.codelionx

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.backuity.clist
import org.backuity.clist.CliMain
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}


object HTTPLogEntry {
  def fromString(logString: String): Try[HTTPLogEntry] = Try {
    val dateTimeFormat = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z")
    val firstSpace = logString.indexOf(" ")
    val host = logString.substring(0, firstSpace)

    val openBrace = logString.indexOf("[")
    val closeBrace = logString.indexOf("]")
    val dateString = logString.substring(openBrace + 1, closeBrace)

    val firstQuote = logString.indexOf("\"") + 1
    val secondQuote = logString.indexOf("\"", firstQuote)
    val httpLine = logString.substring(firstQuote, secondQuote)
    val httpLineEntries = httpLine.split(" ")
    val httpMethod = httpLineEntries(0)
    val resource = httpLineEntries(1)

    val nextSpace = logString.indexOf(" ", secondQuote) + 1
    val nextSpace2 = logString.indexOf(" ", nextSpace)
    val statusCodeString = logString.substring(nextSpace, nextSpace2)
    val responseCode = try {
      statusCodeString.toInt
    } catch {
      case _: Exception => 0
    }

    val responseBytesString = logString.substring(nextSpace2 + 1, logString.length - 1)
    val responseBytes = try {
      responseBytesString.toInt
    } catch {
      case _: Exception => 0
    }

    HTTPLogEntry(
      host = host,
      date = ZonedDateTime.parse(dateString, dateTimeFormat),
      httpMethod = httpMethod,
      resource = resource,
      responseCode = responseCode,
      responseBytes = responseBytes
    )
  }
}
case class HTTPLogEntry(host: String, date: ZonedDateTime, httpMethod: String, resource: String, responseCode: Int, responseBytes: Int)


/**
 * Skeleton for our Flink Job.
 */
object AnalyzeHTTPLog extends CliMain[Unit](
  name = "AnalyzeHTTPLog",
  description = "Flink job for analyzing NASAs HTTP log"
) {

  var path = clist.opt[String](description = "specify path to the log file to analyze", default = "./access_log_Aug95")
  var cores = clist.opt[Int](description = "number of cores / parallelism", default = 4)

  private val log = LoggerFactory.getLogger(this.getClass)

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

    // time characteristic
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env
  }

  private def getLogStream(env: StreamExecutionEnvironment): DataStream[HTTPLogEntry] = {
    val textStream = env.readTextFile(path, "UTF-8")
    val entryStream = textStream.flatMap( (logLine: String, collector: Collector[HTTPLogEntry]) => {
      HTTPLogEntry.fromString(logLine) match {
        case Success(logEntry) => collector.collect(logEntry)
        case Failure(e) => log.warn(s"Ignoring log entry $logLine", e)
      }
    })
    entryStream.assignAscendingTimestamps( logEntry => logEntry.date.toInstant.getEpochSecond )
  }
}
