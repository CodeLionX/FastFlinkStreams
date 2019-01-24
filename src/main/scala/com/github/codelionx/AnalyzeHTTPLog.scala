package com.github.codelionx

import java.time._
import java.time.format.DateTimeFormatter

import org.apache.flink.api.common.accumulators.AverageAccumulator
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStreamUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.backuity.clist
import org.backuity.clist.CliMain
import org.slf4j.LoggerFactory

import scala.collection.mutable
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

    val requestsPerDayFile = "requestsPerDay"
    val uniqueVisitorsFile = "uniqueVisitors"
    val serverErrorsFile = "serverErrors"
    averageRequestsPerDay(httpTextLog)
      .writeAsCsv(requestsPerDayFile, WriteMode.OVERWRITE)
      .setParallelism(1)
    uniqueVisitors(httpTextLog)
      .writeAsText(uniqueVisitorsFile, WriteMode.OVERWRITE)
      .setParallelism(1)
    serverErrorsPerMonth(httpTextLog)
      .writeAsCsv(serverErrorsFile, WriteMode.OVERWRITE)
      .setParallelism(1)

    // execute program
    env.execute("HTTP log analysis")

    // collect output measurements
    //    val requestsPerDayCol = new DataStreamUtils(requestsPerDay).collect().toSeq
    val requestsPerDayCol = io.Source.fromFile(requestsPerDayFile).getLines().map( _.split(",")(1).toLong)
    val result = requestsPerDayCol
      .map( requests => 1 -> requests )
      .reduce( (t1, t2) =>
        (t1._1 + t2._1) -> (t1._2 + t2._2)
      )
    println(s"Avg. requests per day: ${result._2 / result._1}")
//
//    val uniqueVisitorsADayCol = new DataStreamUtils(uniqueVisitorsStream).collect().toSeq
    val uniqueVisitorsADayCol = io.Source.fromFile(uniqueVisitorsFile).getLines().map(_.toLong).toSeq
    val minUnqiueVisitors = uniqueVisitorsADayCol.min
    val maxUniqueVisitors = uniqueVisitorsADayCol.max
    println(s"Min. unique visitors a day: $minUnqiueVisitors")
    println(s"Max. unique visitors a day: $maxUniqueVisitors")
//
//    val serverErrorsCol = new DataStreamUtils(serverErrors).collect().toSeq
    val serverErrorsCol = io.Source.fromFile(serverErrorsFile).getLines().map( line => {
      val values = line.split(",")
      values(0) -> values(1).toInt
    }).toSeq
    serverErrorsCol.foreach{ case (descr, errors) =>
      println(s"Server errors $descr : $errors")
    }
  }

  private def averageRequestsPerDay(httpTextLog: DataStream[HTTPLogEntry]): DataStream[(LocalDate, Int)] = {
    // count all requests per day
    val requestsPerDay = httpTextLog
      .timeWindowAll(Time.days(1))
      .apply( (window, events, out: Collector[(LocalDate, Int)]) =>
        out.collect((
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(window.getStart), ZoneId.of("UTC")).toLocalDate,
          events.count(_ => true)
        ))
      )

//    requestsPerDay
//      .map(t => s"Requests/day for day ${t._1} : ${t._2}")
//      .print()

    requestsPerDay
  }

  private def uniqueVisitors(logStream: DataStream[HTTPLogEntry]): DataStream[Long] = {
    logStream
      .map(_.host)
      .timeWindowAll(Time.days(1))
      .aggregate(new AggregateFunction[String, Set[String], Long] {
        override def createAccumulator(): Set[String] = Set.empty
        override def add(value: String, accumulator: Set[String]): Set[String] = accumulator + value
        override def getResult(accumulator: Set[String]): Long = accumulator.size
        override def merge(a: Set[String], b: Set[String]): Set[String] = a ++ b
      })
  }

  private def serverErrorsPerMonth(logStream: DataStream[HTTPLogEntry]): DataStream[(String, Long)] = {
    logStream
      .filter( _.responseCode >= 500 )
      .windowAll(TumblingEventTimeWindows.of(Time.days(30)))
      .apply( (window, events, out: Collector[(String, Long)]) => {
        val startDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(window.getStart), ZoneId.of("UTC")).toLocalDate
        val endDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(window.getEnd), ZoneId.of("UTC")).toLocalDate
        val windowDescription = s"from $startDate to $endDate"
        out.collect(windowDescription -> events.count(_ => true))
      })
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
    entryStream.assignAscendingTimestamps( logEntry => logEntry.date.toInstant.toEpochMilli )
  }
}
