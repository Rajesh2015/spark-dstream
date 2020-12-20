package com.demo.descrete.streaming

import java.util.regex.Matcher

import org.apache.spark.streaming.{Seconds, StreamingContext}
import Utilities._
import org.apache.spark.storage.StorageLevel

object StramingLogAnalysisDemo {
  def main(args: Array[String]): Unit = {
    var ssc=new StreamingContext("local[*]",
    "LogParser", Seconds(15))
    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)


    // Extract the request field from each log line
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})

    // Extract the URL from the request
    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})

    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(15))

    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()

    // Kick it off
    ssc.checkpoint("../checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }

}
