package com.demo.descrete.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkWordCountWithDstream {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[*]").appName("Dstream-WordCount").getOrCreate();
    sparkSession.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(10))
try {

  val socketstream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK)

  var streamedWord = socketstream.flatMap(rec => rec.split(" ")).map(rec => (rec, 1)).reduceByKey((a, b) => a + b)
  streamedWord.print()
  ssc.start()
  ssc.awaitTermination()
}
    catch{
      case e:Exception=>
        {
          println("some exception happened while checking file...")
        }
    }


  }

}
