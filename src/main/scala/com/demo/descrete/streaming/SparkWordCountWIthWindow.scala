package com.demo.descrete.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkWordCountWIthWindow {

  def main(args: Array[String]): Unit = {
val sparkSession=SparkSession.builder().master("local[*]").appName("Wordcount-with-window").getOrCreate()
sparkSession.sparkContext.setLogLevel("ERROR")
    var ssc= new StreamingContext(sparkSession.sparkContext,Seconds(10))
val socketStream=ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_AND_DISK)
val wordcountdstream=socketStream
  .flatMap(rec=>rec.split(" "))
  .map(rec=>(rec,1))
  .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(60),Seconds(10))
    wordcountdstream.print()
    ssc.start()
    ssc.awaitTermination()


  }

}
