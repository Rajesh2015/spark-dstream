package com.demo.descrete.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkFileDStream {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("Dstream-WordCount").getOrCreate();
    sparkSession.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(40))
    val fileStream=ssc.textFileStream(args(0))
    val filestreamDStream=fileStream.flatMap(rec=>rec.split(" "))
      .map(rec=>(rec,1))
      .reduceByKey((a,b)=>a+b)
    filestreamDStream.print()
    ssc.start()
    ssc.awaitTermination()


  }
}
