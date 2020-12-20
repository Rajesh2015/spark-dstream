package com.demo.descrete.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWithJoin {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("Dstream-WordCount").getOrCreate();
    sparkSession.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(10))
    val customerRdd = sparkSession.sparkContext.textFile("src/main/resources/customers.csv")
      .map(x => x.split(","))
      .map(x => (x(0),x(1)))

    val custDF = sparkSession.read
      .option("delimiter",",")
      .csv("src/main/resources/customers.csv")
      .toDF("customerId","Name")
    val socketstream=ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_AND_DISK)

    val salesStreamPair = socketstream.map(x => x.split(","))
      .map(x => (x(1),x(3).toDouble))
    salesStreamPair.print()

    val totalSalesStream = socketstream.map(x => x.split(","))
      .map(x => (x(1),x(3).toDouble))
      .reduceByKey(_+_)

    totalSalesStream.print()
    import sparkSession.implicits._
    val transformedStream = totalSalesStream.transform(rdd => {
      val salesDF = rdd.toDF("customerId","totalAmount")
      val joindDf = salesDF.join(custDF,"customerId")
      joindDf.rdd
    })
    transformedStream.print()
    totalSalesStream.foreachRDD(rdd => {
      val salesDF = rdd.toDF("customerId","totalAmount")
      salesDF.join(custDF,"customerId")
        .write.mode("append")
        .jdbc("jdbc:mysql://localhost:3306/ecommerce","sales_stream",props)
    })

    ssc.start()
    ssc.awaitTermination()


  }
}
