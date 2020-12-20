package com.demo.descrete.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkKafkaDstream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SocketWordCountApp")

    val streamingCtx = new StreamingContext(conf,Seconds(10))

    streamingCtx.sparkContext.setLogLevel("ERROR")

    /*
      group.id: Kafka source will create a unique group id for each query automatically.
      key.deserializer: Keys are always deserialized as byte arrays with ByteArrayDeserializer.
        We need to explicitly deserialize the values.
      value.deserializer: Values are always deserialized as byte arrays with ByteArrayDeserializer.
        We need to explicitly deserialize the values.
      enable.auto.commit: Kafka source doesn’t commit any offset.
      auto.offset.reset: Set the source option startingOffsets to specify where to start instead.
        values are earliest / latest
     */

    val kafkaParms = Map("bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "sales_stream_2",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /* Creating Kafka Stream
       PreferConsistent : Use this in most cases, it will consistently distribute partitions across all executors.
       PreferBrokers : Use this only if your executors are on the same nodes as your Kafka brokers.
       PreferFixed : Use this to place particular TopicPartitions on particular hosts if your load is uneven.
       Subscribe: Subscribe to a collection of topics.
     */

    val kafkaSalesDream = KafkaUtils.createDirectStream[String,String](
      streamingCtx,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](List("sales_stream"),kafkaParms)
    )

//    kafkaSalesDream
//      .map(x => x.value.toString)
//      .print()

    kafkaSalesDream.foreachRDD(rdd => {

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.map(x => x.value).foreach(println)

      kafkaSalesDream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })

    streamingCtx.start()
    streamingCtx.awaitTermination()

  }
}
