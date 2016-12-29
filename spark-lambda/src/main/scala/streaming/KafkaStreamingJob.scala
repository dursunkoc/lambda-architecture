package streaming

import config.Settings
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.SparkUtils

/**
  * Created by dursun on 12/29/16.
  */
object KafkaStreamingJob extends App{
  val sc: SparkContext = SparkUtils.getSparkContext("Kafka Streaming App", SparkUtils.isIde)

  val streamingApp = (sc:SparkContext, batchDuration:Duration)=> {
    val ssc = new StreamingContext(sc, batchDuration)
    val kafkaParams = Map(
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "lambda",
      "auto.offset.reset" -> "largest"
    )
    val topics = Map(
      Settings.WebLogGen.kafkaTopic -> 1
    )
    val dStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics,StorageLevel.MEMORY_AND_DISK)
    dStream.transform(rdd=>{
      val values: RDD[String] = rdd.map(_._2)
      values.foreach(println)
      values
    }).print()
    ssc
  }

  val ssc: StreamingContext = SparkUtils.getStreamingContext(streamingApp, sc, Seconds(4))
  ssc.start()
  ssc.awaitTermination()

}
