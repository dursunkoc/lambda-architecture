package streaming

import config.Settings
import domain.Activity
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.SparkUtils._

/**
  * Created by dursun on 12/29/16.
  */
object KafkaDirectStreaming extends App{
  val MS_IN_HOUR = 1000 * 60 * 60
  val wlc = Settings.WebLogGen
  val sc: SparkContext = getSparkContext("Kafka Direct Streaming App", isIde)
  def streamingApp = (sc:SparkContext, batchDuration:Duration) =>{
    val ssc = new StreamingContext(sc, batchDuration)
    val kafkaParams = Map(
      "metadata.broker.list" -> "localhost:9092",
      "group.id" -> "lambda",
      "auto.offset.reset" -> "largest"
    )

    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,Set(wlc.kafkaTopic))
    dStream.transform(rdd=>{
      rdd.map(_._2).map(_.split("\t")).flatMap(_ match {
        case r:Array[String] if r.length == 7 => Some(Activity(r(0).toLong / MS_IN_HOUR * MS_IN_HOUR, r(1), r(2), r(3), r(4), r(5), r(6)))
        case _ => None
      })
    }).print()
    ssc
  }
  val ssc = getStreamingContext(streamingApp, sc, Seconds(4))
  ssc.start()
  ssc.awaitTermination()

}
