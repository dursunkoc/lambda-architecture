package streaming

import domain.{Activity}
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import utils.SparkUtils._
import config.Settings
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.sql.SaveMode


/**
  * Created by dursun on 12/29/16.
  */
object KafkaDirectStreaming extends App {
  val wlc = Settings.WebLogGen
  val hdfsPath = wlc.hdfsPath

  val sc: SparkContext = getSparkContext("Kafka Direct Streaming App", true)

  val ssc = getStreamingContext(streamingApp, sc, Seconds(4))


  def streamingApp = (sc: SparkContext, batchDuration: Duration) => {
    val ssc = new StreamingContext(sc, batchDuration)
    val kafkaParams = Map(
      "metadata.broker.list" -> "192.168.99.100:9092",
      "group.id" -> "lambda",
      "auto.offset.reset" -> "smallest"
    )

    val sqlContext = getSqlContext(sc)
    import sqlContext.implicits._

    val hdfsPath = wlc.hdfsPath
//    val hdfsData = sqlContext.read.parquet(hdfsPath)

//    val fromOffsets =
//            hdfsData.groupBy("topic", "kafkaPartition").agg(max("untilOffset").as("untilOffset"))
//              .collect().map { row =>
//              (TopicAndPartition(row.getAs[String]("topic"), row.getAs[Int]("kafkaPartition")), row.getAs[String]("untilOffset").toLong + 1)
//            }.toMap

    val kafkaStream = //fromOffsets.isEmpty match {
//      case true =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, Set(wlc.kafkaTopic)
        )
//      case false =>
//        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
//          ssc, kafkaParams, fromOffsets, { mmd: MessageAndMetadata[String, String] => (mmd.key(), mmd.message()) }
//        )
//    }
    val activityStream = kafkaStream
      .transform(function.rddToActivityRDD)

    activityStream.foreachRDD(rdd => {
      val activityDF = rdd
        .toDF()
        .selectExpr("timestamp_hour", "visitor", "product", "inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition", "inputProps.fromOffset as fromOffset", "inputProps.untilOffset as untilOffset")
      activityDF.foreach(println)
      println("WRITING....")
      activityDF
        .write
        .partitionBy("topic", "kafkaPartition", "timestamp_hour")
        .mode(SaveMode.Append)
        .parquet(hdfsPath)
    })



    ssc
  }

  ssc.start()
  ssc.awaitTermination()

}
