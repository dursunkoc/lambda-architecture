import domain.Activity
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

/**
  * Created by dursun on 12/30/16.
  */
package object function {
  val MS_IN_HOUR = 1000 * 60 * 60

  def rddToActivityRDD: (RDD[(String, String)]) => RDD[Activity] = (input) => {
    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges

    input.mapPartitionsWithIndex({ (index, it) =>
      it
        .map(_._2)
        .map(_.split("\t"))
        .flatMap(stringToActivity(offsetRangesToOptions(offsetRanges, index)))
    })
  }

  def offsetRangesToOptions(offsetRanges: Array[OffsetRange], index: Int): Map[String, String] = {
    val or = offsetRanges(index)
    val options: Map[String, String] = Map("topic" -> or.topic, "kafkaPartition" -> or.partition.toString,
      "fromOffset" -> or.fromOffset.toString, "untilOffset" -> or.untilOffset.toString)
    options
  }

  def stringToActivity: (Map[String, String]) => (Array[String]) => TraversableOnce[Activity] = (options) => (a) =>
    a match {
      case r: Array[String] if r.length == 7 => Some(Activity(r(0).toLong / MS_IN_HOUR * MS_IN_HOUR, r(1), r(2), r(3), r(4), r(5), r(6), options))
      case _ => None
    }
}
