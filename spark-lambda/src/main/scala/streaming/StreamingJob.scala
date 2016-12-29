package streaming


import domain.{Activity, ActivityByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import utils.SparkUtils._


/**
  * Created by dursun on 12/24/16.
  */
object StreamingJob {

  val MS_IN_HOUR = 1000 * 60 * 60

  def getStreamingAppForMap(sc: SparkContext, batchDuration: Duration): StreamingContext = {
    val ssc: StreamingContext = new StreamingContext(sc, batchDuration)
    readTextFile(ssc)
      .transform(rdd => {
        rdd.map(_.split("\t")).flatMap(_ match {
          case r: Array[String] if r.length == 7 => Some(Activity(r(0).toLong / MS_IN_HOUR * MS_IN_HOUR, r(1), r(2), r(3), r(4), r(5), r(6)))
          case _ => None
        })
          .keyBy(a => (a.product, a.timestamp_hour))
          .groupByKey()
          .mapValues(as => as.foldLeft((0, 0, 0)) { (acc, nxt) =>
            nxt.action match {
              case "purchase" => (acc._1 + 1, acc._2 + 0, acc._3 + 0)
              case "add_to_cart" => (acc._1 + 0, acc._2 + 1, acc._3 + 0)
              case "page_view" => (acc._1 + 0, acc._2 + 0, acc._3 + 1)
            }
          }).map(pr => (
          (pr._1._1, pr._1._2),
          ActivityByProduct(pr._1._1, pr._1._2, pr._2._1, pr._2._2, pr._2._3)
          ))

      }).mapWithState(StateSpec.function(mapActivityStateFunc).timeout(Seconds(30)))
      .print()

    ssc
  }

  def mapActivityStateFunc = (k: (String, Long), v: Option[ActivityByProduct], state: State[(Long, Long, Long)]) => {
    val now: Long = System.currentTimeMillis()
    val (pc: Long, acc: Long, pvc: Long) = state.getOption().getOrElse((0l, 0l, 0l))

    val newState: (Long, Long, Long) = v match {
      case Some(a) => (a.purchaseCount + pc, a.addToCartCount + acc, a.pageViewCount + pvc)
      case _ => (pc, acc, pvc)
    }

    state.update(newState)

    newState match {
      case (0L, _, _) => 0L
      case (pc, _, pvc) => pvc / pc
    }
  }

  def main(args: Array[String]) {
    val sc: SparkContext = getSparkContext("Lambda with Spark", isIde)
    val batchDuration = Seconds(4)

    val ssc = getStreamingContext(getStreamingAppForMap, sc, batchDuration)

    ssc.start()
    ssc.awaitTermination()
  }

  def getStreamingApp(sc: SparkContext, batchDuration: Duration): StreamingContext = {
    val ssc: StreamingContext = new StreamingContext(sc, batchDuration)
    implicit val isc = sc
    val sqlContext = getSqlContext
    import sqlContext.implicits._

    val textDStream: DStream[String] = readTextFile(ssc)

    val activityDStream = textDStream.transform(input => {
      input.map(_.split("\t")).flatMap(r =>
        r match {
          case a: Array[String] if a.length == 7 => Some(Activity(r(0).toLong / MS_IN_HOUR * MS_IN_HOUR, r(1), r(2), r(3), r(4), r(5), r(6)))
          case _ => None
        })
    })

    activityDStream.transform(rdd => {
      val df = rdd.toDF()
      df.registerTempTable("activity");
      val dataFrame: DataFrame = sqlContext.sql(
        """
          |SELECT product, timestamp_hour,
          |sum(case action when 'purchase' then 1 else 0 end) as purchase_count,
          |sum(case action when 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
          |sum(case action when 'page_view' then 1 else 0 end) as page_view_count
          |FROM activity
          |GROUP BY product, timestamp_hour
        """.stripMargin)

      dataFrame.map(r => ((r.getString(0), r.getLong(1)),
        ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))))
    }).updateStateByKey((newItemsPerKey: Seq[ActivityByProduct], currentState: Option[(Long, Long, Long, Long)]) => {
      val now: Long = System.currentTimeMillis()
      println(s"Current time is $now and currentState is $currentState")
      val (prevTimestamp: Long, purchaseCount: Long, addToCartCount: Long, pageViewCount: Long) = currentState.getOrElse(
        (now, 0l, 0l, 0l)
      )
      newItemsPerKey match {
        case t if t.isEmpty && (now - prevTimestamp > 30000 + 4000) => None
        case t if t.isEmpty => Some((now, purchaseCount, addToCartCount, pageViewCount))
        case _ => Some(newItemsPerKey.foldLeft((now, purchaseCount, addToCartCount, pageViewCount)) { (acc: (Long, Long, Long, Long), next: ActivityByProduct) =>
          (now, acc._1 + next.purchaseCount, acc._2 + next.addToCartCount, acc._3 + next.pageViewCount)
        })
      }
    }).print()

    ssc
  }

  def readTextFile(ssc: StreamingContext): DStream[String] = {
    val inputPath = isIde match {
      case true => "file:///Users/dursun/Vagrants/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
      case false => "file:///vagrant/input"
    }
    ssc.textFileStream(inputPath)
  }

}
