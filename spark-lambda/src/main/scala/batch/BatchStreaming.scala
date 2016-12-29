package batch

import domain.Activity
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import utils.SparkUtils._

/**
  * Created by dursun on 12/24/16.
  */
object BatchStreaming {
  def main(args: Array[String]) {

    val sc: SparkContext = getSparkContext("Lambda with Spark", false)
    implicit val sqlContext = getSqlContext(sc)

    val sourceFile = "file:///vagrant/data.tsv"
    val input = sc.textFile(sourceFile)

    val MS_IN_HOUR = 1000 * 60 * 60
    val inputRDD = input.map(_.split("\t")).flatMap(r =>
      r match {
        case a: Array[String] if a.length == 7 => Some(Activity(r(0).toLong / MS_IN_HOUR * MS_IN_HOUR, r(1), r(2), r(3), r(4), r(5), r(6)))
        case _ => None
      })


    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val inputDF = inputRDD.toDF()

    inputDF.select(add_months(from_unixtime(inputDF("timestamp_hour")), 1).as("timestamp_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")).cache()

    inputDF.registerTempTable("activity")

    val visitorsByProduct = sqlContext.sql("""SELECT product, timestamp_hour, count(distinct visitor) as unique_visitors
                                             |FROM activity
                                             |GROUP by product,timestamp_hour""".stripMargin)

    visitorsByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")
  }
}
