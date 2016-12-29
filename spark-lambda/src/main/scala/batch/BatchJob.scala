package batch

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import domain.Activity
import config.Settings
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object BatchJob {

  def main(args: Array[String]) {
    // get spark configuration
    val conf = new SparkConf()
      .setAppName("Lambda with Spark")
    conf.setMaster("local[*]")

    // setup spark context
    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    // initialize input RDD
    val sourceFile = "file://" + Settings.WebLogGen.filePath;
    val input = sc.textFile(sourceFile)
    val MS_IN_HOUR = 1000 * 60 * 60

    val inputRDD = input.map(_.split("\\t")).flatMap { record =>
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }
    rddOperations(sc, inputRDD)
    
    val inputDF = inputRDD.toDF
    sqlOperations(sqlContext = sqlContext, inputDF = inputDF)
  }

  def sqlOperations(implicit sqlContext: SQLContext = null, inputDF: DataFrame) = {
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    sqlContext.udf.register("UnderExposed", (vc: Long, pc: Long) => if (pc == 0) 0 else vc / pc)

    val df = inputDF.select(add_months(from_unixtime(inputDF("timestamp_hour")), 1).as("timestamp_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")).cache()

    df.registerTempTable("activity")
    val visitorsByProduct = sqlContext.sql("""SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
                      |FROM activity
                      |GROUP BY product, timestamp_hour""".stripMargin)
    visitorsByProduct.printSchema()

    val activityByProduct = sqlContext.sql("""
                      |SELECT product, timestamp_hour,
                      |sum(case action when 'purchase' then 1 else 0 end) as purchase_count, 
                      |sum(case action when 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                      |sum(case action when 'page_view' then 1 else 0 end) as page_view_count
                      |FROM activity
                      |GROUP BY product, timestamp_hour
""".stripMargin)
    activityByProduct.registerTempTable("activityByProduct")

    val underExposedProducts = sqlContext.sql("""
                      |SELECT product, timestamp_hour, UnderExposed(page_view_count, purchase_count) as negative_exposure 
                      |FROM activityByProduct
                      |order by negative_exposure DESC limit 5
""".stripMargin)

    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
    underExposedProducts.foreach(println)
  }

  def rddOperations(sc:SparkContext, inputRDD: RDD[Activity]) = {
    implicit val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    
    inputRDD.toDF
    
    val keyyedByProduct = inputRDD.keyBy { a => (a.product, a.timestamp_hour) }.cache()
    val visitorsByProduct = keyyedByProduct.mapValues { a => a.product }.distinct().countByKey()
    val activityByProduct = keyyedByProduct.mapValues {
      _.action match {
        case "purchase" => (1, 0, 0)
        case "add_to_cart" => (0, 1, 0)
        case "page_view" => (0, 0, 1)
      }
    }.reduceByKey((acc, n) => (acc._1 + n._1, acc._2 + n._2, acc._3 + n._3))

    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
  }
}