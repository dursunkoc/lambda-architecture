package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object SparkUtils {

  def isIde = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkContext(appName: String, isIde: Boolean): SparkContext = {
    val conf = new SparkConf().setAppName(appName)
    if (isIde) {
      conf.setMaster("local[*]")
    }
    val cpd = if (isIde) "/Users/dursun/temp" else "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    val sc: SparkContext = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(cpd)

    sc
  }

  def getSqlContext(implicit sc: SparkContext): SQLContext = {
    SQLContext.getOrCreate(sc)
  }

  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext,
                          sc: SparkContext,
                          batchDuration: Duration): StreamingContext = {
    val creatingFunc = () => {
      streamingApp(sc, batchDuration)
    }

    val scc: StreamingContext = sc.getCheckpointDir match {
      case Some(checkpointDir) =>
        StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(scc.checkpoint(_))

    scc
  }

}