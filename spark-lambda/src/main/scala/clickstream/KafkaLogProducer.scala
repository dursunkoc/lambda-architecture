package clickstream

import java.io.{File, FileWriter}
import java.util.Properties

import config.Settings
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * Created by dursun on 12/29/16.
  */
object KafkaLogProducer extends App{
  // WebLog config
  val wlc = Settings.WebLogGen

  val Products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val Referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray
  val Visitors = (0 to wlc.visitors).map("Visitor-" + _)
  val Pages = (0 to wlc.pages).map("Page-" + _)
  val topic = wlc.kafkaTopic

  val rnd = new Random()

  val filePath = wlc.filePath
  val destPath = wlc.destPath
  val numberOfFiles = wlc.numberOfFiles

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG,"all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG,"WebLogProducer")

  private val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)

  for (i <- 1 to numberOfFiles) {

    // introduce a bit of randomness to time increments for demo purposes
    val incrementTimeEvery = rnd.nextInt(math.min(wlc.records, 100) - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    for (iteration <- 1 to wlc.records) {
      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * wlc.timeMultiplier)
      timestamp = System.currentTimeMillis() // move all this to a function
      val action = iteration % (rnd.nextInt(200) + 1) match {
          case 0 => "purchase"
          case 1 => "add_to_cart"
          case _ => "page_view"
        }
      val referrer = Referrers(rnd.nextInt(Referrers.length - 1))
      val prevPage = referrer match {
        case "Internal" => Pages(rnd.nextInt(Pages.length - 1))
        case _ => ""
      }
      val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
      val page = Pages(rnd.nextInt(Pages.length - 1))
      val product = Products(rnd.nextInt(Products.length - 1))

      val line = s"$adjustedTimestamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"
      val pr = new ProducerRecord(topic,line)
      kafkaProducer.send(pr)


      if(iteration % incrementTimeEvery == 0){
        println(s"Sent $iteration messages!")

        val sleeping  = rnd.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }
    }
  }
  kafkaProducer.close()
}
