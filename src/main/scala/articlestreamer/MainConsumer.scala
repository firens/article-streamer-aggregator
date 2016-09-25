package articlestreamer

import articlestreamer.kafka.{KafkaConsumerWrapper, KafkaProducerWrapper}
import articlestreamer.twitter.TwitterStreamer
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer._
import twitter4j.Status
import scala.concurrent.duration._

object MainConsumer extends App {

  override def main(args: Array[String]) {

   val consumer = new KafkaConsumerWrapper

    println("Starting polling")

    consumer.poll(5 seconds, 10)

    println("Polling stopped")

    consumer.stopConsumer()

  }

}
