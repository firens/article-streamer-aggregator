package articlestreamer

import articlestreamer.kafka.KafkaProducerWrapper
import articlestreamer.twitter.TwitterStreamer
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer._
import twitter4j.Status

import scala.util.Random

object MainProducer extends App {

  private val appConfig = ConfigFactory.load()

  override def main(args: Array[String]) {

    val producer = new KafkaProducerWrapper

    val twitterStreamer = TwitterStreamer(tweetHandler(producer), stopHandler(producer))

    println("Starting streaming")
    twitterStreamer.startStreaming()

    Thread.sleep(10000)

    println("Stopping streaming")
    twitterStreamer.stop()
    println("Streaming stopped")

//    val record = new ProducxerRecord[String, String]("tweets", "tweet" + Random.nextInt(), "tweet_value_" + Random.nextInt())
//    producer.send(record)

    producer.stopProducer()

  }

  def tweetHandler(producer: KafkaProducerWrapper): (Status) => Unit = {
    (status: Status) => {
      import scala.pickling.Defaults._
      import scala.pickling.json._

      println(s"Status received: ${status.getCreatedAt}")

      val topic = appConfig.getString("kafka.topic")

      val record = new ProducerRecord[String, AnyRef](topic, s"tweet${status.getId}", status.pickle.value)
      producer.send(record)

    }
  }

  def stopHandler(producer: KafkaProducerWrapper): () => Unit = {
    () => producer.stopProducer()
  }

}
