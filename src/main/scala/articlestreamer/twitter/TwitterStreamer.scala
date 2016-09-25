package articlestreamer.twitter

import twitter4j._

case class TwitterStreamer(onStatusFct: (Status) => Unit, onStop: () => Unit) {

  val stream = new TwitterStreamFactory(TwitterAuthorizationConfig.config).getInstance

  def simpleStatusListener = new StatusListener() {
    def onStatus(status: Status) = onStatusFct(status)
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {println("limitation : " + numberOfLimitedStatuses)}
    def onException(ex: Exception) { println(ex) }
    def onScrubGeo(arg0: Long, arg1: Long) {}
    def onStallWarning(warning: StallWarning) {}
  }

  def startStreaming() = {
    println("Twitter Streamer : Starting streaming")
    stream.addListener(simpleStatusListener)
    stream.filter(new FilterQuery().track("#Twitter"))
    println("Twitter Streamer : Streaming started")
  }

  def stop() = {
    stream.cleanUp()
    stream.shutdown()
    onStop()
  }

}