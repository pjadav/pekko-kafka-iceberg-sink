package io.github.prakashjadav.icebergsink.components

import io.github.prakashjadav.icebergsink.model.Config
import org.apache.pekko.Done
import org.apache.pekko.kafka.{CommitWhen, CommitterSettings, ConsumerMessage}

import java.time.Duration
import scala.concurrent.Future
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.kafka.scaladsl.Committer
import org.apache.pekko.kafka.CommitDelivery

class CommitterSink(maxBatchSize: Int, maxInterval: Int)(implicit actorSystem: ActorSystem) {

  def sink: Sink[ConsumerMessage.Committable, Future[Done]] = {
    val committerSettings = CommitterSettings(actorSystem)
      .withCommitWhen(CommitWhen.offsetFirstObserved)
      .withMaxBatch(maxBatchSize)
      .withMaxInterval(Duration.ofSeconds(maxInterval))
      .withDelivery(CommitDelivery.waitForAck)
    Committer.sink(committerSettings)
  }
}

object CommitterSink {
  def apply(config: Config)(implicit actorSystem: ActorSystem): CommitterSink =
    new CommitterSink(config.kafkaCommitMaxBatch, config.kafkaCommitMaxIntervalSeconds)
}
