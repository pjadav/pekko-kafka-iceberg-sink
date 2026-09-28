package io.github.prakashjadav.icebergsink.components

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.NotUsed
import org.apache.pekko.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import org.apache.pekko.stream.scaladsl.Flow

import java.nio.charset.StandardCharsets
import java.time.{Instant, ZoneOffset}
import scala.concurrent.duration.FiniteDuration

final case class IcebergRecord(
  payload: String,
  topic: String,
  partition: Int,
  offset: Long,
  kafkaTimestamp: Long,
  ingestionTimestamp: Long,
  yearMonthDay: String,
  ingestYearMonthDay: String
)

final case class PreparedIcebergBatch(
  records: Seq[IcebergRecord],
  offsets: CommittableOffsetBatch,
  recordCount: Int
)

class JsonConverter(batchSize: Int, batchMaxBytes: Long, batchWindow: FiniteDuration) extends StrictLogging {

  def flow: Flow[
    CommittableMessage[String, JsonNode],
    PreparedIcebergBatch,
    NotUsed
  ] = Flow[CommittableMessage[String, JsonNode]]
    .map(message => message -> System.currentTimeMillis())
    .groupedWeightedWithin(batchMaxBytes, batchSize, batchWindow) { case (message, _) =>
      message.record.value().toString.getBytes(StandardCharsets.UTF_8).length.toLong + 1L
    }
    .map { messages =>
      val records = messages.map { case (message, ingestionTimestampMs) =>
        val record           = message.record
        val eventTimestampMs = record.timestamp()
        IcebergRecord(
          payload            = record.value().toString,
          topic              = record.topic(),
          partition          = record.partition(),
          offset             = record.offset(),
          kafkaTimestamp     = eventTimestampMs,
          ingestionTimestamp = ingestionTimestampMs,
          yearMonthDay       = yearMonthDay(eventTimestampMs),
          ingestYearMonthDay = yearMonthDay(ingestionTimestampMs)
        )
      }

      PreparedIcebergBatch(
        records     = records,
        offsets     = CommittableOffsetBatch(messages.map(_._1.committableOffset)),
        recordCount = messages.size
      )
    }

  private def yearMonthDay(timestampMs: Long): String =
    Instant.ofEpochMilli(timestampMs).atZone(ZoneOffset.UTC).toLocalDate.toString
}

object JsonConverter {
  def apply(batchSize: Int, batchMaxBytes: Long, batchWindow: FiniteDuration): JsonConverter =
    new JsonConverter(batchSize, batchMaxBytes, batchWindow)
}
