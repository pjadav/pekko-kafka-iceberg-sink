package io.github.prakashjadav.icebergsink.components

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.header.Headers
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
    .groupedWeightedWithin(batchMaxBytes, batchSize, batchWindow) { message =>
      message.record.value().toString.getBytes(StandardCharsets.UTF_8).length.toLong + 1L
    }
    .map { messages =>
      val records = messages.map { message =>
        val record             = message.record
        val eventTimestampMs   = record.timestamp()
        val ingestYearMonthDay = yearMonthDay(System.currentTimeMillis())
        IcebergRecord(
          payload            = record.value().toString,
          topic              = record.topic(),
          partition          = record.partition(),
          offset             = record.offset(),
          kafkaTimestamp     = eventTimestampMs,
          yearMonthDay       = yearMonthDay(eventTimestampMs),
          ingestYearMonthDay = ingestYearMonthDay
        )
      }

      PreparedIcebergBatch(
        records     = records,
        offsets     = CommittableOffsetBatch(messages.map(_.committableOffset)),
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
