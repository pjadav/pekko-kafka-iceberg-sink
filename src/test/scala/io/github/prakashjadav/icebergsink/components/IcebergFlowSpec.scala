package io.github.prakashjadav.icebergsink.components

import io.github.prakashjadav.icebergsink.TestFixtures
import org.apache.iceberg.data.Record

import java.time.{Instant, ZoneOffset}

class IcebergFlowSpec extends TestFixtures {

  "IcebergCatalog" should "use a raw JSON payload schema without event-specific fields" in {
    import scala.jdk.CollectionConverters._

    IcebergCatalog.schema.columns().asScala.map(_.name()) shouldBe Seq(
      "payload",
      "event_date",
      "ingest_date",
      "source_topic",
      "table_name",
      "kafka_metadata",
      "provenance",
      "event_time",
      "ingestion_time"
    )
    IcebergCatalog.schema.findField("event_time").`type`() shouldBe
      org.apache.iceberg.types.Types.TimestampType.withZone()
    IcebergCatalog.schema.findField("ingestion_time").`type`() shouldBe
      org.apache.iceberg.types.Types.TimestampType.withZone()
  }

  it should "map payload and Kafka timestamps into a generic Iceberg record" in {
    val eventTimestamp     = 1_700_000_000_000L
    val ingestionTimestamp = 1_700_000_001_000L
    val record             = IcebergCatalog.toRecord(
      IcebergRecord(
        payload            = """{"message":"hello"}""",
        topic              = "example-events",
        partition          = 2,
        offset             = 42L,
        kafkaTimestamp     = eventTimestamp,
        ingestionTimestamp = ingestionTimestamp,
        yearMonthDay       = "2023-11-14",
        ingestYearMonthDay = "2023-11-14"
      ),
      "events"
    )

    record.getField("payload") shouldBe """{"message":"hello"}"""
    record.getField("event_time") shouldBe Instant.ofEpochMilli(eventTimestamp).atOffset(ZoneOffset.UTC)
    record.getField("ingestion_time") shouldBe Instant.ofEpochMilli(ingestionTimestamp).atOffset(ZoneOffset.UTC)

    val metadata = record.getField("kafka_metadata").asInstanceOf[Record]
    metadata.getField("timestamp") shouldBe eventTimestamp
    metadata.getField("ingested_at") shouldBe IcebergCatalog.isoDateTime(ingestionTimestamp)
  }

  it should "honour an explicit warehouse override" in {
    IcebergCatalog.warehouse(
      appConfig.copy(
        icebergWarehouse = Some("s3://example-bucket/warehouse/")
      )
    ) shouldBe "s3://example-bucket/warehouse/"
  }

  it should "include optional Glue client properties when configured" in {
    val properties = IcebergCatalog.catalogProperties(
      appConfig.copy(
        icebergGlueCatalogId         = Some("123456789012"),
        icebergCatalogClientFactory  = Some("org.apache.iceberg.aws.AssumeRoleAwsClientFactory"),
        icebergAssumeRoleArn         = Some("arn:aws:iam::123456789012:role/example-iceberg-writer"),
        icebergAssumeRoleRegion      = Some("us-east-1"),
        icebergAssumeRoleSessionName = Some("example-iceberg-writer")
      )
    )

    properties.get("glue.id") shouldBe "123456789012"
    properties.get("client.factory") shouldBe "org.apache.iceberg.aws.AssumeRoleAwsClientFactory"
    properties.get(
      "client.assume-role.arn"
    ) shouldBe "arn:aws:iam::123456789012:role/example-iceberg-writer"
    properties.get("client.assume-role.region") shouldBe "us-east-1"
    properties.get("client.assume-role.session-name") shouldBe "example-iceberg-writer"
  }

  it should "configure local AWS-compatible endpoints" in {
    val properties = IcebergCatalog.catalogProperties(
      appConfig.copy(
        icebergGlueEndpoint      = Some("http://moto:5000"),
        icebergS3Endpoint        = Some("http://moto:5000"),
        icebergS3PathStyleAccess = true
      )
    )

    properties.get("client.region") shouldBe "eu-west-1"
    properties.get("glue.endpoint") shouldBe "http://moto:5000"
    properties.get("s3.endpoint") shouldBe "http://moto:5000"
    properties.get("s3.path-style-access") shouldBe "true"
  }
}
