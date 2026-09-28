package io.github.prakashjadav.icebergsink.model

import java.util.Locale
import scala.concurrent.duration._

final case class Config(
  environment: String               = "local",
  bootstrapServers: String          = "localhost:9092",
  applicationId: String             = "pekko-iceberg-sink",
  reconnectBackoffInMillis: Long    = 50,
  reconnectMaxBackoffInMillis: Long = 1000,
  consumerMaxPollRecords: Int       = 500,
  enableAutoCommit: Boolean         = false,
  inputTopics: Seq[String],
  autoOffsetReset: String,
  s3Bucket: String,
  s3Region: String,
  s3PathPrefix: String                         = "data",
  icebergDatabase: String                      = "default",
  icebergTable: String                         = "events",
  icebergTableNameMode: String                 = "static",
  icebergPartitionFields: Seq[String]          = Seq("event_date", "ingest_date"),
  icebergWarehouse: Option[String]             = None,
  icebergGlueCatalogId: Option[String]         = None,
  icebergGlueEndpoint: Option[String]          = None,
  icebergS3Endpoint: Option[String]            = None,
  icebergS3PathStyleAccess: Boolean            = false,
  icebergCatalogClientFactory: Option[String]  = None,
  icebergAssumeRoleArn: Option[String]         = None,
  icebergAssumeRoleRegion: Option[String]      = None,
  icebergAssumeRoleSessionName: Option[String] = None,
  schemaRegistryUrl: String,
  valueSubjectNameStrategy: String = "io.confluent.kafka.serializers.subject.RecordNameStrategy",
  cacheCapacity: Int,
  s3BatchSize: Int                   = 5000,                 // flush when record limit is reached
  s3BatchMaxBytes: Long              = 128L * 1024L * 1024L, // flush when raw byte limit is reached
  s3BatchWindowSeconds: Int          = 60,                   // flush when time limit is reached
  icebergCommitIntervalSeconds: Int  = 300,                  // commit accumulated files at this interval
  kafkaCommitMaxBatch: Int           = 500,
  kafkaCommitMaxIntervalSeconds: Int = 5,
  topicExcludeRegex: String          = "(?:_.*|internal-.*|.*Dlq$|.*\\.internal$|.*DLQ$)"
) {
  private def normalizedTableNameMode: String                   = icebergTableNameMode.toLowerCase(Locale.ROOT)
  private def normalizeCatalogIdentifier(value: String): String =
    value
      .trim
      .toLowerCase(Locale.ROOT)
      .replaceAll("[^a-z0-9_]", "_")
      .replaceAll("_+", "_")
      .stripPrefix("_")
      .stripSuffix("_")

  def s3BatchWindow: FiniteDuration = s3BatchWindowSeconds.seconds

  def topicSubscriptionPattern: String = s"^(?!(?:$topicExcludeRegex)).+$$"

  def normalizedIcebergDatabase: String = normalizeCatalogIdentifier(icebergDatabase)

  def tableNameForTopic(topic: String): String =
    normalizedTableNameMode match {
      case "topic" => normalizeCatalogIdentifier(topic)
      case _       => normalizeCatalogIdentifier(icebergTable)
    }

  def validate(): Unit = {
    require(
      inputTopics.nonEmpty || topicExcludeRegex.trim.nonEmpty,
      "Configure INPUT_TOPICS or INPUT_TOPIC_EXCLUDE_REGEX"
    )
    require(s3Bucket.trim.nonEmpty, "S3_BUCKET must not be empty")
    require(s3Region.trim.nonEmpty, "S3_REGION must not be empty")
    require(normalizedIcebergDatabase.nonEmpty, "ICEBERG_DATABASE must be a valid Glue database name")
    require(
      Set("static", "topic").contains(normalizedTableNameMode),
      "ICEBERG_TABLE_NAME_MODE must be either 'static' or 'topic'"
    )
    require(icebergPartitionFields.nonEmpty, "ICEBERG_PARTITION_FIELDS must contain at least one field")
    require(
      icebergPartitionFields.distinct.size == icebergPartitionFields.size,
      "ICEBERG_PARTITION_FIELDS must not contain duplicates"
    )
    val allowedPartitionFields = Set("event_date", "ingest_date", "source_topic", "table_name")
    require(
      icebergPartitionFields.forall(allowedPartitionFields.contains),
      s"ICEBERG_PARTITION_FIELDS must use fields from ${allowedPartitionFields.toSeq.sorted.mkString(", ")}"
    )
    require(
      normalizedTableNameMode != "static" || tableNameForTopic(icebergTable).nonEmpty,
      "ICEBERG_TABLE must be a valid Glue table name"
    )
    require(schemaRegistryUrl.trim.nonEmpty, "SCHEMA_REGISTRY_URL must not be empty")
    require(s3BatchSize > 0, "S3_BATCH_SIZE must be greater than zero")
    require(s3BatchMaxBytes > 0, "S3_BATCH_MAX_BYTES must be greater than zero")
    require(s3BatchWindowSeconds > 0, "S3_BATCH_WINDOW_SECONDS must be greater than zero")
    require(icebergCommitIntervalSeconds > 0, "ICEBERG_COMMIT_INTERVAL_SECONDS must be greater than zero")
    require(kafkaCommitMaxBatch > 0, "KAFKA_COMMIT_MAX_BATCH must be greater than zero")
    require(kafkaCommitMaxIntervalSeconds > 0, "KAFKA_COMMIT_MAX_INTERVAL_SECONDS must be greater than zero")
    require(cacheCapacity > 0, "CACHE_CAPACITY must be greater than zero")
  }
}

object Config {
  private def getOptionalEnvVar(name: String): Option[String] =
    sys.env
      .get(name)
      .orElse(sys.props.get(name))
      .map(_.trim)
      .filter(_.nonEmpty)

  private def getBooleanEnvVar(name: String, default: Boolean): Boolean =
    getOptionalEnvVar(name).map(_.toBoolean).getOrElse(default)

  def apply(): Config = {
    val config = new Config(
      environment                 = getOptionalEnvVar("ENVIRONMENT").getOrElse("local"),
      bootstrapServers            = getOptionalEnvVar("KAFKA_BOOTSTRAP_SERVERS").getOrElse("localhost:9092"),
      applicationId               = getOptionalEnvVar("KAFKA_CONSUMER_GROUP").getOrElse("pekko-iceberg-sink"),
      reconnectBackoffInMillis    = getOptionalEnvVar("KAFKA_RECONNECT_BACKOFF_MS").map(_.toLong).getOrElse(50L),
      reconnectMaxBackoffInMillis = getOptionalEnvVar("KAFKA_RECONNECT_MAX_BACKOFF_MS").map(_.toLong).getOrElse(1000L),
      consumerMaxPollRecords      = getOptionalEnvVar("KAFKA_MAX_POLL_RECORDS").map(_.toInt).getOrElse(500),
      enableAutoCommit            = getBooleanEnvVar("KAFKA_ENABLE_AUTO_COMMIT", false),
      inputTopics     = getOptionalEnvVar("INPUT_TOPICS").toSeq.flatMap(_.split(",").map(_.trim).filter(_.nonEmpty)),
      autoOffsetReset = getOptionalEnvVar("AUTO_OFFSET_RESET").getOrElse("latest"),
      s3Bucket        = getOptionalEnvVar("S3_BUCKET").getOrElse("example-iceberg-bucket"),
      s3Region        = getOptionalEnvVar("S3_REGION").getOrElse("us-east-1"),
      s3PathPrefix    = getOptionalEnvVar("S3_PATH_PREFIX").getOrElse("data"),
      icebergDatabase = getOptionalEnvVar("ICEBERG_DATABASE").getOrElse("default"),
      icebergTable    = getOptionalEnvVar("ICEBERG_TABLE").getOrElse("events"),
      icebergTableNameMode   = getOptionalEnvVar("ICEBERG_TABLE_NAME_MODE").getOrElse("static"),
      icebergPartitionFields = getOptionalEnvVar("ICEBERG_PARTITION_FIELDS")
        .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
        .getOrElse(Seq("event_date", "ingest_date")),
      icebergWarehouse             = getOptionalEnvVar("ICEBERG_WAREHOUSE"),
      icebergGlueCatalogId         = getOptionalEnvVar("ICEBERG_GLUE_ID"),
      icebergGlueEndpoint          = getOptionalEnvVar("ICEBERG_GLUE_ENDPOINT"),
      icebergS3Endpoint            = getOptionalEnvVar("ICEBERG_S3_ENDPOINT"),
      icebergS3PathStyleAccess     = getBooleanEnvVar("ICEBERG_S3_PATH_STYLE_ACCESS", false),
      icebergCatalogClientFactory  = getOptionalEnvVar("ICEBERG_CATALOG_CLIENT_FACTORY"),
      icebergAssumeRoleArn         = getOptionalEnvVar("ICEBERG_ASSUME_ROLE_ARN"),
      icebergAssumeRoleRegion      = getOptionalEnvVar("ICEBERG_ASSUME_ROLE_REGION"),
      icebergAssumeRoleSessionName = getOptionalEnvVar("ICEBERG_ASSUME_ROLE_SESSION_NAME"),
      schemaRegistryUrl            = getOptionalEnvVar("SCHEMA_REGISTRY_URL").getOrElse("http://localhost:8081"),
      valueSubjectNameStrategy     = getOptionalEnvVar("SCHEMA_REGISTRY_VALUE_SUBJECT_NAME_STRATEGY")
        .getOrElse("io.confluent.kafka.serializers.subject.RecordNameStrategy"),
      cacheCapacity        = getOptionalEnvVar("CACHE_CAPACITY").map(_.toInt).getOrElse(2000),
      s3BatchSize          = getOptionalEnvVar("S3_BATCH_SIZE").map(_.toInt).getOrElse(5000),
      s3BatchMaxBytes      = getOptionalEnvVar("S3_BATCH_MAX_BYTES").map(_.toLong).getOrElse(128L * 1024L * 1024L),
      s3BatchWindowSeconds = getOptionalEnvVar("S3_BATCH_WINDOW_SECONDS").map(_.toInt).getOrElse(60),
      icebergCommitIntervalSeconds  = getOptionalEnvVar("ICEBERG_COMMIT_INTERVAL_SECONDS").map(_.toInt).getOrElse(300),
      kafkaCommitMaxBatch           = getOptionalEnvVar("KAFKA_COMMIT_MAX_BATCH").map(_.toInt).getOrElse(500),
      kafkaCommitMaxIntervalSeconds =
        getOptionalEnvVar("KAFKA_COMMIT_MAX_INTERVAL_SECONDS").map(_.toInt).getOrElse(5),
      topicExcludeRegex = getOptionalEnvVar("INPUT_TOPIC_EXCLUDE_REGEX")
        .getOrElse("(?:_.*|internal-.*|.*Dlq$|.*\\.internal$|.*DLQ$)")
    )

    config.validate()
    config
  }
}
