package io.github.prakashjadav.icebergsink

class ConfigSpec extends TestFixtures {

  "Config" should "accept multiple topics" in {
    appConfig.inputTopics shouldBe Seq("test-topic-a", "test-topic-b")
  }

  it should "build an exclusion pattern for all-topic subscriptions" in {
    appConfig.copy(inputTopics = Seq.empty).topicSubscriptionPattern shouldBe
      "^(?!(?:(?:_.*|internal-.*|.*Dlq$|.*\\.internal$|.*DLQ$))).+$"
  }

  it should "use bootstrap servers for source" in {
    appConfig.bootstrapServers shouldBe "dummy:9092"
  }

  it should "use latest as auto offset reset" in {
    appConfig.autoOffsetReset shouldBe "latest"
  }

  it should "have S3 destination details" in {
    appConfig.s3Bucket shouldBe "test-bucket"
    appConfig.s3Region shouldBe "eu-west-1"
    appConfig.s3PathPrefix shouldBe "data"
  }

  it should "derive batch window duration from seconds" in {
    appConfig.s3BatchWindow.toSeconds shouldBe 30L
  }

  it should "default Iceberg commit interval to five minutes" in {
    appConfig.icebergCommitIntervalSeconds shouldBe 300
  }

  it should "derive Iceberg table names from topics when topic mode is enabled" in {
    appConfig
      .copy(icebergTableNameMode = "topic")
      .tableNameForTopic("users.messages.pushnotification.Bounce") shouldBe
      "users_messages_pushnotification_bounce"
  }

  it should "normalize Glue identifiers to lowercase" in {
    val config = appConfig.copy(
      icebergDatabase      = "Example_Raw_Events",
      icebergTable         = "UserEvents",
      icebergTableNameMode = "static"
    )

    config.normalizedIcebergDatabase shouldBe "example_raw_events"
    config.tableNameForTopic("ignored") shouldBe "userevents"
  }

  it should "reject unsupported table naming modes" in {
    val error = intercept[IllegalArgumentException] {
      appConfig.copy(icebergTableNameMode = "per-topic").validate()
    }

    error.getMessage should include("ICEBERG_TABLE_NAME_MODE")
  }

  it should "reject invalid commit settings" in {
    val error = intercept[IllegalArgumentException] {
      appConfig.copy(kafkaCommitMaxBatch = 0).validate()
    }

    error.getMessage should include("KAFKA_COMMIT_MAX_BATCH")
  }
}
