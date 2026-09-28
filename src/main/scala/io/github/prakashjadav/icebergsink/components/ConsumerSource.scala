package io.github.prakashjadav.icebergsink.components

import com.fasterxml.jackson.databind.JsonNode
import io.github.prakashjadav.icebergsink.model.BaseConfig
import io.github.prakashjadav.icebergsink.model.Config
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.json.{KafkaJsonSchemaDeserializer, KafkaJsonSchemaSerializerConfig}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.scaladsl.Consumer
import org.apache.pekko.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import org.apache.pekko.stream.scaladsl.Source

import java.time.Duration
import java.util
import scala.jdk.CollectionConverters._

/**
 * Committable consumer source for cross-cluster replication.
 *
 * Uses `Consumer.committableSource` which commits offsets back to the SOURCE cluster
 * (where the consumer group coordinator lives).
 */
class ConsumerSource(
  inputTopics: Seq[String],
  topicSubscriptionPattern: String,
  kafkaConfig: BaseConfig,
  autoOffsetReset: String,
  schemaRegistryClient: SchemaRegistryClient,
  schemaRegistryUrl: String
)(implicit
  actorSystem: ActorSystem
) {

  val valueSerde                                   = buildSerde(schemaRegistryClient, schemaRegistryUrl)
  private val kafkaProperties: Map[String, String] = {
    val common = Map(
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG         -> kafkaConfig.consumerMaxPollRecords.toString,
      ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG     -> kafkaConfig.reconnectBackoffInMillis.toString,
      ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG -> kafkaConfig.reconnectMaxBackoffInMillis.toString,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       -> false.toString,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> autoOffsetReset,
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG   -> sys.env.getOrElse("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    )
    if (common(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG) == "PLAINTEXT") common
    else
      common ++ Map(
        "sasl.mechanism"   -> sys.env.getOrElse("KAFKA_SASL_MECHANISM", "AWS_MSK_IAM"),
        "sasl.jaas.config" -> sys.env.getOrElse(
          "KAFKA_SASL_JAAS_CONFIG",
          "software.amazon.msk.auth.iam.IAMLoginModule required;"
        ),
        "sasl.client.callback.handler.class" -> sys.env.getOrElse(
          "KAFKA_SASL_CALLBACK_HANDLER",
          "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        )
      )
  }

  private val consumerSettings: ConsumerSettings[String, JsonNode] =
    ConsumerSettings(actorSystem, new StringDeserializer(), valueSerde)
      .withBootstrapServers(kafkaConfig.bootstrapServers)
      .withGroupId(kafkaConfig.applicationId)
      .withProperties(kafkaProperties)
      .withStopTimeout(Duration.ofSeconds(45))

  def committableSource: Source[ConsumerMessage.CommittableMessage[String, JsonNode], Consumer.Control] = {
    val subscription =
      if (inputTopics.nonEmpty) Subscriptions.topics(inputTopics: _*)
      else Subscriptions.topicPattern(topicSubscriptionPattern)

    Consumer.committableSource(consumerSettings, subscription)
  }

  private def buildSerde(client: SchemaRegistryClient, schemaRegUrl: String): KafkaJsonSchemaDeserializer[JsonNode] = {
    val serdeConf: util.Map[String, String] = Map(
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegUrl,
      AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS      -> false.toString,
      AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY -> "io.confluent.kafka.serializers.subject.RecordNameStrategy",
      KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA -> "true"
    ).asJava

    new KafkaJsonSchemaDeserializer(client, serdeConf)
  }
}

object ConsumerSource {
  def apply(config: Config, schemaRegistryClient: SchemaRegistryClient)(implicit
    actorSystem: ActorSystem
  ): ConsumerSource =
    new ConsumerSource(
      config.inputTopics,
      config.topicSubscriptionPattern,
      config.baseConfig,
      config.autoOffsetReset,
      schemaRegistryClient,
      config.schemaRegistryUrl
    )
}
