package io.github.prakashjadav.icebergsink.model

final case class BaseConfig(
  environment: String               = "local",
  bootstrapServers: String          = "localhost:9092",
  applicationId: String             = "pekko-iceberg-sink",
  reconnectBackoffInMillis: Long    = 50,
  reconnectMaxBackoffInMillis: Long = 1000,
  consumerMaxPollRecords: Int       = 500,
  enableAutoCommit: Boolean         = false
)

object BaseConfig {
  def applyFromEnv(): BaseConfig =
    BaseConfig(
      environment                 = env("ENVIRONMENT", "local"),
      bootstrapServers            = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      applicationId               = env("KAFKA_CONSUMER_GROUP", "pekko-iceberg-sink"),
      reconnectBackoffInMillis    = env("KAFKA_RECONNECT_BACKOFF_MS", 50L),
      reconnectMaxBackoffInMillis = env("KAFKA_RECONNECT_MAX_BACKOFF_MS", 1000L),
      consumerMaxPollRecords      = env("KAFKA_MAX_POLL_RECORDS", 500),
      enableAutoCommit            = env("KAFKA_ENABLE_AUTO_COMMIT", false)
    )

  private def env[T](name: String, default: T)(implicit reader: EnvReader[T]): T =
    sys.env.get(name).filter(_.trim.nonEmpty).map(reader.read(name, _)).getOrElse(default)

  private trait EnvReader[T] {
    def read(name: String, value: String): T
  }

  private object EnvReader {
    implicit val stringReader: EnvReader[String]   = (_, value) => value.trim
    implicit val intReader: EnvReader[Int]         = (name, value) => value.toInt
    implicit val longReader: EnvReader[Long]       = (name, value) => value.toLong
    implicit val booleanReader: EnvReader[Boolean] = (name, value) => value.toBoolean
  }
}
