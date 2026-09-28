package io.github.prakashjadav.icebergsink

import io.github.prakashjadav.icebergsink.model.Config
import io.github.prakashjadav.icebergsink.model.BaseConfig
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.SystemMaterializer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream

trait TestFixtures
    extends AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit val system: ActorSystem = ActorSystem("pekko-iceberg-test-system")
  implicit val materializer        = SystemMaterializer.get(system).materializer

  val baseConfig: BaseConfig = BaseConfig(
    environment                 = "test",
    bootstrapServers            = "dummy:9092",
    applicationId               = "test-kafka-s3-worker",
    reconnectBackoffInMillis    = 10,
    reconnectMaxBackoffInMillis = 10,
    consumerMaxPollRecords      = 5,
    enableAutoCommit            = false
  )

  val appConfig: Config = Config(
    baseConfig           = baseConfig,
    inputTopics          = Seq("test-topic-a", "test-topic-b"),
    autoOffsetReset      = "latest",
    s3Bucket             = "test-bucket",
    s3Region             = "eu-west-1",
    s3PathPrefix         = "data",
    s3BatchSize          = 100,
    s3BatchWindowSeconds = 30,
    kafkaCommitMaxBatch  = 10,
    cacheCapacity        = 10,
    schemaRegistryUrl    = "test"
  )

  protected def gunzipToString(bytes: Array[Byte]): String = {
    val gis = new GZIPInputStream(new ByteArrayInputStream(bytes))
    try {
      val out = scala.io.Source.fromInputStream(gis, StandardCharsets.UTF_8.name()).mkString
      out
    } finally gis.close()
  }

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }
}
