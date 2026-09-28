package io.github.prakashjadav.icebergsink

import io.github.prakashjadav.icebergsink.components.{CommitterSink, ConsumerSource, IcebergFlow, JsonConverter}
import io.github.prakashjadav.icebergsink.model.Config
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{Materializer, SystemMaterializer}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class StreamRunner(config: Config)(implicit
  actorSystem: ActorSystem
) extends StrictLogging {
  implicit val materializer: Materializer = SystemMaterializer.get(actorSystem).materializer
  implicit val ec: ExecutionContext       = actorSystem.dispatcher
  val csrClient                           = new CachedSchemaRegistryClient(
    config.schemaRegistryUrl,
    config.cacheCapacity,
    List[io.confluent.kafka.schemaregistry.SchemaProvider](new JsonSchemaProvider()).asJava,
    null
  )

  def run(): Future[Done] = {
    val source        = ConsumerSource(config, csrClient).committableSource
    val jsonConverter =
      JsonConverter(config.s3BatchSize, config.s3BatchMaxBytes, config.s3BatchWindow).flow
    val committerSink = CommitterSink(config)
    val icebergFlow   = IcebergFlow(config).flow

    source.via(jsonConverter).via(icebergFlow).runWith(committerSink.sink)
  }
}

object StreamRunner {
  def apply(config: Config)(implicit actorSystem: ActorSystem): StreamRunner =
    new StreamRunner(config)(actorSystem)
}
