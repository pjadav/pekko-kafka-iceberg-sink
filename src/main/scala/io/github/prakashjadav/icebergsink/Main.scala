package io.github.prakashjadav.icebergsink

import io.github.prakashjadav.icebergsink.model.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.management.scaladsl.PekkoManagement

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object Main extends App with StrictLogging {

  logger.info("Starting Pekko Connectors Iceberg")

  logger.info("Building Config")
  val config = Config()

  logger.info("Starting Actor System")
  implicit val system: ActorSystem  = ActorSystem("kafka-s3-worker")
  implicit val ec: ExecutionContext = system.dispatcher

  // Start Pekko Management HTTP server (exposes /ready on port 8080)
  logger.info("Starting Pekko Management health endpoints")
  PekkoManagement(system).start()

  logger.info("Building StreamRunner")
  val runner = StreamRunner(config)(system)

  val streamsFuture = runner.run()

  val topicSelection =
    if (config.inputTopics.nonEmpty) config.inputTopics.mkString(",") else "all topics matching exclusion pattern"
  logger.info(s"Shared stream started for $topicSelection, marking application as healthy")
  streamsFuture.onComplete {
    case Failure(ex) =>
      logger.error(s"Stream failed: ${ex.getMessage}", ex)

    case Success(_) =>
      logger.info("Stream completed normally, shutting down")
      system.terminate().onComplete(_ => sys.exit(0))
  }

  // Graceful shutdown
  sys.addShutdownHook {
    logger.info("Shutdown hook triggered, cleaning up")
    system.terminate()
  }
}
