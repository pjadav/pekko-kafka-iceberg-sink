import sbt._

object Dependencies {
  lazy val scalaTest = Seq("org.scalatest" %% "scalatest" % "3.2.20" % Test)
  lazy val ConfluentVersion = "7.7.0"
  lazy val PekkoVersion = "1.1.2"
  lazy val PekkoConnectorsVersion = "1.1.0"

  lazy val kafka = Seq("io.confluent" % "kafka-json-schema-serializer" % ConfluentVersion)

  lazy val pekko = Seq(
    "org.apache.pekko" %% "pekko-connectors-kafka" % PekkoConnectorsVersion,
    "org.apache.pekko" %% "pekko-management" % "1.1.0",
    "org.apache.pekko" %% "pekko-testkit" % PekkoVersion % Test
  )

  lazy val iceberg = Seq(
    "org.apache.iceberg" % "iceberg-aws" % "1.7.1",
    "org.apache.iceberg" % "iceberg-core" % "1.7.1",
    "org.apache.iceberg" % "iceberg-data" % "1.7.1",
    "org.apache.iceberg" % "iceberg-parquet" % "1.7.1",
    "org.apache.hadoop" % "hadoop-common" % "3.3.6",
    "software.amazon.awssdk" % "glue" % "2.25.62",
    "software.amazon.awssdk" % "s3" % "2.25.62"
  )

  lazy val observability = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
    "ch.qos.logback" % "logback-classic" % "1.5.18"
  )

  lazy val deps = scalaTest ++ kafka ++ pekko ++ iceberg ++ observability
}
