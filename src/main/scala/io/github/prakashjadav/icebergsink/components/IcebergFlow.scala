package io.github.prakashjadav.icebergsink.components

import io.github.prakashjadav.icebergsink.model.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.apache.iceberg.catalog.SupportsNamespaces
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.{PartitionKey, PartitionSpec, Schema, Table}
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.Types
import org.apache.pekko.kafka.ConsumerMessage.CommittableOffsetBatch
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow

import java.time.{Instant, ZoneOffset}
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

final private case class WrittenIcebergBatch(
  tableIdentifier: TableIdentifier,
  table: Table,
  dataFile: org.apache.iceberg.DataFile,
  offsets: CommittableOffsetBatch,
  records: Seq[IcebergRecord]
)

class IcebergFlow(config: Config)(implicit materializer: Materializer, ec: ExecutionContext)
    extends StrictLogging {

  private val catalog: Catalog = IcebergCatalog(config)
  private val tables           = TrieMap.empty[String, Table]

  def flow: Flow[PreparedIcebergBatch, CommittableOffsetBatch, org.apache.pekko.NotUsed] =
    Flow[PreparedIcebergBatch]
      .mapAsync(1)(writeDataFiles)
      .mapConcat(identity)
      .groupedWithin(Int.MaxValue, config.icebergCommitIntervalSeconds.seconds)
      .mapAsync(1)(commit)

  private def writeDataFiles(batch: PreparedIcebergBatch): Future[Seq[WrittenIcebergBatch]] =
    Future {
      batch.records.groupBy(record => config.tableNameForTopic(record.topic)).toSeq.flatMap {
        case (tableName, records) =>
          val table  = resolveTable(tableName)
          val schema = table.schema()

          records.groupBy(record => config.icebergPartitionFields.map(partitionValue(_, record, tableName))).toSeq.map {
            case (_, partitionRecords) =>
              val fileName           = s"data/${UUID.randomUUID().toString}.parquet"
              val output: OutputFile = table.io().newOutputFile(s"${table.location()}/$fileName")
              val partitionRecord    = GenericRecord.create(schema)
              config.icebergPartitionFields.foreach { field =>
                partitionRecord.setField(field, partitionValue(field, partitionRecords.head, tableName))
              }

              val partition = new PartitionKey(table.spec(), schema)
              partition.partition(partitionRecord)

              val writer: org.apache.iceberg.io.DataWriter[org.apache.iceberg.data.Record] = Parquet.writeData(output)
                .forTable(table)
                .schema(schema)
                .withPartition(partition)
                .createWriterFunc(GenericParquetWriter.buildWriter _)
                .overwrite()
                .build()

              try
                partitionRecords.foreach { value =>
                  writer.write(IcebergCatalog.toRecord(value, tableName))
                }
              finally writer.close()

              WrittenIcebergBatch(identifierFor(tableName), table, writer.toDataFile(), batch.offsets, partitionRecords)
          }
      }
    }.recoverWith { case ex =>
      Future.failed(ex)
    }

  private def commit(batches: Seq[WrittenIcebergBatch]): Future[CommittableOffsetBatch] =
    Future {
      batches.groupBy(_.tableIdentifier).values.foreach { tableBatches =>
        val append = tableBatches.head.table.newAppend()
        tableBatches.foreach(batch => append.appendFile(batch.dataFile))
        append.commit()
      }

      batches.tail.foldLeft(batches.head.offsets) { (offsets, batch) =>
        offsets.updated(batch.offsets)
      }
    }

  private def identifierFor(tableName: String): TableIdentifier =
    TableIdentifier.of(Namespace.of(config.normalizedIcebergDatabase), tableName)

  private def resolveTable(tableName: String): Table = {
    val identifier = identifierFor(tableName)
    tables.getOrElseUpdate(identifier.toString, IcebergCatalog.loadOrCreate(catalog, identifier, config))
  }

  private def partitionValue(field: String, record: IcebergRecord, tableName: String): String =
    field match {
      case "event_date"   => record.yearMonthDay
      case "ingest_date"  => record.ingestYearMonthDay
      case "source_topic" => record.topic
      case "table_name"   => tableName
      case other          => throw new IllegalArgumentException(s"Unsupported partition field: $other")
    }
}

object IcebergFlow {
  def apply(config: Config)(implicit materializer: Materializer, ec: ExecutionContext): IcebergFlow =
    new IcebergFlow(config)
}

private[icebergsink] object IcebergCatalog {
  val kafkaMetadataType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(1001, "topic", Types.StringType.get()),
    Types.NestedField.required(1002, "partition", Types.IntegerType.get()),
    Types.NestedField.required(1003, "offset", Types.LongType.get()),
    Types.NestedField.required(1004, "timestamp", Types.LongType.get()),
    Types.NestedField.required(1005, "datetime", Types.StringType.get()),
    Types.NestedField.required(1006, "ingested_at", Types.StringType.get())
  )

  val provenanceType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(1101, "record_id", Types.StringType.get()),
    Types.NestedField.required(1102, "source", Types.StringType.get())
  )

  val schema: Schema = new Schema(
    Types.NestedField.required(1, "payload", Types.StringType.get()),
    Types.NestedField.required(2, "event_date", Types.StringType.get()),
    Types.NestedField.required(3, "ingest_date", Types.StringType.get()),
    Types.NestedField.optional(4, "source_topic", Types.StringType.get()),
    Types.NestedField.optional(5, "table_name", Types.StringType.get()),
    Types.NestedField.required(6, "kafka_metadata", kafkaMetadataType),
    Types.NestedField.required(7, "provenance", provenanceType),
    Types.NestedField.optional(8, "event_time", Types.TimestampType.withZone()),
    Types.NestedField.optional(9, "ingestion_time", Types.TimestampType.withZone())
  )

  def partitionSpec(config: Config): PartitionSpec = {
    val builder = PartitionSpec.builderFor(schema)
    config.icebergPartitionFields.foreach(builder.identity)
    builder.build()
  }

  def isoDateTime(epochMs: Long): String =
    Instant.ofEpochMilli(epochMs).atZone(ZoneOffset.UTC).toLocalDateTime.toString

  def timestamp(epochMs: Long): java.time.OffsetDateTime =
    Instant.ofEpochMilli(epochMs).atOffset(ZoneOffset.UTC)

  def toRecord(value: IcebergRecord, tableName: String): GenericRecord = {
    val record = GenericRecord.create(schema)
    record.setField("payload", value.payload)
    record.setField("event_time", timestamp(value.kafkaTimestamp))
    record.setField("ingestion_time", timestamp(value.ingestionTimestamp))
    record.setField("event_date", value.yearMonthDay)
    record.setField("ingest_date", value.ingestYearMonthDay)
    record.setField("source_topic", value.topic)
    record.setField("table_name", tableName)

    val metadata = GenericRecord.create(kafkaMetadataType)
    metadata.setField("topic", value.topic)
    metadata.setField("partition", value.partition)
    metadata.setField("offset", value.offset)
    metadata.setField("timestamp", value.kafkaTimestamp)
    metadata.setField("datetime", isoDateTime(value.kafkaTimestamp))
    metadata.setField("ingested_at", isoDateTime(value.ingestionTimestamp))
    record.setField("kafka_metadata", metadata)

    val provenance = GenericRecord.create(provenanceType)
    provenance.setField("record_id", s"${value.topic}-${value.partition}-${value.offset}")
    provenance.setField("source", value.topic)
    record.setField("provenance", provenance)
    record
  }

  def apply(config: Config): Catalog = {
    val properties = catalogProperties(config)
    org.apache.iceberg.CatalogUtil.loadCatalog(
      "org.apache.iceberg.aws.glue.GlueCatalog",
      "glue",
      properties,
      null
    )
  }

  private[icebergsink] def catalogProperties(config: Config): java.util.HashMap[String, String] = {
    val properties = new java.util.HashMap[String, String]()
    properties.put("warehouse", warehouse(config))
    properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    properties.put("client.region", config.s3Region)
    config.icebergGlueCatalogId.foreach(properties.put("glue.id", _))
    config.icebergGlueEndpoint.foreach(properties.put("glue.endpoint", _))
    config.icebergS3Endpoint.foreach(properties.put("s3.endpoint", _))
    properties.put("s3.path-style-access", config.icebergS3PathStyleAccess.toString)
    config.icebergCatalogClientFactory.foreach(properties.put("client.factory", _))
    config.icebergAssumeRoleArn.foreach(properties.put("client.assume-role.arn", _))
    config.icebergAssumeRoleRegion.foreach(properties.put("client.assume-role.region", _))
    config.icebergAssumeRoleSessionName.foreach(properties.put("client.assume-role.session-name", _))
    properties
  }

  private[icebergsink] def warehouse(config: Config): String = {
    val prefix = config.s3PathPrefix.stripPrefix("/").stripSuffix("/")
    config.icebergWarehouse.getOrElse {
      if (prefix.isEmpty) s"s3://${config.s3Bucket}/iceberg" else s"s3://${config.s3Bucket}/$prefix/iceberg"
    }
  }

  def loadOrCreate(catalog: Catalog, identifier: TableIdentifier, config: Config): Table =
    try {
      val table                   = catalog.loadTable(identifier)
      val missingTimestampColumns = Seq("event_time", "ingestion_time").filter(table.schema().findField(_) == null)
      if (missingTimestampColumns.nonEmpty) {
        val update = table.updateSchema()
        missingTimestampColumns.foreach(update.addColumn(_, Types.TimestampType.withZone()))
        update.commit()
        table.refresh()
      }
      table
    } catch {
      case _: org.apache.iceberg.exceptions.NoSuchTableException =>
        val namespaces = catalog.asInstanceOf[SupportsNamespaces]
        if (!namespaces.listNamespaces().contains(identifier.namespace())) {
          namespaces.createNamespace(identifier.namespace())
        }
        catalog.createTable(identifier, schema, partitionSpec(config))
    }
}
