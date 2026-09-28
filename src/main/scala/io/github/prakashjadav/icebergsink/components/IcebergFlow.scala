package io.github.prakashjadav.icebergsink.components

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
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
import scala.collection.JavaConverters._
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
                  val payloadNode = IcebergCatalog.objectMapper.readTree(value.payload)
                  val record      = GenericRecord.create(schema)
                  if (config.normalizedIcebergSchemaMode == "envelope") {
                    val headerNode = Option(payloadNode.get("header")).getOrElse(IcebergCatalog.objectMapper.createObjectNode())
                    val eventNode = Option(payloadNode.get("event")).getOrElse(IcebergCatalog.objectMapper.createObjectNode())
                    record.setField("header", IcebergCatalog.toRecord(IcebergCatalog.headerType, headerNode))
                    record.setField("event", IcebergCatalog.toRecord(IcebergCatalog.eventType, eventNode))
                    record.setField("year_month_day", value.yearMonthDay)
                    record.setField("ingest_year_month_day", value.ingestYearMonthDay)
                    record.setField("pipeline_source", value.topic)
                    record.setField("full_table_name", tableName)
                  } else {
                    record.setField("payload", value.payload)
                    record.setField("event_date", value.yearMonthDay)
                    record.setField("ingest_date", value.ingestYearMonthDay)
                    record.setField("source_topic", value.topic)
                    record.setField("table_name", tableName)
                  }
                  val metadataNode = IcebergCatalog.objectMapper.createObjectNode()
                  if (config.normalizedIcebergSchemaMode == "envelope") {
                    record.setField(
                      "_kafka_metadata",
                      IcebergCatalog.toRecord(
                        IcebergCatalog.kafkaMetadataType,
                        metadataNode
                          .put("kafka_topic", value.topic)
                          .put("kafka_partition", value.partition)
                          .put("kafka_offset", value.offset)
                          .put("kafka_timestamp", value.kafkaTimestamp)
                          .put("kafka_datetime", IcebergCatalog.isoDateTime(value.kafkaTimestamp))
                          .put("smt_datetime", IcebergCatalog.isoDateTime(System.currentTimeMillis()))
                      )
                    )
                    record.setField(
                      "provenance",
                      IcebergCatalog.toRecord(
                        IcebergCatalog.provenanceType,
                        IcebergCatalog.objectMapper.createObjectNode()
                          .put("unique_id", s"${value.topic}-${value.partition}-${value.offset}")
                          .put("producer", value.topic)
                          .put("schema_name", "iceberg-worker")
                          .put("schema_version", "unknown")
                          .put("api_version", "v1")
                          .put("source", value.topic)
                      )
                    )
                  } else {
                    record.setField(
                      "kafka_metadata",
                      IcebergCatalog.toRecord(
                        IcebergCatalog.genericKafkaMetadataType,
                        metadataNode
                          .put("topic", value.topic)
                          .put("partition", value.partition)
                          .put("offset", value.offset)
                          .put("timestamp", value.kafkaTimestamp)
                          .put("datetime", IcebergCatalog.isoDateTime(value.kafkaTimestamp))
                          .put("ingested_at", IcebergCatalog.isoDateTime(System.currentTimeMillis()))
                      )
                    )
                    record.setField(
                      "provenance",
                      IcebergCatalog.toRecord(
                        IcebergCatalog.genericProvenanceType,
                        IcebergCatalog.objectMapper.createObjectNode()
                          .put("record_id", s"${value.topic}-${value.partition}-${value.offset}")
                          .put("source", value.topic)
                      )
                    )
                  }
                  writer.write(record)
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
      case "event_date" | "year_month_day" => record.yearMonthDay
      case "ingest_date" | "ingest_year_month_day" => record.ingestYearMonthDay
      case "source_topic" | "pipeline_source" => record.topic
      case "table_name" | "full_table_name" => tableName
      case other => throw new IllegalArgumentException(s"Unsupported partition field: $other")
    }
}

object IcebergFlow {
  def apply(config: Config)(implicit materializer: Materializer, ec: ExecutionContext): IcebergFlow =
  new IcebergFlow(config)
}

private[icebergsink] object IcebergCatalog {
  val objectMapper = new ObjectMapper()

  val nameValueType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(1001, "name", Types.StringType.get()),
    Types.NestedField.required(1002, "value", Types.StringType.get())
  )

  val userContextType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(2001, "clientId", Types.StringType.get()),
    Types.NestedField.required(2002, "customerId", Types.StringType.get()),
    Types.NestedField.required(2003, "consent", Types.StringType.get()),
    Types.NestedField.required(2004, "additionalContext", Types.StringType.get())
  )

  val additionalInfoItemType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(3001, "name", Types.StringType.get()),
    Types.NestedField.required(3002, "value", Types.StringType.get())
  )

  val actionItemType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(3101, "name", Types.StringType.get()),
    Types.NestedField.required(3102, "value", Types.StringType.get())
  )

  val eventType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(4001, "treatment", Types.StringType.get()),
    Types.NestedField.required(4002, "logRequest", Types.BooleanType.get()),
    Types.NestedField.optional(4003, "additionalInfo", Types.ListType.ofRequired(4004, additionalInfoItemType)),
    Types.NestedField.optional(4005, "actions", Types.ListType.ofRequired(4006, actionItemType)),
    Types.NestedField.required(4007, "userContext", userContextType),
    Types.NestedField.required(4008, "routingKey", Types.StringType.get()),
    Types.NestedField.required(4009, "policy", Types.StringType.get()),
    Types.NestedField.required(4010, "trackingId", Types.StringType.get())
  )

  val headerType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(5001, "producerReference", Types.StringType.get()),
    Types.NestedField.required(5002, "conversationId", Types.StringType.get()),
    Types.NestedField.required(5003, "origin", Types.StringType.get()),
    Types.NestedField.required(5004, "contextUri", Types.StringType.get()),
    Types.NestedField.required(5005, "serviceName", Types.StringType.get()),
    Types.NestedField.required(5006, "affiliateId", Types.StringType.get()),
    Types.NestedField.required(5007, "serviceVersion", Types.StringType.get()),
    Types.NestedField.required(5008, "managedGroupId", Types.StringType.get()),
    Types.NestedField.required(5009, "environmentName", Types.StringType.get()),
    Types.NestedField.required(5010, "appRegisterId", Types.LongType.get()),
    Types.NestedField.required(5011, "entryPointId", Types.StringType.get()),
    Types.NestedField.required(5012, "eventTime", Types.StringType.get()),
    Types.NestedField.required(5013, "eventName", Types.StringType.get()),
    Types.NestedField.required(5014, "managedGroupTenantId", Types.StringType.get())
  )

  val kafkaMetadataType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(6001, "kafka_topic", Types.StringType.get()),
    Types.NestedField.required(6002, "kafka_partition", Types.IntegerType.get()),
    Types.NestedField.required(6003, "kafka_offset", Types.LongType.get()),
    Types.NestedField.required(6004, "kafka_timestamp", Types.LongType.get()),
    Types.NestedField.required(6005, "kafka_datetime", Types.StringType.get()),
    Types.NestedField.required(6006, "smt_datetime", Types.StringType.get())
  )

  val provenanceType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(7001, "unique_id", Types.StringType.get()),
    Types.NestedField.required(7002, "producer", Types.StringType.get()),
    Types.NestedField.required(7003, "schema_name", Types.StringType.get()),
    Types.NestedField.required(7004, "schema_version", Types.StringType.get()),
    Types.NestedField.required(7005, "api_version", Types.StringType.get()),
    Types.NestedField.required(7006, "source", Types.StringType.get())
  )

  val genericKafkaMetadataType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(1001, "topic", Types.StringType.get()),
    Types.NestedField.required(1002, "partition", Types.IntegerType.get()),
    Types.NestedField.required(1003, "offset", Types.LongType.get()),
    Types.NestedField.required(1004, "timestamp", Types.LongType.get()),
    Types.NestedField.required(1005, "datetime", Types.StringType.get()),
    Types.NestedField.required(1006, "ingested_at", Types.StringType.get())
  )

  val genericProvenanceType: Types.StructType = Types.StructType.of(
    Types.NestedField.required(1101, "record_id", Types.StringType.get()),
    Types.NestedField.required(1102, "source", Types.StringType.get())
  )

  val schema = new Schema(
    Types.NestedField.required(1, "header", headerType),
    Types.NestedField.required(2, "event", eventType),
    Types.NestedField.required(3, "year_month_day", Types.StringType.get()),
    Types.NestedField.required(4, "ingest_year_month_day", Types.StringType.get()),
    Types.NestedField.optional(5, "pipeline_source", Types.StringType.get()),
    Types.NestedField.optional(6, "full_table_name", Types.StringType.get()),
    Types.NestedField.required(7, "_kafka_metadata", kafkaMetadataType),
    Types.NestedField.required(8, "provenance", provenanceType)
  )

  val partitionSpec: PartitionSpec = PartitionSpec.builderFor(schema)
    .identity("year_month_day")
    .identity("ingest_year_month_day")
    .build()

  val genericSchema: Schema = new Schema(
    Types.NestedField.required(1, "payload", Types.StringType.get()),
    Types.NestedField.required(2, "event_date", Types.StringType.get()),
    Types.NestedField.required(3, "ingest_date", Types.StringType.get()),
    Types.NestedField.optional(4, "source_topic", Types.StringType.get()),
    Types.NestedField.optional(5, "table_name", Types.StringType.get()),
    Types.NestedField.required(6, "kafka_metadata", genericKafkaMetadataType),
    Types.NestedField.required(7, "provenance", genericProvenanceType)
  )

  def schema(config: Config): Schema =
    if (config.normalizedIcebergSchemaMode == "envelope") schema else genericSchema

  def partitionSpec(config: Config): PartitionSpec = {
    val targetSchema = schema(config)
    val builder = PartitionSpec.builderFor(targetSchema)
    config.icebergPartitionFields.foreach(builder.identity)
    builder.build()
  }

  def nestedFieldValue(node: JsonNode, fieldName: String, fallback: String = ""): String =
    Option(node.get(fieldName)).map(_.asText()).getOrElse(fallback)

  def toRecord(structType: Types.StructType, node: JsonNode): GenericRecord = {
    val record = GenericRecord.create(structType)
    structType.fields().forEach { field =>
      val fieldName = field.name()
      val valueNode = Option(node.get(fieldName)).orElse(Option(node.get(fieldName.replace("_", ""))))
      if (valueNode.isDefined) {
        field.`type`() match {
          case t: Types.StringType  => record.setField(fieldName, valueNode.get.asText())
          case t: Types.LongType    => record.setField(fieldName, valueNode.get.asLong())
          case t: Types.IntegerType => record.setField(fieldName, valueNode.get.asInt())
          case t: Types.BooleanType => record.setField(fieldName, valueNode.get.asBoolean())
          case t: Types.ListType    =>
            val values = valueNode.get.elements().asScala.toSeq.map { child =>
              if (child.isObject) toRecord(t.elementType.asInstanceOf[Types.StructType], child)
              else child.asText()
            }
            record.setField(fieldName, values.asJava)
          case t: Types.StructType =>
            record.setField(fieldName, toRecord(t, valueNode.get))
          case _ => record.setField(fieldName, valueNode.get.asText())
        }
      }
    }
    record
  }

  def isoDateTime(epochMs: Long): String =
    Instant.ofEpochMilli(epochMs).atZone(ZoneOffset.UTC).toLocalDateTime.toString

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
    config.icebergGlueCatalogId.foreach(properties.put("glue.id", _))
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
    try catalog.loadTable(identifier)
    catch {
      case _: org.apache.iceberg.exceptions.NoSuchTableException =>
        val namespaces = catalog.asInstanceOf[SupportsNamespaces]
        if (!namespaces.listNamespaces().contains(identifier.namespace())) {
          namespaces.createNamespace(identifier.namespace())
        }
        catalog.createTable(identifier, schema(config), partitionSpec(config))
    }
}
