# Pekko Connectors Iceberg

Open-source Scala 2.13 worker that consumes JSON Schema records from Kafka with
[Apache Pekko](https://pekko.apache.org/) and writes partitioned Parquet data to
Apache Iceberg tables in Amazon S3 through AWS Glue Catalog.

## Features

- Pekko Kafka consumer with manual offset commits after Iceberg commits
- JSON Schema Registry deserialization
- Bounded batching by record count, byte size, and time window
- Static table routing or one Iceberg table per Kafka topic
- Automatic Glue namespace/table creation
- Configurable identity partition fields
- Kafka metadata and provenance fields
- Generic payload schema by default; legacy envelope schema is optional
- Health state through Pekko Management
- Local-friendly defaults; AWS MSK IAM can be enabled through environment variables

## Run locally

Requirements: JDK 17+, sbt, Kafka, Schema Registry, and an Iceberg-compatible
AWS account or local S3-compatible service.

```bash
cp .env.example .env
set -a && source .env && set +a
sbt run
```

The example configuration is intentionally non-secret and points to local
Kafka/Schema Registry. Replace storage and security settings before using it
with AWS.

## Configuration

See `.env.example` for all supported variables. Important settings:

| Variable | Example | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `INPUT_TOPICS` | `events` | Comma-separated topics; empty subscribes by pattern |
| `S3_BUCKET` | `example-iceberg-bucket` | S3 bucket |
| `S3_REGION` | `us-east-1` | AWS region |
| `ICEBERG_TABLE_NAME_MODE` | `static` or `topic` | Destination routing |
| `ICEBERG_SCHEMA_MODE` | `generic` or `envelope` | Generic open-source schema or legacy envelope |
| `ICEBERG_PARTITION_FIELDS` | `event_date,ingest_date` | Comma-separated identity partition fields |
| `ICEBERG_WAREHOUSE` | `s3://bucket/warehouse` | Optional warehouse override |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Use `SASL_SSL` for secured clusters |

Never commit access keys, tokens, private endpoints, or production
configuration. Prefer AWS IAM roles or workload identity.

## Build and test

```bash
sbt test
sbt assembly
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
