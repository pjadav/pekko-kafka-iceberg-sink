#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${SKIP_BUILD:-false}" != "true" ]]; then
  echo "Building connector assembly (tests skipped; run 'sbt test' separately)..."
  sbt -batch 'set assembly / test := {}' assembly
fi

echo "Starting local stack..."
docker compose up -d --build --wait

echo "Creating Kafka topic..."
docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic events \
  --partitions 1 \
  --replication-factor 1

echo "Publishing JSON Schema message..."
printf '%s\n' '{"message":"hello iceberg","amount":42.5}' |
  docker compose exec -T schema-registry kafka-json-schema-console-producer \
    --bootstrap-server kafka:29092 \
    --topic events \
    --property schema.registry.url=http://schema-registry:8081 \
    --property 'value.schema={"title":"ExampleEvent","type":"object","properties":{"message":{"type":"string"},"amount":{"type":"number"}},"required":["message"]}'

echo "Waiting for Iceberg commit..."
MOTO=http://localhost:5000
for _ in $(seq 1 30); do
  if curl -sf -X POST "$MOTO/" \
    -H 'Content-Type: application/x-amz-json-1.1' \
    -H 'X-Amz-Target: AWSGlue.GetTable' \
    -H 'Authorization: AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/glue/aws4_request' \
    -d '{"DatabaseName":"default","Name":"events"}' | grep -Eqi '"table_type": ?"ICEBERG"' &&
    curl -sf "$MOTO/iceberg-warehouse?list-type=2" | grep -q '\.parquet</Key>'; then
    echo "Smoke test passed: Iceberg Glue table and Parquet data file exist."
    echo "Data files:"
    curl -sf "$MOTO/iceberg-warehouse?list-type=2" | grep -o '<Key>[^<]*\.parquet</Key>' | sed 's/<[^>]*>//g'
    exit 0
  fi
  sleep 2
done

echo "Smoke test failed. Connector logs:"
docker compose logs --tail 100 connector
exit 1
