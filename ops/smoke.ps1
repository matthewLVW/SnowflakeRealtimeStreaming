param(
  [string]$RawTopic = "ticks.raw",
  [string]$AggTopic = "ticks.agg.1s"
)

$composeArgs = @('-f', 'docker/compose.yml')

Write-Host "Starting Kafka stack..."
docker compose @composeArgs up -d

Write-Host "Creating raw topic '$RawTopic' (idempotent)..."
docker compose @composeArgs exec -T kafka kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic $RawTopic --partitions 1 --replication-factor 1 | Out-Host

Write-Host "Creating agg topic '$AggTopic' (idempotent)..."
docker compose @composeArgs exec -T kafka kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic $AggTopic --partitions 1 --replication-factor 1 | Out-Host

Write-Host "Producing one test message to raw topic..."
docker compose @composeArgs exec -T kafka bash -lc "printf '%s\n' '{""smoke"":true}' | kafka-console-producer.sh --bootstrap-server kafka:29092 --topic $RawTopic 1>/dev/null"

Write-Host "Consuming one message (from beginning) from raw topic..."
docker compose @composeArgs exec -T kafka kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic $RawTopic --from-beginning --max-messages 1 --timeout-ms 5000

Write-Host "Smoke test complete."
