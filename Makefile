COMPOSE = docker compose -f docker/compose.yml
BROKER_INT ?= kafka:29092
RAW_TOPIC ?= ticks.raw
AGG_TOPIC ?= ticks.agg.1s
ENV_FILE ?= .env

.PHONY: help up down ps logs topic-create-raw topic-create-agg topics-bootstrap topic-list produce-test consume-once consume-agg smoke producer replay aggregate dashboard snowflake-loader pipeline pipeline_rt

help:
	@echo "Targets:"
	@echo "  up                 - Start Kafka, Zookeeper, and Kafka UI"
	@echo "  down               - Stop and remove containers/volumes"
	@echo "  ps                 - Show service status"
	@echo "  logs               - Tail Kafka logs"
	@echo "  topics-bootstrap   - Create raw + agg topics"
	@echo "  topic-list         - List topics"
	@echo "  produce-test       - Produce one test message to raw topic"
	@echo "  consume-once       - Consume one message from raw topic"
	@echo "  consume-agg        - Consume one message from agg topic"
	@echo "  smoke              - Up, create topics, produce and consume once"
	@echo "  producer           - Run yfinance producer"
	@echo "  replay             - Run historical replay producer"
	@echo "  aggregate          - Run 1 s aggregator"
	@echo "  dashboard          - Launch Streamlit dashboard"
	@echo "  snowflake-loader   - Stream 5 min micro-batches into Snowflake"
	@echo "  pipeline           - Launch API + replay + aggregator (+ loader)"
	@echo "  pipeline_rt        - Launch realtime API + replay + aggregator (+ loader)"

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f kafka

topic-create-raw:
	$(COMPOSE) exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $(BROKER_INT) --create --if-not-exists --topic $(RAW_TOPIC) --partitions 1 --replication-factor 1 || true

topic-create-agg:
	$(COMPOSE) exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $(BROKER_INT) --create --if-not-exists --topic $(AGG_TOPIC) --partitions 1 --replication-factor 1 || true

topics-bootstrap: topic-create-raw topic-create-agg

topic-list:
	$(COMPOSE) exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $(BROKER_INT) --list

produce-test:
	@echo '{"smoke":true}' | $(COMPOSE) exec -T kafka bash -lc "/opt/bitnami/kafka/bin/kafka-console-producer.sh --bootstrap-server $(BROKER_INT) --topic $(RAW_TOPIC) 1>/dev/null"

consume-once:
	$(COMPOSE) exec -T kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server $(BROKER_INT) --topic $(RAW_TOPIC) --from-beginning --max-messages 1 --timeout-ms 5000

consume-agg:
	$(COMPOSE) exec -T kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server $(BROKER_INT) --topic $(AGG_TOPIC) --from-beginning --max-messages 1 --timeout-ms 5000

smoke: up topics-bootstrap produce-test consume-once
	@echo "Smoke test complete. If you saw one JSON line above, Kafka is working."

producer:
	python ops/run_producer.py --env $(ENV_FILE) --default-topic $(RAW_TOPIC)

replay:
	python ops/run_replay.py --env $(ENV_FILE) --default-topic $(RAW_TOPIC)

aggregate:
	python processors/aggregate_1s.py --env $(ENV_FILE)

ifdef OS
ifeq ($(OS),Windows_NT)
dashboard:
	set "DASHBOARD_ENV_FILE=$(ENV_FILE)" && streamlit run dashboard/app.py --server.headless true
else
dashboard:
	DASHBOARD_ENV_FILE=$(ENV_FILE) streamlit run dashboard/app.py --server.headless true
endif
endif

snowflake-loader:
	python consumers/snowflake_loader_minute.py --env $(ENV_FILE)

pipeline:
	python ops/run_pipeline.py --env $(ENV_FILE)

pipeline_rt:
	python ops/run_pipeline.py --env $(ENV_FILE) --api realtime --stream-speed 1
