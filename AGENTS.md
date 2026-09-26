# AGENTS.md — kafka-app

Maven multi-module, Spring Boot 3.5 (Java 17). Two CLI processes share `order-common` via Kafka topic `orders`. Docker runs Kafka ONLY — never containerize the apps.

## Modules
- `order-common/` — plain jar, `Order` POJO only (no Spring).
- `order-producer/` — CLI (non-web: `starter` + `starter-json` + `spring-kafka`). `ProducerApplication`, `producer/OrderProducer` (`send` async + `sendAndWait`), `producer/OrderRunner` (`CommandLineRunner`: up to `max-orders`, random `min/max-delay-ms` sleep, exits when done).
- `order-consumer/` — CLI worker (non-web: `starter` + `starter-json` + `spring-kafka`; the `-json` starter is REQUIRED for `JsonDeserializer` — verified crash without it). `ConsumerApplication`, `consumer/OrderConsumer` (`ConsumerRecord` + `Consumer` params; logs partition/offset + `endOffsets-position` backlog; random work sleep; then CSV), `service/CsvOrderWriter` (single `@Autowired` String ctor + test-only `Path` ctor; synchronized; creates dirs + header).

## Commands (root `C:\anup\code\java\kafka-app` unless noted)
- Kafka: `docker compose up -d` (host broker `localhost:29092`); `down` / `down -v` to stop/wipe
- Build: `mvn -q clean install -DskipTests` (needed once so modules resolve `order-common`)
- Host run, two terminals: T1 `cd order-consumer; mvn spring-boot:run`, T2 `cd order-producer; mvn spring-boot:run`
- All tests: `mvn -q clean test`. Single test: `mvn -q -pl order-consumer -Dtest=CsvOrderWriterTest test`
- Smoke test overrides: `APP_ORDER_MAX_ORDERS=6 APP_ORDER_MIN_DELAY_MS=1000 APP_ORDER_MAX_DELAY_MS=2000` (producer), `APP_ORDER_PROCESSING_MIN_DELAY_MS=2000 APP_ORDER_PROCESSING_MAX_DELAY_MS=3000` + temp `APP_ORDER_CSV_PATH` (consumer)
- Broker peek: `docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 5`

## Quirks
- Topic ensured by `KafkaTopicConfig` in BOTH services (3 partitions, 1 replica — single broker only); broker also has auto-create on.
- Consumer JSON: trusted `com.example.orders.*`, default type `com.example.orders.common.Order` — keep package in sync with producer.
- Consumer sleep (20–60s default) blocks the poll loop; default `max.poll.interval.ms` (5 min) tolerates it. Backlog estimate is approximate (auto-commit races position).
- Env: `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `APP_ORDER_TOPIC`, `APP_ORDER_CSV_PATH`, `APP_ORDER_MAX_ORDERS`, `APP_ORDER_MIN/MAX_DELAY_MS`, `APP_ORDER_PROCESSING_MIN/MAX_DELAY_MS`, `SPRING_KAFKA_CONSUMER_GROUP_ID`.
- No Dockerfiles in repo (deleted by design). `data/` volume removed; CSV lives on host (`C:/temp/OrderDetails.csv`).
- Sibling dirs (`../ecom-app`, `../helloworld-docker`) are separate repos — ignore.
