# Kafka Orders — CLI Producer + CLI Consumer (Docker for Kafka only)

Two **separate CLI processes** wired together by Kafka (which is the only thing in Docker):

```text
Terminal 1: order-producer (exits)            Kafka in Docker               Terminal 2: order-consumer (runs forever)
┌──────────────────────────────┐      ┌─────────────────┐      ┌─────────────────────────────────┐
│ Publishes up to 100 orders,  │      │  kafka:29092    │      │ Sleeps 20-60s per order (work   │
│ sleeping a random 1-30s      │─────▶│  topic: orders  │─────▶│ sim), logs partition/offset +   │
│ between orders, then exits   │      │                 │      │ queue backlog, appends CSV row  │
└──────────────────────────────┘      └─────────────────┘      └─────────────────────────────────┘
```

Each consumed order is saved as one CSV row: `orderId,createdTime,orderTotal` (default `C:\temp\OrderDetails.csv`).

## Repo layout

| Path | What it is |
|---|---|
| `order-common/` | Shared `Order` model (`orderId`, `createdTime`, `orderTotal`) |
| `order-producer/` | CLI app: `ProducerApplication` + `OrderRunner` (loop of max 100, random 1–30s delays) |
| `order-consumer/` | CLI app: `ConsumerApplication` + `OrderConsumer` (20–60s work sim, backlog/offset logs, CSV) |
| `docker-compose.yml` | Kafka only (KRaft, no ZooKeeper) |

## Prereqs

- **Docker Desktop** running
- JDK 17+ and Maven 3.9+

## 1. Start Kafka (Docker)

```powershell
cd C:\anup\code\java\kafka-app
docker compose up -d
docker compose logs --tail=3 kafka   # expect "Kafka Server started"
```

> Stop later with `docker compose down`. Fresh broker (wipe topics) with `docker compose down -v`.

## 2. Build

```powershell
mvn clean install -DskipTests
```

## 3. Two terminals — consumer first, then producer

**Terminal 1 — Consumer** (runs until you press `Ctrl+C`):

```powershell
cd C:\anup\code\java\kafka-app\order-consumer
mvn spring-boot:run
```

**Terminal 2 — Producer** (publishes up to 100 orders with random 1–30s pauses, then exits by itself):

```powershell
cd C:\anup\code\java\kafka-app\order-producer
mvn spring-boot:run
```

(Or run the jars instead: `java -jar order-consumer\target\order-consumer-1.0.0.jar` from the repo root, same for producer.)

## 4. Verify

- Terminal 2 shows `Produced order i/100 id=... partition=.. offset=..` plus `Sleeping .. ms ..`, ending with `Producer finished: 100/100 orders published`.
- Terminal 1 shows, per order:
  `Processing order id=... partition=.. offset=.. | ~N orders waiting in queue`
  then `Processed offset .. (order ..) in .. ms -> CSV`.
- `C:\temp\OrderDetails.csv` grows one row per order: `orderId,createdTime,orderTotal`.
- Optional broker-level check (first 5 messages, from the repo root):
  ```powershell
  docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 5
  ```

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Broker (host apps use the external listener) |
| `APP_ORDER_TOPIC` | `orders` | Kafka topic |
| `APP_ORDER_MAX_ORDERS` | `100` | Producer stops after this many (producer only) |
| `APP_ORDER_MIN_DELAY_MS` / `APP_ORDER_MAX_DELAY_MS` | `1000` / `30000` | Random pause between orders (producer only) |
| `APP_ORDER_PROCESSING_MIN_DELAY_MS` / `APP_ORDER_PROCESSING_MAX_DELAY_MS` | `20000` / `60000` | Simulated work per order (consumer only) |
| `APP_ORDER_CSV_PATH` | `C:/temp/OrderDetails.csv` | Where the consumer writes (use forward slashes) |
| `SPRING_KAFKA_CONSUMER_GROUP_ID` | `order-processor` | Consumer group (consumer only) |

Quick smoke test with small numbers:

```powershell
$env:APP_ORDER_MAX_ORDERS="5"; $env:APP_ORDER_MIN_DELAY_MS="1000"; $env:APP_ORDER_MAX_DELAY_MS="2000"
```

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| Producer: broker `NOT_AVAILABLE` / connection refused | Kafka not up — `docker compose up -d`, wait ~30s, rerun producer |
| Consumer idles, no `Processing` lines | Producer hasn't published yet, or consumer group already consumed everything (offsets committed) — rerun producer for new orders |
| CSV stays header-only | Consumer still "working" (20–60s per order is normal) — watch its log |
| Port `29092` already in use | Old Kafka still running — `docker compose down` first |
| Windows CSV path fails | Use forward slashes: `C:/temp/OrderDetails.csv` |

## Tests

```powershell
cd C:\anup\code\java\kafka-app
mvn test   # CsvOrderWriterTest: header + append + row format
```
