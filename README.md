# 🚀 Distributed Event Processing System

A production-grade distributed event ingestion and processing pipeline built using **Go, Kafka, Redis, and PostgreSQL**.

Designed to handle **high-throughput workloads (50k+ events/min)** with strong guarantees around **reliability, scalability, and fault tolerance**.

---

## 📌 Overview

This system simulates a real-world event-driven backend where events are:

1. Ingested via an HTTP API
2. Published to Kafka
3. Consumed and processed concurrently
4. Persisted to PostgreSQL
5. Retried on failure using Redis
6. Moved to DLQ after retry exhaustion

---

## 🧠 Key Features

* ⚡ **High Throughput Processing** (50k+ events/min)
* 🔁 **At-least-once delivery guarantee**
* 🧵 **Worker Pool Concurrency (Go routines)**
* 🧠 **Idempotent Processing (Redis + DB constraints)**
* 🔄 **Retry Queue with Failure Handling**
* ☠️ **Dead Letter Queue (DLQ)**
* 📦 **Kafka Manual Offset Commit**
* 🛑 **Graceful Shutdown (no data loss)**
* 🗄️ **PostgreSQL Persistence**

---

## ⚙️ Tech Stack

* **Language:** Go (Golang)
* **Messaging:** Apache Kafka
* **Cache/Queue:** Redis
* **Database:** PostgreSQL
* **Framework:** Gin
* **Infra:** Docker, Docker Compose

---

## 📂 Project Structure

```
producer/
  ├── handler/
  ├── service/
  ├── model/
  └── main.go

consumer/
  ├── service/
  ├── worker/
  │    ├── worker.go
  │    ├── retry.go
  │    └── retry_worker.go
  └── main.go
```

---

## ▶️ Getting Started

### 1. Start Infrastructure

```bash
docker compose up -d
```

---

### 2. Create Kafka Topic

```bash
docker exec -it kafka kafka-topics \
  --create \
  --topic events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

---

### 3. Run Producer

```bash
cd producer
go run main.go
```

---

### 4. Run Consumer

```bash
cd consumer
go run main.go
```

---

## 🧪 Test the System

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "key": "order-123",
    "type": "order_created",
    "payload": {
      "order_id": "order-123"
    }
  }'
```

---

## 🔁 Retry & DLQ Flow

* Failed events → pushed to `retry_queue` (Redis)
* Retry worker processes events asynchronously
* After exceeding retry limit → moved to `dlq`

---

## 🧠 Design Decisions

### 🔹 Kafka Partitioning

Ensures ordering per entity (e.g., `order_id`)

### 🔹 Manual Offset Commit

Offsets committed only after successful processing

### 🔹 Idempotency

* Redis: fast duplicate detection
* PostgreSQL: `ON CONFLICT DO NOTHING`

### 🔹 Worker Pool

Prevents goroutine explosion and provides backpressure

### 🔹 Retry Strategy

Decoupled retry pipeline using Redis for simplicity and speed

---

## 📈 Performance Considerations

* Handles **~800–2000 events/sec** depending on DB throughput
* Kafka and Redis scale horizontally
* PostgreSQL is the primary bottleneck

---

## ⚠️ Limitations

* No batch DB writes (yet)
* Retry uses Redis (not Kafka retry topics)
* No observability (metrics/log aggregation)

---

## 🚀 Next Steps (High Impact Improvements)

### 🔥 Performance

* [ ] Batch PostgreSQL inserts (major throughput boost)
* [ ] Increase Kafka partitions for parallelism

---

### 📊 Observability

* [ ] Add Prometheus metrics
* [ ] Add Grafana dashboards

---

### 🔁 Reliability

* [ ] Exponential backoff retry strategy
* [ ] Kafka-based retry topics (instead of Redis)

---

### ⚙️ Production Readiness

* [ ] Config via config.yaml + env override
* [ ] Health checks (liveness/readiness)
* [ ] Dockerize producer/consumer services

---

### ☸️ Scalability

* [ ] Kubernetes deployment
* [ ] Horizontal consumer scaling
---

## 👨‍💻 Author

**Rushikesh Jalvi**

---

## ⭐ If you found this useful

Give it a ⭐ on GitHub!
