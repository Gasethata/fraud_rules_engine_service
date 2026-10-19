# Fraud Rule Engine Service
### Overview

This project implements a real-time **Fraud Detection Engine** built on an event-driven architecture.

It consumes transaction events from Kafka, evaluates fraud detection rules, and persists flagged transactions into PostgreSQL for later retrieval via API. Transactions are simulated from Kaggle fraud dataset and are published to Kafka to trigger the pipeline.

The system is designed to demonstrate:

- Event-driven architecture
- Real-time transaction processing
- Rule-based fraud evaluation
- Micro-services
- Containerized deployment
- Unit and Integration testing strategy
- CI/CD pipeline with unit, integration, and security scan jobs

### Architecture
This engine is microservices and every service runs on its own container.
Below are all the containers, and all run through a `docker compose` for multi-services control. 


| Container | Folder | Role |
|---|---|---|
| `api` | `api/` | FastAPI REST layer - serves flagged transactions |
| `postgres` | `database/` | PostgreSQL - stores transactions, rules, and flags |
| `broker` | `kafka_broker/` | Kafka single-node cluster |
| `topic` | `kafka_broker/` | Creates a topic in the above Kafka broker |
| `consumer` | `fraud_engine/` | Consumes transactions from the above Kafka topic |
| `producer` | `transactions_producer/` | Publishes simulated transactions into Kafka topic|
|

### Components

- **Kafka** – Event streaming platform
- **Fraud Engine** – Consumer + fraud rule processor
- **PostgreSQL** – Data persistence
- **REST API** – Retieve flagged transactions
- **Docker Compose** – Service orchestration

### Technology Stack

| Component        | Technology                                   |
|------------------|----------------------------------------------|
| Language         | Python v3.11+                                |
| Event Streaming  | Apache Kafka v2.15.1                         |
| Database         | PostgreSQL v17+                              |
| ORM / DB Access  | psycopg2 v2.9.13                             |
| API Framework    | FastAPI v0.104.1                             |
| Auth             | JWT + bcrypt v4.0.1                         |   
| Metrics	       | Prometheus v0.19.0                           |
| Resilience	   | Tenacity v8.2.3                              |
| Testing          | Pytest v7.4.3                                |
| Containerization | Docker v27.5.1+ & Docker Compose v2.32.4+    |
| CI/CD	           | GitHub Actions |
|

## How It Works

### 1. Transaction Ingestion
This is done in the `transaction_producer` container.
The producer is fed a simulated transaction data from `transaction_producer/data/testdata.csv`
This data was extracted from Kaggle dataset as described below. 
The file is too big and cannot be pushed to git. It can be downloaded and placed at `transaction_producer/data/` if you would like to run the would dataset, it contains more than 1 million transactions. Make sure to amend the variable in .env if you change the file `TRANSACTIONS_SIMULATION_FILE`.
Transactions are published to a Kafka topic (`transactions`) using Kafka Producer

Each transaction includes fields such as:

- transaction_id
- account_id
- amount
- timestamp
- merchant
- names

The Fraud Engine subscribes to this topic as a consumer.

### 2. Rule Evaluation

When the fraud engine consumes a transaction:

1. Context is pre-built (hour of day, weekend flag) once before rule evaluation.
2. All active rules are fetched from the database at startup.
3. Each rule is evaluated via an operator map - adding a new rule type never touches the core processing loop.
4. Triggered rules are collected.
5. If rules triggered: the transaction and its flag record are written atomically in one database commit.
6. If no rules triggered: the transaction is saved for audit purposes only.

Rules are stored in `database/db/init/fraud_rules.csv`, loaded by `database/db/init/2load_rules.sql` at database initialisation. Modifying rules requires no code changes.

### 3. Database Layer

| Table | Purpose |
|---|---|
| `transactions` | All processed transactions (flagged and clean) |
| `flagged_transactions` | Transactions that triggered one or more rules |
| `fraud_rules` | Configurable fraud detection rules |
| `users` | API admin users |
|

Indexes are defined on high-query columns:

- Composite index on `(cc_num, trans_date_trans_time DESC)` - primary fraud rule query pattern
- Index on `trans_date_trans_time DESC` - time-range queries
- Index on `category` - category-based rules
- Index on `flagged_transactions(transaction_id)` and `flagged_at DESC` - API lookups

### 4. API Layer

A JWT-secured FastAPI service exposes flagged transactions. Routes are versioned under `/v1/`.

## Setup

### Prerequisites

- Docker 27.5.1+
- Docker Compose 2.32.4+
- Python 3.11+ (for running tests locally)

### Environment Configuration

```bash
cp sample.env .env
```

Edit `.env` and fill in all values.

Generate a strong JWT secret and set it in the `.env` file:
```bash
openssl rand -hex 32
```

### Build

```bash
docker compose build
```

### Run

```bash
docker compose up -d
```

Start order: Kafka > topic init > Fraud Engine > PostgreSQL > API > Producer

To start without the producer (e.g. for integration testing):
```bash
docker compose up --scale transaction_producer=0 -d
```

### View Logs

```bash
docker compose logs -f <service name>
```

---



## API Reference

Base URL: `http://localhost:8000`

Interactive docs: `http://localhost:8000/docs`


#### Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/health/live` | No | Liveness probe - is the service running? |
| GET | `/health/ready` | No | Readiness probe - is the database reachable? |
| GET | `/metrics` | No | Prometheus metrics |
| POST | `/v1/login` | No | Authenticate, returns JWT |
| GET | `/v1/flagged` | Yes | Paginated list of flagged transactions |
| GET | `/v1/flagged/{transaction_id}` | Yes | Single flagged transaction by ID |
|

#### Authentication

All transaction endpoints require a JWT Bearer token. Login first:

```bash
curl -X POST http://localhost:8000/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username": "<ADMIN_USERNAME>", "password": "<ADMIN_PASSWORD>"}'
```
Note that this can also be done on the swagger UI via the login endpoint.

Use the returned `access_token` as `Authorization: Bearer <token>` on protected requests.

#### Paginated Response — `/v1/flagged`

```json
{
  "total": <total flagged transactions>,
  "limit": <limit per page>,
  "offset": <offset>,
  "items": [...]
}
```

Use `?limit=50&offset=100` to page through results.

#### Command-line interface examples

```
# 1. Login
curl -X POST http://localhost:8000/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username": "<user>", "password": "<password>"}'

# 2. Get flagged transactions
curl -X GET http://localhost:8000/v1/flagged?limit=50&offset=0" \
  -H "Authorization: Bearer <token>"

# 3. Get specific transaction
curl -X GET http://localhost:8000/v1/flagged/<transaction_id> \
  -H "Authorization: Bearer <the token here>"
```
### Database direct access

```
    docker exec -it fraud_database psql -U <POSTGRES_USER> -d frauddb
    SELECT * FROM flagged_transactions ORDER BY flagged_at DESC LIMIT 10;
```


## Testing

### Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r tests/requirements.txt
```

### Unit Tests

Tests fraud rule logic in isolation using a `MockRepo`

```bash
pytest -m unit --cov=fraud_engine.engine
```
Expected: 9 tests passing on `engine.py`.

### Integration Tests

Tests the full pipeline against real running services.

```bash
# Start the stack without the producer
docker compose up --scale transaction_producer=0 --build -d

# Wait ~15 seconds for services to initialise, then run
pytest -m integration -v
```

If you ran the stack previously, clear state first:
```bash
docker compose down -v
```

Expected: 11 tests passing, covering health endpoints, authentication, full pipeline (publish > consume > flag > retrieve), pagination, metrics, and correlation ID propagation.

### Test Structure

```
tests/
├── requirements.txt
├── unit/
│   └── unit_test.py        # Rule operator tests with MockRepo
└── integration/
    └── integration_test.py # Full pipeline + API endpoint tests
```

---

## CI/CD

GitHub Actions pipeline at `.github/workflows/ci-cd.yml`.

**Triggers:** push to `main` or `develop`, pull requests to `main`.

**Jobs:**

| Job | When | What |
|---|---|---|
| `test` | Every push/PR | Unit tests + coverage |
| `integration` | Push only | Full stack integration tests |
| `security` | Every push/PR | Trivy CVE scan (CRITICAL/HIGH) |

**Required GitHub Secrets:**

Add these under repo Settings > Secrets and variables > Actions:

- `KAFKA_CLUSTER_ID`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- `JWT_SECRET`

---
## Dataset Used

This is a simulated credit card transaction dataset containing legitimate and fraud transactions from the duration 1st Jan 2019 - 31st Dec 2020. It covers credit cards of 1000 customers doing transactions with a pool of 800 merchants.

Link: *https://www.kaggle.com/datasets/kartik2112/fraud-detection?resource=download&select=fraudTrain.csv*

## Assumptions

The following are the assumptions taken during the development of the service:

| Assumption | Decision Made |
|---|---|
| Single admin user | Only one API user is needed - created automatically at startup |
| Rules are static at runtime | Fraud rules are loaded once at startup |
| All services on one host | Docker Compose assumes co-location |
| Transactions are append-only | No update or delete of transaction records |
| Rule evaluation is synchronous | Each transaction is evaluated in-process, not offloaded to a worker |
| Dataset is pre-existing | Transactions come from a CSV file, not a live payment system |




