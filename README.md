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

### Architecture


### Components

- **Kafka** – Event streaming platform
- **Fraud Engine** – Consumer + fraud rule processor
- **PostgreSQL** – Data persistence
- **REST API** – Retieve flagged transactions
- **Docker Compose** – Service orchestration

### Technology Stack

| Component        | Technology              |
|------------------|-------------------------|
| Language         | Python 3.11+            |
| Event Streaming  | Apache Kafka            |
| Database         | PostgreSQL              |
| ORM / DB Access  | psycopg2                |
| API Framework    | FastAPI                 |
| Testing          | Pytest                  |
| Containerization | Docker & Docker Compose |

## How It Works

### Transaction Ingestion

Transactions are published to a Kafka topic (`transactions`) using Kafka Producer

Each transaction includes fields such as:

- transaction_id
- account_id
- amount
- timestamp
- merchant
- names

The Fraud Engine subscribes to this topic as a consumer.

### Rule Evaluation

When a transaction is consumed:

1. The transaction is saved to the  `transactions` database table for audit and historical ppurposes.
2. Context is built for rule evaluation (e.g - calculate hours, weeks, time).
3. All fraud rules are retrieved from the database.
4. Each rule is evaluated dynamically.
5. Triggered rules are collected.
6. If rules trigger, record is inserted into `flagged_transactions` table of the database.

Rules are stored in the database and include:

- field
- condition
- threshold
- action
- description

This allows dynamic rule modification without changing application code.

### Database Layer

Two main tables:

#### transactions
Stores all processed transactions (only useful columns are targeted for persistence). 

#### flagged_transactions
Stores only transactions that triggered fraud rules.

Each flagged record contains:

- transaction_id
- rule_ids
- reason
- timestamp

#### API Layer

The FastAPI exposes endpoints such as:

- GET /flagged
- GET /flagged/{transaction_id}

These endpoints allow:

- Retrieval of all flagged transactions
- Lookup by transaction ID

This enables visibility into detected fraud transactions.

## How to Build

### Clone Repository

```
    git clone <repo-url>
    cd <project-folder>
```

### Build Containers

```
    docker compose build
```

## How to Run
### Start all Services

This ensures that the transactions publication simulation is activated.

```
    docker compose --profiles demo up -d
```

This will start Kafka Producer -> Kafka -> Fraud Engine + Kafka Consumer -> PostgreSQL -> API

### View logs

```
    docker compose logs -f fraud_engine
```

## Testing
The project implements a layered testing strategy to validate both:

- Business logic correctness (Unit Tests)
- End-to-end system behavior (Integration Tests)

The system under test follows this architecture:

Kafka → Fraud Engine (Consumer) → PostgreSQL

Testing is structured to reflect this separation of concerns:

- **Unit Tests** validate fraud rule logic in isolation.
- **Integration Tests** validate full pipeline execution using real services.

#### Testing Structure
- pytest.ini *(pytest path setup and markers)*
- tests/unit/test_engine.py
- tests/integration/integration_test.py

#### Setup For Tests
Create virtual environment to install dependencies and acticate it. Then install all the requirements
```
    python3 -m venv venv
    source venv/bin/activate
    pip install -r tests/requirements.txt
```

### Running Tests
#### Run Unit Tests
```
    pytest -m unit --cov=fraud_engine.engine
```

#### Run Integration Tests
- Make sure the pipeline was not pre-build (was not ran before testing else containers will have cache data)
- If the containers were ran before the tests, then make sure to remove them with (docker compose down -v)
- This test does not show coverage as it run outside Pytest, and test intergration of individual components
```
    docker compose up --scale transaction_producer=0 --build -d
    pytest -m integration
```

## View the Results
### Use the API to retrieve the flagged transactions

#### Through the web
```
    http://<the-ip-set-on-the-env>:8000/flagged
```
#### On the terminal
```
    curl http://<the-ip-set-on-the-env>:8000/flagged
```

### Directly from the Database

```
    docker exec -it postgres psql -U postgres -d frauddb
    SELECT * FROM flagged_transactions;
```

## Dataset Used

This is a simulated credit card transaction dataset containing legitimate and fraud transactions from the duration 1st Jan 2019 - 31st Dec 2020. It covers credit cards of 1000 customers doing transactions with a pool of 800 merchants.

Link: *https://www.kaggle.com/datasets/kartik2112/fraud-detection?resource=download&select=fraudTrain.csv*




