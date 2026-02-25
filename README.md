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
Built with FastAPI, JWT authentication, and retrieving the data from the a PostgreSQL database table.

- Framework: FastAPI (Python 3.11)
- Authentication: JWT tokens with bcrypt password hashing
- Database: PostgreSQL with psycopg2

Below are the variables used for the API, they are set in the .env file, they can be changed as per your needs.
Admin User (created automatically on startup)
```
ADMIN_USERNAME=
ADMIN_PASSWORD=
```

JWT Configuration
```
JWT_SECRET=
ACCESS_TOKEN_EXPIRE_MINUTES=
```

#### Through the web
Currently running locally but can be set to run at a specific host
```
    http://localhost:8000/docs
```
##### Login to get the token
```
Endpoint: POST /login
```
You should see the request body with username and password. Enter the ADMIN_USERNAME and ADMIN_PASSWORD configured in the .env.
The response will be a access_token and a token_type.

##### Use the token to authenticate the request
Copy the token generated above.

##### Use the Swagger UI

Click the "Authorize" button at the top right.
Enter your token in the format: `Bearer <your-token>` or just the token.
Click "Authorize" and then "Close".
Now you can test protected endpoints directly from the UI.

###### Protected Endpoints (Require Authentication)

`GET	/flagged`	                 Get all flagged transactions (with pagination at 100)
`GET	/flagged/{transaction_id}`   Get specific transaction by ID

###### Public Endpoints

`POST	/login`	    Authenticate and get JWT token
`GET	/health`	Health check endpoint

#### Through the command-line interface

```
# 1. Login
curl -X POST http://localhost:8000/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "your_password"}'

# 2. Get flagged transactions (using the token)
curl -X GET http://localhost:8000/flagged \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# 3. Get specific transaction
curl -X GET http://localhost:8000/flagged/TX123456 \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

### Directly from the Database

```
    docker exec -it postgres psql -U postgres -d frauddb
    SELECT * FROM flagged_transactions;
```

## Dataset Used

This is a simulated credit card transaction dataset containing legitimate and fraud transactions from the duration 1st Jan 2019 - 31st Dec 2020. It covers credit cards of 1000 customers doing transactions with a pool of 800 merchants.

Link: *https://www.kaggle.com/datasets/kartik2112/fraud-detection?resource=download&select=fraudTrain.csv*




