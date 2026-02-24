## Testing
#### Overview
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

## Running Tests
#### Run Unit Tests
```
    pytest -m unit --cov=fraud_engine.engine
```

#### Run Integration Tests
- Make sure the pipeline was not pre-build (was not ran before testing else containers will have cache data)
- If the containers were ran before the tests, then make sure to remove them with (docker compose down -v)
- This test does not show coverage as it run outside Pytest, and test intergration of individual components
```
    docker compose up -d
    pytest -m integration
```


