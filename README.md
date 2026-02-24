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
