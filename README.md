## Setup For Tests

python3 -m venv venv
source venv/bin/activate
pip install -r tests/requirements.txt

## Run Unit Tests
## Testing the core logic of the engine and validating the rules behaviour. This should show Unit Coverage of +90%
pytest -m unit --cov=fraud_engine.engine

## Run Integration Tests
## Make sure the pipeline was not pre-build (was not ran before testing else containers will have cache data)
## If the containers were ran before the tests, then make sure to remove them with (docker compose down -v)
## This test does not show coverage as it run outside Pytest, and test intergration of individual components
docker compose up
pytest -m integration


