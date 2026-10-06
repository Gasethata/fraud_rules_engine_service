FROM python:3.11-slim

RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /fraud_engine
COPY fraud_engine/ .

RUN pip install --no-cache-dir -r requirements.txt && \
    chown -R appuser:appuser /fraud_engine

USER appuser

CMD ["python3", "-u", "main.py"]
