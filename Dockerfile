FROM python:3.11-slim

RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /kafka_broker
COPY kafka_broker/ .
RUN pip install --no-cache-dir -r requirements.txt && \
    chown -R appuser:appuser /kafka_broker

USER appuser

CMD ["python3", "-u", "main.py"]
