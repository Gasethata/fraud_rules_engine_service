FROM python:3.11-slim

RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /transactions_producer
COPY transactions_producer/ .

RUN pip install --no-cache-dir -r requirements.txt && \
    chown -R appuser:appuser /transactions_producer

USER appuser

CMD ["python3", "-u", "main.py"]