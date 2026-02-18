FROM python:3.11-slim

WORKDIR /transactions_producer
COPY transactions_producer/ .
RUN chmod -R 777 /transactions_producer 

RUN pip install -r requirements.txt

CMD ["python3", "-u", "main.py"]
