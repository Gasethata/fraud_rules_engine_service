FROM python:3.11-slim

WORKDIR /kafka_broker
COPY kafka_broker/ .
RUN chmod -R 777 /kafka_broker 

RUN pip install -r requirements.txt

CMD ["python3", "-u", "main.py"]
