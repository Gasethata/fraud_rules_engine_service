FROM python:3.11-slim

WORKDIR /fraud_engine
COPY fraud_engine/ .
RUN chmod -R 777 /fraud_engine 

RUN pip install -r requirements.txt

CMD ["python3", "-u", "main.py"]
